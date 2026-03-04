import os
import struct
import time
import threading
import socket
import psutil
from flask import Flask, render_template, render_template_string, jsonify, send_from_directory
from peppi_py import read_slippi, live_slippi_frame_numbers, live_slippi_info
import pyarrow as pa
import asyncio
import websockets
import traceback
import queue
import sys
from pathlib import Path
import glob
import json
import requests
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import urllib.parse
from statx import statx
import csv

app = Flask(__name__)

# Configuration
try:
    _BASE_DIR = Path(__file__).resolve().parent
except Exception:
    _BASE_DIR = Path.cwd()
FTP_ROOT = str(_BASE_DIR / "ftp-files")

# Ensure FTP directory exists
os.makedirs(FTP_ROOT, exist_ok=True)

WS_URL = "wss://spectatormode.tv/bridge_socket/websocket?stream_count=1"
spectatormode_live_files = {}

SLIPPILAB_UPLOADS_FILE = "slippilab_uploaded.json"
slippilab_uploaded = {}  # {abs_path: slippilab_id}

# Optional CSV mapping for creation times (epoch seconds) by filename
CREATION_TIMES_CSV = "creation_times.csv"
creation_times_map = {}
creation_times_csv_path = None
creation_times_csv_mtime = None

# --- Lightweight caching for /api/replays ---
REPLAYS_SNAPSHOT_TTL = 1.0  # seconds
gameinfo_cache = {}
replays_snapshot = None
replays_snapshot_mtime = 0.0
replays_snapshot_lock = threading.Lock()

# --- Live/append activity tracking ---
# Some clients upload via repeated APPE (append) calls with pauses. During pauses there may be
# no active FTP data transfer, but the replay is still "live" and should be treated as active.
LIVE_FILE_ACTIVE_GRACE_SECONDS = 120.0
recent_file_activity = {}  # {abs_path: last_activity_epoch}
recent_file_activity_lock = threading.Lock()

# "Finalized" means: we observed the file to be quiet long enough to run
# finalize actions (header fix + SlippiLab upload). For the API, this is a
# stronger signal than the grace window and should mark the replay as finished.
# If more data arrives (APPE bursts), the finalized flag is cleared.
finalized_files = {}  # {abs_path: finalized_epoch}
finalized_files_lock = threading.Lock()

# "Finalize" actions (header fix + SlippiLab upload) should not run on every APPE burst.
# We wait until the file has been quiet (no new activity) for a short window.
FINALIZE_QUIET_SECONDS = 3.0
finalize_timers = {}  # {abs_path: threading.Timer}
finalize_timers_lock = threading.Lock()


def mark_file_activity(path):
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    try:
        now = time.time()
        with recent_file_activity_lock:
            recent_file_activity[abs_path] = now
        with finalized_files_lock:
            # New activity means the file is no longer finalized.
            finalized_files.pop(abs_path, None)
    except Exception:
        pass


def mark_file_finalized(path):
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    try:
        with finalized_files_lock:
            finalized_files[abs_path] = time.time()
    except Exception:
        pass


def get_file_finalized_at(path):
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    try:
        with finalized_files_lock:
            return finalized_files.get(abs_path)
    except Exception:
        return None


def is_file_recently_active(path, grace_seconds=LIVE_FILE_ACTIVE_GRACE_SECONDS):
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    try:
        with recent_file_activity_lock:
            last = recent_file_activity.get(abs_path)
        return last is not None and (time.time() - last) <= grace_seconds
    except Exception:
        return False


def get_last_file_activity(path):
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    try:
        with recent_file_activity_lock:
            return recent_file_activity.get(abs_path)
    except Exception:
        return None


def _finalize_slp_file(abs_path):
    try:
        if not abs_path or not abs_path.lower().endswith('.slp'):
            return
        if not os.path.exists(abs_path) or os.path.getsize(abs_path) == 0:
            return

        # Fix header length (best-effort)
        fix_slp_header_length(abs_path)

        # Upload to Slippi Lab if not already uploaded
        if abs_path not in slippilab_uploaded:
            replay_id = upload_to_slippilab(abs_path)
            if replay_id:
                slippilab_uploaded[abs_path] = replay_id
                save_slippilab_uploaded()

        # Mark finalized for API purposes (cleared automatically if more data arrives).
        mark_file_finalized(abs_path)
    except Exception as e:
        print(f"[WARN] Finalize failed for {abs_path}: {e}", flush=True)


def schedule_finalize_slp(path, quiet_seconds=FINALIZE_QUIET_SECONDS):
    """Schedule finalization after the file has been quiet for quiet_seconds."""
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        abs_path = path
    if not abs_path or not str(abs_path).lower().endswith('.slp'):
        return

    def _timer_cb(target):
        try:
            last = get_last_file_activity(target)
            if last is not None and (time.time() - last) < quiet_seconds:
                # Still active; reschedule.
                schedule_finalize_slp(target, quiet_seconds=quiet_seconds)
                return
            _finalize_slp_file(target)
        finally:
            with finalize_timers_lock:
                finalize_timers.pop(target, None)

    with finalize_timers_lock:
        old = finalize_timers.get(abs_path)
        if old is not None:
            try:
                old.cancel()
            except Exception:
                pass
        t = threading.Timer(quiet_seconds, _timer_cb, args=(abs_path,))
        t.daemon = True
        finalize_timers[abs_path] = t
        t.start()

def load_creation_times_csv(force=False):
    """Load and cache the optional creation_times.csv if present.

    CSV format:
        timestamp,filename
        1760832724,Game_20251019T131204.slp
    """
    global creation_times_map, creation_times_csv_mtime, creation_times_csv_path
    path = CREATION_TIMES_CSV
    creation_times_csv_path = path
    if not os.path.exists(path):
        # Clear cache if previously loaded
        if creation_times_map:
            creation_times_map = {}
            creation_times_csv_mtime = None
        return
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None
    if not force and creation_times_csv_mtime is not None and mtime == creation_times_csv_mtime:
        return  # Up-to-date
    tmp = {}
    try:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                ts = (row.get('timestamp') or '').strip()
                fname = (row.get('filename') or '').strip()
                if not ts or not fname:
                    continue
                try:
                    tmp[fname] = int(ts)
                except Exception:
                    continue
        creation_times_map = tmp
        creation_times_csv_mtime = mtime
        print(f"[INFO] Loaded {CREATION_TIMES_CSV} with {len(creation_times_map)} entries from {path}", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to load {CREATION_TIMES_CSV} from {path}: {e}", flush=True)

def get_created_time_string(filename, filepath):
    """Return created time string 'YYYY-mm-dd HH:MM:SS' using CSV override if available."""
    try:
        # Refresh cache if CSV exists/changed
        load_creation_times_csv()
        ts = creation_times_map.get(filename)
        if ts:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
    except Exception:
        pass
    # Fallbacks: statx btime -> st_ctime (Windows creation) -> st_mtime
    try:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(statx(filepath).btime))
    except Exception:
        try:
            st = os.stat(filepath)
            t = getattr(st, 'st_ctime', None) or st.st_mtime
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
        except Exception:
            return ''

def load_slippilab_uploaded():
    global slippilab_uploaded
    if os.path.exists(SLIPPILAB_UPLOADS_FILE):
        try:
            with open(SLIPPILAB_UPLOADS_FILE, "r", encoding="utf-8") as f:
                slippilab_uploaded = json.load(f)
        except Exception as e:
            print(f"[ERROR] Failed to load {SLIPPILAB_UPLOADS_FILE}: {e}", flush=True)
            slippilab_uploaded = {}
    else:
        slippilab_uploaded = {}

def save_slippilab_uploaded():
    try:
        with open(SLIPPILAB_UPLOADS_FILE, "w", encoding="utf-8") as f:
            json.dump(slippilab_uploaded, f, indent=2)
    except Exception as e:
        print(f"[ERROR] Failed to save {SLIPPILAB_UPLOADS_FILE}: {e}", flush=True)
    

def fix_slp_header_length(file):
    # fix SLP header length if this is a .slp file since it will always be 0 for reasons
    try:
        with open(file, "rb+") as f:
            data = f.read()
            header = b'U\x03raw[$U#l'
            idx = data.find(header)
            if idx == -1:
                print(f"[ERROR] Could not find raw header in file: {file}", flush=True)
                return False
            length_offset = idx + len(header)
            raw_data_start = length_offset + 4
            next_key = b'U\x08metadata'
            next_key_idx = data.find(next_key, raw_data_start)
            if next_key_idx == -1:
                print(f"[ERROR] Could not find metadata key in file: {file}", flush=True)
                return False
            raw_length = next_key_idx - raw_data_start
            # Check if header length is 0
            current_length = struct.unpack(">I", data[length_offset:length_offset+4])[0]
            if current_length == 0:
                f.seek(length_offset)
                f.write(struct.pack(">I", raw_length))
                print(f"[DEBUG] Fixed header length to {raw_length} bytes for {file}.", flush=True)
                return True
            else:
                print(f"[DEBUG] Header length is already set to {current_length} bytes for {file}.", flush=True)
                return True
    except Exception as e:
        print(f"[ERROR] Failed to fix SLP header length for {file}: {e}", flush=True)
    return False

def upload_to_slippilab(filepath):
    # fix SLP header length before uploading
    if filepath.lower().endswith('.slp'):
        result = fix_slp_header_length(filepath)
        if result != True:
            print(f"[ERROR] Failed to fix SLP header length for {filepath}.", flush=True)
            return

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    with open(filepath, 'rb') as f:
        data = f.read()
        
    try:
        print(f"[SLIPPILAB] Uploading {filepath} ({len(data)} bytes)...", flush=True)
        response = requests.post('https://slippilab.com/api/replay', headers=headers, data=data)
        if response.ok:
            result = response.json()
            replay_id = result.get("id")
            if replay_id:
                print(f"[SLIPPILAB] Uploaded {filepath} as {replay_id}", flush=True)
                return replay_id
            else:
                print(f"[SLIPPILAB] Upload succeeded but no ID returned: {result}", flush=True)
        else:
            print(f"[SLIPPILAB] Upload failed: {response.status_code} {response.text}", flush=True)
    except Exception as e:
        print(f"[SLIPPILAB] Exception uploading {filepath}: {e}", flush=True)
    return None

def batch_upload_missing_to_slippilab():
    if not os.path.exists(FTP_ROOT):
        return
    for filename in os.listdir(FTP_ROOT):
        if not filename.endswith('.slp'):
            continue
        filepath = os.path.abspath(os.path.join(FTP_ROOT, filename))
        if filepath in slippilab_uploaded:
            continue
        replay_id = upload_to_slippilab(filepath)
        if replay_id:
            slippilab_uploaded[filepath] = replay_id
            save_slippilab_uploaded()

def find_raw_offset_and_length(slp_path):
    with open(slp_path, "rb") as f:
        header = f.read(65536)
        idx = header.find(b'raw')
        if idx == -1:
            print(f"[DEBUG] Could not find 'raw' key in SLP file: {slp_path}")
            print(f"[DEBUG] First 128 bytes: {header[:128].hex()}")
            print(f"[DEBUG] File size: {os.path.getsize(slp_path)} bytes")
            raise Exception("Could not find 'raw' key in SLP file")
        arr_start = header.find(b'[$U#l', idx)
        if arr_start == -1:
            print(f"[DEBUG] Could not find UBJSON array header for 'raw' in SLP file: {slp_path}")
            print(f"[DEBUG] Context around 'raw' key: {header[max(0,idx-16):idx+32].hex()}")
            raise Exception("Could not find UBJSON array header for 'raw'")
        length_offset = arr_start + 5
        raw_len = int.from_bytes(header[length_offset:length_offset+4], 'big')
        raw_data_offset = length_offset + 4
        return raw_data_offset, raw_len

def slp_packet_iterator(f, raw_offset, raw_len):
    pos = raw_offset
    f.seek(0, os.SEEK_END)
    file_end = f.tell()
    while pos + 2 > file_end:
        time.sleep(0.05)
        f.seek(0, os.SEEK_END)
        file_end = f.tell()
    f.seek(pos)
    first_cmd = f.read(1)
    if not first_cmd:
        raise Exception("Failed to read first event at raw_offset")
    if first_cmd[0] != 0x35:
        raise Exception(f"First event at raw_offset is not 0x35 (Event Payloads), got: {first_cmd.hex()}")
    payload_size_byte = f.read(1)
    payload_size = payload_size_byte[0]
    payload_bytes = b''
    while len(payload_bytes) < payload_size - 1:
        chunk = f.read(payload_size - 1 - len(payload_bytes))
        if not chunk:
            time.sleep(0.05)
            continue
        payload_bytes += chunk
    mapping = {}
    i = 0
    while i + 3 <= len(payload_bytes):
        cmd = payload_bytes[i]
        size = int.from_bytes(payload_bytes[i+1:i+3], 'big')
        mapping[cmd] = size
        i += 3
    event_payloads_packet = first_cmd + payload_size_byte + payload_bytes
    scan_pos = pos + 1 + 1 + (payload_size - 1)
    f.seek(0, os.SEEK_END)
    file_end = f.tell()
    last_complete_pos = scan_pos
    last_gamestart_offset = None
    last_gamestart_packet = None
    while scan_pos + 1 <= file_end:
        f.seek(scan_pos)
        cmd_byte = f.read(1)
        if not cmd_byte or len(cmd_byte) < 1:
            break
        cmd = cmd_byte[0]
        payload_len = mapping.get(cmd)
        if payload_len is None:
            scan_pos += 1
            continue
        if scan_pos + 1 + payload_len > file_end:
            break
        payload = f.read(payload_len)
        if cmd == 0x36:
            last_gamestart_offset = scan_pos
            last_gamestart_packet = cmd_byte + payload
        scan_pos += 1 + payload_len
        last_complete_pos = scan_pos
    yield pos, event_payloads_packet
    if last_gamestart_packet is not None:
        yield last_gamestart_offset, last_gamestart_packet
    pos = last_complete_pos
    first_event = True
    while True:
        f.seek(0, os.SEEK_END)
        file_end = f.tell()
        if pos + 1 > file_end:
            time.sleep(0.05)
            continue
        f.seek(pos)
        cmd_byte = f.read(1)
        if not cmd_byte or len(cmd_byte) < 1:
            time.sleep(0.05)
            continue
        cmd = cmd_byte[0]
        payload_len = mapping.get(cmd)
        if payload_len is None:
            pos += 1
            continue
        if pos + 1 + payload_len > file_end:
            time.sleep(0.05)
            continue
        payload = f.read(payload_len)
        if len(payload) < payload_len:
            time.sleep(0.05)
            continue
        packet = cmd_byte + payload
        if first_event:
            first_event = False
        yield pos, packet
        pos += 1 + payload_len

def frame_to_game_timer(frame, starting_timer_seconds=None):
    """
    Returns a string like 'mm:ss.SS' for the timer, or 'Unknown'/'Infinite'.
    timer_type: 'DECREASING', 'INCREASING', or other.
    starting_timer_seconds: int or None
    """
    if starting_timer_seconds is None:
        return "Unknown"
    centiseconds = int(round((((60 - (frame % 60)) % 60) * 99) / 59))
    total_seconds = starting_timer_seconds - frame / 60
    if total_seconds < 0:
        total_seconds = 0
    return f"{int(total_seconds // 60):02}:{int(total_seconds % 60):02}.{centiseconds:02}"


class MonitoringFTPHandler(FTPHandler):
    slippitv_started_files = set()  # {(console_name, abs_path)} to avoid re-queueing same file
    slippitv_started_files_lock = threading.Lock()

    def _start_spectatormode_stream(self, abs_path):
        if not abs_path.endswith('.slp'):
            return
        base = os.path.basename(abs_path)
        parts = base.rsplit('.', 2)
        if len(parts) == 3 and parts[2] == 'slp' and parts[1]:
            console_name = parts[1]
        else:
            console_name = 'unknown'
        if console_name not in MonitoringFTPHandler.console_streams:
            MonitoringFTPHandler.console_streams[console_name] = {}
        if 'spectatormode' not in MonitoringFTPHandler.console_streams[console_name]:
            print(f"[DEBUG] Starting persistent SpectatorMode.tv thread for console {console_name}", flush=True)
            file_queue = queue.Queue()
            stop_event = threading.Event()
            t = threading.Thread(target=self._spectatormode_persistent_stream, args=(console_name, file_queue, stop_event), daemon=True)
            MonitoringFTPHandler.console_streams[console_name]['spectatormode'] = {'thread': t, 'stop_event': stop_event, 'queue': file_queue}
            t.start()
        MonitoringFTPHandler.console_streams[console_name]['spectatormode']['queue'].put(abs_path)

    def _get_console_name_from_path(self, abs_path):
        base = os.path.basename(abs_path)
        parts = base.rsplit('.', 2)
        if len(parts) == 3 and parts[2] == 'slp' and parts[1]:
            return parts[1]
        return 'unknown'

    def _resolve_target_fs_path(self, ftp_arg):
        """Resolve an incoming STOR/APPE/RETR argument to a filesystem path.

        Some clients send absolute Windows paths (e.g. 'K:\\...\\ftp-files\\Game.slp').
        Treat those as just a filename and keep the write under FTP_ROOT.
        """
        s = (ftp_arg or '').strip()
        if not s:
            return s
        try:
            # Drive letter or UNC path
            is_windows_abs = (
                (len(s) >= 3 and s[1] == ':' and (s[2] == '\\' or s[2] == '/'))
                or s.startswith('\\\\')
            )
            if is_windows_abs:
                return os.path.abspath(os.path.join(FTP_ROOT, os.path.basename(s)))
        except Exception:
            pass

        try:
            return self.fs.ftp2fs(s)
        except Exception:
            return os.path.abspath(os.path.join(FTP_ROOT, os.path.basename(s)))

    def _ensure_slippitv_stream(self, abs_path):
        if not abs_path.endswith('.slp'):
            return
        console_name = self._get_console_name_from_path(abs_path)
        if console_name not in MonitoringFTPHandler.console_streams:
            MonitoringFTPHandler.console_streams[console_name] = {}
        if 'slippitv' not in MonitoringFTPHandler.console_streams[console_name]:
            print(f"[DEBUG] Starting SlippiTV streaming thread for console {console_name}", flush=True)
            stop_event = threading.Event()
            file_queue = queue.Queue()
            t = threading.Thread(
                target=self._stream_slp_to_slippitv_per_console,
                args=(console_name, file_queue, stop_event),
                daemon=True,
            )
            MonitoringFTPHandler.console_streams[console_name]['slippitv'] = {
                'thread': t,
                'stop_event': stop_event,
                'queue': file_queue,
            }
            t.start()

        key = (console_name, os.path.abspath(abs_path))
        with MonitoringFTPHandler.slippitv_started_files_lock:
            if key in MonitoringFTPHandler.slippitv_started_files:
                return
            MonitoringFTPHandler.slippitv_started_files.add(key)
        MonitoringFTPHandler.console_streams[console_name]['slippitv']['queue'].put(abs_path)
    def _spectatormode_persistent_stream(self, console_name, file_queue, stop_event):
        ws_url = WS_URL
        print(f"[SpectatorMode] Persistent thread started for console {console_name}", flush=True)
        async def stream_loop():
            retry_file = None
            last_forwarded_file = None
            INACTIVITY_TIMEOUT = 3600  # 1 hour in seconds
            while not stop_event.is_set():
                ws = None
                stream_id = None
                last_activity_time = time.time()
                try:
                    ws = await websockets.connect(ws_url, max_size=None)
                    print(f"[SpectatorMode] Connected to {ws_url} for console {console_name}", flush=True)
                    greeting = await ws.recv()
                    print(f"[SpectatorMode] Server greeting: {greeting}", flush=True)
                    msg = json.loads(greeting)
                    if "stream_ids" not in msg or not msg["stream_ids"]:
                        print("[SpectatorMode] Error: No stream_ids in server greeting.")
                        return
                    stream_id = msg["stream_ids"][0]
                    print(f"[SpectatorMode] Using stream_id: {stream_id}", flush=True)# stream_id in the shared console_streams dict for access by main thread
                    MonitoringFTPHandler.console_streams[console_name]['spectatormode']['stream_id'] = stream_id
                    print(f"[SpectatorMode] Set MonitoringFTPHandler.console_streams['{console_name}']['spectatormode']['stream_id'] = {stream_id}", flush=True)
                    while not stop_event.is_set():
                        # Check for inactivity timeout
                        if time.time() - last_activity_time > INACTIVITY_TIMEOUT:
                            print(f"[SpectatorMode] Closing connection for {console_name} due to 1 hour of inactivity", flush=True)
                            break
                        try:
                            if retry_file is not None:
                                slp_path = retry_file
                                retrying = True
                                retry_file = None
                            else:
                                print(f"[SpectatorMode] Waiting for next file for console {console_name} (queue size: {file_queue.qsize()})", flush=True)
                                slp_path = file_queue.get(timeout=1)
                                retrying = False
                                print(f"[SpectatorMode] Got file from queue for {console_name}: {slp_path}", flush=True)
                        except queue.Empty:
                            continue

                        if not retrying and last_forwarded_file == slp_path:
                            print(f"[SpectatorMode] Duplicate file detected in queue for {console_name}: {slp_path}, skipping.", flush=True)
                            continue
                        last_forwarded_file = slp_path
                        last_activity_time = time.time()  # Update activity time when starting new file
                        print(f"[SpectatorMode] Detected SLP file for {console_name}: {slp_path}", flush=True)
                        # Wait for file to exist and have nonzero size before starting streaming
                        waited = 0
                        while (not os.path.exists(slp_path) or os.path.getsize(slp_path) == 0) and waited < 60 and not stop_event.is_set():
                            if not os.path.exists(slp_path):
                                print(f"[SpectatorMode] Waiting for file to exist: {slp_path}", flush=True)
                                await asyncio.sleep(0.1)
                                waited += 0.1
                                continue
                            print(f"[SpectatorMode] File exists but is empty: {slp_path}", flush=True)
                            await asyncio.sleep(0.1)
                            waited += 0.1
                        if not os.path.exists(slp_path) or os.path.getsize(slp_path) == 0:
                            print(f"[SpectatorMode] File did not appear or remained empty for streaming: {slp_path}", flush=True)
                            continue

                        try:
                            raw_offset, _ = find_raw_offset_and_length(slp_path)
                            print(f"[SpectatorMode] Forwarding file {slp_path} (raw_offset={raw_offset})", flush=True)
                            with open(slp_path, "rb") as f:
                                sent_packets = 0
                                # If we're retrying after a dropped connection, resend from the beginning
                                for offset, packet in slp_packet_iterator(f, raw_offset, None):
                                    # print(f"[SpectatorMode] Sending packet at offset {offset} (len={len(packet)}) for {slp_path}", flush=True)
                                    header = struct.pack('<II', stream_id, len(packet))
                                    if packet and packet[0] == 0x39:
                                        print("[SpectatorMode] Game End (0x39) event encountered. Stopping forwarding.", flush=True)
                                        await ws.send(header + packet)
                                        break
                                    await ws.send(header + packet)
                                    sent_packets += 1
                                    await asyncio.sleep(0.001)
                                print(f"[SpectatorMode] Finished forwarding {slp_path}, total packets sent: {sent_packets}", flush=True)
                        except Exception as e:
                            msg = str(e).lower()
                            if (
                                "no close frame received or sent" in msg
                                or "no close frame received" in msg
                                or "keepalive ping timeout" in msg
                                or "code=1011" in msg
                                or " 1011" in msg
                            ):
                                print(f"[SpectatorMode] Connection dropped while forwarding {slp_path}: {e}. Reconnecting and retrying...", flush=True)
                                retry_file = slp_path
                                break  # break inner loop to reconnect
                            else:
                                print(f"[SpectatorMode] Error forwarding {slp_path}: {e}", flush=True)
                except Exception as e:
                    print(f"[SpectatorMode] Connection error for {console_name}: {e}", flush=True)
                finally:
                    if ws is not None:
                        try:
                            await ws.close()
                        except Exception:
                            pass
                    print(f"[SpectatorMode] WebSocket closed for console {console_name}", flush=True)

                if not stop_event.is_set():
                    await asyncio.sleep(1)
        asyncio.run(stream_loop())
    active_connections = []
    active_transfers = []
    # Map of console_name -> {'spectatormode': {'thread': t, 'stop_event': e, 'queue': q}, 'slippitv': {'thread': t, 'stop_event': e, 'queue': q}}
    console_streams = {}
    
    def on_connect(self):
        connection_info = {
            'ip': self.remote_ip,
            'port': self.remote_port,
            'socket': self.socket,
            'socket_fd': self.socket.fileno(),
            'connected_at': time.time(),
            'username': None,
            'current_file': None,
            'current_filepath': None,
            'bytes_transferred': 0,
            'transfer_start_time': None,
            'data_socket': None,
            'data_socket_fd': None,
            'transfer_active': False
        }
        MonitoringFTPHandler.active_connections.append(connection_info)
        self.connection_info = connection_info
        print(f"[{time.strftime('%H:%M:%S')}] Connection from {self.remote_ip}", flush=True)
        
    def on_disconnect(self):
        if hasattr(self, 'connection_info'):
            print(f"[{time.strftime('%H:%M:%S')}] Disconnected: {self.connection_info['ip']}", flush=True)
            MonitoringFTPHandler.active_connections.remove(self.connection_info)
            
    def on_login(self, username):
        if hasattr(self, 'connection_info'):
            self.connection_info['username'] = username
            print(f"[{time.strftime('%H:%M:%S')}] Login: {username} from {self.connection_info['ip']}", flush=True)
    
    def on_file_sent(self, file):
        if hasattr(self, 'connection_info'):
            file_size = os.path.getsize(file) if os.path.exists(file) else 0
            self.connection_info['current_file'] = f"Sent: {os.path.basename(file)} ({file_size} bytes)"
            self.connection_info['bytes_transferred'] = file_size
            self.connection_info['transfer_active'] = False
            print(f"[{time.strftime('%H:%M:%S')}] File sent: {os.path.basename(file)} ({file_size} bytes)", flush=True)
            
    def on_file_received(self, file):
        if hasattr(self, 'connection_info'):
            file_size = os.path.getsize(file) if os.path.exists(file) else 0
            self.connection_info['current_file'] = f"Received: {os.path.basename(file)} ({file_size} bytes)"
            self.connection_info['bytes_transferred'] = file_size
            self.connection_info['transfer_active'] = False
            print(f"[{time.strftime('%H:%M:%S')}] File received: {os.path.basename(file)} ({file_size} bytes)", flush=True)

        # Treat this as activity (APPE uploads may complete in small bursts)
        mark_file_activity(file)
        schedule_finalize_slp(file)
        # fix SLP header length if this is a .slp file since it will always be 0 for reasons
        # NOTE: finalize actions are debounced via schedule_finalize_slp().
    
    # New methods to detect transfer start
    # NOTE: pyftpdlib's ftp_APPE implementation calls ftp_STOR(file, mode="a"),
    # so our override must accept the same signature.
    def ftp_STOR(self, file, mode="w"):
        if hasattr(self, 'connection_info'):
            filename = (file or '').strip()
            verb = "Appending" if str(mode).lower().startswith('a') else "Uploading"
            print(f"[DEBUG] ftp_STOR called with filename: {filename} (mode={mode})", flush=True)
            self.connection_info['current_file'] = f"{verb}: {filename}"
            self.connection_info['transfer_start_time'] = time.time()
            self.connection_info['transfer_active'] = True
            self.connection_info['bytes_transferred'] = 0
            abs_path = self._resolve_target_fs_path(filename)
            self.connection_info['current_filepath'] = abs_path
            mark_file_activity(abs_path)
            self._start_spectatormode_stream(abs_path)
            self._ensure_slippitv_stream(abs_path)
        return super().ftp_STOR(file, mode=mode)

    def ftp_APPE(self, file):
        # Support APPE (append) uploads; delegate to ftp_STOR in append mode.
        # Avoid calling super().ftp_APPE(), which would call ftp_STOR again.
        print(f"[DEBUG] ftp_APPE called with filename: {(file or '').strip()}", flush=True)
        return self.ftp_STOR(file, mode="a")
    
    def ftp_RETR(self, line):
        if hasattr(self, 'connection_info'):
            filename = line.strip()
            self.connection_info['current_file'] = f"Downloading: {filename}"
            self.connection_info['transfer_start_time'] = time.time()
            self.connection_info['transfer_active'] = True
            self.connection_info['bytes_transferred'] = 0
            self.connection_info['current_filepath'] = self._resolve_target_fs_path(filename)
        return super().ftp_RETR(line)

    def ftp_REST(self, line):
        """Support the REST command (restart transfer) for resuming RETR/STOR.

        pyftpdlib already implements this; we override only to ensure it remains
        available alongside our custom STOR/APPE/RETR overrides, and to surface
        the requested offset in logs/connection_info.
        """
        try:
            marker = int(str(line).strip())
        except Exception:
            marker = None
        if hasattr(self, 'connection_info'):
            try:
                self.connection_info['restart_position'] = marker
                if marker is not None:
                    print(f"[DEBUG] ftp_REST called (restart_position={marker})", flush=True)
            except Exception:
                pass
        return super().ftp_REST(line)
    
    # Override data connection methods
    def data_channel(self):
        datachannel = super().data_channel()
        if hasattr(self, 'connection_info') and datachannel:
            try:
                # Try to get the data socket file descriptor
                if hasattr(datachannel, 'socket'):
                    data_fd = datachannel.socket.fileno()
                    self.connection_info['data_socket_fd'] = data_fd
                elif hasattr(datachannel, 'sock'):
                    data_fd = datachannel.sock.fileno()
                    self.connection_info['data_socket_fd'] = data_fd
            except Exception as e:
                pass
        return datachannel
            
    def on_incomplete_file_sent(self, file):
        if hasattr(self, 'connection_info'):
            self.connection_info['current_file'] = f"Incomplete send: {os.path.basename(file)}"
            self.connection_info['transfer_active'] = False
            
    def on_incomplete_file_received(self, file):
        if hasattr(self, 'connection_info'):
            self.connection_info['current_file'] = f"Incomplete receive: {os.path.basename(file)}"
            self.connection_info['transfer_active'] = False

    def _stream_slp_to_slippitv_per_console(self, console_name, file_queue, stop_event):
        ws_url = f"wss://slippi-tv.azurewebsites.net:443/stream/{console_name.upper()}"
        print(f"[SLIPPITV] Streaming thread started for console {console_name}", flush=True)

        def is_transfer_active(filepath):
            abs_path = os.path.abspath(filepath)
            for conn in MonitoringFTPHandler.active_connections:
                if not conn.get('transfer_active'):
                    continue
                fp = conn.get('current_filepath')
                if not fp:
                    continue
                try:
                    if os.path.abspath(fp) == abs_path:
                        return True
                except Exception:
                    if fp == abs_path:
                        return True
            # Also treat recently appended files as active (APPE bursts)
            return is_file_recently_active(abs_path)

        async def persistent_stream_loop():
            retry_file = None
            INACTIVITY_TIMEOUT = 3600  # 1 hour in seconds
            while not stop_event.is_set():
                ws = None
                last_activity_time = time.time()
                try:
                    print(f"[SLIPPITV] Connecting to {ws_url} for console {console_name}", flush=True)
                    ws = await websockets.connect(ws_url, max_size=None)
                    print(f"[SLIPPITV] Connected to {ws_url}", flush=True)
                    while not stop_event.is_set():
                        # Check for inactivity timeout
                        if time.time() - last_activity_time > INACTIVITY_TIMEOUT:
                            print(f"[SLIPPITV] Closing connection for {console_name} due to 1 hour of inactivity", flush=True)
                            break
                        try:
                            if retry_file is not None:
                                filepath = retry_file
                                retry_file = None
                                is_retry = True
                            else:
                                filepath = file_queue.get(timeout=1)
                                is_retry = False
                        except queue.Empty:
                            continue
                        last_activity_time = time.time()  # Update activity time when starting new file
                        print(f"[SLIPPITV] Streaming file {filepath} on console {console_name}{' (retry)' if is_retry else ''}", flush=True)
                        last_pos = 0
                        waited = 0
                        while not os.path.exists(filepath) and not stop_event.is_set():
                            if waited % 10 == 0:
                                print(f"[DEBUG] Waiting for file to exist: {filepath}", flush=True)
                            await asyncio.sleep(0.1)
                            waited += 1
                        if stop_event.is_set():
                            break
                        try:
                            with open(filepath, 'rb') as f:
                                while not stop_event.is_set():
                                    f.seek(last_pos)
                                    chunk = f.read(4096)
                                    if chunk:
                                        await ws.send(chunk)
                                        print(f"[SLIPPITV] Sent {len(chunk)} bytes at offset {last_pos} for {filepath}", flush=True)
                                        last_pos += len(chunk)
                                    else:
                                        if not is_transfer_active(filepath):
                                            print(f"[SLIPPITV] Finished streaming file {filepath} (FTP transfer ended / inactive)", flush=True)
                                            break
                                        await asyncio.sleep(0.2)
                        except FileNotFoundError:
                            print(f"[SLIPPITV] File not found: {filepath}", flush=True)
                        except Exception as e:
                            msg = str(e).lower()
                            if (
                                "no close frame received or sent" in msg
                                or "no close frame received" in msg
                                or "keepalive ping timeout" in msg
                                or "code=1011" in msg
                                or " 1011" in msg
                            ):
                                print(f"[SLIPPITV] Connection dropped while sending {filepath}: {e}. Reconnecting and retrying...", flush=True)
                                retry_file = filepath
                                break  # break inner loop to reconnect and retry
                            else:
                                print(f"[SLIPPITV] Error reading/sending file {filepath}: {e}", flush=True)
                except websockets.ConnectionClosed as e:
                    print(f"[SLIPPITV] Connection closed: code={getattr(e, 'code', None)}, reason={getattr(e, 'reason', None)}", flush=True)
                except Exception as e:
                    print(f"[SLIPPITV] Error in persistent stream for {console_name}: {e}\n{traceback.format_exc()}", flush=True)
                finally:
                    if ws is not None:
                        try:
                            await ws.close()
                            print(f"[SLIPPITV] WebSocket closed for console {console_name}", flush=True)
                        except Exception as e:
                            print(f"[SLIPPITV] Error closing WebSocket for {console_name}: {e}", flush=True)
                if not stop_event.is_set():
                    await asyncio.sleep(2)

        asyncio.run(persistent_stream_loop())

CHARACTER_ID_MAP = {
    0x00: 'captain_falcon',
    0x01: 'donkey_kong',
    0x02: 'fox',
    0x03: 'game_and_watch',
    0x04: 'kirby',
    0x05: 'bowser',
    0x06: 'link',
    0x07: 'luigi',
    0x08: 'mario',
    0x09: 'marth',
    0x0A: 'mewtwo',
    0x0B: 'ness',
    0x0C: 'peach',
    0x0D: 'pikachu',
    0x0E: 'ice_climbers',
    0x0F: 'jigglypuff',
    0x10: 'samus',
    0x11: 'yoshi',
    0x12: 'zelda',
    0x13: 'sheik',
    0x14: 'falco',
    0x15: 'young_link',
    0x16: 'dr_mario',
    0x17: 'roy',
    0x18: 'pichu',
    0x19: 'ganondorf',
    0x1A: 'master_hand',
    0x1B: 'wireframe_male',
    0x1C: 'wireframe_female',
    0x1D: 'giga_bowser',
    0x1E: 'crazy_hand',
    0x1F: 'sandbag',
    0x20: 'popo',
    0x21: 'none'
}
STAGE_ID_MAP = {
    0: 'Dummy',
    1: 'TEST',
    2: 'Fountain of Dreams',
    3: 'Pokémon Stadium',
    4: "Princess Peach's Castle",
    5: 'Kongo Jungle',
    6: 'Brinstar',
    7: 'Corneria',
    8: "Yoshi's Story",
    9: 'Onett',
    10: 'Mute City',
    11: 'Rainbow Cruise',
    12: 'Jungle Japes',
    13: 'Great Bay',
    14: 'Hyrule Temple',
    15: 'Brinstar Depths',
    16: "Yoshi's Island",
    17: 'Green Greens',
    18: 'Fourside',
    19: 'Mushroom Kingdom I',
    20: 'Mushroom Kingdom II',
    22: 'Venom',
    23: 'Poké Floats',
    24: 'Big Blue',
    25: 'Icicle Mountain',
    26: 'Icetop',
    27: 'Flat Zone',
    28: 'Dream Land N64',
    29: "Yoshi's Island N64",
    30: 'Kongo Jungle N64',
    31: 'Battlefield',
    32: 'Final Destination',
}

# NAT Configuration - Set your public IP address here
# You can get your public IP from https://www.whatismyip.com/
MASQUERADE_ADDRESS = "118.67.199.230"  # Set this to your public IP address (e.g., "123.456.789.012")

# Passive port range for data connections
PASSIVE_PORTS = range(64739, 64840)  # Ports 64739-64839

def frame_to_game_timer(frame, starting_timer_seconds=None):
    """
    Returns a string like 'mm:ss.SS' for the timer, or 'Unknown'/'Infinite'.
    timer_type: 'DECREASING', 'INCREASING', or other.
    starting_timer_seconds: int or None
    """
    if starting_timer_seconds is None:
        return "Unknown"
    centiseconds = int(round((((60 - (frame % 60)) % 60) * 99) / 59))
    total_seconds = starting_timer_seconds - frame / 60
    if total_seconds < 0:
        total_seconds = 0
    return f"{int(total_seconds // 60):02}:{int(total_seconds % 60):02}.{centiseconds:02}"


def get_socket_info():
    """Get detailed socket information from the system"""
    try:
        # Try to get connections, but handle permission errors gracefully
        connections = psutil.net_connections(kind='tcp')
        ftp_connections = []
        
        for conn in connections:
            # Skip if no local address
            if not conn.laddr:
                continue
                
            # FTP control connections (port 2121)
            if conn.laddr.port == 2121:
                ftp_connections.append({
                    'fd': getattr(conn, 'fd', 'N/A'),
                    'local_addr': f"{conn.laddr.ip}:{conn.laddr.port}",
                    'remote_addr': f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else "N/A",
                    'status': conn.status,
                    'pid': getattr(conn, 'pid', 'N/A'),
                    'type': 'CONTROL'
                })
            # FTP data connections (typically high ports or port 20)
            elif conn.raddr and conn.status == 'ESTABLISHED':
                # Check if this might be an FTP data connection from known client IPs
                client_ips = [conn_info['ip'] for conn_info in MonitoringFTPHandler.active_connections]
                if conn.raddr.ip in client_ips:
                    # This looks like a data connection from our FTP client
                    ftp_connections.append({
                        'fd': getattr(conn, 'fd', 'N/A'),
                        'local_addr': f"{conn.laddr.ip}:{conn.laddr.port}",
                        'remote_addr': f"{conn.raddr.ip}:{conn.raddr.port}",
                        'status': conn.status,
                        'pid': getattr(conn, 'pid', 'N/A'),
                        'type': 'DATA'
                    })
        
        return ftp_connections
    except psutil.AccessDenied as e:
        return [{'error': f'Permission denied - run with sudo for full socket info (pid={e.pid})'}]
    except Exception as e:
        return [{'error': f'Error getting socket info: {str(e)}'}]

def get_simple_socket_info():
    """Get basic socket information using netstat-like approach"""
    try:
        import subprocess
        # Use netstat to get socket information
        result = subprocess.run(['netstat', '-an'], capture_output=True, text=True, timeout=2)
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            ftp_sockets = []
            for line in lines:
                if ':2121 ' in line or (any(ip in line for ip in [conn['ip'] for conn in MonitoringFTPHandler.active_connections])):
                    ftp_sockets.append(line.strip())
            return ftp_sockets
    except:
        pass
    return []

def monitor_upload_directory():
    """Monitor the upload directory for file changes to detect active transfers"""
    last_files = {}
    import psutil
    pid = os.getpid()
    while True:
        try:
            if os.path.exists(FTP_ROOT):
                current_files = {}
                for filename in os.listdir(FTP_ROOT):
                    filepath = os.path.join(FTP_ROOT, filename)
                    if os.path.isfile(filepath):
                        stat = os.stat(filepath)
                        current_files[filename] = {
                            'size': stat.st_size,
                            'mtime': stat.st_mtime
                        }
                # Check for files that are growing (active transfers)
                for filename, info in current_files.items():
                    is_growing = filename in last_files and info['size'] > last_files[filename]['size']
                    if is_growing:
                        # Any growth counts as activity (helps APPE burst uploads)
                        try:
                            mark_file_activity(os.path.join(FTP_ROOT, filename))
                        except Exception:
                            pass
                        # File is growing - active transfer
                        abs_path = os.path.abspath(os.path.join(FTP_ROOT, filename))
                        for conn in MonitoringFTPHandler.active_connections:
                            fp = conn.get('current_filepath')
                            if not fp:
                                continue
                            try:
                                if os.path.abspath(fp) == abs_path:
                                    conn['bytes_transferred'] = info['size']
                                    conn['transfer_active'] = True
                            except Exception:
                                if fp == abs_path:
                                    conn['bytes_transferred'] = info['size']
                                    conn['transfer_active'] = True
                last_files = current_files.copy()
        except Exception as e:
            pass  # Ignore errors
            
        time.sleep(1)  # Check every second

def show_active_connections():
    # Just log basic info occasionally, no clearing terminal
    while True:
        if MonitoringFTPHandler.active_connections:
            for conn in MonitoringFTPHandler.active_connections:
                status = "active" if conn.get('transfer_active') else "idle"
                current_file = conn.get('current_file')
                if current_file is None:
                    current_file = ''
                current_file = (
                    current_file
                    .replace('Uploading: ', '')
                    .replace('Appending: ', '')
                    .replace('Downloading: ', '')
                ) or 'connected'
                # Only log when status changes or files change
                if not hasattr(conn, 'last_logged_status') or conn.get('last_logged_status') != (status, current_file):
                    print(f"[{time.strftime('%H:%M:%S')}] {conn['ip']} - {status} - {current_file}")
                    conn['last_logged_status'] = (status, current_file)
        
        time.sleep(5)  # Check less frequently
        
        
def port_to_number(port):
    # goofer goober method to convert "P1"->0, "P2"->1, or returns int if already a number
    # live replays use "P1" etc for ports for some reason lol
    if isinstance(port, int):
        return port
    if isinstance(port, str) and port.upper().startswith('P'):
        try:
            return int(port[1:]) - 1
        except Exception:
            pass
    try:
        return int(port)
    except Exception:
        return port  # fallback

# --- Snapshot builder for /api/replays to avoid per-request heavy work ---
def compute_replays_snapshot():
    replays = []
    active_filepaths = set()

    # Build set of active filepaths
    for conn in MonitoringFTPHandler.active_connections:
        if conn.get('transfer_active'):
            fp = conn.get('current_filepath')
            if fp:
                try:
                    active_filepaths.add(os.path.abspath(fp))
                except Exception:
                    active_filepaths.add(fp)

    if os.path.exists(FTP_ROOT):
        for filename in os.listdir(FTP_ROOT):
            filepath = os.path.join(FTP_ROOT, filename)
            if not (os.path.isfile(filepath) and filename.endswith('.slp')):
                continue
            try:
                stat_obj = os.stat(filepath)
            except Exception:
                continue
            size_mb = round(stat_obj.st_size / (1024 * 1024), 2)
            modified_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat_obj.st_mtime))
            created_time = get_created_time_string(filename, filepath)
            abs_fp = os.path.abspath(filepath)
            is_active = abs_fp in active_filepaths or is_file_recently_active(abs_fp)

            # If finalize ran and there has been no newer activity since, treat as finished.
            # This avoids keeping replays "active" for the full grace period after the last APPE.
            finalized_at = get_file_finalized_at(abs_fp)
            if finalized_at:
                try:
                    last_act = get_last_file_activity(abs_fp)
                except Exception:
                    last_act = None
                if (last_act is None or last_act <= finalized_at) and stat_obj.st_mtime <= finalized_at:
                    is_active = False

            cache_key = filename
            cache_entry = gameinfo_cache.get(cache_key)
            if (not is_active and cache_entry and cache_entry[0] == stat_obj.st_mtime):
                game_info = cache_entry[1]
            else:
                stage_id = None
                stage_name = None
                players = []
                game_info = {}
                try:
                    if is_active:
                        # Live: avoid heavy lists; iterate to get last frame only
                        info = live_slippi_info(filepath)
                        last_frame = None
                        try:
                            for fr in live_slippi_frame_numbers(filepath):
                                last_frame = fr
                        except Exception:
                            last_frame = None

                        stage_id = int(info['start']['stage']) if info.get('start') and 'stage' in info['start'] else None
                        stage_name = STAGE_ID_MAP.get(stage_id, f"Stage {stage_id}") if stage_id is not None else None
                        if info.get('start', {}).get('is_frozen_ps', False) and stage_name == "Pokémon Stadium":
                            stage_name = "Frozen Pokémon Stadium"

                        player_list = info.get('start', {}).get('players', [])
                        player_ports = [p['port'] for p in player_list] if player_list else []
                        min_port = min(player_ports) if player_ports else 0
                        players = []
                        for p in player_list:
                            char_id = int(p['character'])
                            costume = int(p['costume'])
                            char_name = CHARACTER_ID_MAP.get(char_id, f"char_{char_id}")
                            port_key = 0 if p['port'] == min_port else 1
                            stocks = None
                            percent = None
                            try:
                                if last_frame is not None and hasattr(last_frame, 'ports') and len(last_frame.ports) > port_key:
                                    port_data = last_frame.ports[port_key]
                                    if hasattr(port_data, 'leader') and 'post' in port_data.leader and 'stocks' in port_data.leader['post']:
                                        stocks = port_data.leader['post']['stocks']
                                    if hasattr(port_data, 'leader') and 'post' in port_data.leader and 'percent' in port_data.leader['post']:
                                        percent = port_data.leader['post']['percent']
                            except Exception:
                                stocks = None
                                percent = None
                            icon = f"/icon/chara_2_{char_name}_{str(costume).zfill(2)}.png"
                            players.append({
                                'port': port_to_number(p['port']),
                                'character_id': char_id,
                                'character': char_name,
                                'costume': costume,
                                'stock_count': stocks,
                                'percent': percent,
                                'icon': icon
                            })

                        starting_timer_seconds = info.get('start', {}).get('timer', None)
                        timer_str = "Unknown"
                        if last_frame is not None and hasattr(last_frame, 'id'):
                            timer_str = frame_to_game_timer(last_frame.id, starting_timer_seconds)

                        console_name = info.get('metadata', {}).get('consoleNick')
                        if not console_name:
                            base = os.path.basename(filepath)
                            parts = base.rsplit('.', 2)
                            if len(parts) == 3 and parts[2] == 'slp' and parts[1]:
                                console_name = parts[1]

                        game_info = {
                            'stage_id': stage_id,
                            'stage_name': stage_name,
                            'players': players,
                            'timer': timer_str,
                            'console_name': console_name
                        }
                        # Look up SpectatorMode stream_id from nested structure
                        console_stream_id = None
                        if console_name:
                            streams = MonitoringFTPHandler.console_streams.get(console_name)
                            if streams:
                                spec = streams.get('spectatormode')
                                if spec:
                                    console_stream_id = spec.get('stream_id')
                        if console_stream_id:
                            game_info['spectatormode_stream_id'] = console_stream_id
                    else:
                        game = read_slippi(filepath, skip_frames=False)

                        if hasattr(game, 'start') and hasattr(game.start, 'stage'):
                            stage_id = int(game.start.stage)
                        if stage_id is not None:
                            stage_name = STAGE_ID_MAP.get(stage_id, f"Stage {stage_id}")
                            if game.start.is_frozen_ps and stage_name == "Pokémon Stadium":
                                stage_name = "Frozen Pokémon Stadium"
                        if hasattr(game, 'start') and hasattr(game.start, 'players'):
                            player_ports = [player.port for player in game.start.players]
                            min_port = min(player_ports)
                            for p in game.start.players:
                                char_id = int(p.character)
                                costume = int(p.costume)
                                char_name = CHARACTER_ID_MAP.get(char_id, f"char_{char_id}")
                                port_key = 0 if p.port == min_port else 1
                                stocks = game.frames.ports[port_key].leader.post.stocks[-1].as_py()
                                percent = game.frames.ports[port_key].leader.post.percent[-1].as_py()
                                icon = f"/icon/chara_2_{char_name}_{str(costume).zfill(2)}.png"
                                players.append({
                                    'port': port_to_number(p.port),
                                    'character_id': char_id,
                                    'character': char_name,
                                    'costume': costume,
                                    'stock_count': stocks,
                                    'percent': percent,
                                    'icon': icon
                                })
                        timer_str = "Unknown"
                        frame_count = None
                        try:
                            last_frame = game.frames.id[-1].as_py()
                            starting_timer_seconds = game.start.timer
                            if last_frame is not None:
                                frame_count = last_frame
                                timer_str = frame_to_game_timer(last_frame, starting_timer_seconds)
                        except Exception as e:
                            timer_str = f"Error: {e}"
                        game_info = {
                            'stage_id': stage_id,
                            'stage_name': stage_name,
                            'players': players,
                            'timer': timer_str,
                            'frame_count': frame_count,
                            'console_name': game.metadata['consoleNick']
                        }
                except Exception as e:
                    game_info = {'error': str(e)}

                if not is_active:
                    gameinfo_cache[cache_key] = (stat_obj.st_mtime, game_info)

            # Add Slippi Lab link if present
            slippilab_id = slippilab_uploaded.get(abs_fp)
            if slippilab_id:
                try:
                    game_info['slippilab_id'] = slippilab_id
                    game_info['slippilab_link'] = f"https://www.slippilab.com/{slippilab_id}"
                except Exception:
                    pass

            replays.append({
                'filename': filename,
                'size_bytes': stat_obj.st_size,
                'size_mb': size_mb,
                'modified_time': modified_time,
                'created_time': created_time,
                'is_active_transfer': is_active,
                'game_info': game_info
            })

    replays.sort(key=lambda x: x['filename'], reverse=True)
    return {
        'total_files': len(replays),
        'active_transfers': len([r for r in replays if r['is_active_transfer']]),
        'replays': replays
    }

def replays_snapshot_worker():
    global replays_snapshot, replays_snapshot_mtime
    while True:
        try:
            data = compute_replays_snapshot()
            with replays_snapshot_lock:
                replays_snapshot = data
                replays_snapshot_mtime = time.time()
        except Exception as e:
            print(f"[WARN] Snapshot worker error: {e}", flush=True)
        time.sleep(REPLAYS_SNAPSHOT_TTL)

def main():
    # Check if port 2121 is already in use
    import socket as pysocket
    sock = pysocket.socket(pysocket.AF_INET, pysocket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", 2121))
        sock.close()
    except OSError:
        print("[ERROR] Port 2121 is already in use. FTP server will not start.")
        return
    # Set up FTP server
    authorizer = DummyAuthorizer()
    
    # Add anonymous user with write permissions
    authorizer.add_anonymous(FTP_ROOT, perm="elr")
    
    # You can also add a specific user for Nintendont
    authorizer.add_user("nintendont", "password", FTP_ROOT, perm="elradfmwMT")
    
    handler = MonitoringFTPHandler
    handler.authorizer = authorizer
    if PASSIVE_PORTS:
        handler.passive_ports = PASSIVE_PORTS
        print(f"NAT Configuration: Passive ports set to {PASSIVE_PORTS.start}-{PASSIVE_PORTS.stop-1}")
    if MASQUERADE_ADDRESS:
        handler.masquerade_address = MASQUERADE_ADDRESS
        print(f"NAT Configuration: Masquerade address set to {MASQUERADE_ADDRESS}")
    else:
        print("NAT Configuration: No masquerade address set - you may need to configure this for NAT/gateway setups")
    upload_monitor_thread = threading.Thread(target=monitor_upload_directory, daemon=True)
    upload_monitor_thread.start()
    monitor_thread = threading.Thread(target=show_active_connections, daemon=True)
    monitor_thread.start()
    # Start snapshot worker for /api/replays caching
    snapshot_thread = threading.Thread(target=replays_snapshot_worker, daemon=True)
    snapshot_thread.start()
    server = FTPServer(("0.0.0.0", 2121), handler)
    print("FTP Server starting...")
    print("Port 2121 - FTP uploads")
    print("Port 9876 - Web interface")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    # Suppress Flask GET request logging
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.WARNING)

    DEBUG = True
    USE_RELOADER = True

    # When Flask reloader is enabled, the parent process should NOT start
    # background threads (it will spawn the child which runs the app).
    # Start FTP + snapshot worker only in the reloader child.
    is_reloader_child = (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if (not USE_RELOADER) or is_reloader_child:
        ftp_thread = threading.Thread(target=main, daemon=True)
        ftp_thread.start()
        
    load_slippilab_uploaded()
    # batch_upload_missing_to_slippilab()

    # Flask web server
    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/api/replays')
    def api_replays():
        # Serve cached snapshot; refresh on TTL or build on-demand
        global replays_snapshot, replays_snapshot_mtime
        try:
            with replays_snapshot_lock:
                snapshot = replays_snapshot
                snap_mtime = replays_snapshot_mtime

            is_stale = (snapshot is None) or ((time.time() - (snap_mtime or 0.0)) > REPLAYS_SNAPSHOT_TTL)
            if is_stale:
                # Compute synchronously (also covers cases where the snapshot
                # worker thread isn't running, e.g. under Flask reloader).
                snapshot = compute_replays_snapshot()
                with replays_snapshot_lock:
                    replays_snapshot = snapshot
                    replays_snapshot_mtime = time.time()
            resp = jsonify(snapshot)
            # Hint to clients they can cache briefly
            resp.headers['Cache-Control'] = 'public, max-age=1, must-revalidate'
            return resp
        except Exception as e:
            return jsonify({'error': str(e), 'replays': [], 'total_files': 0, 'active_transfers': 0})

    @app.route('/icon/<path:filename>')
    def serve_icon(filename):
        return send_from_directory('icon', filename)
    @app.route('/assets/<path:filename>')
    def serve_assets(filename):
        return send_from_directory('assets', filename)

    app.run(host='0.0.0.0', port=9876, debug=DEBUG, use_reloader=USE_RELOADER)
