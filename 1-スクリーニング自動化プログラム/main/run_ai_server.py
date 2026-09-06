import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

SERVER_DIR = Path(r"D:\kabu\main\1-スクリーニング自動化プログラム\main")
HOST = "127.0.0.1"
PORT = 8000
HEALTH_URL = f"http://{HOST}:{PORT}/health"
LOG_PATH = SERVER_DIR / "server.log"

def log(msg: str):
    line = f"[run_ai_server] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    try:
        SERVER_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line)

def port_open() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.7)
        return s.connect_ex((HOST, PORT)) == 0

def our_server_healthy(timeout=1.5) -> bool:
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=timeout) as r:
            if r.status != 200:
                return False
            data = json.loads(r.read().decode("utf-8", errors="replace"))
            return data.get("ok") is True and data.get("service") == "stock_ai_server"
    except Exception:
        return False

def start_server() -> int:
    if not SERVER_DIR.is_dir():
        log(f"ERROR server directory not found: {SERVER_DIR}")
        return 2

    # 既に新サーバーが動いていれば正常終了。二重起動しない。
    if our_server_healthy():
        log(f"already running: {HEALTH_URL}")
        return 0

    # 8000が別プロセスに使われている場合は、勝手にkillせず安全停止。
    if port_open():
        log(f"ERROR port {PORT} is already in use, but /health is not our server. Not starting another process.")
        return 3

    command = [
        sys.executable, "-m", "uvicorn",
        "stock_ai_server:app",
        "--host", HOST,
        "--port", str(PORT),
    ]

    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    log("starting uvicorn on http://127.0.0.1:8000")
    with LOG_PATH.open("a", encoding="utf-8", buffering=1) as log_file:
        subprocess.Popen(
            command,
            cwd=str(SERVER_DIR),
            creationflags=creationflags,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            close_fds=False,
        )

    # 起動確認。launcher自体は確認後に終了する。
    deadline = time.time() + 15
    while time.time() < deadline:
        if our_server_healthy():
            log("startup PASS")
            return 0
        time.sleep(0.5)

    log("ERROR startup timeout: /health did not become ready")
    return 4

if __name__ == "__main__":
    raise SystemExit(start_server())
