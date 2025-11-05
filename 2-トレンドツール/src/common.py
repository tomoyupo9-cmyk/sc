import json
import os
from datetime import datetime, timezone, timedelta
import yaml

ISO = "%Y-%m-%dT%H:%M:%S%z"
# JST timezone (UTC+9)
JST = timezone(timedelta(hours=9))

def now_iso(tz: timezone = JST) -> str:
    """現在時刻(デフォルトJST)をISO8601で返す。例: 2025-09-22T11:37:47+09:00"""
    return datetime.now(tz).isoformat(timespec="seconds")

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def dump_json(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

# === Centralized SQLite connection (WAL + tuning) ===
import sqlite3

_DB_SINGLETON: sqlite3.Connection | None = None
_DB_PATH: str | None = None

def set_db_path(path: str):
    """実行時にDBパスを切り替えたい場合に使用。初回接続前に呼んでください。"""
    global _DB_PATH
    _DB_PATH = path

def _get_db_conn() -> sqlite3.Connection:
    """必要時に一度だけ開き、以後は使い回す（WAL等のPRAGMA付き・高速化）。"""
    global _DB_SINGLETON, _DB_PATH
    if _DB_SINGLETON is not None:
        return _DB_SINGLETON

    db_path = _DB_PATH or r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
    conn = sqlite3.connect(
        db_path,
        timeout=60.0,
        isolation_level=None,        # autocommit（大きい更新はBEGIN〜COMMIT推奨）
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    cur = conn.cursor()
    try:
        # このPCスペック向けの即効チューン（RAM 64GB 前提）
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=60000;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        cur.execute("PRAGMA mmap_size=1073741824;")   # 1GB
        cur.execute("PRAGMA cache_size=-600000;")     # ≈586MB (KiB)
        cur.execute("PRAGMA wal_autocheckpoint=3000;")
        cur.execute("PRAGMA foreign_keys=ON;")
        conn.commit()
    finally:
        cur.close()

    _DB_SINGLETON = conn
    return _DB_SINGLETON

def _close_db_conn():
    """必要なら明示的にクローズ（通常は不要）。"""
    global _DB_SINGLETON
    if _DB_SINGLETON is not None:
        try:
            _DB_SINGLETON.close()
        finally:
            _DB_SINGLETON = None
# === /DB connection ===
