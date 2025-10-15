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
