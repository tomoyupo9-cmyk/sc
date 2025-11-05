# -*- coding: utf-8 -*-
"""
common.py — 共通ユーティリティ & SQLite 接続（WAL + 高速チューン一元化）

互換維持:
- now_iso, load_config, ensure_dir, dump_json, clamp01, set_db_path,
  _get_db_conn, _close_db_conn のシグネチャを維持

拡張:
- RAM量から mmap_size / cache_size を自動決定（psutil が無ければ安全側の既定）
- 環境変数で DB パスや各 PRAGMA を上書き可能
    COMMON_DB_PATH
    COMMON_SQLITE_MMAP_SIZE_BYTES   (例: "536870912")
    COMMON_SQLITE_CACHE_SIZE_KIB    (KiBで負数指定: 例 "-400000")
    COMMON_SQLITE_WAL_AUTOCHECKPT   (例: "3000")
- 大量書き込み用 begin_immediate() を追加
"""

from __future__ import annotations
import os
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
import yaml

# ===== 時刻ユーティリティ =====
ISO = "%Y-%m-%dT%H:%M:%S%z"
JST = timezone(timedelta(hours=9))

def now_iso(tz: timezone = JST) -> str:
    """現在時刻(デフォルトJST)をISO8601で返す。例: 2025-09-22T11:37:47+09:00"""
    return datetime.now(tz).isoformat(timespec="seconds")

# ===== ファイルユーティリティ =====
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

# ===== SQLite 接続（シングルトン） =====
_DB_SINGLETON: Optional[sqlite3.Connection] = None
_DB_PATH: Optional[str] = None  # set_db_path() で設定／未設定なら既定 or 環境変数

def set_db_path(path: str):
    """実行時にDBパスを切り替えたい場合に使用。初回接続前に呼んでください。"""
    global _DB_PATH
    _DB_PATH = path

def _detect_ram_and_sizes() -> Tuple[int, int, int]:
    """
    (mmap_size_bytes, cache_size_kib_negative, wal_autocheckpoint) を返す。
    - 環境変数があればそちらを優先
    - なければ RAM から安全に推定（psutil 無ければ 16GB 想定）
    """
    # --- env override 最優先 ---
    env_mmap = os.environ.get("COMMON_SQLITE_MMAP_SIZE_BYTES")
    env_cache = os.environ.get("COMMON_SQLITE_CACHE_SIZE_KIB")
    env_wal_auto = os.environ.get("COMMON_SQLITE_WAL_AUTOCHECKPT")

    if env_mmap and env_cache:
        try:
            mmap_bytes = int(env_mmap)
            cache_kib  = int(env_cache)  # KiB（負数指定で実サイズ）
            wal_auto   = int(env_wal_auto) if env_wal_auto else 3000
            return mmap_bytes, cache_kib, wal_auto
        except Exception:
            pass  # フォールバックで決め直す

    # --- RAM 推定 ---
    try:
        import psutil  # type: ignore
        total = int(psutil.virtual_memory().total)
        gb = total // (1024 ** 3)
    except Exception:
        gb = 16  # 取得不可時の想定

    # ざっくり安全側（あなたの 64GB なら 1GB / ~800MB まで広げる）
    if gb <= 16:
        mmap_bytes = 128 * 1024 * 1024      # 128MB
        cache_kib  = -200_000               # ≈ 200MB
    elif gb <= 32:
        mmap_bytes = 256 * 1024 * 1024      # 256MB
        cache_kib  = -300_000               # ≈ 300MB
    elif gb <= 64:
        mmap_bytes = 1_073_741_824          # 1GB
        cache_kib  = -800_000               # ≈ 781MB
    else:
        mmap_bytes = 1_610_612_736          # ~1.5GB
        cache_kib  = -1_000_000             # ~976MB

    wal_auto = 3000
    return mmap_bytes, cache_kib, wal_auto

def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """
    速度と実用耐久のバランスを意識して PRAGMA を一括適用。
    非対応環境でも動くように個別 try/except。
    """
    mmap_bytes, cache_kib, wal_auto = _detect_ram_and_sizes()
    cur = conn.cursor()
    try:
        # ジャーナル／同期
        try: cur.execute("PRAGMA journal_mode=WAL;")
        except Exception: pass
        try: cur.execute("PRAGMA synchronous=NORMAL;")
        except Exception: pass

        # 競合／一時／外部キー
        try: cur.execute("PRAGMA busy_timeout=60000;")
        except Exception: pass
        try: cur.execute("PRAGMA temp_store=MEMORY;")
        except Exception: pass
        try: cur.execute("PRAGMA foreign_keys=ON;")
        except Exception: pass

        # キャッシュ／mmap／WAL 自動チェックポイント
        try: cur.execute(f"PRAGMA mmap_size={int(mmap_bytes)};")
        except Exception: pass
        try: cur.execute(f"PRAGMA cache_size={int(cache_kib)};")  # 負: KiB 指定
        except Exception: pass
        try: cur.execute(f"PRAGMA wal_autocheckpoint={int(wal_auto)};")
        except Exception: pass

        conn.commit()
    finally:
        cur.close()

def _get_db_conn() -> sqlite3.Connection:
    """
    必要時に一度だけ開き、以後は使い回す（WAL 等の PRAGMA 付与済み）。
    - isolation_level=None（Autocommit）。大量更新は BEGIN IMMEDIATE〜COMMIT 推奨。
    - check_same_thread=False（将来の並列化にも耐性）
    """
    global _DB_SINGLETON, _DB_PATH
    if _DB_SINGLETON is not None:
        return _DB_SINGLETON

    # 既定パス: 環境変数 COMMON_DB_PATH > set_db_path() > 既定
    db_path = (
        os.environ.get("COMMON_DB_PATH")
        or _DB_PATH
        or r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
    )

    # ★ここで再帰呼び出しをしないこと（素で接続）
    conn = sqlite3.connect(
        db_path,
        timeout=60.0,
        isolation_level=None,          # autocommit
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    _apply_pragmas(conn)
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

# ===== 任意: 大量UPSERT向け =====
def begin_immediate(conn: sqlite3.Connection) -> None:
    """
    大量INSERT/UPDATE前に使用:
        conn = _get_db_conn()
        begin_immediate(conn)
        try:
            ... bulk upsert ...
            conn.commit()
        except:
            conn.rollback(); raise
    """
    conn.execute("BEGIN IMMEDIATE;")
