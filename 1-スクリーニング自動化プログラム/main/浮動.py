# -*- coding: utf-8 -*-
"""yfinanceから浮動株数/発行済株式数を補完する低頻度producer。"""
from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

DEFAULT_DB_PATH = os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    for col, typ in [
        ("浮動株数", "REAL"),
        ("発行済株式数", "REAL"),
        ("浮動株更新日時", "TEXT"),
        ("発行済株式数更新日時", "TEXT"),
    ]:
        try:
            conn.execute(f'ALTER TABLE screener ADD COLUMN "{col}" {typ}')
        except sqlite3.OperationalError:
            pass
    conn.commit()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="浮動株/発行済株式数の低頻度更新")
    ap.add_argument("--db", default=DEFAULT_DB_PATH)
    ap.add_argument("--refresh-days", type=int, default=7, help="取得済み値を再確認する間隔。0なら全件")
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--max-retries", type=int, default=3)
    args = ap.parse_args(argv)

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db, timeout=60.0)
    _ensure_schema(conn)

    cutoff = (datetime.now() - timedelta(days=max(0, args.refresh_days))).isoformat(timespec="seconds")
    if args.refresh_days <= 0:
        rows = conn.execute("SELECT コード FROM screener WHERE コード IS NOT NULL").fetchall()
    else:
        rows = conn.execute("""
            SELECT コード FROM screener
            WHERE コード IS NOT NULL AND (
                浮動株数 IS NULL OR 発行済株式数 IS NULL
                OR 浮動株更新日時 IS NULL OR 発行済株式数更新日時 IS NULL
                OR 浮動株更新日時 < ? OR 発行済株式数更新日時 < ?
            )
        """, (cutoff, cutoff)).fetchall()

    total = len(rows)
    if total == 0:
        print("[float-shares] 更新対象なし")
        conn.close()
        return 0

    ok = fail = 0
    print(f"[float-shares] 対象={total} refresh_days={args.refresh_days}")
    try:
        for i, (code,) in enumerate(rows, 1):
            code_s = str(code).strip()
            if code_s.endswith(".0"):
                code_s = code_s[:-2]
            code_s = code_s.zfill(4) if code_s.isdigit() else code_s
            symbol = f"{code_s}.T"
            float_shares = shares_out = None
            last_error = None
            for attempt in range(max(1, args.max_retries)):
                try:
                    info = yf.Ticker(symbol).info or {}
                    float_shares = info.get("floatShares")
                    shares_out = info.get("sharesOutstanding")
                    if float_shares is not None or shares_out is not None:
                        break
                    last_error = RuntimeError("floatShares/sharesOutstanding both missing")
                except Exception as e:
                    last_error = e
                    msg = str(e)
                    if "Too Many Requests" in msg or "Rate limited" in msg:
                        time.sleep(15 * (attempt + 1))
                    else:
                        break

            now = datetime.now().isoformat(timespec="seconds")
            if float_shares is None and shares_out is None:
                fail += 1
                print(f"[{i}/{total}] {symbol}: ERROR {last_error or 'empty'}")
            else:
                # transient欠損で既存の良い値をNULL上書きしない。
                conn.execute("""
                    UPDATE screener SET
                      浮動株数 = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株数 END,
                      発行済株式数 = CASE WHEN ? IS NOT NULL THEN ? ELSE 発行済株式数 END,
                      浮動株更新日時 = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株更新日時 END,
                      発行済株式数更新日時 = CASE WHEN ? IS NOT NULL THEN ? ELSE 発行済株式数更新日時 END
                    WHERE コード=?
                """, (
                    float_shares, float_shares,
                    shares_out, shares_out,
                    float_shares, now,
                    shares_out, now,
                    code,
                ))
                conn.commit()
                ok += 1
                print(f"[{i}/{total}] {symbol}: float={float_shares} shares={shares_out}")

            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        conn.close()

    print(f"[float-shares] 完了 ok={ok} fail={fail}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
