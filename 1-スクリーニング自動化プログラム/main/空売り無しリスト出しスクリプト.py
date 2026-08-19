# -*- coding: utf-8 -*-
"""karauri.net 機関空売り producer（軽量HTTP版）。

重要:
- institution_short_sales: 明細履歴
- institution_short_snapshot: 取得成否と「本当に空売りなし」を分離
- 取得失敗を「空売りなし」と扱わない
- 同日再実行時は成功済み銘柄をスキップして失敗分だけ再試行

P3-56 (2026-08-18): Playwrightをaiohttp + BeautifulSoupへ置換。
実測ベンチマークでは3,587銘柄で detail=3187 / no_short=391 / failed=9
（Playwright版と完全一致）を119.1秒で取得した。Playwright版は64分46秒。
HTTP 404/構造異常は成功扱いにせずsnapshotへ失敗として保存する。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

DEFAULT_INPUT_CODES = Path(os.environ.get(
    "KABU_CODES_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt",
))
DEFAULT_DB_PATH = Path(os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
))
DEFAULT_MARKER = Path(os.environ.get(
    "KABU_KARAURI_MARKER",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\.karauri_executed_today",
))
BASE_URL = "https://karauri.net"
BLOCKED_TOKENS = (
    "access denied", "captcha", "just a moment", "too many requests",
    "アクセスが集中", "アクセス制限", "認証が必要",
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.8,en;q=0.7",
}


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS institution_short_sales (
                code TEXT NOT NULL, calc_date TEXT NOT NULL, institution_name TEXT NOT NULL,
                ratio REAL, ratio_change REAL, shares INTEGER, shares_change INTEGER, note TEXT,
                PRIMARY KEY (code, calc_date, institution_name)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iss_code_date ON institution_short_sales(code, calc_date DESC)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS institution_short_snapshot (
                code TEXT NOT NULL, snapshot_date TEXT NOT NULL, crawl_success INTEGER NOT NULL,
                has_short INTEGER, detail_count INTEGER NOT NULL DEFAULT 0, checked_at TEXT NOT NULL,
                error TEXT, PRIMARY KEY (code, snapshot_date)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iss_snapshot_date ON institution_short_snapshot(snapshot_date, crawl_success, has_short)")
        conn.commit()
    finally:
        conn.close()


def _norm_code(raw) -> str | None:
    s = str(raw or "").strip().upper()
    if s.endswith(".0"):
        s = s[:-2]
    if re.fullmatch(r"\d{1,4}", s):
        s = s.zfill(4)
        return s if int(s) >= 1000 else None
    return s if re.fullmatch(r"\d{3}[A-Z]", s) else None


def load_codes(input_path: Path, db_path: Path) -> list[str]:
    codes: list[str] = []
    if input_path.exists():
        try:
            for line in input_path.read_text(encoding="utf-8").splitlines():
                if code := _norm_code(line):
                    codes.append(code)
        except Exception as e:
            print(f"[karauri][WARN] code file read failed: {e}", flush=True)
    if not codes:
        try:
            conn = sqlite3.connect(db_path, timeout=30.0)
            rows = conn.execute("SELECT DISTINCT コード FROM screener WHERE コード IS NOT NULL").fetchall()
            conn.close()
            codes = [code for row in rows if (code := _norm_code(row[0]))]
        except Exception as e:
            print(f"[karauri][ERROR] DB code fallback failed: {e}", flush=True)
    return list(dict.fromkeys(codes))


def already_successful_today(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        return {str(row[0]) for row in conn.execute(
            "SELECT code FROM institution_short_snapshot WHERE snapshot_date=? AND crawl_success=1",
            (today_str(),),
        )}
    finally:
        conn.close()


def parse_num(text: str, *, is_float: bool = False):
    s = str(text or "").strip()
    if s in {"", "-", "--", "―", "－", "N/A"}:
        return None
    clean = re.sub(r"[^\d.\-+]", "", s)
    if clean in {"", "+", "-", "."}:
        return None
    try:
        return float(clean) if is_float else int(float(clean))
    except ValueError:
        return None


class DefinitiveFetchError(RuntimeError):
    """再試行しても改善しないHTTP/HTML状態。"""


def parse_html(code: str, html: str) -> tuple[dict, list[tuple]]:
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text(" ", strip=True)
    lowered = body_text.lower()
    if any(token in lowered for token in BLOCKED_TOKENS):
        raise DefinitiveFetchError("blocked HTML")

    rows = soup.select("table tr")
    details: list[tuple] = []
    for row in rows:
        cols = row.select("td")
        if len(cols) < 6:
            continue
        date_s = cols[0].get_text(" ", strip=True)
        if not re.match(r"^\d{4}/\d{2}/\d{2}$", date_s):
            continue
        institution = cols[1].get_text(" ", strip=True)
        if not institution:
            continue
        details.append((
            code, date_s.replace("/", "-"), institution,
            parse_num(cols[2].get_text(" ", strip=True), is_float=True),
            parse_num(cols[3].get_text(" ", strip=True), is_float=True),
            parse_num(cols[4].get_text(" ", strip=True)),
            parse_num(cols[5].get_text(" ", strip=True)),
            cols[6].get_text(" ", strip=True) if len(cols) >= 7 else "",
        ))
    if details:
        return {"code": code, "success": 1, "has_short": 1,
                "detail_count": len(details), "error": None}, details

    explicit_none = "空売り" in body_text and ("なし" in body_text or "ありません" in body_text)
    # P3-54互換: 明細表の見出しだけの正常ページも空売りなしと認定する。
    if explicit_none or (rows and body_text):
        return {"code": code, "success": 1, "has_short": 0,
                "detail_count": 0, "error": None}, []
    raise DefinitiveFetchError("unrecognized no-short page structure")


async def fetch_one(session: aiohttp.ClientSession, code: str, semaphore: asyncio.Semaphore,
                    interval_s: float, retries: int) -> tuple[dict, list[tuple]]:
    async with semaphore:
        if interval_s:
            await asyncio.sleep(interval_s)
        last: Exception | None = None
        for attempt in range(max(1, retries + 1)):
            try:
                async with session.get(f"{BASE_URL}/{code}/", allow_redirects=True) as response:
                    html = await response.text(errors="replace")
                    if response.status == 200:
                        return parse_html(code, html)
                    if 400 <= response.status < 500 and response.status != 429:
                        raise DefinitiveFetchError(f"HTTP {response.status}")
                    raise RuntimeError(f"transient HTTP {response.status}")
            except DefinitiveFetchError as e:
                return {"code": code, "success": 0, "has_short": None,
                        "detail_count": 0, "error": f"{type(e).__name__}: {e}"[:500]}, []
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
                last = e
                if attempt < retries:
                    wait = 0.4 * (2 ** attempt)
                    print(f"[karauri] retry {attempt + 1}/{retries} wait={wait:.1f}s: {e}", flush=True)
                    await asyncio.sleep(wait)
        return {"code": code, "success": 0, "has_short": None,
                "detail_count": 0, "error": f"{type(last).__name__}: {last}"[:500]}, []


def save_results(db_path: Path, results: list[tuple[dict, list[tuple]]]) -> tuple[int, int, int]:
    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.execute("PRAGMA busy_timeout=60000;")
    success = failed = no_short = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        for snap, details in results:
            if details:
                conn.executemany("""
                    INSERT INTO institution_short_sales
                    (code,calc_date,institution_name,ratio,ratio_change,shares,shares_change,note)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(code,calc_date,institution_name) DO UPDATE SET
                      ratio=excluded.ratio, ratio_change=excluded.ratio_change,
                      shares=excluded.shares, shares_change=excluded.shares_change, note=excluded.note
                """, details)
            conn.execute("""
                INSERT INTO institution_short_snapshot
                (code,snapshot_date,crawl_success,has_short,detail_count,checked_at,error)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(code,snapshot_date) DO UPDATE SET
                  crawl_success=excluded.crawl_success, has_short=excluded.has_short,
                  detail_count=excluded.detail_count, checked_at=excluded.checked_at, error=excluded.error
            """, (snap["code"], today_str(), int(snap["success"]), snap["has_short"],
                  int(snap["detail_count"]), now_str(), snap["error"]))
            if snap["success"]:
                success += 1
                no_short += int(snap["has_short"] == 0)
            else:
                failed += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return success, failed, no_short


def touch_marker(marker: Path) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()


async def run(args) -> int:
    db_path, marker = Path(args.db), Path(args.marker)
    init_db(db_path)
    all_codes = load_codes(Path(args.codes), db_path)
    if not all_codes:
        print("[karauri][ERROR] 対象コードなし", flush=True)
        return 2
    codes = list(all_codes)
    if not args.force:
        done = already_successful_today(db_path)
        codes = [code for code in codes if code not in done]
        if not codes:
            print("[karauri] 本日全銘柄のsnapshot取得済み", flush=True)
            touch_marker(marker)
            return 0
        print(f"[karauri] 同日resume: 未完了={len(codes)}", flush=True)

    print(f"[karauri][HTTP] targets={len(codes)} workers={args.workers} retries={args.retries}", flush=True)
    semaphore = asyncio.Semaphore(max(1, args.workers))
    timeout = aiohttp.ClientTimeout(total=args.timeout_sec, connect=min(10, args.timeout_sec))
    connector = aiohttp.TCPConnector(limit=args.workers, limit_per_host=args.workers, ttl_dns_cache=300)
    interval_s = max(0.0, args.interval_ms) / 1000.0
    total = len(codes)
    processed = total_ok = total_fail = total_no_short = 0
    started = time.monotonic()
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        batch_size = max(args.workers, int(args.batch_size))
        for start in range(0, total, batch_size):
            part = codes[start:start + batch_size]
            tasks = [asyncio.create_task(fetch_one(session, code, semaphore, interval_s, args.retries)) for code in part]
            batch_results: list[tuple[dict, list[tuple]]] = []
            for task in asyncio.as_completed(tasks):
                batch_results.append(await task)
                processed += 1
                if processed == 1 or processed % 25 == 0 or processed == total:
                    elapsed = max(0.001, time.monotonic() - started)
                    rate = processed / elapsed
                    eta = (total - processed) / rate
                    print(f"[karauri][PROGRESS] {processed}/{total} rate={rate:.2f}/s "
                          f"elapsed={elapsed / 60:.1f}m eta={eta / 60:.1f}m", flush=True)
            ok, fail, no_short = save_results(db_path, batch_results)
            total_ok += ok
            total_fail += fail
            total_no_short += no_short
            print(f"[karauri][COMMIT] {min(start + len(part), total)}/{total} "
                  f"ok={total_ok} fail={total_fail} no_short={total_no_short}", flush=True)

    print(f"[karauri] 今回 ok={total_ok} fail={total_fail} no_short={total_no_short}", flush=True)
    done = already_successful_today(db_path)
    remaining = set(all_codes) - done
    if not remaining:
        touch_marker(marker)
        print(f"[karauri] authoritative snapshot complete: {len(all_codes)} codes", flush=True)
        return 0
    print(f"[karauri][PARTIAL] 未完了={len(remaining)} sample={sorted(remaining)[:10]}", flush=True)
    return 2


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="karauri.net 機関空売り日次snapshot（HTTP版）")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH))
    ap.add_argument("--codes", default=str(DEFAULT_INPUT_CODES))
    ap.add_argument("--marker", default=str(DEFAULT_MARKER))
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--batch-size", type=int, default=200, help="DBへ途中保存する銘柄数")
    ap.add_argument("--interval-ms", type=float, default=80)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--timeout-sec", type=float, default=15.0)
    ap.add_argument("--force", action="store_true", help="今日成功済みの銘柄も再取得")
    return asyncio.run(run(ap.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
