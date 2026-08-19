# -*- coding: utf-8 -*-
"""karauri.net 軽量HTTP transportの読み取り専用ベンチマーク。

P3-55 (2026-08-18)
Playwright版は3,587銘柄で約65分を要した。まずaiohttp+BeautifulSoupで
同じページを安全に解釈できるかを確認する。DB・marker・既存snapshotは
一切更新しない。正常HTMLを判別できない応答は空売りなしにせずfailedにする。
"""
from __future__ import annotations

import argparse
import asyncio
import re
import time
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://karauri.net"
DEFAULT_CODES = Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt")
CODE_RE = re.compile(r"^(?:\d{4}|\d{3}[A-Z])$")
BLOCKED = ("access denied", "captcha", "just a moment", "too many requests", "アクセスが集中", "アクセス制限", "認証が必要")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.8,en;q=0.7",
}


def norm_code(value: str) -> str | None:
    value = str(value).strip().upper()
    if value.endswith(".0"):
        value = value[:-2]
    if value.isdigit():
        value = value.zfill(4)
        return value if int(value) >= 1000 else None
    return value if CODE_RE.fullmatch(value) else None


def load_codes(path: Path) -> list[str]:
    values = [norm_code(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return list(dict.fromkeys(code for code in values if code))


def classify_html(html: str) -> tuple[str, int]:
    """returns (detail|no_short|failed, detail row count)."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    lower = text.lower()
    if any(token in lower for token in BLOCKED):
        return "failed", 0
    rows = soup.select("table tr")
    detail_count = 0
    for row in rows:
        cols = row.select("td")
        if len(cols) < 6:
            continue
        first = cols[0].get_text(" ", strip=True)
        if re.match(r"^\d{4}/\d{2}/\d{2}$", first):
            detail_count += 1
    if detail_count:
        return "detail", detail_count
    explicit_none = "空売り" in text and ("なし" in text or "ありません" in text)
    if explicit_none or (rows and text):
        return "no_short", 0
    return "failed", 0


async def fetch_one(session: aiohttp.ClientSession, code: str, sem: asyncio.Semaphore,
                    interval_s: float, retries: int) -> tuple[str, int, str | None]:
    async with sem:
        if interval_s:
            await asyncio.sleep(interval_s)
        url = f"{BASE_URL}/{code}/"
        last = None
        for attempt in range(retries + 1):
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    html = await resp.text(errors="replace")
                    if resp.status == 200:
                        kind, rows = classify_html(html)
                        return kind, rows, None if kind != "failed" else "unrecognized HTML"
                    if 400 <= resp.status < 500 and resp.status != 429:
                        return "failed", 0, f"HTTP {resp.status}"
                    last = f"HTTP {resp.status}"
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = f"{type(e).__name__}: {e}"
            if attempt < retries:
                await asyncio.sleep(0.4 * (2 ** attempt))
        return "failed", 0, last or "unknown"


async def main_async(args) -> int:
    codes = load_codes(Path(args.codes))
    if args.limit:
        codes = codes[:args.limit]
    print(f"[http-bench][START] codes={len(codes)} workers={args.workers} retries={args.retries}", flush=True)
    timeout = aiohttp.ClientTimeout(total=args.timeout_sec, connect=min(10, args.timeout_sec))
    connector = aiohttp.TCPConnector(limit=args.workers, limit_per_host=args.workers, ttl_dns_cache=300)
    sem = asyncio.Semaphore(args.workers)
    started = time.monotonic()
    counts = {"detail": 0, "no_short": 0, "failed": 0, "detail_rows": 0}
    examples: list[str] = []
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        tasks = [asyncio.create_task(fetch_one(session, c, sem, args.interval_ms / 1000.0, args.retries)) for c in codes]
        for done, task in enumerate(asyncio.as_completed(tasks), 1):
            kind, row_count, error = await task
            counts[kind] += 1
            counts["detail_rows"] += row_count
            if error and len(examples) < 10:
                examples.append(error)
            if done == 1 or done % 25 == 0 or done == len(codes):
                elapsed = time.monotonic() - started
                rate = done / max(elapsed, 0.001)
                eta = (len(codes) - done) / rate
                print(f"[http-bench][PROGRESS] {done}/{len(codes)} rate={rate:.2f}/s "
                      f"elapsed={elapsed/60:.1f}m eta={eta/60:.1f}m", flush=True)
    elapsed = time.monotonic() - started
    print(f"[http-bench][DONE] elapsed={elapsed:.1f}s detail={counts['detail']} "
          f"no_short={counts['no_short']} failed={counts['failed']} detail_rows={counts['detail_rows']} "
          f"errors={examples}", flush=True)
    return 0 if counts["failed"] == 0 else 2


def main() -> int:
    ap = argparse.ArgumentParser(description="karauri.net HTTP read-only benchmark")
    ap.add_argument("--codes", default=str(DEFAULT_CODES))
    ap.add_argument("--limit", type=int, default=200, help="0なら全件。まず200件で検証する")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--interval-ms", type=float, default=80)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--timeout-sec", type=float, default=15.0)
    return asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
