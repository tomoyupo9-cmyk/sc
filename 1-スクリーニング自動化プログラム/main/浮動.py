# -*- coding: utf-8 -*-
"""
浮動株/発行済株式数 producer V2
主系: JPX無料「浮動株比率一覧」を固定ページから自動発見して一括取得
補完: JPXで浮動株数を計算できない銘柄だけ yfinance

重要:
- JPX CSVは「浮動株比率(FFW)」であり浮動株数そのものではない。
- 発行済株式数がDBにあれば、浮動株数 = 発行済株式数 × JPX FFW で更新する。
- 発行済株式数がDBに無い銘柄、およびJPX CSV非収録銘柄だけyfinanceで補完する。
- 一時的な取得失敗で既存の良い値をNULL上書きしない。
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import yfinance as yf

DEFAULT_DB_PATH = os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
)
JPX_PAGE = "https://www.jpx.co.jp/markets/indices/revisions-indices/index.html"
UA = "Mozilla/5.0 JPX-FFW-Producer/2.0"
JPX_TIMEOUT_SEC = int(os.environ.get("KABU_JPX_FFW_TIMEOUT_SEC", "30"))


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    for col, typ in [
        ("浮動株数", "REAL"),
        ("発行済株式数", "REAL"),
        ("浮動株更新日時", "TEXT"),
        ("発行済株式数更新日時", "TEXT"),
        ("浮動株ソース", "TEXT"),
        ("浮動株比率_JPX", "REAL"),
        ("浮動株比率_JPX基準日", "TEXT"),
    ]:
        try:
            conn.execute(f'ALTER TABLE screener ADD COLUMN "{col}" {typ}')
        except sqlite3.OperationalError:
            pass
    conn.commit()


def _canon_code(v) -> str:
    s = str(v or "").strip().upper()
    s = re.sub(r"\.0$", "", s)
    m = re.search(r"(?<![0-9A-Z])(\d{4}|\d{3}[A-Z])(?![0-9A-Z])", s)
    return m.group(1) if m else ""


def _fetch(url: str, timeout: int = JPX_TIMEOUT_SEC) -> tuple[bytes, str]:
    req = Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urlopen(req, timeout=timeout) as r:
        return r.read(), (r.headers.get("Content-Type") or "")


def _discover_jpx_ffw_url() -> str:
    raw, _ = _fetch(JPX_PAGE)
    html = raw.decode("utf-8", errors="replace")
    anchors = re.findall(
        r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html, flags=re.I | re.S
    )
    candidates = []
    for href, body in anchors:
        label = re.sub(r"<[^>]+>", " ", unescape(body))
        label = re.sub(r"\s+", " ", label).strip()
        url = urljoin(JPX_PAGE, href)
        if "浮動株比率" in label:
            candidates.insert(0, url)
        elif "ffw" in (href + " " + label).lower() and ".csv" in href.lower():
            candidates.append(url)
    candidates = list(dict.fromkeys(candidates))
    if not candidates:
        raise RuntimeError("JPX固定ページから浮動株比率CSVリンクを発見できません")
    return candidates[0]


def _decode_csv(data: bytes) -> list[list[str]]:
    for enc in ("cp932", "utf-8-sig", "shift_jis", "utf-8"):
        try:
            rows = list(csv.reader(io.StringIO(data.decode(enc))))
            if rows:
                return rows
        except Exception:
            pass
    raise RuntimeError("JPX浮動株比率CSVの文字コードを判定できません")


def _to_float(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s in {"-", "－", "—"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_jpx_ffw(rows: list[list[str]]) -> tuple[dict[str, float], str]:
    """
    JPX CSVの実レイアウトに耐えるFFW parser。
    まずヘッダ名でコード列/FFW列を検出し、見つからなければ
    「銘柄コードらしい列」と「FFWらしい数値列」の組合せを実データから探索する。
    """
    if not rows:
        raise RuntimeError("JPX CSVが空です")

    def norm(v):
        return re.sub(r"[\s　]+", "", str(v or "")).lower()

    # 1) header-based detection
    for ri, row in enumerate(rows[:50]):
        code_candidates = []
        ffw_candidates = []
        for ci, cell in enumerate(row):
            h = norm(cell)
            if any(k in h for k in ("コード", "code", "localcode", "銘柄コード")):
                code_candidates.append(ci)
            if ("浮動株比率" in h) or ("ffw" in h) or ("浮動株" in h and "比率" in h):
                ffw_candidates.append(ci)

        for code_idx in code_candidates:
            for ffw_idx in ffw_candidates:
                parsed = {}
                for r in rows[ri + 1:]:
                    if code_idx >= len(r) or ffw_idx >= len(r):
                        continue
                    code = _canon_code(r[code_idx])
                    val = _to_float(r[ffw_idx])
                    if not code or val is None:
                        continue
                    ratio = val / 100.0 if val > 1.0 else val
                    if 0.0 <= ratio <= 1.0:
                        parsed[code] = ratio
                if len(parsed) >= 100:
                    print(
                        f"[float-shares][JPX] parser=header header_row={ri} "
                        f"code_col={code_idx} ffw_col={ffw_idx}",
                        flush=True,
                    )
                    return parsed, ""

    # 2) data-driven fallback.
    width = max((len(r) for r in rows), default=0)
    best = None
    sample = rows[: min(len(rows), 1000)]
    for code_idx in range(width):
        code_hits = 0
        for r in sample:
            if code_idx < len(r) and _canon_code(r[code_idx]):
                code_hits += 1
        if code_hits < 50:
            continue

        for ffw_idx in range(width):
            if ffw_idx == code_idx:
                continue
            parsed = {}
            plausible = 0
            for r in sample:
                if code_idx >= len(r) or ffw_idx >= len(r):
                    continue
                code = _canon_code(r[code_idx])
                val = _to_float(r[ffw_idx])
                if not code or val is None:
                    continue
                ratio = val / 100.0 if val > 1.0 else val
                # FFWは0～1。0も許容するが、実用上は正値を強く評価。
                if 0.0 <= ratio <= 1.0:
                    parsed[code] = ratio
                    if ratio > 0:
                        plausible += 1
            score = (len(parsed), plausible)
            if best is None or score > best[0]:
                best = (score, code_idx, ffw_idx)

    if best and best[0][0] >= 100:
        _, code_idx, ffw_idx = best
        parsed = {}
        for r in rows:
            if code_idx >= len(r) or ffw_idx >= len(r):
                continue
            code = _canon_code(r[code_idx])
            val = _to_float(r[ffw_idx])
            if not code or val is None:
                continue
            ratio = val / 100.0 if val > 1.0 else val
            if 0.0 <= ratio <= 1.0:
                parsed[code] = ratio
        if len(parsed) >= 100:
            print(
                f"[float-shares][JPX] parser=fallback code_col={code_idx} "
                f"ffw_col={ffw_idx} parsed={len(parsed)}",
                flush=True,
            )
            return parsed, ""

    # Failure diagnostics: print first rows so a future JPX format change is immediately visible.
    print("[float-shares][JPX][DEBUG] CSV先頭5行:", flush=True)
    for i, r in enumerate(rows[:5]):
        print(f"  row{i}={r}", flush=True)
    raise RuntimeError("JPX FFW列/コード列を特定できません")

def _jpx_reference_date_from_url(url: str) -> str:
    m = re.search(r"ffw(\d{4})(\d{2})(\d{2})", url, re.I)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""


def _db_rows(conn):
    return conn.execute("""
        SELECT コード, 浮動株数, 発行済株式数, 浮動株更新日時, 発行済株式数更新日時
        FROM screener
        WHERE コード IS NOT NULL
    """).fetchall()


def _update_jpx(conn, ffw: dict[str, float], ref_date: str, refresh_days: int):
    now = datetime.now().isoformat(timespec="seconds")
    cutoff = datetime.now() - timedelta(days=max(0, refresh_days))
    updated = 0
    need_yf = []

    for code, old_float, shares_out, float_ts, shares_ts in _db_rows(conn):
        c = _canon_code(code)
        ratio = ffw.get(c)
        shares = _to_float(shares_out)

        # JPXだけでは今回の浮動株数を計算できない場合でも、
        # 既に有効な浮動株数をDBに持っている銘柄はyfinanceへ再取得しない。
        # yfinance補完は「JPX処理後にも浮動株数が欠けている銘柄」だけに限定する。
        if ratio is None or shares is None or shares <= 0:
            old_float_num = _to_float(old_float)
            if old_float_num is None or old_float_num <= 0:
                need_yf.append(c)
            continue

        # 既存値が新しければ毎週同じ計算を繰り返さない。
        if refresh_days > 0 and float_ts:
            try:
                if datetime.fromisoformat(str(float_ts)) >= cutoff:
                    continue
            except Exception:
                pass

        float_shares = shares * ratio
        conn.execute("""
            UPDATE screener SET
              浮動株数=?,
              浮動株更新日時=?,
              浮動株ソース='JPX_FFW',
              浮動株比率_JPX=?,
              浮動株比率_JPX基準日=?
            WHERE コード=?
        """, (float_shares, now, ratio, ref_date or None, code))
        updated += 1

    conn.commit()
    return updated, sorted(set(x for x in need_yf if x))


def _yfinance_fallback(
    conn, codes, ffw: dict[str, float], ref_date: str,
    sleep_s: float, max_retries: int, rate_wait: float
):
    float_direct_ok = shares_only_jpx_calc = shares_only_no_ffw = fail = 0
    total = len(codes)

    for i, code in enumerate(codes, 1):
        symbol = f"{code}.T"
        float_shares = shares_out = None
        last_error = None
        ticker = yf.Ticker(symbol)

        for attempt in range(max(1, max_retries)):
            try:
                info = ticker.info or {}
                float_shares = info.get("floatShares")
                shares_out = info.get("sharesOutstanding")
                if float_shares is not None or shares_out is not None:
                    break
                last_error = RuntimeError("floatShares/sharesOutstanding both missing")
                break
            except Exception as e:
                last_error = e
                msg = str(e)
                limited = ("Too Many Requests" in msg or "Rate limited" in msg)
                if limited and attempt + 1 < max(1, max_retries):
                    print(f"[yf {i}/{total}] {symbol}: RATE_LIMIT wait={rate_wait}s", flush=True)
                    if rate_wait > 0:
                        time.sleep(rate_wait)
                    continue
                break

        now = datetime.now().isoformat(timespec="seconds")
        ratio = ffw.get(code)

        # yfinanceが発行済株式数だけ返した場合でも、JPX FFWがあれば浮動株数を算出。
        source = None
        if float_shares is not None:
            source = "YFINANCE"
            float_direct_ok += 1
        elif shares_out is not None and ratio is not None:
            try:
                float_shares = float(shares_out) * float(ratio)
                source = "JPX_FFW+YF_SHARES"
                shares_only_jpx_calc += 1
            except Exception:
                float_shares = None

        if float_shares is None and shares_out is None:
            fail += 1
            print(f"[yf {i}/{total}] {symbol}: ERROR {last_error or 'empty'}", flush=True)
        else:
            if float_shares is None:
                shares_only_no_ffw += 1
                print(
                    f"[yf {i}/{total}] {symbol}: shares_only={shares_out} "
                    f"JPX_FFWなし -> 浮動株数は既存値維持",
                    flush=True,
                )
            elif source == "JPX_FFW+YF_SHARES":
                print(
                    f"[yf {i}/{total}] {symbol}: shares={shares_out} "
                    f"JPX_FFW={ratio:.6f} -> float={float_shares:.0f}",
                    flush=True,
                )
            else:
                print(
                    f"[yf {i}/{total}] {symbol}: float={float_shares} shares={shares_out}",
                    flush=True,
                )

            conn.execute("""
                UPDATE screener SET
                  浮動株数 = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株数 END,
                  発行済株式数 = CASE WHEN ? IS NOT NULL THEN ? ELSE 発行済株式数 END,
                  浮動株更新日時 = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株更新日時 END,
                  発行済株式数更新日時 = CASE WHEN ? IS NOT NULL THEN ? ELSE 発行済株式数更新日時 END,
                  浮動株ソース = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株ソース END,
                  浮動株比率_JPX = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株比率_JPX END,
                  浮動株比率_JPX基準日 = CASE WHEN ? IS NOT NULL THEN ? ELSE 浮動株比率_JPX基準日 END
                WHERE コード=?
            """, (
                float_shares, float_shares,
                shares_out, shares_out,
                float_shares, now,
                shares_out, now,
                float_shares, source,
                ratio, ratio,
                ratio, (ref_date or None),
                code,
            ))
            conn.commit()

        if sleep_s > 0:
            time.sleep(sleep_s)

    return {
        "float_direct_ok": float_direct_ok,
        "shares_only_jpx_calc": shares_only_jpx_calc,
        "shares_only_no_ffw": shares_only_no_ffw,
        "fail": fail,
        "processed": total,
    }

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="JPX主系 + yfinance不足分補完 浮動株producer")
    ap.add_argument("--db", default=DEFAULT_DB_PATH)
    ap.add_argument("--refresh-days", type=int, default=7)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--max-retries", type=int, default=2)
    ap.add_argument("--rate-limit-wait", type=float, default=5.0)
    ap.add_argument("--no-yfinance", action="store_true", help="JPXのみ。yfinance補完を行わない")
    args = ap.parse_args(argv)

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db, timeout=60.0)
    _ensure_schema(conn)

    try:
        print(f"[float-shares][JPX] 固定ページ={JPX_PAGE}", flush=True)
        url = _discover_jpx_ffw_url()
        print(f"[float-shares][JPX] CSV={url}", flush=True)
        data, _ = _fetch(url)
        rows = _decode_csv(data)
        ffw, _ = _parse_jpx_ffw(rows)
        ref_date = _jpx_reference_date_from_url(url)
        print(f"[float-shares][JPX] FFW={len(ffw)} 基準日={ref_date or '不明'}", flush=True)

        jpx_updated, need_yf = _update_jpx(conn, ffw, ref_date, args.refresh_days)
        print(
            f"[float-shares][JPX] DB更新={jpx_updated} "
            f"yfinance補完候補(浮動株数欠損のみ)={len(need_yf)}",
            flush=True,
        )

        if args.no_yfinance or not need_yf:
            print("[float-shares] 完了 JPX主系のみ", flush=True)
            return 0

        print(
            f"[float-shares][yf] JPXで浮動株数を作れない銘柄だけ補完します "
            f"targets={len(need_yf)}",
            flush=True,
        )
        yf_result = _yfinance_fallback(
            conn, need_yf, ffw, ref_date,
            args.sleep, args.max_retries, args.rate_limit_wait
        )
        print(
            f"[float-shares] 完了 JPX更新={jpx_updated} "
            f"yf_float直接={yf_result['float_direct_ok']} "
            f"yf発行済+JPX計算={yf_result['shares_only_jpx_calc']} "
            f"yf発行済のみ={yf_result['shares_only_no_ffw']} "
            f"yf完全失敗={yf_result['fail']} "
            f"yf処理={yf_result['processed']}",
            flush=True,
        )
        # JPX主系が成功しているので、補完の一部失敗だけではproducer全体を失敗にしない。
        return 0
    except Exception as e:
        print(f"[float-shares][JPX][ERROR] {type(e).__name__}: {e}", flush=True)
        print("[float-shares][JPX][ERROR] DB既存値はNULLクリアしません。", flush=True)
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
