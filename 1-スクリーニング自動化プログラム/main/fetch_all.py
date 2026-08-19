#!/usr/bin/env python
# -*- coding: utf-8 -*-

# fetch_all.py — TDnetデータクローラー（決算・TOB・増資取得＆DB保存専用スリム版）
# ※ダッシュボードHTML出力機能やBBS取得機能は自動スクリーニング.pyに委譲し削除済み。
# ※common.pyやsentiment.pyなどの外部ファイルに依存せず、単独で動作します。

import os
import io
import zipfile
import re
import html
import json
import calendar
import xml.etree.ElementTree as ET
import time
import requests
import sqlite3
import warnings
import logging
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib.parse import unquote

try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

# --- ログ抑制 ---
logging.getLogger("PyPDF2").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)
warnings.filterwarnings("ignore", category=UserWarning)

# --- 環境設定・DBパス ---
JST = timezone(timedelta(hours=9))
DB_PATH = os.environ.get('KABU_DB_PATH', r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db")

# v4: バックフィル中に一度失敗したURLを再試行しない
_FAILED_DISCLOSURE_URLS: set[str] = set()

def get_db_conn() -> sqlite3.Connection:
    """独立して動くためのシンプルなDB接続関数"""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

def now_iso() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

# ------------------ 簡易センチメント ------------------
POS_KW = ["上方修正","増益","最高益","過去最高","増配","黒字転換","上振れ","好調","好決算","上乗せ","大幅増","公開買付け","公開買い付け","TOB","ＴＯＢ"]
NEG_KW = ["下方修正","減益","赤字","減配","未達","下振れ","特損","不適正","監理","大幅減","業績悪化"]

def _judge_sentiment(title: str) -> tuple[str, int, str]:
    t = title or ""
    for kw in POS_KW:
        if kw in t: return ("positive", +2, kw)
    for kw in NEG_KW:
        if kw in t: return ("negative", -2, kw)
    return ("neutral", 0, "")

def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def summarize_title_simple(title: str) -> str:
    t = str(title or "")
    t = re.sub(r"【.*?】|\[.*?\]|\(.*?\)|（.*?）|＜.*?＞|〈.*?〉", "", t)
    t = re.sub(r"^\s*適時開示\s*[:：-]?\s*", "", t)
    t = re.sub(r"\s*のお知らせ\s*$", "", t)
    t = _normalize_spaces(t)
    return (t[:80] + "…") if len(t) > 80 else t

def _summarize_title_simple(title: str):
    summary = summarize_title_simple(title)
    label, score, hit_kw = _judge_sentiment(title) 
    return summary, label, hit_kw

# ------------------ TDnet: API取得基盤 ------------------
EARNINGS_KW = ["決算","短信","四半期","通期","上方修正","下方修正","業績","配当","進捗"]
TOB_KW = ["公開買付け", "公開買い付け", "公開買付", "公開買い付", "TOB", "ＴＯＢ"]
OFFERING_KW = [
    "公募増資","第三者割当","第三者割当増資","新株発行","自己株式の処分",
    "株式の売出","募集新株予約権","新株予約権発行","MSワラント","ワラント",
    "転換社債","CB","EB債","ライツ・オファリング","行使", "行使状況", "新株予約権の行使"
]

SHINDEN_DOC_KW = [
    "決算", "短信", "業績予想", "上方修正", "下方修正",
    "決算説明", "説明資料", "中期経営計画", "中期計画", "事業計画",
    "受注", "大型受注", "量産", "稼働", "価格改定", "値上げ",
    "新店", "出店", "新工場", "子会社化", "M&A"
]

def _unescape_text(s: str) -> str:
    if not s: return ""
    try:
        if r"\u" in s:
            s = s.encode("utf-8").decode("unicode_escape")
    except Exception: pass
    return html.unescape(s).strip()

from urllib.parse import quote
def _http_get_json(url: str, retries: int = 3, sleep_sec: float = 0.8):
    global _HTTP
    try:
        _HTTP  # type: ignore
    except NameError:
        _HTTP = requests.Session()
        try:
            _RETRY = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
            _ADPT  = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=_RETRY)
        except Exception:
            _ADPT  = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        _HTTP.mount("http://", _ADPT); _HTTP.mount("https://", _ADPT)

    for i in range(retries):
        try:
            r = _HTTP.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/json" not in ctype and not r.text.strip().startswith(("{","[")):
                if i == retries - 1:
                    print(f"[_http_get_json] non-json {ctype or '(unknown)'} url={url}")
                else:
                    time.sleep(sleep_sec)
                continue
            return r.json()
        except Exception as e:
            if i == retries - 1:
                msg = str(e).splitlines()[0]
                print(f"[_http_get_json] error: {msg} url={url}")
                return None
            time.sleep(sleep_sec)
    return None

def fetch_tdnet_by_keywords(
    days: int = 90,
    keywords: list[str] | None = None,
    per_day_limit: int = 300,
    slice_escalation=(12, 6, 3, 1, 0.5)
) -> List[Dict[str, Any]]:
    time_capable = False
    KEY_RE_LOCAL = re.compile("|".join(map(re.escape, keywords or []))) if (keywords and len(keywords) > 0) else None

    def _fetch_tdnet_by_range(dt_from: datetime, dt_to: datetime) -> list[dict]:
        nonlocal time_capable
        s_day  = dt_from.strftime("%Y%m%d"); e_day  = dt_to.strftime("%Y%m%d")
        s_isoT = dt_from.strftime("%Y-%m-%dT%H:%M:%S"); e_isoT = dt_to.strftime("%Y-%m-%dT%H:%M:%S")
        s_nosep= dt_from.strftime("%Y%m%d%H%M%S");      e_nosep= dt_to.strftime("%Y%m%d%H%M%S")
        s_spc  = dt_from.strftime("%Y%m%d %H:%M:%S");   e_spc  = dt_to.strftime("%Y%m%d %H:%M:%S")

        url_day = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json"
        js_day = _http_get_json(url_day)
        items_day = js_day.get("items") if isinstance(js_day, dict) else (js_day if isinstance(js_day, list) else None)
        if isinstance(items_day, list):
            return items_day or []

        variants = [
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json",     "list/day",  False),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_nosep}-{e_nosep}.json", "list/nosep", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_isoT}-{e_isoT}.json",   "list/isoT",  True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{quote(s_spc)}-{quote(e_spc)}.json", "list/space", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list_time/{s_nosep}-{e_nosep}.json", "list_time/nosep", True),
        ]
        for url, tag, is_time in variants:
            js = _http_get_json(url)
            items = js.get("items") if isinstance(js, dict) else (js if isinstance(js, list) else None)
            if isinstance(items, list):
                if is_time: time_capable = True
                return items or []
        return []

    def _fetch_day_extra_pages(s_day: str, e_day: str, page_try: int = 15) -> list[dict]:
        collected = []
        base = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json"
        patterns = ["?page={p}", "?p={p}"]
        for p in range(2, 2 + max(1, page_try)):
            hit = 0
            for fmt in patterns:
                url = base + fmt.format(p=p)
                js = _http_get_json(url)
                items = js.get("items") if isinstance(js, dict) else (js if isinstance(js, list) else None)
                if isinstance(items, list) and items:
                    collected.extend(items); hit += len(items)
                    break
            if hit == 0: break
            time.sleep(0.15)
        return collected

    def _dedup_key(it: dict) -> str:
        td = it.get("Tdnet") or it
        return (td.get("id") or td.get("document_url") or td.get("title") or "") + "|" + (td.get("pubdate") or "")

    def _slice_fetch(dt_from: datetime, dt_to: datetime, level: int = 0) -> list[dict]:
        items = _fetch_tdnet_by_range(dt_from, dt_to)
        if len(items) < per_day_limit:
            return items
        if not time_capable:
            if (dt_from.hour, dt_from.minute, dt_from.second) == (0, 0, 0) and (dt_to.hour, dt_to.minute, dt_to.second) == (23, 59, 59) and dt_from.date() == dt_to.date():
                s_day = dt_from.strftime("%Y%m%d"); e_day = dt_to.strftime("%Y%m%d")
                extras = _fetch_day_extra_pages(s_day, e_day)
                if extras: return items + extras
            return items
        if level >= len(slice_escalation): return items
        
        width_h = slice_escalation[level]
        step_sec = int(max(width_h * 3600, 1800))
        parts: list[dict] = []
        cur = dt_from
        while cur < dt_to:
            nxt = min(cur + timedelta(seconds=step_sec), dt_to)
            parts.extend(_slice_fetch(cur, nxt, level + 1))
            cur = nxt
            time.sleep(0.12)
        return parts

    def _ts(x: dict) -> float:
        td = x.get("Tdnet") or {}
        s = (td.get("pubdate") or "").replace("/", "-")
        try: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception: return 0.0

    all_items: list[dict] = []
    seen: set[str] = set()
    end = datetime.now(JST).replace(microsecond=0)

    for i in range(days):
        day_to   = (end - timedelta(days=i)).replace(hour=23, minute=59, second=59)
        day_from = (end - timedelta(days=i)).replace(hour=0,  minute=0,  second=0)
        day_items = _slice_fetch(day_from, day_to)

        for it in day_items:
            td = it.get("Tdnet") or it
            title = _unescape_text(td.get("title") or "")
            if KEY_RE_LOCAL and title and not KEY_RE_LOCAL.search(title):
                continue
            td["title"] = title
            k = _dedup_key(it)
            if not k or k in seen: continue
            seen.add(k)
            all_items.append(it)
        time.sleep(0.2)

    all_items.sort(key=_ts, reverse=True)
    return all_items

def fetch_earnings_tdnet_only(days: int = 90, per_day_limit: int = 300) -> List[Dict[str, Any]]:
    return fetch_tdnet_by_keywords(days=days, keywords=EARNINGS_KW, per_day_limit=per_day_limit)

def fetch_tdnet_tob(days: int = 90, per_day_limit: int = 300) -> List[dict]:
    items = fetch_tdnet_by_keywords(days=days, keywords=TOB_KW, per_day_limit=per_day_limit)
    rows = []
    for it in items:
        td = it.get("Tdnet") or it
        title = (td.get("title") or "")
        if any(k in title for k in TOB_KW):
            rows.append(tdnet_item_to_tob_row(it))
    return rows

# --------- PDF解析 / 決算採点処理 ---------
def _download_pdf_bytes(url: str, timeout: int = 25) -> bytes:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
        r.raise_for_status()
        return r.content or b""
    except Exception: return b""

def _extract_text_pdfminer(pdf_bytes: bytes) -> str:
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        with io.BytesIO(pdf_bytes) as bio:
            return (extract_text(bio) or "").strip()
    except Exception: return ""

def _extract_text_pypdf2(pdf_bytes: bytes) -> str:
    try:
        import PyPDF2  # type: ignore
        out = []
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            try: out.append(page.extract_text() or "")
            except Exception: out.append("")
        return "\n".join(out).strip()
    except Exception: return ""

def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if not pdf_bytes: return ""
    text = _extract_text_pdfminer(pdf_bytes)
    if text and len(text) > 30: return text
    return _extract_text_pypdf2(pdf_bytes)

def _safe_extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        txt = _extract_text_from_pdf(pdf_bytes)
        if isinstance(txt, str) and txt.strip(): return txt
    except Exception: pass
    return ""


def _strip_markup_text(raw: bytes) -> str:
    """HTML/XML/XBRLを最低限のプレーンテキストへ変換。"""
    if not raw:
        return ""
    s = ""
    for enc in ("utf-8", "cp932", "shift_jis", "utf-16"):
        try:
            s = raw.decode(enc)
            break
        except Exception:
            pass
    if not s:
        s = raw.decode("utf-8", errors="ignore")
    s = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", s)
    s = re.sub(r"(?s)<!--.*?-->", " ", s)
    s = re.sub(r"(?i)</?(?:tr|p|div|br|li|table|h[1-6])\b[^>]*>", "\n", s)
    s = re.sub(r"(?i)</?(?:td|th)\b[^>]*>", " ", s)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    s = html.unescape(s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _extract_text_from_zip(zip_bytes: bytes) -> str:
    if not zip_bytes:
        return ""
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception:
        return ""

    names = [n for n in zf.namelist() if not n.endswith("/")]
    out = []

    # PDFがあれば最優先
    for n in [n for n in names if n.lower().endswith(".pdf")][:20]:
        try:
            t = _safe_extract_pdf_text(zf.read(n))
            if t:
                out.append(t)
        except Exception:
            pass
    if out:
        return "\n\n".join(out).strip()

    # 古いTDnet決算ではZIP内がXBRL/HTML/XMLの場合がある
    exts = (".htm", ".html", ".xhtml", ".xml", ".xbrl", ".txt")
    for n in [n for n in names if n.lower().endswith(exts)][:80]:
        try:
            b = zf.read(n)
            if len(b) > 8_000_000:
                continue
            t = _strip_markup_text(b)
            if len(t) >= 20:
                out.append(t)
        except Exception:
            pass
    return "\n\n".join(out).strip()


def _unwrap_yanoshin_rd_url(url: str) -> str:
    u = str(url or "").strip()
    marker = "/rd.php?"
    if marker not in u:
        return ""
    inner = u.split(marker, 1)[1]
    try:
        inner = unquote(inner)
    except Exception:
        pass
    return inner.strip()


def _candidate_disclosure_urls(td: dict) -> list[str]:
    keys = (
        "url_report_type_summary",
        "url_report_type_earnings_forecast",
        "url_report_type_fs_consolidated",
        "url_report_type_fs_non_consolidated",
        "document_url",
        "pdf_url",
        "url",
        "url_xbrl",
    )
    urls = []
    seen = set()
    for k in keys:
        u = str(td.get(k) or "").strip()
        if not u:
            continue
        for cand in (u, _unwrap_yanoshin_rd_url(u)):
            if cand and cand not in seen:
                seen.add(cand)
                urls.append(cand)
    return urls




def _jpx_permanent_url(code: str, source_url: str) -> str:
    """
    TDnetの期限切れURLをJPX永続URLへ変換する。

    例:
      code=3131
      .../140120250508534614.pdf
        ->
      https://www2.jpx.co.jp/disc/31310/140120250508534614.pdf

    東証上場会社情報サービス側では、4桁銘柄コードに末尾0を付けた
    5桁ディレクトリが使われる。
    """
    c = _normalize_code(code)
    if not c:
        return ""

    # 通常の4桁銘柄コードはJPX側で末尾0
    if len(c) == 4:
        code_dir = c + "0"
    else:
        code_dir = c

    u = str(source_url or "").strip()
    if not u:
        return ""

    # yanoshin rd.php の内側URLを使う
    inner = _unwrap_yanoshin_rd_url(u)
    if inner:
        u = inner

    try:
        path = urlparse(u).path
    except Exception:
        path = u

    filename = path.rstrip("/").split("/")[-1]
    if not filename:
        return ""

    # JPX永続側で主に使うのはPDF。
    # ZIPも同じファイル名で存在する場合があるため拡張子は維持する。
    if "." not in filename:
        return ""

    return f"https://www2.jpx.co.jp/disc/{code_dir}/{filename}"


def _download_disclosure_text_from_url(
    url: str,
    timeout: int = 8,
    *,
    label: str = "",
    max_candidates: int = 4,
) -> tuple[str, str, str]:
    """
    v4高速版:
      - 1 URL 最大8秒
      - 1資料につき最大4候補
      - TRY/TIMEOUT/HTTP/OK を表示
      - 一度失敗したURLは実行中は再試行しない
    """
    global _FAILED_DISCLOSURE_URLS

    candidates = []
    u = str(url or "").strip()
    if u:
        candidates.append(u)
        inner = _unwrap_yanoshin_rd_url(u)
        if inner and inner not in candidates:
            candidates.append(inner)

    expanded = []
    for c in candidates:
        if c not in expanded:
            expanded.append(c)

        if "release.tdnet.info" in c:
            if c.startswith("http://"):
                alt = "https://" + c[len("http://"):]
            elif c.startswith("https://"):
                alt = "http://" + c[len("https://"):]
            else:
                alt = ""
            if alt and alt not in expanded:
                expanded.append(alt)

    expanded = expanded[:max_candidates]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/pdf,application/zip,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.release.tdnet.info/",
    }

    for i, cand in enumerate(expanded, start=1):
        if cand in _FAILED_DISCLOSURE_URLS:
            print(f"[backfill][SKIP FAILCACHE] {label} {cand[:95]}")
            continue

        print(f"[backfill][TRY {i}/{len(expanded)}] {label} {cand[:95]}")

        try:
            r = requests.get(
                cand,
                timeout=timeout,
                headers=headers,
                allow_redirects=True,
            )

            if r.status_code >= 400:
                print(f"[backfill][HTTP {r.status_code}] {label}")
                _FAILED_DISCLOSURE_URLS.add(cand)
                continue

            b = r.content or b""
            if not b:
                print(f"[backfill][EMPTY BODY] {label}")
                _FAILED_DISCLOSURE_URLS.add(cand)
                continue

            ctype = (r.headers.get("Content-Type") or "").lower()
            low_url = (r.url or cand).lower()

            if b[:5] == b"%PDF-" or "application/pdf" in ctype or low_url.endswith(".pdf"):
                t = _safe_extract_pdf_text(b)
                if len(t.strip()) >= 30:
                    print(f"[backfill][URL OK] {label} pdf {len(t):,}文字")
                    return t, r.url or cand, "pdf"

            if b[:2] == b"PK" or "zip" in ctype or low_url.endswith(".zip"):
                t = _extract_text_from_zip(b)
                if len(t.strip()) >= 30:
                    print(f"[backfill][URL OK] {label} zip {len(t):,}文字")
                    return t, r.url or cand, "zip"

            head = b[:500].lstrip().lower()
            if (
                "text/html" in ctype
                or "xml" in ctype
                or head.startswith(b"<")
                or b"<html" in head
                or b"<?xml" in head
            ):
                t = _strip_markup_text(b)
                if len(t.strip()) >= 100 and "404 not found" not in t.lower():
                    print(f"[backfill][URL OK] {label} markup {len(t):,}文字")
                    return t, r.url or cand, "markup"

            print(f"[backfill][UNUSABLE] {label} ctype={ctype[:40]} bytes={len(b):,}")
            _FAILED_DISCLOSURE_URLS.add(cand)

        except requests.exceptions.Timeout:
            print(f"[backfill][TIMEOUT {timeout}s] {label}")
            _FAILED_DISCLOSURE_URLS.add(cand)
        except requests.exceptions.RequestException as e:
            print(f"[backfill][REQUEST ERROR] {label} {type(e).__name__}")
            _FAILED_DISCLOSURE_URLS.add(cand)
        except Exception as e:
            print(f"[backfill][PARSE ERROR] {label} {type(e).__name__}: {e}")
            _FAILED_DISCLOSURE_URLS.add(cand)

    return "", "", ""





def _download_disclosure_text(it: dict, timeout: int = 5) -> tuple[str, str, str]:
    """
    v13 fast path:
      - 公開から32日超: JPX永続URLを先に試す
      - 直近: TDnet/releaseを先に試す
    """
    td = it.get("Tdnet", it) or {}
    code, _name, title, _url, pub = _tdnet_fields(it)

    title_short = str(title or "")[:42]
    pub_short = str(pub or "")[:10]
    label = f"{pub_short} {title_short}".strip()

    raw_urls = _candidate_disclosure_urls(td)[:4]
    if not raw_urls:
        print(f"[backfill][NO URL] {label}")
        return "", "", ""

    is_old = False
    try:
        dt = _parse_ts_str(pub)
        if dt:
            now = datetime.now(JST)
            # aware/naive差を吸収
            if getattr(dt, "tzinfo", None) is None:
                dt = JST.localize(dt) if hasattr(JST, "localize") else dt.replace(tzinfo=JST)
            is_old = (now - dt).days >= 32
    except Exception:
        is_old = False

    attempts = []
    seen = set()

    for u in raw_urls:
        u = str(u or "").strip()
        if not u:
            continue

        jpx = _jpx_permanent_url(code, u)

        ordered = (
            (("jpx", jpx), ("normal", u))
            if is_old else
            (("normal", u), ("jpx", jpx))
        )

        for kind_hint, cand in ordered:
            if cand and cand not in seen:
                seen.add(cand)
                attempts.append((kind_hint, cand))

    attempts = attempts[:8]

    for idx, (kind_hint, u) in enumerate(attempts, start=1):
        if kind_hint == "jpx":
            print(f"[backfill][JPX FAST {idx}/{len(attempts)}] {label} {u[:110]}")
            max_candidates = 1
        else:
            print(f"[backfill][SOURCE {idx}/{len(attempts)}] {label}")
            # 古い資料のnormalはfallbackなので無駄なhttp派生を増やさない
            max_candidates = 1 if is_old else 3

        text, resolved, kind = _download_disclosure_text_from_url(
            u,
            timeout=timeout,
            label=label,
            max_candidates=max_candidates,
        )

        if text:
            if kind_hint == "jpx":
                print(f"[backfill][JPX OK] {code} {pub_short} {kind} {len(text):,}文字")
                return text, resolved or u, f"jpx_{kind}"
            return text, resolved or u, kind

    return "", "", ""

def _parse_earnings_metrics(text: str) -> Dict[str, Any]:
    if not text: return {"metrics": {}, "progress": None}

    def _num(s):
        s = s.replace(",", "").replace("，", "").replace("％", "%").replace("▲", "-").replace("△", "-")
        s = re.sub(r"[^\d\.\-\+%]", "", s)
        try:
            if s.endswith("%"): return float(s[:-1])
            return float(s)
        except: return None

    UNIT_PAT = r"(百万円|億円|万円|円)?"
    fields = {
        "売上高": r"(売上高)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "営業利益": r"(営業利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "経常利益": r"(経常利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "純利益": r"(当期純利益|親会社株主に帰属する当期純利益|純利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "EPS": r"(EPS|1株当たり当期純利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "進捗率": r"(進捗率)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*%",
    }

    metrics = {}
    yoy = {}

    def _scan_yoy_near(pos: int, text: str):
        if pos < 0: return None
        s = text[max(0, pos-120): pos+220].replace('％','%').replace('▲','-').replace('△','-')
        m = re.search(r'([\-+]?\d+(?:\.\d+)?)\s*%', s)
        if not m: return None
        try: val = float(m.group(1))
        except Exception: return None
        around = s[max(0, m.start()-10): m.end()+10]
        if ('-' not in m.group(1) and '+' not in m.group(1)):
            if re.search(r'(減|悪化|縮小)', around): val = -val
        return val

    for key, pat in fields.items():
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            raw = m.group(2) if len(m.groups()) >= 2 else None
            val = _num(raw or "")
            if val is not None:
                unit = m.group(3) if len(m.groups()) >= 3 else ""
                if key != "EPS" and key != "進捗率" and unit and "百万円" in unit:
                    val = val / 100.0
                metrics[key] = val
            try:
                y = _scan_yoy_near(text.find(key), text)
                if y is not None: yoy[key] = y
            except Exception: pass

    progress = float(metrics["進捗率"]) if "進捗率" in metrics else None
    return {"metrics": metrics, "progress": progress, "yoy": yoy}

def _summarize_earnings_text(text: str, max_chars: int = 240) -> str:
    if not text: return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    KEY = ["上方修正", "下方修正", "通期", "四半期", "増益", "減益", "増収", "減収", "進捗率", "配当", "業績予想", "修正"]
    hits = [l for l in lines if any(k in l for k in KEY)]
    base = " / ".join(hits[:4]) if hits else " ".join(lines[:6])
    return re.sub(r"\s+", " ", base).strip()[:max_chars]

def _grade_earnings(title: str, text: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    reasons: list[str] = []
    score = 0.0

    T = title or ""
    X = text or ""
    met = (parsed or {}).get("metrics", {})
    prog = (parsed or {}).get("progress", None)
    yoy = (parsed or {}).get("yoy", {})

    def _hit(ws): return any((w in T) or (w in X) for w in ws)

    if _hit(["上方修正", "通期上方", "通期増額", "上期予想修正（増額）", "業績予想の修正（増額）", "通期予想修正（増額）"]):
        score += 2; reasons.append("上方修正（増額）で+2")
    if _hit(["増配", "復配", "自社株買い", "配当予想の修正（増額）"]):
        score += 1; reasons.append("配当増額/復配/自社株買いで+1")
    if _hit(["最高益", "過去最高益"]): score += 1; reasons.append("最高益で+1")
    if _hit(["黒字転換"]): score += 1; reasons.append("黒字転換で+1")
    if _hit(["下方修正", "通期予想修正（減額）"]): score -= 3; reasons.append("下方修正（減額）で-3")
    if _hit(["減配", "配当予想の修正（減額）"]): score -= 2; reasons.append("減配で-2")
    if _hit(["特損", "特別損失"]): score -= 2; reasons.append("特損で-2")

    if isinstance(prog, (int, float)):
        if prog >= 70: score += 1; reasons.append(f"進捗率{prog:.0f}%（高進捗）")
        elif prog <= 30: score -= 1; reasons.append(f"進捗率{prog:.0f}%（低進捗）")

    neg_profit_yoy = 0
    pos_profit_yoy = 0
    for k in ("売上高", "営業利益", "経常利益", "純利益"):
        y = yoy.get(k)
        if not isinstance(y, (int, float)) or not (-120.0 <= y <= 120.0): continue
        if k in ("営業利益", "経常利益", "純利益"):
            if y <= -5: neg_profit_yoy += 1
            if y >= 1:  pos_profit_yoy += 1
        if y >= 5: score += 1; reasons.append(f"{k}YoY+{y:.1f}%")
        elif y >= 1: score += 0.5; reasons.append(f"{k}YoY+{y:.1f}%")
        elif y <= -5: score -= 1; reasons.append(f"{k}YoY{y:.1f}%")
        elif y <= -1: score -= 0.5; reasons.append(f"{k}YoY{y:.1f}%")

    if neg_profit_yoy >= 2:
        score -= 1.0; reasons.append("利益YoYが複数項目で減少（総じて減益傾向）")

    for k in ("営業利益", "経常利益", "純利益"):
        v = met.get(k, None)
        if isinstance(v, (int, float)):
            if v > 0:
                if pos_profit_yoy > 0: score += 0.3; reasons.append(f"{k}が黒字（増益傾向と整合）")
                else: reasons.append(f"{k}が黒字（単独要素のため加点なし）")
            if v < 0: score -= 0.5; reasons.append(f"{k}が赤字")

    verdict = "good" if score >= 1.5 else ("bad" if score <= -1.5 else "neutral")
    return {"verdict": verdict, "score": score, "reasons": reasons}

def _summarize_one_pdf_row(row: dict) -> dict:
    link = (row or {}).get("link") or ""
    if not link: return row
    try:
        b = _download_pdf_bytes(link)
        if not b: return row
        text = _safe_extract_pdf_text(b)
        if not text: return row

        parsed = _parse_earnings_metrics(text) or {}
        sum2 = _summarize_earnings_text(text)
        judge = _grade_earnings(row.get("title",""), text, parsed)

        out = dict(row)
        if sum2: out["summary"] = sum2
        out["metrics"]  = parsed.get("metrics", {})
        out["progress"] = parsed.get("progress")
        out["verdict"]  = judge.get("verdict")
        out["score_judge"] = judge.get("score")
        out["reasons"]  = judge.get("reasons", [])
        
        if out["verdict"] == "good": out["sentiment"] = "positive"
        elif out["verdict"] == "bad": out["sentiment"] = "negative"
        return out
    except Exception: return row

EARNINGS_SUMMARY_MAX = 40
def summarize_earnings_rows_parallel(rows: list, max_items: int = 40, max_workers: int = 8):
    if not rows: return rows
    targets = [(i, r) for i, r in enumerate(rows[:max_items]) if r.get("link")]
    if not targets: return rows
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut2idx = {ex.submit(_summarize_one_pdf_row, r): i for i, r in targets}
            for fut in as_completed(fut2idx):
                try: rows[fut2idx[fut]] = fut.result()
                except Exception: pass
    except Exception: pass
    return rows

def tdnet_items_to_earnings_rows(tdnet_items):
    rows = []
    POS_KEYS = ("決算短信","四半期","通期","決算")
    EXCLUDE  = ("動画","説明資料")

    for it in tdnet_items:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not any(k in title for k in POS_KEYS) or any(k in title for k in EXCLUDE):
            continue

        code_raw = str(td.get("company_code") or td.get("code") or "").strip()
        if re.fullmatch(r"\d{5}", code_raw): ticker = code_raw[:4]
        elif re.fullmatch(r"\d{3}[0-9a-zA-Z]", code_raw): ticker = code_raw.upper()
        else: ticker = ""

        link = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        time_str = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","")

        s_t, label_guess, why_t = _summarize_title_simple(title)
        
        row = {
            "ticker": ticker or "0000",
            "name":   (td.get("company_name") or "").strip() or ticker,
            "score":  0,
            "sentiment": td.get("sentiment") or label_guess or "neutral",
            "title": title,
            "link":  link,
            "time":  time_str or "",
            "summary": s_t,
            "reasons": [why_t] if why_t else [],
            "verdict": "neutral",
            "score_judge": 0,
            "metrics": {},
            "progress": None,
        }
        rows.append(row)

    def _ts(s):
        try: return datetime.strptime((s or "").replace("/","-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except: return 0
    rows.sort(key=lambda r: _ts(r["time"]), reverse=True)
    return summarize_earnings_rows_parallel(rows, max_items=EARNINGS_SUMMARY_MAX, max_workers=8)


# ------------------ DB スキーマ定義・保存処理 ------------------
def ensure_earnings_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS earnings_events(
        コード        TEXT NOT NULL,
        銘柄名        TEXT,
        タイトル      TEXT,
        リンク        TEXT,
        発表日時      TEXT,
        提出時刻      TEXT NOT NULL,
        要約          TEXT,
        判定          TEXT,
        判定スコア    INTEGER,
        理由JSON      TEXT,
        指標JSON      TEXT,
        進捗率        REAL,
        センチメント TEXT,
        素点          INTEGER,
        created_at    TEXT DEFAULT (datetime('now','localtime'))
    );
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_earn_code_teishutsu ON earnings_events(コード, 提出時刻);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_earn_teishutsu_desc ON earnings_events(提出時刻 DESC);")
    conn.commit()

def upsert_earnings_rows(conn: sqlite3.Connection, rows: list[dict]):
    import json
    cur = conn.cursor()
    for r in rows:
        jp_vals = {
            "コード": r.get("ticker", "0000"),
            "銘柄名": r.get("name", ""),
            "タイトル": r.get("title", ""),
            "リンク": r.get("link", ""),
            "発表日時": r.get("time", ""),
            "提出時刻": r.get("time", "") or now_iso(),
            "要約": r.get("summary", ""),
            "判定": r.get("verdict", "").strip(),
            "判定スコア": int(r.get("score_judge") or 0),
            "理由JSON": json.dumps(r.get("reasons") or [], ensure_ascii=False),
            "指標JSON": json.dumps(r.get("metrics") or {}, ensure_ascii=False),
            "進捗率": r.get("progress"),
            "センチメント": r.get("sentiment", ""),
            "素点": int(r.get("score") or 0),
        }
        
        insert_cols = list(jp_vals.keys())
        insert_vals = list(jp_vals.values())
        placeholders = ",".join(["?"] * len(insert_cols))
        
        cur.execute(f'INSERT OR IGNORE INTO earnings_events ({", ".join(insert_cols)}) VALUES ({placeholders})', insert_vals)
        if cur.rowcount == 0:
            set_clause = ", ".join([f'"{k}"=?' for k in insert_cols if k not in ("コード", "提出時刻")])
            update_vals = [jp_vals[k] for k in insert_cols if k not in ("コード", "提出時刻")]
            update_vals.extend([jp_vals["コード"], jp_vals["提出時刻"]])
            cur.execute(f'UPDATE earnings_events SET {set_clause} WHERE "コード"=? AND "提出時刻"=?', update_vals)


# ============================================================================
# シンデン型: TDnet本文保存 / 予想達成履歴バックフィル
# ============================================================================

def _normalize_code(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    # TDnet APIのcompany_codeは通常 31310 のように末尾0付き。
    if re.fullmatch(r"\d{5}", s):
        return s[:4]
    if re.fullmatch(r"\d{4}", s):
        return s
    if re.fullmatch(r"\d{3}[A-Z]0?", s):
        return s[:4]
    return s[:4] if len(s) >= 4 else s


def ensure_tdnet_documents_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS tdnet_documents(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        コード TEXT NOT NULL,
        銘柄名 TEXT,
        タイトル TEXT NOT NULL,
        URL TEXT,
        提出時刻 TEXT,
        document_type TEXT,
        本文 TEXT,
        text_hash TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(コード, 提出時刻, タイトル)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tdnet_documents_code ON tdnet_documents(コード, 提出時刻 DESC);")
    conn.commit()


def ensure_forecast_achievement_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS forecast_achievement_history(
        コード TEXT NOT NULL,
        fiscal_key TEXT NOT NULL,
        initial_forecast_op REAL,
        initial_forecast_eps REAL,
        last_forecast_op REAL,
        last_forecast_eps REAL,
        actual_op REAL,
        actual_eps REAL,
        initial_achievement_op REAL,
        last_achievement_op REAL,
        upward_revisions INTEGER DEFAULT 0,
        downward_revisions INTEGER DEFAULT 0,
        initial_forecast_date TEXT,
        last_revision_date TEXT,
        actual_date TEXT,
        parse_confidence TEXT,
        evidence_json TEXT,
        source TEXT DEFAULT 'tdnet_backfill',
        updated_at TEXT,
        PRIMARY KEY(コード, fiscal_key)
    );
    """)
    # 旧版テーブルが既に存在する場合に不足列だけ追加
    cols = {r[1] for r in conn.execute("PRAGMA table_info(forecast_achievement_history)")}
    additions = [
        ("last_forecast_op","REAL"),("last_forecast_eps","REAL"),
        ("initial_achievement_op","REAL"),("last_achievement_op","REAL"),
        ("initial_forecast_date","TEXT"),("last_revision_date","TEXT"),("actual_date","TEXT"),
        ("parse_confidence","TEXT"),("evidence_json","TEXT"),("source","TEXT"),("updated_at","TEXT")
    ]
    for c,t in additions:
        if c not in cols:
            try: conn.execute(f'ALTER TABLE forecast_achievement_history ADD COLUMN "{c}" {t}')
            except sqlite3.OperationalError: pass
    conn.commit()




def ensure_forecast_history_schema(conn: sqlite3.Connection):
    """
    TDnet正式予想イベント履歴。

    v21 migration:
    旧・株探ファンダ_new.py が作った
      forecast_history(captured_at NOT NULL, ...)
    を検出した場合、
      forecast_observation_history
    へコピーしてから旧テーブルを退避し、
    TDnet正式スキーマの forecast_history を新規作成する。
    """

    # --- 株探観測履歴の受け皿 ---
    conn.execute("""
    CREATE TABLE IF NOT EXISTS forecast_observation_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        コード TEXT NOT NULL,
        fiscal_key TEXT,
        captured_at TEXT NOT NULL,
        forecast_revenue REAL,
        forecast_op REAL,
        forecast_net REAL,
        forecast_eps REAL,
        source TEXT DEFAULT 'kabutan',
        UNIQUE(コード, fiscal_key, captured_at, source)
    );
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_forecast_observation_code_date
    ON forecast_observation_history(コード, captured_at);
    """)

    # --- 旧forecast_historyが存在するか確認 ---
    exists = conn.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='forecast_history'
        LIMIT 1
    """).fetchone()

    if exists:
        info = conn.execute("PRAGMA table_info(forecast_history)").fetchall()
        colmap = {r[1]: r for r in info}

        # 旧株探版の決定的特徴:
        # captured_at があり、NOT NULL。
        legacy_kabutan = (
            "captured_at" in colmap
            and int(colmap["captured_at"][3] or 0) == 1
        )

        if legacy_kabutan:
            print("[forecast_history][MIGRATE] legacy Kabutan table detected")

            cols = set(colmap)
            # 旧株探データを観測履歴へ安全コピー
            select_parts = {
                "コード": '"コード"' if "コード" in cols else "NULL",
                "fiscal_key": "fiscal_key" if "fiscal_key" in cols else "''",
                "captured_at": "captured_at",
                "forecast_revenue": "forecast_revenue" if "forecast_revenue" in cols else "NULL",
                "forecast_op": "forecast_op" if "forecast_op" in cols else "NULL",
                "forecast_net": "forecast_net" if "forecast_net" in cols else "NULL",
                "forecast_eps": "forecast_eps" if "forecast_eps" in cols else "NULL",
                "source": "COALESCE(source,'kabutan')" if "source" in cols else "'kabutan'",
            }

            conn.execute(f"""
                INSERT OR IGNORE INTO forecast_observation_history(
                    コード,fiscal_key,captured_at,
                    forecast_revenue,forecast_op,forecast_net,forecast_eps,source
                )
                SELECT
                    {select_parts['コード']},
                    {select_parts['fiscal_key']},
                    {select_parts['captured_at']},
                    {select_parts['forecast_revenue']},
                    {select_parts['forecast_op']},
                    {select_parts['forecast_net']},
                    {select_parts['forecast_eps']},
                    {select_parts['source']}
                FROM forecast_history
                WHERE {select_parts['コード']} IS NOT NULL
                  AND {select_parts['captured_at']} IS NOT NULL
            """)

            # バックアップを1本だけ残す。
            backup_exists = conn.execute("""
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='forecast_history_kabutan_legacy'
                LIMIT 1
            """).fetchone()

            if not backup_exists:
                conn.execute("""
                    ALTER TABLE forecast_history
                    RENAME TO forecast_history_kabutan_legacy
                """)
                print("[forecast_history][MIGRATE] old table -> forecast_history_kabutan_legacy")
            else:
                # 既にバックアップがある場合は観測履歴へコピー済みなので現行legacyを削除。
                conn.execute("DROP TABLE forecast_history")
                print("[forecast_history][MIGRATE] duplicate legacy table removed after copy")

            conn.commit()

    # --- TDnet正式履歴 ---
    conn.execute("""
    CREATE TABLE IF NOT EXISTS forecast_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        コード TEXT NOT NULL,
        fiscal_key TEXT NOT NULL,
        forecast_date TEXT NOT NULL,
        forecast_type TEXT,
        forecast_op REAL,
        forecast_eps REAL,
        basis TEXT,
        タイトル TEXT,
        URL TEXT,
        parse_method TEXT,
        evidence_json TEXT,
        source TEXT DEFAULT 'tdnet_daily',
        updated_at TEXT
    );
    """)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(forecast_history)")}
    additions = [
        ("コード","TEXT"),
        ("fiscal_key","TEXT"),
        ("forecast_date","TEXT"),
        ("forecast_type","TEXT"),
        ("forecast_op","REAL"),
        ("forecast_eps","REAL"),
        ("basis","TEXT"),
        ("タイトル","TEXT"),
        ("URL","TEXT"),
        ("parse_method","TEXT"),
        ("evidence_json","TEXT"),
        ("source","TEXT"),
        ("updated_at","TEXT"),
    ]
    for c,t in additions:
        if c not in cols:
            try:
                conn.execute(f'ALTER TABLE forecast_history ADD COLUMN "{c}" {t}')
            except sqlite3.OperationalError:
                pass

    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_tdnet_forecast_history_code_fiscal_date
    ON forecast_history(コード, fiscal_key, forecast_date);
    """)
    conn.commit()


def _classify_shinden_doc(title: str) -> str:
    t = title or ""
    if "決算説明" in t or "説明資料" in t: return "決算説明資料"
    if "中期経営" in t or "中期計画" in t or "事業計画" in t: return "中期計画"
    if "受注" in t: return "受注"
    if "量産" in t or "稼働" in t: return "量産/稼働"
    if "価格改定" in t or "値上げ" in t: return "価格改定"
    if "新店" in t or "出店" in t: return "出店"
    if "子会社化" in t or "M&A" in t: return "M&A"
    if "業績予想" in t or "上方修正" in t or "下方修正" in t: return "業績予想"
    if "決算" in t or "短信" in t: return "決算短信"
    return "その他"


def _tdnet_fields(it: dict) -> tuple[str,str,str,str,str]:
    td = it.get("Tdnet", it) or {}
    code = _normalize_code(td.get("company_code") or td.get("code") or td.get("ticker"))
    name = str(td.get("company_name") or td.get("companyname") or code or "").strip()
    title = _unescape_text(td.get("title") or "")
    url = str(td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
    pub = str(td.get("pubdate") or td.get("publish_datetime") or td.get("date") or "").replace("T"," ").replace("+09:00","").replace("/","-").strip()
    return code,name,title,url,pub




def _download_and_store_tdnet_doc(conn: sqlite3.Connection, it: dict, forced_code: str|None=None) -> dict|None:
    code,name,title,url,pub = _tdnet_fields(it)
    if forced_code:
        code = _normalize_code(forced_code)
    if not title or not code:
        return None

    # v13: DB cache first
    cached = conn.execute("""
        SELECT 本文, URL, document_type
        FROM tdnet_documents
        WHERE コード=? AND 提出時刻=? AND タイトル=?
        LIMIT 1
    """, (code, pub, title)).fetchone()

    if cached and cached[0] and len(str(cached[0]).strip()) >= 50:
        text = str(cached[0])
        final_url = str(cached[1] or url or "")
        kind = str(cached[2] or "cache")
        print(f"[backfill][DOC CACHE HIT] {code} {pub[:10]} {len(text):,}文字 {title[:40]}")
        return {
            "code":code, "name":name, "title":title,
            "url":final_url, "pub":pub, "text":text, "kind":"cache"
        }

    text, resolved_url, kind = _download_disclosure_text(it, timeout=5)

    if not text or len(text.strip()) < 50:
        td = it.get("Tdnet", it) or {}
        raw_url = str(td.get("document_url") or "")
        print(f"[backfill][DOC EMPTY] {code} {pub} {title[:52]} raw={raw_url[:85]}")
        return None

    final_url = resolved_url or url
    h = hashlib.sha256(text.encode("utf-8","ignore")).hexdigest()

    conn.execute("""
        INSERT INTO tdnet_documents(コード,銘柄名,タイトル,URL,提出時刻,document_type,本文,text_hash)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
          銘柄名=excluded.銘柄名,URL=excluded.URL,document_type=excluded.document_type,
          本文=excluded.本文,text_hash=excluded.text_hash
    """, (code,name,title,final_url,pub,_classify_shinden_doc(title),text,h))

    print(f"[backfill][DOC OK] {code} {pub[:10]} {kind} {len(text):,}文字 {title[:40]}")
    return {
        "code":code, "name":name, "title":title,
        "url":final_url, "pub":pub, "text":text, "kind":kind
    }


def upsert_tdnet_document_texts(conn: sqlite3.Connection, items: list[dict], max_workers: int=4, forced_code: str|None=None) -> int:
    # sqlite connectionはスレッド越しに使わない。ダウンロードのみ並列化すると複雑になるため
    # バックフィルは安全性優先で逐次保存。
    ensure_tdnet_documents_schema(conn)
    n=0
    for it in items or []:
        try:
            if _download_and_store_tdnet_doc(conn,it,forced_code=forced_code): n+=1
        except Exception as e:
            print(f"[backfill][STORE ERROR] {forced_code or ''}: {e}")
    conn.commit()
    return n


def fetch_tdnet_by_code(code: str, limit: int=300) -> list[dict]:
    code = _normalize_code(code)
    url = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{code}.json?limit={int(limit)}"
    js = _http_get_json(url, retries=3, sleep_sec=0.8)
    items = js.get("items") if isinstance(js,dict) else (js if isinstance(js,list) else [])
    return items if isinstance(items,list) else []


# 全角→半角は辞書方式。2本文字列maketransの文字数不一致事故を完全回避。
_BACKFILL_TRANS = str.maketrans({
    "０":"0","１":"1","２":"2","３":"3","４":"4","５":"5","６":"6","７":"7","８":"8","９":"9",
    "．":".","，":",","％":"%","△":"-","▲":"-","－":"-","＋":"+","　":" "
})


def _norm_backfill_text(text: str) -> str:
    x=(text or "").translate(_BACKFILL_TRANS)
    x=x.replace("百 万 円","百万円").replace("１株","1株")
    return re.sub(r"[ \t]+"," ",x)


def _to_float_num(s: Any) -> float|None:
    if s is None: return None
    x=str(s).translate(_BACKFILL_TRANS).strip().replace(",","")
    x=x.replace("△","-").replace("▲","-")
    m=re.search(r"[-+]?\d+(?:\.\d+)?",x)
    if not m: return None
    try:return float(m.group())
    except:return None


def _parse_jp_fiscal_key(s: str) -> str|None:
    """'2026年3月期' / '令和8年3月期' -> '2026-03'"""
    if not s:
        return None
    x=str(s).translate(_BACKFILL_TRANS)

    m=re.search(r"(20\d{2})\s*年\s*(\d{1,2})\s*月期",x)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"

    m=re.search(r"令和\s*(元|\d{1,2})\s*年\s*(\d{1,2})\s*月期",x)
    if m:
        y = 1 if m.group(1) == "元" else int(m.group(1))
        return f"{2018 + y:04d}-{int(m.group(2)):02d}"

    m=re.search(r"平成\s*(元|\d{1,2})\s*年\s*(\d{1,2})\s*月期",x)
    if m:
        y = 1 if m.group(1) == "元" else int(m.group(1))
        return f"{1988 + y:04d}-{int(m.group(2)):02d}"

    return None


def _title_fiscal_key(title: str) -> str|None:
    return _parse_jp_fiscal_key(title)



def _is_full_year_earnings(title: str) -> bool:
    """
    v9: バックフィル対象の「本決算」は決算短信だけ。
    決算説明資料・補足説明資料・差異のお知らせ・訂正文書を混ぜない。
    """
    t = str(title or "")
    if "決算短信" not in t:
        return False

    bad = (
        "第1四半期","第１四半期",
        "第2四半期","第２四半期",
        "第3四半期","第３四半期",
        "中間期",
        "訂正",
    )
    return not any(x in t for x in bad)



_NUM_TOKEN_RE = re.compile(r"(?:△|▲|-|\+)?\s*\d[\d,]*(?:\.\d+)?")

def _metric_num(s: str) -> float|None:
    """TDnet表の△/▲付き数値をfloat化。"""
    if s is None:
        return None
    x = str(s).translate(_BACKFILL_TRANS).strip()
    x = x.replace("△", "-").replace("▲", "-").replace("−", "-")
    x = re.sub(r"\s+", "", x).replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", x)
    if not m:
        return None
    try:
        return float(m.group())
    except Exception:
        return None


def _numbers_after_label(block: str, label_pattern: str, max_chars: int=500) -> list[float]:
    """
    指定行ラベルの直後だけから数値列を取る。
    「営業利益」見出しの後ろを探す旧方式と違い、年度や「通期」行を起点にする。
    """
    x = _norm_backfill_text(block)
    m = re.search(label_pattern, x, re.I)
    if not m:
        return []

    tail = x[m.end():m.end()+max_chars]
    vals = []
    for tok in _NUM_TOKEN_RE.findall(tail):
        v = _metric_num(tok)
        if v is not None:
            vals.append(v)
    return vals


def _slice_between(text: str, start_patterns: tuple[str,...], end_patterns: tuple[str,...], max_chars: int=8000) -> str:
    x = _norm_backfill_text(text)
    starts = []
    for pat in start_patterns:
        m = re.search(pat, x, re.I|re.S)
        if m:
            starts.append((m.start(), m.end()))
    if not starts:
        return ""
    _, st = min(starts, key=lambda z: z[0])
    end = min(len(x), st + max_chars)
    for pat in end_patterns:
        m = re.search(pat, x[st:end], re.I|re.S)
        if m:
            end = min(end, st + m.start())
    return x[st:end]


def _fiscal_regex_from_key(fiscal_key: str) -> str:
    """2026-03 -> 2026年3月期（全角数字は正規化済み前提）"""
    try:
        y, m = fiscal_key.split("-", 1)
        return rf"{int(y)}\s*年\s*0?{int(m)}\s*月期"
    except Exception:
        return ""



def _split_nonempty_lines(text: str) -> list[str]:
    x = _norm_backfill_text(text)
    return [ln.strip() for ln in x.splitlines() if ln.strip()]


def _numbers_in_line(line: str) -> list[float]:
    vals = []
    for tok in _NUM_TOKEN_RE.findall(str(line)):
        v = _metric_num(tok)
        if v is not None:
            vals.append(v)
    return vals


def _find_line_idx(lines: list[str], pattern: str, start: int=0, end: int|None=None) -> int|None:
    end = len(lines) if end is None else min(end, len(lines))
    rx = re.compile(pattern, re.I)
    for i in range(max(0,start), end):
        if rx.search(lines[i]):
            return i
    return None


def _fiscal_line_candidates(lines: list[str], fiscal_key: str, start: int=0, end: int|None=None) -> list[int]:
    pat = _fiscal_regex_from_key(fiscal_key)
    end = len(lines) if end is None else min(end, len(lines))
    rx = re.compile(pat, re.I)
    out = []
    for i in range(max(0,start), end):
        if rx.search(lines[i]):
            out.append(i)
    return out


def _collect_row_numbers(lines: list[str], row_idx: int, max_follow_lines: int=8) -> list[float]:
    """
    PDF抽出で1表の1行が複数テキスト行に分割されることがあるため、
    年度行/通期行から次の年度行・主要見出しまで数値を集める。
    """
    vals = []
    stop_rx = re.compile(
        r"(20\d{2}\s*年\s*\d{1,2}\s*月期|令和\s*\d+\s*年\s*\d+\s*月期|"
        r"連結財政状態|財政状態|キャッシュ・フロー|配当の状況|業績予想|"
        r"今回修正予想|前回発表予想|今回発表予想)",
        re.I
    )

    # 起点行
    vals.extend(_numbers_in_line(lines[row_idx]))

    for j in range(row_idx+1, min(len(lines), row_idx+1+max_follow_lines)):
        ln = lines[j]
        if stop_rx.search(ln):
            break
        vals.extend(_numbers_in_line(ln))
    return vals


def _drop_leading_fiscal_numbers(vals: list[float], fiscal_key: str|None) -> list[float]:
    """
    年度行から拾った 2026, 3 等の年度・月を先頭から落とす。
    """
    out = list(vals)
    if not fiscal_key:
        return out
    try:
        fy, fm = map(int, fiscal_key.split("-"))
    except Exception:
        return out

    # 2026,3 / 2026.0,3.0 などを除外
    if out and abs(out[0] - fy) < 0.001:
        out.pop(0)
    if out and abs(out[0] - fm) < 0.001:
        out.pop(0)
    return out


def _choose_operating_profit_from_row(vals: list[float]) -> float|None:
    """
    標準の決算短信表:
      売上高 / 増減率 / 営業利益 / 増減率 / 経常利益 / ...
    ただしPDF抽出では増減率の列が落ちる場合がある。
    そこで候補パターンを保守的に判定。
    """
    if not vals:
        return None

    # 明らかな日付・巨大連結値を排除
    vals = [v for v in vals if not (2000 <= abs(v) <= 2099 and float(v).is_integer())]

    # 典型9列以上
    if len(vals) >= 8:
        return vals[2]

    # 増減率列が落ちた 5列程度: 売上,営業,経常,純益,EPS
    if 5 <= len(vals) <= 7:
        return vals[1]

    return None


def _extract_eps_near_fiscal(text: str, fiscal_key: str) -> float|None:
    lines = _split_nonempty_lines(text)
    # EPS見出しを探し、その後の年度行だけを見る
    eps_idx = _find_line_idx(lines, r"1\s*株当たり(?:当期)?(?:純)?利益|基本的1\s*株当たり")
    if eps_idx is None:
        return None
    cands = _fiscal_line_candidates(lines, fiscal_key, eps_idx, min(len(lines), eps_idx+80))
    for idx in cands:
        vals = _drop_leading_fiscal_numbers(_collect_row_numbers(lines, idx, 4), fiscal_key)
        # EPS表は年度行の最初の実数がEPSであることが多い
        for v in vals:
            if -100000 < v < 100000:
                return v
    return None



def _extract_actual_table_row(text: str, fiscal_key: str) -> tuple[float|None,float|None]:
    """
    v8: 決算短信1ページ目の「連結経営成績」内で、
    対象年度行を見つけ、その行と直後の分割行だけを読む。
    別表・決算説明資料の数値を混ぜない。
    """
    if not fiscal_key:
        return None, None

    lines = _split_nonempty_lines(text)

    sec = _find_line_idx(lines, r"連結経営成績")
    if sec is None:
        sec = _find_line_idx(lines, r"経営成績")
    if sec is None:
        return None, None

    end = _find_line_idx(lines, r"連結財政状態|財政状態", sec+1, min(len(lines), sec+120))
    if end is None:
        end = min(len(lines), sec+120)

    for idx in _fiscal_line_candidates(lines, fiscal_key, sec, end):
        vals = _collect_row_numbers(lines, idx, max_follow_lines=6)
        vals = _drop_leading_fiscal_numbers(vals, fiscal_key)
        op = _choose_operating_profit_from_row(vals)

        # OP sanity: 売上より極端に大きい/小数比率らしい値を排除
        if op is not None:
            if 2000 <= abs(op) <= 2099 and float(op).is_integer():
                op = None

        eps = _extract_eps_near_fiscal(text, fiscal_key)
        if op is not None or eps is not None:
            return op, eps

    return None, None


def _forecast_section(text: str) -> tuple[str|None,str]:
    """
    「3. 2027年3月期の連結業績予想」等の見出しを特定。
    配当表の「2027年3月期（予想）」を誤認しない。
    """
    x = _norm_backfill_text(text)
    patterns = (
        r"(\d{4})\s*年\s*(\d{1,2})\s*月期\s*の\s*連結業績予想",
        r"(\d{4})\s*年\s*(\d{1,2})\s*月期\s*の\s*業績予想",
        r"連結業績予想\s*[（(]\s*(\d{4})\s*年",
    )
    for pat in patterns:
        m = re.search(pat, x, re.I)
        if not m:
            continue
        if len(m.groups()) >= 2 and m.group(2):
            fk = f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"
        else:
            # fallback: heading周辺から通常パーサ
            fk = _parse_jp_fiscal_key(x[m.start():m.start()+300])
        return fk, x[m.start():m.start()+7000]
    return None, ""



def _extract_forecast_table_row(text: str) -> tuple[str|None,float|None,float|None]:
    """
    v8: 「○年○月期の連結業績予想」セクション内の通期行だけ読む。
    """
    fk, block = _forecast_section(text)
    if not fk or not block:
        return None, None, None

    lines = _split_nonempty_lines(block)
    tidx = _find_line_idx(lines, r"^通期$|通期")
    if tidx is None:
        return fk, None, None

    vals = _collect_row_numbers(lines, tidx, max_follow_lines=6)
    # 通期自体には年度数字がない想定
    op = _choose_operating_profit_from_row(vals)

    # EPSは標準9列型なら末尾近辺
    eps = None
    if len(vals) >= 9:
        eps = vals[8]
    elif len(vals) >= 5:
        # 増減率が落ちた5列型
        eps = vals[-1]

    if op is not None and 2000 <= abs(op) <= 2099 and float(op).is_integer():
        op = None

    return fk, op, eps






def _normalize_yen_sen_for_stream(text: str) -> str:
    """
    '450 円 95 銭' -> '450.95'
    '1,061円05銭' -> '1061.05'
    にして、EPSを数値1個として扱う。
    """
    s = str(text or "")

    def repl(m):
        yen_s = m.group(1).replace(",", "")
        sen_s = m.group(2) or "0"
        try:
            yen = float(yen_s)
            sen = int(sen_s)
            sign = -1 if yen < 0 else 1
            v = yen + sign * (sen / 100.0)
            return f" {v:.2f} "
        except Exception:
            return m.group(0)

    return re.sub(
        r"([-+]?\d[\d,]*(?:\.\d+)?)\s*円"
        r"(?:\s*([-+]?\d{1,2})\s*銭)?",
        repl,
        s,
        flags=re.I,
    )


def _revision_numeric_tokens(text: str) -> list[dict]:
    """
    PDF抽出順の数値ストリームを位置情報付きで返す。
    日付等も含むが、後段で known previous OP をアンカーに除外する。
    """
    s = _normalize_yen_sen_for_stream(_norm_backfill_text(text))
    out = []

    for m in re.finditer(r"[-+]?\d[\d,]*(?:\.\d+)?", s):
        raw = m.group(0)
        v = _to_float_num(raw)
        if v is None:
            continue
        out.append({
            "value": float(v),
            "start": m.start(),
            "end": m.end(),
            "raw": raw,
        })
    return out


def _almost_equal_amount(a: float|None, b: float|None) -> bool:
    if a is None or b is None:
        return False
    aa, bb = float(a), float(b)
    tol = max(0.51, abs(bb) * 0.0005)
    return abs(aa - bb) <= tol



def _resolve_revision_by_previous_op(
    text: str,
    known_previous_op: float|None,
    fiscal_key: str|None=None,
) -> dict:
    """
    v15:
    PDF抽出が「行方向」ではなく「列方向」に並ぶケースへ対応。

    実際のTDnet業績予想修正PDFでは、抽出後にしばしば

        前回OP, 今回OP, 増減額

    の3値が連続する。

    例:
        1900, 1200, -700
        1700, 1250, -450
        1600, 1150, -450

    ここで
        今回OP - 前回OP == 増減額
    が成立する候補だけを採用する。

    このfallbackではEPSは推測しない。
    """
    result = {
        "fk": fiscal_key,
        "previous_op": known_previous_op,
        "new_op": None,
        "new_eps": None,
        "direction": "unknown",
        "score": None,
        "anchor_index": None,
        "window": [],
        "method": "numeric_delta_unresolved",
        "delta": None,
    }

    if not _is_plausible_op(known_previous_op):
        return result

    toks = _revision_numeric_tokens(text)
    if not toks:
        return result

    prev = float(known_previous_op)
    candidates = []

    for i, t in enumerate(toks):
        if not _almost_equal_amount(t["value"], prev):
            continue

        # 最有力は直後2個:
        # prev_op, new_op, delta
        # PDF抽出差に備えて new/delta を少しだけ先まで探索。
        for new_offset in range(1, 5):
            j = i + new_offset
            if j >= len(toks):
                continue

            new_op = float(toks[j]["value"])
            if not _is_plausible_op(new_op):
                continue

            expected_delta = new_op - prev

            for delta_offset in range(1, 4):
                k = j + delta_offset
                if k >= len(toks):
                    continue

                delta = float(toks[k]["value"])

                # 増減額一致判定。
                tol = max(1.0, abs(expected_delta) * 0.002)
                err = abs(delta - expected_delta)

                if err > tol:
                    continue

                score = 100.0

                # もっとも自然な連続3値を強く優先
                if new_offset == 1:
                    score += 30
                else:
                    score -= (new_offset - 1) * 8

                if delta_offset == 1:
                    score += 30
                else:
                    score -= (delta_offset - 1) * 8

                # 完全一致に近いほど加点
                if err <= 0.01:
                    score += 20
                elif err <= 0.5:
                    score += 10

                # 年度数字らしいOPを弱く抑制
                if 2000 <= abs(new_op) <= 2099 and float(new_op).is_integer():
                    score -= 15

                candidates.append({
                    "score": score,
                    "anchor": i,
                    "new_idx": j,
                    "delta_idx": k,
                    "new_op": new_op,
                    "delta": delta,
                    "expected_delta": expected_delta,
                    "error": err,
                })

    if not candidates:
        return result

    candidates.sort(
        key=lambda d: (
            d["score"],
            -d["error"],
            -d["new_idx"],
        ),
        reverse=True,
    )
    best = candidates[0]

    # 恒等式一致済みなので閾値はかなり高く置く
    if best["score"] < 120:
        result["score"] = best["score"]
        return result

    new_op = best["new_op"]

    direction = (
        "up" if new_op > prev
        else "down" if new_op < prev
        else "flat"
    )

    lo = max(0, best["anchor"] - 6)
    hi = min(len(toks), best["delta_idx"] + 8)

    result.update({
        "new_op": new_op,
        "new_eps": None,
        "direction": direction,
        "score": best["score"],
        "anchor_index": best["anchor"],
        "window": [x["value"] for x in toks[lo:hi]],
        "method": "numeric_delta_identity",
        "delta": best["delta"],
    })
    return result


def _extract_revision_pdf_detailed(text: str, title: str="") -> dict:
    """
    v13: 業績予想修正PDF専用。
    PDF抽出で「今 回 修 正 予 想」のように文字間へ空白が入っても認識する。
    """
    x = _norm_backfill_text(text)
    compact = re.sub(r"[ \t]+", " ", x).replace("\r", "\n")

    def spaced_literal(s: str) -> str:
        # 日本語ラベルの各文字間に任意空白/改行を許す
        return r"\s*".join(re.escape(ch) for ch in s)

    # fiscal key
    fk = None
    fiscal_patterns = (
        r"(20\d{2})\s*年\s*(\d{1,2})\s*月期[\s\S]{0,180}?"
        + spaced_literal("業績予想") + r"[\s\S]{0,40}?" + spaced_literal("修正"),
        r"(20\d{2})\s*年\s*(\d{1,2})\s*月期",
    )
    for pat in fiscal_patterns:
        m = re.search(pat, compact, re.I)
        if m:
            fk = f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"
            break
    if not fk:
        fk = _title_fiscal_key(title) or _parse_jp_fiscal_key(title)

    prev_label = (
        spaced_literal("前回発表予想")
        + r"\s*[（(]?\s*[AＡ]\s*[）)]?"
    )
    prev_label2 = (
        spaced_literal("前回予想")
        + r"\s*[（(]?\s*[AＡ]?\s*[）)]?"
    )
    new_label = (
        spaced_literal("今回修正予想")
        + r"\s*[（(]?\s*[BＢ]\s*[）)]?"
    )
    new_label2 = (
        spaced_literal("今回発表予想")
        + r"\s*[（(]?\s*[BＢ]?\s*[）)]?"
    )
    new_label3 = (
        spaced_literal("今回予想")
        + r"\s*[（(]?\s*[BＢ]?\s*[）)]?"
    )

    end_change = spaced_literal("増減額")
    end_rate = spaced_literal("増減率")
    end_ref = spaced_literal("ご参考")
    end_prev_actual = spaced_literal("前期実績")
    end_reason = spaced_literal("修正の理由")

    def row_span(start_patterns, end_patterns, limit=1200):
        matches = []
        for p in start_patterns:
            m = re.search(p, compact, re.I | re.S)
            if m:
                matches.append(m)
        if not matches:
            return ""

        start = min(matches, key=lambda m:m.start())
        st = start.end()
        en = min(len(compact), st + limit)

        for p in end_patterns:
            m = re.search(p, compact[st:en], re.I | re.S)
            if m:
                en = min(en, st + m.start())

        return compact[st:en].strip()

    prev_span = row_span(
        (prev_label, prev_label2),
        (new_label, new_label2, new_label3),
    )
    new_span = row_span(
        (new_label, new_label2, new_label3),
        (end_change, end_rate, end_ref, end_prev_actual, end_reason),
    )

    def parse_row(span: str) -> dict:
        out = {
            "sales":None, "op":None, "ordinary":None, "net":None,
            "eps":None, "numbers":[], "span":span,
        }
        if not span:
            return out

        # EPS: "1,061 円 05 銭" / "450円95銭"
        eps = None
        em = re.search(
            r"([-+]?\d[\d,]*(?:\.\d+)?)\s*円"
            r"(?:\s*([-+]?\d{1,2})\s*銭)?",
            span
        )
        if em:
            yen = _to_float_num(em.group(1))
            sen = _to_float_num(em.group(2)) if em.group(2) else 0.0
            if yen is not None:
                sign = -1.0 if yen < 0 else 1.0
                eps = yen + sign * ((sen or 0.0) / 100.0)

        cleaned = re.sub(
            r"[-+]?\d[\d,]*(?:\.\d+)?\s*円"
            r"(?:\s*[-+]?\d{1,2}\s*銭)?",
            " ",
            span
        )
        cleaned = re.sub(spaced_literal("百万円"), " ", cleaned)

        nums = []
        for tok in re.findall(r"[-+]?\d[\d,]*(?:\.\d+)?", cleaned):
            v = _to_float_num(tok)
            if v is not None:
                nums.append(v)

        # 先頭に日付が混入した場合だけ除去
        if len(nums) >= 7 and 2000 <= nums[0] <= 2099:
            y = nums.pop(0)
            if nums and 1 <= nums[0] <= 12:
                nums.pop(0)
            if nums and 1 <= nums[0] <= 31:
                nums.pop(0)

        out["numbers"] = nums
        out["eps"] = eps

        # 売上 / 営業 / 経常 / 純益
        if len(nums) >= 4:
            out["sales"] = nums[0]
            out["op"] = nums[1]
            out["ordinary"] = nums[2]
            out["net"] = nums[3]

        return out

    prev = parse_row(prev_span)
    new = parse_row(new_span)

    direction = "unknown"
    if _is_plausible_op(prev.get("op")) and _is_plausible_op(new.get("op")):
        direction = (
            "up" if new["op"] > prev["op"]
            else "down" if new["op"] < prev["op"]
            else "flat"
        )

    return {
        "fk":fk,
        "previous_op":prev.get("op"),
        "previous_eps":prev.get("eps"),
        "new_op":new.get("op"),
        "new_eps":new.get("eps"),
        "direction":direction,
        "previous_span":prev_span,
        "new_span":new_span,
        "previous_numbers":prev.get("numbers"),
        "new_numbers":new.get("numbers"),
    }


def _extract_revision_table_row(text: str, title: str="") -> tuple[str|None,float|None,float|None]:
    """
    v8: 修正資料は「今回修正予想(B)」等の行だけを読む。
    """
    x = _norm_backfill_text(text)
    lines = _split_nonempty_lines(x)

    fk = _title_fiscal_key(title)
    if not fk:
        for ln in lines[:80]:
            fk2 = _parse_jp_fiscal_key(ln)
            if fk2:
                fk = fk2
                break

    row_idx = None
    for pat in (
        r"今回修正予想",
        r"今回発表予想",
        r"今回予想",
    ):
        row_idx = _find_line_idx(lines, pat)
        if row_idx is not None:
            break

    if row_idx is None:
        return fk, None, None

    vals = _collect_row_numbers(lines, row_idx, max_follow_lines=5)
    op = _choose_operating_profit_from_row(vals)

    eps = None
    if len(vals) >= 9:
        eps = vals[8]
    elif len(vals) >= 5:
        eps = vals[-1]

    if op is not None and 2000 <= abs(op) <= 2099 and float(op).is_integer():
        op = None

    return fk, op, eps


def _find_metric_in_window(text: str, labels: tuple[str,...], start_pat: str|None=None, window: int=2500) -> float|None:
    x=_norm_backfill_text(text)
    if start_pat:
        m=re.search(start_pat,x,re.I)
        if m:x=x[m.start():m.start()+window]
    # 改行・表崩れ対策。ラベルの後ろ最初の数値を拾う。
    for lab in labels:
        p=re.search(re.escape(lab)+r"[^\d\-+]{0,180}([-+]?\d[\d,]*(?:\.\d+)?)",x,re.I)
        if p:
            return _to_float_num(p.group(1))
    return None



def _extract_actual_from_annual(text: str, fiscal_key: str|None=None) -> tuple[float|None,float|None]:
    """
    v7: 年度行ベースの表パース。
    fiscal_keyが無い場合だけ旧式fallbackを使う。
    """
    if fiscal_key:
        op, eps = _extract_actual_table_row(text, fiscal_key)
        if op is not None or eps is not None:
            return op, eps

    # fallback。ただし年度数字（2000〜2099）を営業利益として採用しない
    x = _norm_backfill_text(text)
    head = x[:7000]
    op = _find_metric_in_window(head, ("営業利益","営業損失"), r"連結業績|経営成績", 3500)
    eps = _find_metric_in_window(
        head,
        ("1株当たり当期純利益","1株当たり当期利益","基本的1株当たり当期利益","EPS"),
        r"連結業績|経営成績",
        6000,
    )
    if op is not None and 2000 <= op <= 2099:
        op = None
    return op, eps



def _extract_next_forecast_from_annual(text: str) -> tuple[str|None,float|None,float|None]:
    """v7: 「○年○月期の連結業績予想」→「通期」行を表として読む。"""
    fk, op, eps = _extract_forecast_table_row(text)
    if fk:
        if op is not None and 2000 <= op <= 2099:
            op = None
        return fk, op, eps
    return None, None, None




def _extract_revision_values(text: str, title: str="") -> tuple[str|None,float|None,float|None]:
    """v12: 業績予想修正PDF専用パーサー。"""
    d = _extract_revision_pdf_detailed(text, title)
    op = d.get("new_op")
    eps = d.get("new_eps")

    if not _is_plausible_op(op):
        op = None

    if eps is not None and not (-100000 <= eps <= 100000):
        eps = None

    return d.get("fk"), op, eps



# ============================================================================
# v9: TDnet XBRL first parser
# ============================================================================

def ensure_tdnet_xbrl_metrics_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS tdnet_xbrl_metrics(
        コード TEXT NOT NULL,
        提出時刻 TEXT NOT NULL,
        タイトル TEXT NOT NULL,
        xbrl_url TEXT,
        actual_fiscal_key TEXT,
        actual_op REAL,
        actual_eps REAL,
        forecast_fiscal_key TEXT,
        forecast_op REAL,
        forecast_eps REAL,
        parse_method TEXT,
        evidence_json TEXT,
        updated_at TEXT,
        PRIMARY KEY(コード, 提出時刻, タイトル)
    );
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_tdnet_xbrl_metrics_code
    ON tdnet_xbrl_metrics(コード, 提出時刻 DESC);
    """)
    conn.commit()


def _xbrl_localname(tag: str) -> str:
    s = str(tag or "")
    if "}" in s:
        s = s.rsplit("}", 1)[-1]
    if ":" in s:
        s = s.rsplit(":", 1)[-1]
    return s


def _xbrl_parse_iso_date(s: str|None) -> date|None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _fiscal_key_end_date(fiscal_key: str|None) -> date|None:
    if not fiscal_key:
        return None
    try:
        y, m = [int(x) for x in fiscal_key.split("-", 1)]
        last = calendar.monthrange(y, m)[1]
        return date(y, m, last)
    except Exception:
        return None


def _next_fiscal_key(fiscal_key: str|None) -> str|None:
    if not fiscal_key:
        return None
    try:
        y, m = [int(x) for x in fiscal_key.split("-", 1)]
        return f"{y+1:04d}-{m:02d}"
    except Exception:
        return None




def _is_full_year_revision_title(title: str) -> bool:
    """
    通期予想履歴に使ってよい業績予想修正だけを判定。

    除外:
      第2四半期だけ / 中間だけ / 第1・第3四半期だけ
    採用:
      「第2四半期累計期間及び通期...」のように通期も含むもの
      四半期表記がなく通常の「業績予想の修正」のもの
    """
    t = str(title or "")

    if "修正" not in t:
        return False
    if "差異" in t:
        return False
    if not ("業績予想" in t or "上方修正" in t or "下方修正" in t):
        return False

    quarter_only_markers = (
        "第1四半期","第１四半期",
        "第2四半期","第２四半期",
        "第3四半期","第３四半期",
        "中間期","中間業績","中間連結",
    )
    has_quarter_marker = any(x in t for x in quarter_only_markers)

    # 四半期/中間を含むタイトルでも「通期」も明記されていれば採用
    if has_quarter_marker and "通期" not in t:
        return False

    return True



def _xbrl_basis_from_evidence(ev: dict|None) -> str:
    """
    v18:
    consolidated / nonconsolidated の判定では
    dimension軸名（例: ConsolidatedOrNonConsolidatedAxis）を見ない。

    判定対象:
      1. context id
      2. explicitMember の値

    dimension軸名には "NonConsolidated" という語が含まれるため、
    そこを見ると ConsolidatedMember まで非連結と誤判定する。
    """
    if not ev:
        return "unknown"

    ctx = str(ev.get("context") or "").lower()
    meta = ev.get("context_meta") or {}

    # explicitMember 値だけ。dimensions は絶対に混ぜない。
    members = " ".join(meta.get("members",[]) or []).lower()
    blob = f"{ctx} {members}"

    # より具体的なMember名だけで判定
    if "nonconsolidatedmember" in blob:
        return "nonconsolidated"
    if "consolidatedmember" in blob:
        return "consolidated"

    # 古い/特殊contextのための限定fallback
    if "nonconsolidated" in ctx:
        return "nonconsolidated"
    if "consolidated" in ctx:
        return "consolidated"

    return "unknown"



def _basis_from_title(title: str) -> str:
    t = str(title or "")
    if "非連結" in t or "個別" in t:
        return "nonconsolidated"
    if "連結" in t:
        return "consolidated"
    return "unknown"


def _effective_basis(rec: dict, which: str) -> str:
    """
    XBRL contextを最優先し、unknownならタイトルから補完。
    which: actual / forecast
    """
    key = "actual_basis" if which == "actual" else "forecast_basis"
    b = rec.get(key) or "unknown"
    if b != "unknown":
        return b
    return _basis_from_title(rec.get("title") or "")


def _revision_indicates_basis_transition(r: dict) -> bool:
    """
    連結決算への移行・連結業績予想の新規公表など、
    年度途中で比較基準が変わる開示を検知。
    """
    t = str(r.get("title") or "")
    markers = (
        "連結決算への移行",
        "連結業績予想の公表",
        "連結業績予想を公表",
        "連結業績予想の策定",
    )
    return any(m in t for m in markers)


def _delete_forecast_history_row(conn: sqlite3.Connection, code: str, fiscal_key: str):
    conn.execute(
        "DELETE FROM forecast_achievement_history WHERE コード=? AND fiscal_key=?",
        (code, fiscal_key)
    )
    conn.commit()


def _candidate_xbrl_urls(it: dict, code: str) -> list[str]:
    """
    v11:
    1. APIが返すxbrl/.zip URL
    2. PDF文書IDからTDnet XBRL ZIPを推定
       - 決算短信系: 0812 + 同一document-id
       - 業績予想修正系: 0912 + 同一document-id
    の両方を試す。

    例:
      140120251107591517.pdf
        -> 091220251107591517.zip
    """
    td = it.get("Tdnet", it) or {}
    urls = []
    seen = set()

    def add(u: str):
        u = str(u or "").strip()
        if not u:
            return
        for cand in (u, _unwrap_yanoshin_rd_url(u)):
            if cand and cand not in seen:
                seen.add(cand)
                urls.append(cand)
            if cand:
                jpx = _jpx_permanent_url(code, cand)
                if jpx and jpx not in seen:
                    seen.add(jpx)
                    urls.append(jpx)

    # 直接XBRL/ZIP
    for k, v in td.items():
        if not isinstance(v, str):
            continue
        u = v.strip()
        kl = str(k).lower()
        ul = u.lower()
        if not u:
            continue
        if ("xbrl" in kl) or ul.endswith(".zip") or ".zip?" in ul:
            add(u)

    # PDF URLからdocument-idを抽出してZIP候補を作る
    pdf_urls = []
    for k, v in td.items():
        if not isinstance(v, str):
            continue
        u = v.strip()
        if ".pdf" in u.lower():
            pdf_urls.append(u)

    for u in pdf_urls:
        inner = _unwrap_yanoshin_rd_url(u) or u
        m = re.search(r"/(1401)(\d{14,})\.pdf(?:\?.*)?$", inner, re.I)
        if not m:
            continue

        tail = m.group(2)
        for prefix in ("0812", "0912"):
            derived = f"https://www.release.tdnet.info/inbs/{prefix}{tail}.zip"
            add(derived)

    return urls



def _download_xbrl_zip(it: dict, code: str, timeout: int=5) -> tuple[bytes|None,str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
        "Referer": "https://www.release.tdnet.info/",
    }

    urls = _candidate_xbrl_urls(it, code)
    if not urls:
        return None, ""

    # v13: old disclosure => www2.jpx.co.jp first
    try:
        _c,_n,_t,_u,pub = _tdnet_fields(it)
        dt = _parse_ts_str(pub)
        if dt:
            now = datetime.now(JST)
            if getattr(dt, "tzinfo", None) is None:
                dt = JST.localize(dt) if hasattr(JST, "localize") else dt.replace(tzinfo=JST)
            old = (now-dt).days >= 32
        else:
            old = False
    except Exception:
        old = False

    if old:
        urls = sorted(urls, key=lambda u: 0 if "www2.jpx.co.jp/disc/" in u else 1)
    else:
        urls = sorted(urls, key=lambda u: 0 if "release.tdnet.info" in u or "yanoshin" in u else 1)

    for u in urls[:10]:
        try:
            print(f"[backfill][XBRL TRY] {code} {u[:110]}")
            r = requests.get(u, timeout=timeout, headers=headers, allow_redirects=True)
            if r.status_code >= 400:
                continue
            b = r.content or b""
            if len(b) >= 4 and b[:2] == b"PK":
                print(f"[backfill][XBRL ZIP OK] {code} {len(b):,} bytes")
                return b, (r.url or u)
        except Exception:
            continue

    return None, ""


def _xbrl_decode_numeric(elem) -> float|None:
    raw = "".join(elem.itertext()).strip()
    if not raw:
        return None

    raw = raw.replace(",", "").replace("△", "-").replace("▲", "-")
    raw = raw.replace("−", "-").replace("－", "-")
    raw = re.sub(r"\s+", "", raw)

    # nil
    for k, v in elem.attrib.items():
        if _xbrl_localname(k).lower() == "nil" and str(v).lower() in ("true","1"):
            return None

    try:
        val = float(raw)
    except Exception:
        return None

    # inline XBRL scale/sign
    scale = elem.attrib.get("scale")
    if scale is not None:
        try:
            val *= 10 ** int(scale)
        except Exception:
            pass
    sign = elem.attrib.get("sign")
    if sign == "-" and val > 0:
        val = -val

    return val



def _xbrl_contexts_and_facts(zip_bytes: bytes) -> tuple[dict,list[dict]]:
    """
    v10:
    - 通常XBRL: <tse-ed-t:OperatingIncome ...>
    - iXBRL:    <ix:nonFraction name="tse-ed-t:OperatingIncome" ...>
    の両方に対応。

    iXBRLでは要素タグそのものは nonFraction なので、
    name属性のQNameを真のconcept名として使う。
    """
    contexts: dict[str,dict] = {}
    facts: list[dict] = []

    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception:
        return contexts, facts

    names = [
        n for n in zf.namelist()
        if not n.endswith("/")
        and n.lower().endswith((".xbrl",".xml",".htm",".html",".xhtml"))
        and not n.lower().endswith(("_lab.xml","_pre.xml","_def.xml","_cal.xml","_ref.xml"))
    ]

    for name in names[:100]:
        try:
            raw = zf.read(name)
            if len(raw) > 15_000_000:
                continue
            root = ET.fromstring(raw)
        except Exception:
            continue

        local_contexts = {}

        # contexts
        for e in root.iter():
            if _xbrl_localname(e.tag).lower() != "context":
                continue
            cid = e.attrib.get("id")
            if not cid:
                continue

            meta = {
                "id": cid,
                "start": None,
                "end": None,
                "instant": None,
                "members": [],
                "dimensions": [],
                "file": name,
            }

            for c in e.iter():
                ln = _xbrl_localname(c.tag)
                tx = (c.text or "").strip()

                if ln == "startDate":
                    meta["start"] = tx[:10]
                elif ln == "endDate":
                    meta["end"] = tx[:10]
                elif ln == "instant":
                    meta["instant"] = tx[:10]
                elif ln == "explicitMember":
                    if tx:
                        meta["members"].append(tx)
                    dim = c.attrib.get("dimension")
                    if dim:
                        meta["dimensions"].append(dim)
                elif ln == "typedMember":
                    dim = c.attrib.get("dimension")
                    if dim:
                        meta["dimensions"].append(dim)

            local_contexts[cid] = meta

            # 同名contextが複数ファイルに存在しても、期間付きcontextを優先
            old = contexts.get(cid)
            if old is None:
                contexts[cid] = meta
            else:
                old_has_period = bool(old.get("end") or old.get("instant"))
                new_has_period = bool(meta.get("end") or meta.get("instant"))
                if new_has_period and not old_has_period:
                    contexts[cid] = meta

        # numeric facts
        for e in root.iter():
            cref = e.attrib.get("contextRef")
            if not cref:
                continue

            val = _xbrl_decode_numeric(e)
            if val is None:
                continue

            tag_local = _xbrl_localname(e.tag)
            tag_lower = tag_local.lower()

            # iXBRL: ix:nonFraction / ix:fraction の name 属性がconcept QName
            concept_qname = ""
            if tag_lower in ("nonfraction", "fraction"):
                concept_qname = str(e.attrib.get("name") or "").strip()

            if concept_qname:
                concept_local = concept_qname.rsplit(":", 1)[-1]
            else:
                concept_qname = tag_local
                concept_local = tag_local

            if not concept_local:
                continue

            # 同じファイルのcontextを優先
            ctx = local_contexts.get(cref) or contexts.get(cref) or {}

            facts.append({
                "name": concept_local,
                "qname": concept_qname,
                "name_l": concept_local.lower(),
                "context": cref,
                "unit": e.attrib.get("unitRef",""),
                "value": val,
                "file": name,
                "context_meta": ctx,
                "is_inline": tag_lower in ("nonfraction","fraction"),
                "decimals": e.attrib.get("decimals"),
                "scale": e.attrib.get("scale"),
            })

    return contexts, facts



def _xbrl_is_op_name(name_l: str) -> bool:
    """
    v10: TDnetサマリーで営業利益として使う概念。
    ChangeInOperatingIncome 等の増減率は絶対除外。
    """
    n = str(name_l or "").lower()

    exact = {
        "operatingincome",
        "operatingincomeifrs",
        "operatingincomeus",
        "operatingprofitlossifrs",
    }
    if n in exact:
        return True

    # 会社拡張でも末尾が明確な場合だけ許容
    if (
        ("operatingincome" in n or "operatingprofitloss" in n)
        and not any(k in n for k in (
            "change", "ratio", "margin", "percentage", "rate",
            "abstract", "correction", "amountchange"
        ))
    ):
        return True

    return False



def _xbrl_is_eps_name(name_l: str) -> bool:
    """
    v10: 基本EPSのみ。希薄化後EPSや配当/純資産1株値は除外。
    """
    n = str(name_l or "").lower()

    exact = {
        "netincome pershare".replace(" ",""),
        "basicearningspershare",
        "basicearningspershareifrs",
        "basicearningspershareus",
        "earningspershare",
    }
    if n in exact:
        return True

    if (
        ("netincome" in n and "pershare" in n)
        or ("earnings" in n and "pershare" in n)
    ):
        if any(k in n for k in ("diluted","dividend","netassets","bookvalue")):
            return False
        return True

    return False


def _xbrl_normalize_op(value: float, unit: str) -> float:
    """
    forecast_achievement_history の営業利益は百万円単位で統一。
    XBRLが円単位なら絶対値が通常1,000万超になるため百万円へ変換。
    """
    v = float(value)
    ul = str(unit or "").lower()

    # unitRefにJPYが明示されるか、値の桁が円単位らしい場合
    if "jpy" in ul or abs(v) >= 10_000_000:
        # 既に百万円値で巨大なケースを避けるため、1000万以上のみ割る
        if abs(v) >= 10_000_000:
            v /= 1_000_000.0
    return v


def _xbrl_normalize_eps(value: float, unit: str) -> float:
    v = float(value)
    # EPSは通常円/株。inline XBRL scaleで既に実値化済み。
    return v


def _context_duration_days(meta: dict) -> int|None:
    s = _xbrl_parse_iso_date(meta.get("start"))
    e = _xbrl_parse_iso_date(meta.get("end"))
    if s and e:
        return (e - s).days
    return None


def _context_end(meta: dict) -> date|None:
    return _xbrl_parse_iso_date(meta.get("end") or meta.get("instant"))



def _xbrl_pick_fact(
    facts: list[dict],
    contexts: dict,
    *,
    metric: str,
    target_fk: str|None,
    forecast: bool,
) -> tuple[float|None,dict|None]:
    """
    v10:
    - concept名
    - ResultMember / ForecastMember
    - fiscal end
    - full-year duration
    で選ぶ。
    """
    expected_end = _fiscal_key_end_date(target_fk)
    candidates = []

    for f in facts:
        nl = f["name_l"]

        if metric == "op":
            if not _xbrl_is_op_name(nl):
                continue
        elif metric == "eps":
            if not _xbrl_is_eps_name(nl):
                continue
        else:
            continue

        ctx = f.get("context_meta") or contexts.get(f["context"], {}) or {}

        members = " ".join(ctx.get("members",[]) or []).lower()
        dimensions = " ".join(ctx.get("dimensions",[]) or []).lower()
        cref = str(f.get("context") or "").lower()
        blob = " ".join([
            str(f.get("qname") or ""),
            f["name"],
            cref,
            members,
            dimensions,
        ]).lower()

        result_member = "resultmember" in blob
        forecast_member = "forecastmember" in blob or "forecast" in members

        end = _context_end(ctx)
        dur = _context_duration_days(ctx)

        score = 0.0

        # Result / Forecast dimension is strongest signal
        if forecast:
            if forecast_member:
                score += 30
            if result_member:
                score -= 30
        else:
            if result_member:
                score += 30
            if forecast_member:
                score -= 30

        # If member names absent, context name may still contain result/forecast
        if forecast and "forecast" in cref:
            score += 12
        if (not forecast) and "result" in cref:
            score += 12

        # target fiscal end
        if expected_end and end:
            if end.year == expected_end.year and end.month == expected_end.month:
                score += 20
                if end.day == expected_end.day:
                    score += 3
            else:
                month_delta = abs(
                    (end.year - expected_end.year) * 12
                    + (end.month - expected_end.month)
                )
                score -= min(month_delta * 3, 30)

        # full-year duration
        if dur is not None:
            if 330 <= dur <= 380:
                score += 10
            elif 250 <= dur <= 430:
                score += 4
            elif dur < 180:
                score -= 15

        # iXBRL summary facts are preferred over financial-statement fallback facts
        if f.get("is_inline"):
            score += 8

        # common TDnet summary namespace/concepts
        qn = str(f.get("qname") or "").lower()
        if "tse-ed-t:" in qn:
            score += 5

        candidates.append((score, f, ctx))

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0], reverse=True)
    score, f, ctx = candidates[0]

    # かなり厳格に。曖昧ならNoneで止める
    if score < 15:
        return None, None

    val = (
        _xbrl_normalize_op(f["value"], f["unit"])
        if metric == "op"
        else _xbrl_normalize_eps(f["value"], f["unit"])
    )

    # OP sanity
    if metric == "op" and not _is_plausible_op(val):
        return None, None

    # EPS sanity（極端な誤抽出を止める）
    if metric == "eps":
        if val is None or not (-100000 <= val <= 100000):
            return None, None

    evidence = {
        "tag": f["name"],
        "qname": f.get("qname"),
        "context": f["context"],
        "unit": f["unit"],
        "raw_value": f["value"],
        "normalized_value": val,
        "context_meta": ctx,
        "score": score,
        "file": f["file"],
        "is_inline": f.get("is_inline"),
    }
    return val, evidence



def _fiscal_key_from_xbrl_evidence(ev: dict|None) -> str|None:
    if not ev:
        return None
    ctx = ev.get("context_meta") or {}
    s = ctx.get("end") or ctx.get("instant")
    d = _xbrl_parse_iso_date(s)
    if not d:
        return None
    return f"{d.year:04d}-{d.month:02d}"




def _xbrl_pick_revision_current_fact(
    facts: list[dict],
    contexts: dict,
    *,
    metric: str,
    target_fk: str|None,
) -> tuple[float|None,dict|None]:
    """
    業績予想修正XBRL専用。

    最重要:
      CurrentMember_ForecastMember  = 今回修正後
      PreviousMember_ForecastMember = 前回予想

    PreviousMemberは採用禁止。
    また、通期履歴なのでQ2累計contextも除外する。
    """
    expected_end = _fiscal_key_end_date(target_fk)
    candidates = []

    for f in facts:
        nl = f["name_l"]
        if metric == "op":
            if not _xbrl_is_op_name(nl):
                continue
        elif metric == "eps":
            if not _xbrl_is_eps_name(nl):
                continue
        else:
            continue

        ctx = f.get("context_meta") or contexts.get(f["context"], {}) or {}
        members = " ".join(ctx.get("members",[]) or []).lower()
        dimensions = " ".join(ctx.get("dimensions",[]) or []).lower()
        cref = str(f.get("context") or "").lower()
        blob = " ".join([
            str(f.get("qname") or ""),
            f["name"],
            cref,
            members,
            dimensions,
        ]).lower()

        # 前回予想は絶対に今回値として採用しない
        if "previousmember" in blob or "previous_member" in blob:
            continue

        # 通期履歴なのでQ1/Q2/Q3累計は除外
        if any(k in blob for k in (
            "accumulatedq1", "accumulatedq2", "accumulatedq3",
            "quarter1", "quarter2", "quarter3",
            "interim",
        )):
            continue

        # ForecastMemberは必須に近い
        is_forecast = "forecastmember" in blob or "forecast" in blob
        if not is_forecast:
            continue

        end = _context_end(ctx)
        dur = _context_duration_days(ctx)

        score = 0.0

        # 今回修正値の最強シグナル
        if "currentmember" in blob or "current_member" in blob:
            score += 60
        else:
            # CurrentMemberが無い古い様式もあるので完全除外はしない
            score += 10

        if "forecastmember" in blob:
            score += 25

        if expected_end and end:
            if end.year == expected_end.year and end.month == expected_end.month:
                score += 20
            else:
                month_delta = abs(
                    (end.year - expected_end.year) * 12 +
                    (end.month - expected_end.month)
                )
                score -= min(month_delta * 4, 40)

        if dur is not None:
            if 330 <= dur <= 380:
                score += 15
            elif 250 <= dur <= 430:
                score += 5
            elif dur < 180:
                score -= 30

        if f.get("is_inline"):
            score += 5

        candidates.append((score, f, ctx))

    if not candidates:
        return None, None

    candidates.sort(key=lambda x:x[0], reverse=True)
    score, f, ctx = candidates[0]

    if score < 30:
        return None, None

    val = (
        _xbrl_normalize_op(f["value"], f["unit"])
        if metric == "op"
        else _xbrl_normalize_eps(f["value"], f["unit"])
    )

    if metric == "op" and not _is_plausible_op(val):
        return None, None
    if metric == "eps" and (val is None or not (-100000 <= val <= 100000)):
        return None, None

    evidence = {
        "tag": f["name"],
        "qname": f.get("qname"),
        "context": f["context"],
        "unit": f["unit"],
        "raw_value": f["value"],
        "normalized_value": val,
        "context_meta": ctx,
        "score": score,
        "file": f["file"],
        "is_inline": f.get("is_inline"),
    }
    return val, evidence


def _extract_xbrl_metrics_for_item(it: dict, code: str, title: str, pub: str) -> dict:
    """
    v11:
    決算短信はタイトル期を基準。
    業績予想修正はタイトルに年度が無くても、
    ForecastMemberのcontext endから fiscal_key を決定する。
    """
    out = {
        "actual_fk": None, "actual_op": None, "actual_eps": None,
        "forecast_fk": None, "forecast_op": None, "forecast_eps": None,
        "xbrl_url": "", "evidence": {},
    }

    b, xurl = _download_xbrl_zip(it, code)
    if not b:
        return out

    contexts, facts = _xbrl_contexts_and_facts(b)
    if not facts:
        return out

    out["xbrl_url"] = xurl

    if _is_full_year_earnings(title):
        afk = _title_fiscal_key(title)
        ffk = _next_fiscal_key(afk)
        out["actual_fk"] = afk
        out["forecast_fk"] = ffk

        aop, ev_aop = _xbrl_pick_fact(
            facts, contexts, metric="op", target_fk=afk, forecast=False
        )
        aeps, ev_aeps = _xbrl_pick_fact(
            facts, contexts, metric="eps", target_fk=afk, forecast=False
        )
        fop, ev_fop = _xbrl_pick_fact(
            facts, contexts, metric="op", target_fk=ffk, forecast=True
        )
        feps, ev_feps = _xbrl_pick_fact(
            facts, contexts, metric="eps", target_fk=ffk, forecast=True
        )

        out.update(
            actual_op=aop, actual_eps=aeps,
            forecast_op=fop, forecast_eps=feps
        )
        out["evidence"] = {
            "actual_op": ev_aop, "actual_eps": ev_aeps,
            "forecast_op": ev_fop, "forecast_eps": ev_feps,
        }

    elif _is_full_year_revision_title(title):
        fk = _title_fiscal_key(title) or _parse_jp_fiscal_key(title)

        # v16: 修正XBRLは CurrentMember を専用選択。
        # PreviousMemberは「前回予想」なので採用しない。
        fop, ev_fop = _xbrl_pick_revision_current_fact(
            facts, contexts, metric="op", target_fk=fk
        )
        feps, ev_feps = _xbrl_pick_revision_current_fact(
            facts, contexts, metric="eps", target_fk=fk
        )

        if not fk:
            fk = (
                _fiscal_key_from_xbrl_evidence(ev_fop)
                or _fiscal_key_from_xbrl_evidence(ev_feps)
            )

        out["forecast_fk"] = fk
        out.update(forecast_op=fop, forecast_eps=feps)
        out["evidence"] = {
            "forecast_op": ev_fop,
            "forecast_eps": ev_feps,
        }

    return out



def _load_cached_xbrl_metrics(
    conn: sqlite3.Connection, code: str, pub: str, title: str
) -> dict|None:
    try:
        row = conn.execute("""
            SELECT xbrl_url,
                   actual_fiscal_key,actual_op,actual_eps,
                   forecast_fiscal_key,forecast_op,forecast_eps,
                   evidence_json
            FROM tdnet_xbrl_metrics
            WHERE コード=? AND 提出時刻=? AND タイトル=?
            LIMIT 1
        """, (code,pub,title)).fetchone()
    except Exception:
        return None

    if not row:
        return None

    try:
        evidence = json.loads(row[7]) if row[7] else {}
    except Exception:
        evidence = {}

    return {
        "xbrl_url": row[0] or "",
        "actual_fk": row[1],
        "actual_op": row[2],
        "actual_eps": row[3],
        "forecast_fk": row[4],
        "forecast_op": row[5],
        "forecast_eps": row[6],
        "evidence": evidence,
        "_cached": True,
    }



def _xbrl_cache_sufficient(x: dict|None, title: str) -> bool:
    if not x:
        return False

    if _is_full_year_earnings(title):
        return _is_plausible_op(x.get("actual_op")) and _is_plausible_op(x.get("forecast_op"))

    if _is_full_year_revision_title(title):
        if not _is_plausible_op(x.get("forecast_op")):
            return False

        ev = ((x.get("evidence") or {}).get("forecast_op") or {})
        ctx = str(ev.get("context") or "").lower()
        meta = ev.get("context_meta") or {}
        blob = " ".join([
            ctx,
            " ".join(meta.get("members",[]) or []),
            " ".join(meta.get("dimensions",[]) or []),
        ]).lower()

        if "previousmember" in blob:
            return False
        if any(k in blob for k in (
            "accumulatedq1","accumulatedq2","accumulatedq3",
            "quarter1","quarter2","quarter3","interim"
        )):
            return False

        # CurrentMemberがある場合は最も信頼できる
        if "currentmember" in blob:
            return True

        # 古い形式でCurrentMemberがなくてもPreviousではない通期Forecastなら許容
        return "forecast" in blob

    return False


def _upsert_xbrl_metrics(conn: sqlite3.Connection, code: str, pub: str, title: str, m: dict):
    ensure_tdnet_xbrl_metrics_schema(conn)
    conn.execute("""
    INSERT INTO tdnet_xbrl_metrics(
      コード,提出時刻,タイトル,xbrl_url,
      actual_fiscal_key,actual_op,actual_eps,
      forecast_fiscal_key,forecast_op,forecast_eps,
      parse_method,evidence_json,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
      xbrl_url=excluded.xbrl_url,
      actual_fiscal_key=excluded.actual_fiscal_key,
      actual_op=excluded.actual_op,
      actual_eps=excluded.actual_eps,
      forecast_fiscal_key=excluded.forecast_fiscal_key,
      forecast_op=excluded.forecast_op,
      forecast_eps=excluded.forecast_eps,
      parse_method=excluded.parse_method,
      evidence_json=excluded.evidence_json,
      updated_at=excluded.updated_at
    """, (
        code,pub,title,m.get("xbrl_url"),
        m.get("actual_fk"),m.get("actual_op"),m.get("actual_eps"),
        m.get("forecast_fk"),m.get("forecast_op"),m.get("forecast_eps"),
        "xbrl_first_v9",
        json.dumps(m.get("evidence") or {}, ensure_ascii=False, default=str),
        now_iso(),
    ))
    conn.commit()


def _is_plausible_op(v: float|None) -> bool:
    if v is None:
        return False
    if not (-1_000_000 <= v <= 1_000_000):
        return False
    # 年度・銘柄コード・比率の誤読を抑制
    if abs(v) < 10:
        return False
    return True






def _validate_pdf_revision_with_known_previous(
    text: str,
    fiscal_key: str|None,
    known_previous_op: float|None,
    parsed_op: float|None,
    pdf_detail: dict|None,
) -> dict:
    """
    v23: PDF業績予想修正の営業利益誤認防止。

    非XBRL修正は、前回OPが分かっている場合:
      A) prev,new,delta の恒等式一致
      B) PDF表から取れた previous_op が既知previous_opと一致
    のどちらかを満たす時だけ new_op を採用する。

    これにより、列崩れしたPDFで「売上高」を営業利益として拾う事故を止める。
    """
    out = {
        "accepted_op": None,
        "method": "revision_pdf_rejected",
        "reason": "no_trust_path",
        "resolved": None,
    }

    if not _is_plausible_op(known_previous_op):
        # 前回OPが無ければ、PDFだけで営業利益列を断定しない。
        # XBRLがある場合はこの関数自体を呼ばない。
        return out

    prev = float(known_previous_op)
    d = pdf_detail or {}

    # 1) 最優先: delta恒等式
    resolved = _resolve_revision_by_previous_op(text or "", prev, fiscal_key)
    out["resolved"] = resolved
    if _is_plausible_op((resolved or {}).get("new_op")):
        out.update({
            "accepted_op": float(resolved["new_op"]),
            "method": resolved.get("method") or "numeric_delta_identity",
            "reason": "delta_identity",
        })
        return out

    # 2) 定型表で previous_op 自体が既知OPと一致しているなら採用可
    pdf_prev = d.get("previous_op")
    pdf_new = d.get("new_op")
    candidate = parsed_op if _is_plausible_op(parsed_op) else pdf_new

    if (
        _is_plausible_op(pdf_prev)
        and _almost_equal_amount(float(pdf_prev), prev)
        and _is_plausible_op(candidate)
    ):
        out.update({
            "accepted_op": float(candidate),
            "method": "revision_pdf_table_verified",
            "reason": "previous_op_matches",
        })
        return out

    # 3) 前回OP不一致/不明なら拒否。
    # 数値の大小だけでは判断しない（大型上方修正を誤って排除しないため）。
    out["reason"] = (
        f"pdf_previous_mismatch known={prev} parsed_prev={pdf_prev} parsed_new={candidate}"
    )
    return out


def _revision_pdf_then_xbrl(
    text: str, title: str, xbrl: dict|None
) -> tuple[str|None,float|None,float|None,str]:
    """
    v12:
    1) 修正XBRLが取れればXBRL
    2) 無ければ業績予想修正PDFの定型表を専用解析
    """
    x = xbrl or {}
    xfk = x.get("forecast_fk")
    xop = x.get("forecast_op")
    xeps = x.get("forecast_eps")

    if _is_plausible_op(xop):
        return xfk or _title_fiscal_key(title), xop, xeps, "revision_xbrl"

    d = _extract_revision_pdf_detailed(text, title)
    pop = d.get("new_op")
    peps = d.get("new_eps")

    if _is_plausible_op(pop):
        return d.get("fk") or xfk, pop, peps, "revision_pdf_table"

    return d.get("fk") or xfk, None, xeps, "revision_unresolved"


def _achievement(actual: float|None, forecast: float|None) -> float|None:
    if actual is None or forecast is None or forecast==0:return None
    return actual/forecast*100.0


def _pub_year(pub: str) -> int|None:
    m=re.match(r"(20\d{2})",pub or "")
    return int(m.group(1)) if m else None




def _get_tdnet_doc_text_for_item(conn: sqlite3.Connection, code: str, pub: str, title: str) -> tuple[str,str]:
    row = conn.execute("""
        SELECT 本文,URL
        FROM tdnet_documents
        WHERE コード=? AND 提出時刻=? AND タイトル=?
        LIMIT 1
    """, (code,pub,title)).fetchone()
    if not row:
        return "", ""
    return str(row[0] or ""), str(row[1] or "")


def _upsert_forecast_snapshot(
    conn: sqlite3.Connection,
    *,
    code: str,
    fiscal_key: str,
    forecast_date: str,
    forecast_type: str,
    forecast_op: float|None,
    forecast_eps: float|None,
    basis: str,
    title: str,
    url: str,
    parse_method: str,
    evidence: dict|None,
    source: str,
) -> bool:
    """
    同一コード・年度・日時・タイトルはUPDATE。
    既存forecast_historyに独自PK/UNIQUEがあっても依存しない。
    """
    if not code or not fiscal_key or not forecast_date:
        return False
    if forecast_op is None and forecast_eps is None:
        return False

    ensure_forecast_history_schema(conn)

    row = conn.execute("""
        SELECT rowid
        FROM forecast_history
        WHERE コード=? AND fiscal_key=? AND forecast_date=? AND COALESCE(タイトル,'')=?
        ORDER BY rowid
        LIMIT 1
    """, (code,fiscal_key,forecast_date,title or "")).fetchone()

    vals = (
        forecast_type,
        forecast_op,
        forecast_eps,
        basis or "unknown",
        title or "",
        url or "",
        parse_method or "",
        json.dumps(evidence or {}, ensure_ascii=False, default=str),
        source,
        now_iso(),
    )

    if row:
        conn.execute("""
            UPDATE forecast_history
            SET forecast_type=?,forecast_op=?,forecast_eps=?,basis=?,
                タイトル=?,URL=?,parse_method=?,evidence_json=?,source=?,updated_at=?
            WHERE rowid=?
        """, vals + (row[0],))
    else:
        conn.execute("""
            INSERT INTO forecast_history(
                コード,fiscal_key,forecast_date,forecast_type,
                forecast_op,forecast_eps,basis,
                タイトル,URL,parse_method,evidence_json,source,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            code,fiscal_key,forecast_date,forecast_type,
            forecast_op,forecast_eps,basis or "unknown",
            title or "",url or "",parse_method or "",
            json.dumps(evidence or {}, ensure_ascii=False, default=str),
            source,now_iso()
        ))
    conn.commit()
    return True


def _forecast_snapshots_for_fiscal(
    conn: sqlite3.Connection,
    code: str,
    fiscal_key: str,
    until_date: str|None=None,
) -> list[dict]:
    sql = """
        SELECT forecast_date,forecast_type,forecast_op,forecast_eps,basis,
               タイトル,URL,parse_method,evidence_json
        FROM forecast_history
        WHERE コード=? AND fiscal_key=?
    """
    params: list[Any] = [code,fiscal_key]
    if until_date:
        sql += " AND forecast_date<=?"
        params.append(until_date)
    sql += " ORDER BY forecast_date ASC, rowid ASC"

    out = []
    for r in conn.execute(sql, params).fetchall():
        try:
            ev = json.loads(r[8]) if r[8] else {}
        except Exception:
            ev = {}
        out.append({
            "forecast_date":r[0],
            "forecast_type":r[1],
            "forecast_op":r[2],
            "forecast_eps":r[3],
            "basis":r[4] or "unknown",
            "title":r[5] or "",
            "url":r[6] or "",
            "parse_method":r[7] or "",
            "evidence":ev,
        })
    return out


def _latest_forecast_snapshot_before(
    conn: sqlite3.Connection,
    code: str,
    fiscal_key: str,
    pub: str,
) -> dict|None:
    rows = _forecast_snapshots_for_fiscal(conn, code, fiscal_key, until_date=pub)
    if not rows:
        return None

    # 最後の有効OP/EPS状態を復元
    op = None
    eps = None
    latest = None
    for r in rows:
        if _is_plausible_op(r.get("forecast_op")):
            op = r.get("forecast_op")
        if r.get("forecast_eps") is not None:
            eps = r.get("forecast_eps")
        latest = r
    if latest is None:
        return None
    z = dict(latest)
    z["effective_op"] = op
    z["effective_eps"] = eps
    return z


def _cached_or_extract_xbrl_for_daily(
    conn: sqlite3.Connection,
    it: dict,
    code: str,
    title: str,
    pub: str,
) -> dict:
    x = _load_cached_xbrl_metrics(conn, code, pub, title)
    if _xbrl_cache_sufficient(x, title):
        return x or {}

    x = _extract_xbrl_metrics_for_item(it, code, title, pub) or {}
    if x:
        _upsert_xbrl_metrics(conn, code, pub, title, x)
    return x


def _finalize_achievement_from_actual(
    conn: sqlite3.Connection,
    *,
    code: str,
    fiscal_key: str,
    actual_op: float|None,
    actual_eps: float|None,
    actual_date: str,
    actual_basis: str,
    actual_title: str,
    actual_url: str,
    actual_method: str,
    actual_evidence: dict|None,
    source: str="tdnet_daily_v20",
    debug: bool=False,
) -> bool:
    """
    forecast_historyだけから、終了年度の
    期初予想→修正→最終予想→実績 を確定。
    """
    if not fiscal_key or not _is_plausible_op(actual_op):
        return False

    rows = _forecast_snapshots_for_fiscal(
        conn, code, fiscal_key, until_date=actual_date
    )
    if not rows:
        return False

    initials = [
        r for r in rows
        if str(r.get("forecast_type") or "").lower() == "initial"
        and _is_plausible_op(r.get("forecast_op"))
    ]
    if not initials:
        # 期初予想が無い年度は「期初予想信頼性」の母集団に入れない。
        if debug:
            print(f"[forecast_history][FINALIZE SKIP] {code} {fiscal_key}: initialなし")
        return False

    ini = initials[0]
    iop = ini.get("forecast_op")
    ieps = ini.get("forecast_eps")

    last_op = iop
    last_eps = ieps
    last_date = ini.get("forecast_date")
    up = 0
    down = 0

    used = [ini]
    started = False
    for r in rows:
        if r is ini:
            started = True
            continue
        if not started:
            continue
        if str(r.get("forecast_type") or "").lower() != "revision":
            continue

        old_op = last_op
        rop = r.get("forecast_op")

        if _is_plausible_op(rop):
            if _is_plausible_op(old_op):
                if rop > old_op:
                    up += 1
                elif rop < old_op:
                    down += 1
            last_op = rop

        if r.get("forecast_eps") is not None:
            last_eps = r.get("forecast_eps")

        last_date = r.get("forecast_date") or last_date
        used.append(r)

    ibasis = ini.get("basis") or "unknown"
    abasis = actual_basis or "unknown"

    known_bases = {
        str(r.get("basis") or "unknown")
        for r in used
        if str(r.get("basis") or "unknown") != "unknown"
    }
    basis_transition = (
        len(known_bases) > 1
        or any(_revision_indicates_basis_transition(r) for r in used)
    )

    if (
        (ibasis != "unknown" and abasis != "unknown" and ibasis != abasis)
        or basis_transition
    ):
        if debug:
            print(
                f"[forecast_history][SKIP BASIS] {code} {fiscal_key}: "
                f"initial={ibasis} actual={abasis} transition={basis_transition}"
            )
        _delete_forecast_history_row(conn, code, fiscal_key)
        return False

    if not _is_plausible_op(last_op):
        last_op = iop

    ia = _achievement(actual_op, iop)
    la = _achievement(actual_op, last_op)

    conf = (
        "HIGH"
        if "xbrl" in str(ini.get("parse_method") or "").lower()
        and "xbrl" in str(actual_method or "").lower()
        else "MEDIUM"
    )

    evid = {
        "snapshots": used,
        "actual": {
            "date":actual_date,
            "title":actual_title,
            "url":actual_url,
            "op":actual_op,
            "eps":actual_eps,
            "basis":actual_basis,
            "method":actual_method,
            "evidence":actual_evidence or {},
        }
    }

    conn.execute("""
    INSERT INTO forecast_achievement_history(
      コード,fiscal_key,
      initial_forecast_op,initial_forecast_eps,
      last_forecast_op,last_forecast_eps,
      actual_op,actual_eps,
      initial_achievement_op,last_achievement_op,
      upward_revisions,downward_revisions,
      initial_forecast_date,last_revision_date,actual_date,
      parse_confidence,evidence_json,source,updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(コード,fiscal_key) DO UPDATE SET
      initial_forecast_op=excluded.initial_forecast_op,
      initial_forecast_eps=excluded.initial_forecast_eps,
      last_forecast_op=excluded.last_forecast_op,
      last_forecast_eps=excluded.last_forecast_eps,
      actual_op=excluded.actual_op,
      actual_eps=excluded.actual_eps,
      initial_achievement_op=excluded.initial_achievement_op,
      last_achievement_op=excluded.last_achievement_op,
      upward_revisions=excluded.upward_revisions,
      downward_revisions=excluded.downward_revisions,
      initial_forecast_date=excluded.initial_forecast_date,
      last_revision_date=excluded.last_revision_date,
      actual_date=excluded.actual_date,
      parse_confidence=excluded.parse_confidence,
      evidence_json=excluded.evidence_json,
      source=excluded.source,
      updated_at=excluded.updated_at
    """, (
        code,fiscal_key,
        iop,ieps,last_op,last_eps,
        actual_op,actual_eps,ia,la,
        up,down,
        ini.get("forecast_date"),last_date,actual_date,
        conf,json.dumps(evid,ensure_ascii=False,default=str),
        source,now_iso()
    ))
    conn.commit()

    if debug:
        print(
            f"[forecast_history][ACHIEVEMENT] {code} {fiscal_key}: "
            f"initial={iop} last={last_op} actual={actual_op} "
            f"initial達成={ia}% final達成={la}% up={up} down={down}"
        )
    return True


def update_daily_forecast_history(
    conn: sqlite3.Connection,
    items: list[dict],
    *,
    finalize: bool=True,
    debug: bool=False,
    source: str="tdnet_daily_v20",
) -> dict:
    """
    通常のfetch_all.pyで毎日呼ぶ。

    - 本決算短信: 翌期会社予想を initial として保存
    - 通期業績予想修正: revision として保存
    - 本決算実績が出たら forecast_achievement_history を確定

    バックフィルからも同じ関数を使い、forecast_historyを初期充填する。
    """
    ensure_forecast_history_schema(conn)
    ensure_forecast_achievement_schema(conn)
    ensure_tdnet_xbrl_metrics_schema(conn)

    targets = []
    for it in items or []:
        code,name,title,url,pub = _tdnet_fields(it)
        if not code or not pub:
            continue
        if _is_full_year_earnings(title) or _is_full_year_revision_title(title):
            targets.append((pub,code,name,title,url,it))

    targets.sort(key=lambda x:(x[0],x[1],x[3]))

    stats = {
        "targets":len(targets),
        "initial_saved":0,
        "revision_saved":0,
        "achievement_updated":0,
        "unresolved":0,
    }

    for pub,code,name,title,url,it in targets:
        text, stored_url = _get_tdnet_doc_text_for_item(conn, code, pub, title)
        if stored_url:
            url = stored_url

        try:
            x = _cached_or_extract_xbrl_for_daily(conn, it, code, title, pub)
        except Exception as e:
            if debug:
                print(f"[forecast_history][XBRL ERROR] {code} {pub[:10]} {e}")
            x = {}

        if _is_full_year_earnings(title):
            ffk = (x or {}).get("forecast_fk")
            fop = (x or {}).get("forecast_op")
            feps = (x or {}).get("forecast_eps")
            ev = (x or {}).get("evidence") or {}
            fbasis = _xbrl_basis_from_evidence(ev.get("forecast_op"))

            if ffk and (fop is not None or feps is not None):
                if _upsert_forecast_snapshot(
                    conn,
                    code=code,
                    fiscal_key=ffk,
                    forecast_date=pub,
                    forecast_type="initial",
                    forecast_op=fop,
                    forecast_eps=feps,
                    basis=fbasis,
                    title=title,
                    url=url,
                    parse_method="annual_xbrl",
                    evidence=ev.get("forecast_op") or {},
                    source=source,
                ):
                    stats["initial_saved"] += 1
                    if debug:
                        print(
                            f"[forecast_history][INITIAL] {code} {ffk} "
                            f"{pub[:10]} OP={fop} EPS={feps} basis={fbasis}"
                        )

            if finalize:
                afk = (x or {}).get("actual_fk")
                aop = (x or {}).get("actual_op")
                aeps = (x or {}).get("actual_eps")
                abasis = _xbrl_basis_from_evidence(ev.get("actual_op"))

                if afk and _is_plausible_op(aop):
                    if _finalize_achievement_from_actual(
                        conn,
                        code=code,
                        fiscal_key=afk,
                        actual_op=aop,
                        actual_eps=aeps,
                        actual_date=pub,
                        actual_basis=abasis,
                        actual_title=title,
                        actual_url=url,
                        actual_method="annual_xbrl",
                        actual_evidence=ev.get("actual_op") or {},
                        source=source,
                        debug=debug,
                    ):
                        stats["achievement_updated"] += 1

        elif _is_full_year_revision_title(title):
            fk = (x or {}).get("forecast_fk") or _title_fiscal_key(title) or _parse_jp_fiscal_key(title)
            xop = (x or {}).get("forecast_op")
            xeps = (x or {}).get("forecast_eps")
            ev = (x or {}).get("evidence") or {}
            basis = _xbrl_basis_from_evidence(ev.get("forecast_op"))

            pdf = _extract_revision_pdf_detailed(text, title) if text else {}
            eps = xeps if xeps is not None else pdf.get("new_eps")
            fk = fk or pdf.get("fk")

            resolved = None
            validation = None
            prev = _latest_forecast_snapshot_before(conn, code, fk, pub) if fk else None

            if _is_plausible_op(xop):
                op = xop
                method = "revision_xbrl"
            else:
                known_prev = (
                    prev.get("effective_op")
                    if prev and _is_plausible_op(prev.get("effective_op"))
                    else None
                )
                validation = _validate_pdf_revision_with_known_previous(
                    text,
                    fk,
                    known_prev,
                    pdf.get("new_op"),
                    pdf,
                )
                resolved = (validation or {}).get("resolved")
                op = (validation or {}).get("accepted_op")
                method = (validation or {}).get("method") or "revision_pdf_rejected"

                if not _is_plausible_op(op):
                    stats["unresolved"] += 1
                    if debug:
                        print(
                            f"[forecast_history][REVISION REJECT] {code} {pub[:10]} "
                            f"fk={fk} prev={known_prev} parsed={pdf.get('new_op')} "
                            f"reason={(validation or {}).get('reason')}"
                        )

            if basis == "unknown":
                basis = _basis_from_title(title)

            # OPが取れなくてもEPSだけ取れた修正はスナップショットとして残す。
            if fk and (op is not None or eps is not None):
                if _upsert_forecast_snapshot(
                    conn,
                    code=code,
                    fiscal_key=fk,
                    forecast_date=pub,
                    forecast_type="revision",
                    forecast_op=op,
                    forecast_eps=eps,
                    basis=basis,
                    title=title,
                    url=url,
                    parse_method=method,
                    evidence={
                        "xbrl":ev.get("forecast_op") or {},
                        "pdf":pdf,
                        "delta_identity":resolved,
                        "pdf_validation":validation,
                    },
                    source=source,
                ):
                    stats["revision_saved"] += 1
                    if debug:
                        print(
                            f"[forecast_history][REVISION] {code} {fk} "
                            f"{pub[:10]} OP={op} EPS={eps} method={method}"
                        )
            else:
                stats["unresolved"] += 1
                if debug:
                    print(
                        f"[forecast_history][UNRESOLVED] {code} {pub[:10]} "
                        f"{title[:42]}"
                    )

    print(
        "[forecast_history] "
        f"対象={stats['targets']} "
        f"initial={stats['initial_saved']} "
        f"revision={stats['revision_saved']} "
        f"achievement={stats['achievement_updated']} "
        f"unresolved={stats['unresolved']}"
    )
    return stats


def _backfill_one_code(conn: sqlite3.Connection, code: str, years: int=5, debug: bool=True) -> int:
    code = _normalize_code(code)
    items = fetch_tdnet_by_code(code, limit=300)
    cutoff = datetime.now(JST) - timedelta(days=max(1, years+2)*370)

    targets = []
    item_map = {}

    for it in items:
        c,n,t,u,p = _tdnet_fields(it)
        if not p:
            continue
        dt = _parse_ts_str(p)
        if dt and dt < cutoff:
            continue

        # v9:
        # 数値履歴に必要なのは「本決算短信」と「業績予想修正」。
        # 決算説明資料は tdnet_documents の一般収集側では使えるが、
        # forecast backfill の数値ソースにはしない。
        is_revision = _is_full_year_revision_title(t)
        if _is_full_year_earnings(t) or is_revision:
            targets.append(it)
            item_map[(p,t)] = it

    print(f"[backfill] {code}: 対象資料 {len(targets)}件")

    # PDF本文はrevision専用パーサー/証跡用として保存
    saved = upsert_tdnet_document_texts(conn, targets, forced_code=code)
    print(f"[backfill] {code}: 本文保存 {saved}件")

    # v20:
    # 過去バックフィル時にもforecast_historyを同時に初期充填。
    # current fiscalの予想・修正も実績未確定のまま履歴として残る。
    update_daily_forecast_history(
        conn,
        targets,
        finalize=False,
        debug=False,
        source="tdnet_backfill_v20",
    )

    docs = conn.execute("""
      SELECT 提出時刻,タイトル,本文,URL
      FROM tdnet_documents
      WHERE コード=?
      ORDER BY 提出時刻 ASC
    """, (code,)).fetchall()

    annual = []
    revisions = []

    for d in docs:
        pub,title,text,url = d[0],d[1],d[2] or "",d[3]
        it = item_map.get((pub,title))

        # v13: 過去runでDBに残った説明資料/差異資料などは解析しない
        if it is None:
            continue

        if _is_full_year_earnings(title):
            x = _load_cached_xbrl_metrics(conn, code, pub, title)
            if _xbrl_cache_sufficient(x, title):
                print(f"[backfill][XBRL CACHE HIT] {code} {pub[:10]} {title[:38]}")
            else:
                x = _extract_xbrl_metrics_for_item(it, code, title, pub) if it else {}
                if x:
                    _upsert_xbrl_metrics(conn, code, pub, title, x)

            afk = x.get("actual_fk") if x else _title_fiscal_key(title)
            ffk = x.get("forecast_fk") if x else _next_fiscal_key(afk)
            aop = x.get("actual_op") if x else None
            aeps = x.get("actual_eps") if x else None
            fop = x.get("forecast_op") if x else None
            feps = x.get("forecast_eps") if x else None

            method = "xbrl"

            # v10: 本決算のOP/EPSはXBRL以外から補完しない。
            # PDF表崩れで銘柄コード/年度を誤取得する事故を完全に止める。
            # XBRLで取れなければNoneのままにして、その年度をSKIPする。

            ev_all = (x or {}).get("evidence") or {}
            rec = {
                "pub":pub, "title":title, "url":url,
                "actual_fk":afk, "actual_op":aop, "actual_eps":aeps,
                "forecast_fk":ffk, "forecast_op":fop, "forecast_eps":feps,
                "method":method,
                "xbrl_url":(x or {}).get("xbrl_url"),
                "actual_basis": _xbrl_basis_from_evidence(ev_all.get("actual_op")),
                "forecast_basis": _xbrl_basis_from_evidence(ev_all.get("forecast_op")),
            }
            annual.append(rec)

            if debug:
                ev = (x or {}).get("evidence") or {}
                aev = ev.get("actual_op") or {}
                fev = ev.get("forecast_op") or {}
                print(
                    f"[DEBUG][ANNUAL][{method}] {code} {pub[:10]} "
                    f"actual={afk} op={aop} eps={aeps} "
                    f"-> forecast={ffk} op={fop} eps={feps}"
                )
                print(
                    f"[DEBUG][XBRL OP] actual_tag={aev.get('qname') or aev.get('tag')} "
                    f"ctx={aev.get('context')} score={aev.get('score')} | "
                    f"forecast_tag={fev.get('qname') or fev.get('tag')} "
                    f"ctx={fev.get('context')} score={fev.get('score')}"
                )

        elif _is_full_year_revision_title(title):
            x = _load_cached_xbrl_metrics(conn, code, pub, title)
            if _xbrl_cache_sufficient(x, title):
                print(f"[backfill][XBRL CACHE HIT] {code} {pub[:10]} {title[:38]}")
            else:
                x = _extract_xbrl_metrics_for_item(it, code, title, pub) if it else {}
                if x:
                    _upsert_xbrl_metrics(conn, code, pub, title, x)

            fk,op,eps,method = _revision_pdf_then_xbrl(text, title, x)

            # タイトルに上方/下方が無い一般的な「修正」でも、
            # 前回OPと今回OPを比較して方向を確定する。
            pdf_detail = _extract_revision_pdf_detailed(text, title)
            direction = (
                "up" if "上方修正" in title else
                ("down" if "下方修正" in title else pdf_detail.get("direction","unknown"))
            )

            rev_ev_for_basis = ((x or {}).get("evidence") or {}).get("forecast_op") or {}
            revisions.append({
                "pub":pub, "title":title, "url":url,
                "fk":fk, "op":op, "eps":eps,
                "direction":direction, "method":method,
                "xbrl_url":(x or {}).get("xbrl_url"),
                "forecast_basis": _xbrl_basis_from_evidence(rev_ev_for_basis),
                "text": text,
            })

            if debug:
                rev_ev = ((x or {}).get("evidence") or {}).get("forecast_op") or {}
                print(
                    f"[DEBUG][REV][{method}] {code} {pub[:10]} "
                    f"fiscal={fk} op={op} eps={eps} dir={direction}"
                )
                print(
                    f"[DEBUG][REV XBRL] tag={rev_ev.get('qname') or rev_ev.get('tag')} "
                    f"ctx={rev_ev.get('context')} score={rev_ev.get('score')} "
                    f"xbrl_url={(x or {}).get('xbrl_url')}"
                )
                print(
                    f"[DEBUG][REV PDF] prev_op={pdf_detail.get('previous_op')} "
                    f"new_op={pdf_detail.get('new_op')} "
                    f"new_eps={pdf_detail.get('new_eps')} "
                    f"direction={pdf_detail.get('direction')} "
                    f"nums={pdf_detail.get('new_numbers')}"
                )

    # 同じ年度に訂正文書等が混ざらないよう、
    # actualは本決算短信のうちXBRLが最も揃うレコードを優先。
    def annual_quality(a):
        q = 0
        if a.get("method") == "xbrl":
            q += 20
        if _is_plausible_op(a.get("actual_op")):
            q += 10
        if _is_plausible_op(a.get("forecast_op")):
            q += 10
        if a.get("actual_eps") is not None:
            q += 2
        if a.get("forecast_eps") is not None:
            q += 2
        return q

    by_actual = {}
    for a in annual:
        fk = a.get("actual_fk")
        if not fk:
            continue
        old = by_actual.get(fk)
        if old is None or annual_quality(a) > annual_quality(old):
            by_actual[fk] = a

    by_initial = {}
    # 各本決算短信の翌期予想 = 翌年度の期初予想。
    # XBRL優先で品質の高いものを選ぶ。
    for a in annual:
        fk = a.get("forecast_fk")
        if not fk:
            continue
        if a.get("forecast_op") is None and a.get("forecast_eps") is None:
            continue
        old = by_initial.get(fk)
        if old is None or annual_quality(a) > annual_quality(old):
            by_initial[fk] = a

    total = 0
    fiscal_keys = sorted(set(by_initial) & set(by_actual))[-years:]

    for fk in fiscal_keys:
        ini = by_initial[fk]
        act = by_actual[fk]

        revs = [
            r for r in revisions
            if r.get("fk") == fk
            and r.get("pub","") >= ini.get("pub","")
            and r.get("pub","") <= act.get("pub","")
        ]
        revs.sort(key=lambda r:r.get("pub",""))

        last_op = ini.get("forecast_op")
        last_eps = ini.get("forecast_eps")
        last_date = ini.get("pub")
        up = down = 0

        evid = [{
            "kind":"initial",
            "date":ini.get("pub"),
            "title":ini.get("title"),
            "url":ini.get("url"),
            "xbrl_url":ini.get("xbrl_url"),
            "op":last_op,
            "eps":last_eps,
            "method":ini.get("method"),
        }]

        for r in revs:
            old_op = last_op

            # v15:
            # XBRLで確定していない修正は、PDFラベル解析の成否にかかわらず
            # 「前回OP・今回OP・増減額」の恒等式で検証する。
            resolved = None
            validation = None
            is_xbrl_revision = str(r.get("method") or "").startswith("revision_xbrl")

            if _is_plausible_op(old_op) and not is_xbrl_revision:
                pdf_detail_now = _extract_revision_pdf_detailed(
                    r.get("text") or "", r.get("title") or ""
                )
                validation = _validate_pdf_revision_with_known_previous(
                    r.get("text") or "",
                    fk,
                    old_op,
                    r.get("op"),
                    pdf_detail_now,
                )
                resolved = (validation or {}).get("resolved")
                old_parsed = r.get("op")
                accepted = (validation or {}).get("accepted_op")

                if _is_plausible_op(accepted):
                    r["op"] = accepted
                    r["method"] = (validation or {}).get("method") or r.get("method")

                    if resolved and _is_plausible_op(resolved.get("new_op")):
                        r["direction"] = resolved.get("direction") or r.get("direction")
                        print(
                            f"[backfill][REV DELTA OK] {code} {fk} {r.get('pub','')[:10]} "
                            f"{old_op} -> {r['op']} "
                            f"delta={resolved.get('delta')} "
                            f"dir={r.get('direction')} "
                            f"score={resolved.get('score')} "
                            f"old_parsed={old_parsed} "
                            f"window={resolved.get('window')}"
                        )
                    else:
                        r["direction"] = (
                            "up" if r["op"] > old_op
                            else "down" if r["op"] < old_op
                            else "flat"
                        )
                        print(
                            f"[backfill][REV PDF VERIFIED] {code} {fk} {r.get('pub','')[:10]} "
                            f"{old_op} -> {r['op']} "
                            f"old_parsed={old_parsed}"
                        )
                else:
                    # ★v23: PDF候補は「妥当そうな数」だけでは採用しない。
                    # 前回OPとの証明が取れなければOP更新を無効化する。
                    r["op"] = None
                    r["method"] = "revision_pdf_rejected"
                    print(
                        f"[backfill][REV PDF REJECT] {code} {fk} {r.get('pub','')[:10]} "
                        f"prevOP={old_op} parsedOP={old_parsed} "
                        f"reason={(validation or {}).get('reason')} "
                        f"score={(resolved or {}).get('score')} "
                        f"window={(resolved or {}).get('window')}"
                    )

            if _is_plausible_op(r.get("op")):
                if _is_plausible_op(old_op):
                    if r["op"] > old_op:
                        up += 1
                    elif r["op"] < old_op:
                        down += 1
                elif r.get("direction") == "up":
                    up += 1
                elif r.get("direction") == "down":
                    down += 1
                last_op = r["op"]
            else:
                if r.get("direction") == "up":
                    up += 1
                elif r.get("direction") == "down":
                    down += 1

            if r.get("eps") is not None:
                last_eps = r["eps"]

            last_date = r.get("pub") or last_date

            evid.append({
                "kind":"revision",
                "date":r.get("pub"),
                "title":r.get("title"),
                "url":r.get("url"),
                "xbrl_url":r.get("xbrl_url"),
                "op":r.get("op"),
                "eps":r.get("eps"),
                "direction":r.get("direction"),
                "method":r.get("method"),
                "anchor_debug": resolved,
                "pdf_validation": validation,
            })

        evid.append({
            "kind":"actual",
            "date":act.get("pub"),
            "title":act.get("title"),
            "url":act.get("url"),
            "xbrl_url":act.get("xbrl_url"),
            "op":act.get("actual_op"),
            "eps":act.get("actual_eps"),
            "method":act.get("method"),
        })

        iop = ini.get("forecast_op")
        ieps = ini.get("forecast_eps")
        aop = act.get("actual_op")
        aeps = act.get("actual_eps")

        # v17: 連結/非連結の比較基準をXBRL context + タイトルで二重判定。
        ibasis = _effective_basis(ini, "forecast")
        abasis = _effective_basis(act, "actual")

        # 年度途中の基準変更も明示的に検知
        basis_transition = any(_revision_indicates_basis_transition(r) for r in revs)

        print(
            f"[DEBUG][BASIS] {code} {fk}: "
            f"initial={ibasis} actual={abasis} "
            f"transition={basis_transition}"
        )

        if (
            (ibasis != "unknown" and abasis != "unknown" and ibasis != abasis)
            or basis_transition
        ):
            print(
                f"[backfill][SKIP BASIS] {code} {fk}: "
                f"期初={ibasis} 実績={abasis} transition={basis_transition} "
                f"(期初OP={iop}, 実績OP={aop})"
            )
            # 過去versionで保存済みの不適切行も消す
            _delete_forecast_history_row(conn, code, fk)
            continue

        # XBRL-firstなのでOPの両側が妥当でない年度は保存しない。
        # EPSだけでMEDIUM保存もしない（今回欲しい中心指標は営業利益）。
        if not _is_plausible_op(iop) or not _is_plausible_op(aop):
            print(
                f"[backfill][SKIP] {code} {fk}: XBRL中心のOPペア成立せず "
                f"(期初OP={iop}, 実績OP={aop})"
            )
            _delete_forecast_history_row(conn, code, fk)
            continue

        # 両方XBRLならHIGH、それ以外はMEDIUM
        both_xbrl = (
            str(ini.get("method","")).startswith("xbrl")
            and str(act.get("method","")).startswith("xbrl")
        )
        conf = "HIGH" if both_xbrl else "MEDIUM"

        ia = _achievement(aop, iop)
        la = _achievement(aop, last_op)

        conn.execute("""
        INSERT INTO forecast_achievement_history(
          コード,fiscal_key,
          initial_forecast_op,initial_forecast_eps,
          last_forecast_op,last_forecast_eps,
          actual_op,actual_eps,
          initial_achievement_op,last_achievement_op,
          upward_revisions,downward_revisions,
          initial_forecast_date,last_revision_date,actual_date,
          parse_confidence,evidence_json,source,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(コード,fiscal_key) DO UPDATE SET
          initial_forecast_op=excluded.initial_forecast_op,
          initial_forecast_eps=excluded.initial_forecast_eps,
          last_forecast_op=excluded.last_forecast_op,
          last_forecast_eps=excluded.last_forecast_eps,
          actual_op=excluded.actual_op,
          actual_eps=excluded.actual_eps,
          initial_achievement_op=excluded.initial_achievement_op,
          last_achievement_op=excluded.last_achievement_op,
          upward_revisions=excluded.upward_revisions,
          downward_revisions=excluded.downward_revisions,
          initial_forecast_date=excluded.initial_forecast_date,
          last_revision_date=excluded.last_revision_date,
          actual_date=excluded.actual_date,
          parse_confidence=excluded.parse_confidence,
          evidence_json=excluded.evidence_json,
          source=excluded.source,
          updated_at=excluded.updated_at
        """, (
            code,fk,
            iop,ieps,last_op,last_eps,
            aop,aeps,ia,la,
            up,down,
            ini.get("pub"),last_date,act.get("pub"),
            conf,json.dumps(evid,ensure_ascii=False,default=str),
            "tdnet_xbrl_backfill_v9",now_iso()
        ))
        conn.commit()
        total += 1

        print(
            f"[backfill][SAVE] {code} {fk}: "
            f"期初OP={iop} 実績OP={aop} 達成={ia}% "
            f"最終OP={last_op} 最終達成={la}% conf={conf}"
        )

    return total



_BACKFILL_ENGINE_VERSION = "v25"
_BACKFILL_COMPAT_COMPLETE_VERSIONS = {"v23", "v24", "v25"}
_BACKFILL_DEFAULT_CLAIM_TTL_MINUTES = 360


def ensure_forecast_backfill_status_schema(conn: sqlite3.Connection):
    """
    会社単位のバックフィル状態 + 複数プロセス用CLAIM。

    v24:
    - running中の銘柄に claim_token / PID / host / claim_started_at を持たせる
    - BEGIN IMMEDIATE で原子的にCLAIMする
    - v23完了データは互換扱いで再処理しない
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS forecast_backfill_status(
        コード TEXT PRIMARY KEY,
        requested_years INTEGER NOT NULL DEFAULT 0,
        saved_years INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        engine_version TEXT,
        started_at TEXT,
        completed_at TEXT,
        last_error TEXT,
        updated_at TEXT,
        claim_token TEXT,
        claim_pid INTEGER,
        claim_host TEXT,
        claim_started_at TEXT
    );
    """)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(forecast_backfill_status)")}
    for col, typ in [
        ("claim_token", "TEXT"),
        ("claim_pid", "INTEGER"),
        ("claim_host", "TEXT"),
        ("claim_started_at", "TEXT"),
    ]:
        if col not in cols:
            try:
                conn.execute(
                    f'ALTER TABLE forecast_backfill_status ADD COLUMN "{col}" {typ}'
                )
            except sqlite3.OperationalError:
                pass

    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_forecast_backfill_status_state
    ON forecast_backfill_status(status, requested_years);
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_forecast_backfill_claim
    ON forecast_backfill_status(status, claim_started_at);
    """)
    conn.commit()



def _mark_backfill_status(
    conn: sqlite3.Connection,
    code: str,
    *,
    years: int,
    saved_years: int=0,
    status: str,
    error: str|None=None,
):
    """
    非CLAIM用途の状態更新。
    legacy bootstrap 等から利用する。
    complete/error/pending ではCLAIM情報を必ずクリアする。
    """
    ensure_forecast_backfill_status_schema(conn)
    now = now_iso()

    if status == "running":
        conn.execute("""
        INSERT INTO forecast_backfill_status(
            コード,requested_years,saved_years,status,engine_version,
            started_at,completed_at,last_error,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(コード) DO UPDATE SET
            requested_years=excluded.requested_years,
            status=excluded.status,
            engine_version=excluded.engine_version,
            started_at=excluded.started_at,
            last_error=NULL,
            updated_at=excluded.updated_at
        """, (
            code,int(years),int(saved_years),"running",_BACKFILL_ENGINE_VERSION,
            now,None,None,now
        ))
    else:
        conn.execute("""
        INSERT INTO forecast_backfill_status(
            コード,requested_years,saved_years,status,engine_version,
            started_at,completed_at,last_error,updated_at,
            claim_token,claim_pid,claim_host,claim_started_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(コード) DO UPDATE SET
            requested_years=MAX(forecast_backfill_status.requested_years, excluded.requested_years),
            saved_years=excluded.saved_years,
            status=excluded.status,
            engine_version=excluded.engine_version,
            completed_at=excluded.completed_at,
            last_error=excluded.last_error,
            updated_at=excluded.updated_at,
            claim_token=NULL,
            claim_pid=NULL,
            claim_host=NULL,
            claim_started_at=NULL
        """, (
            code,int(years),int(saved_years),status,_BACKFILL_ENGINE_VERSION,
            None, now if status=="complete" else None,
            error,now,None,None,None,None
        ))
    conn.commit()


def _legacy_backfill_saved_years(conn: sqlite3.Connection, code: str) -> int:
    """
    v21以前で保存済みだが status テーブルが無い会社の互換判定。
    TDnet backfill由来のachievement行数を数える。
    """
    try:
        row = conn.execute("""
            SELECT COUNT(*)
            FROM forecast_achievement_history
            WHERE コード=?
              AND (
                    source LIKE 'tdnet%backfill%'
                 OR source LIKE 'tdnet_xbrl_backfill%'
                 OR source LIKE 'tdnet_backfill%'
              )
        """, (code,)).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _backfill_is_complete(
    conn: sqlite3.Connection,
    code: str,
    years: int,
    *,
    bootstrap_legacy: bool=True,
) -> tuple[bool,str,int]:
    """
    return: (skipしてよいか, 理由, 保存年度数)

    status=complete かつ requested_years>=今回years → 完了。
    v21以前の互換として、5年要求なら4年以上など
    years-1以上のTDnet backfill実績があれば完了扱いに移行する。
    """
    ensure_forecast_backfill_status_schema(conn)

    row = conn.execute("""
        SELECT requested_years,saved_years,status,engine_version
        FROM forecast_backfill_status
        WHERE コード=?
        LIMIT 1
    """, (code,)).fetchone()

    if row:
        req = int(row[0] or 0)
        saved = int(row[1] or 0)
        status = str(row[2] or "")
        engine_version = str(row[3] or "")
        if (
            status == "complete"
            and req >= int(years)
            and engine_version in _BACKFILL_COMPAT_COMPLETE_VERSIONS
        ):
            return True, f"status complete({req}y,{engine_version})", saved

    if bootstrap_legacy:
        saved = _legacy_backfill_saved_years(conn, code)
        # 連結/非連結移行などで1年度だけ正当に除外される会社を許容。
        threshold = max(1, int(years)-1)
        if saved >= threshold:
            _mark_backfill_status(
                conn, code,
                years=years,
                saved_years=saved,
                status="complete",
            )
            return True, f"legacy bootstrap({saved} rows)", saved

    return False, "", 0




def ensure_forecast_backfill_workers_schema(conn: sqlite3.Connection):
    """バックフィルworkerの生存確認用テーブル。"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS forecast_backfill_workers(
        worker_token TEXT PRIMARY KEY,
        pid INTEGER,
        host TEXT,
        started_at TEXT NOT NULL,
        last_seen TEXT NOT NULL,
        requested_years INTEGER,
        status TEXT NOT NULL DEFAULT 'running'
    );
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_forecast_backfill_workers_status_seen
    ON forecast_backfill_workers(status, last_seen);
    """)
    conn.commit()


def _register_backfill_worker(conn: sqlite3.Connection, worker_token: str, years: int):
    ensure_forecast_backfill_workers_schema(conn)
    now = now_iso()
    host = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "host"
    conn.execute("""
        INSERT INTO forecast_backfill_workers(
            worker_token,pid,host,started_at,last_seen,requested_years,status
        ) VALUES(?,?,?,?,?,?, 'running')
        ON CONFLICT(worker_token) DO UPDATE SET
            pid=excluded.pid,
            host=excluded.host,
            last_seen=excluded.last_seen,
            requested_years=excluded.requested_years,
            status='running'
    """, (worker_token, os.getpid(), host, now, now, int(years)))
    conn.commit()


def _heartbeat_backfill_worker(conn: sqlite3.Connection, worker_token: str):
    try:
        conn.execute("""
            UPDATE forecast_backfill_workers
            SET last_seen=?, status='running'
            WHERE worker_token=?
        """, (now_iso(), worker_token))
        conn.commit()
    except Exception:
        pass


def _unregister_backfill_worker(conn: sqlite3.Connection, worker_token: str, status: str='stopped'):
    try:
        conn.execute("""
            UPDATE forecast_backfill_workers
            SET last_seen=?, status=?
            WHERE worker_token=?
        """, (now_iso(), status, worker_token))
        conn.commit()
    except Exception:
        pass

def _make_backfill_claim_token() -> str:
    host = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "host"
    return f"{host}:{os.getpid()}:{time.time_ns()}"


def _parse_backfill_status_dt(value: str|None) -> datetime|None:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    return None


def _try_claim_backfill_code(
    conn: sqlite3.Connection,
    code: str,
    *,
    years: int,
    claim_token: str,
    ttl_minutes: int=_BACKFILL_DEFAULT_CLAIM_TTL_MINUTES,
    skip_complete: bool=True,
) -> tuple[bool,str]:
    """
    1銘柄を原子的にCLAIMする。

    return:
      (True, "claimed")          -> このプロセスが処理してよい
      (False, "busy:...")        -> 別プロセスが処理中
      (False, "complete:...")    -> 処理済み
    """
    ensure_forecast_backfill_status_schema(conn)

    ttl_minutes = max(1, int(ttl_minutes or _BACKFILL_DEFAULT_CLAIM_TTL_MINUTES))
    now_str = now_iso()
    now_dt = datetime.now(JST).replace(tzinfo=None)
    host = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "host"
    pid = os.getpid()

    try:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute("""
            SELECT requested_years,saved_years,status,engine_version,
                   claim_token,claim_pid,claim_host,claim_started_at,updated_at
            FROM forecast_backfill_status
            WHERE コード=?
            LIMIT 1
        """, (code,)).fetchone()

        if row:
            req = int(row["requested_years"] or 0)
            status = str(row["status"] or "")
            engine_version = str(row["engine_version"] or "")

            if (
                skip_complete
                and status == "complete"
                and req >= int(years)
                and engine_version in _BACKFILL_COMPAT_COMPLETE_VERSIONS
            ):
                conn.commit()
                return False, f"complete:{req}y/{engine_version}"

            if status == "running":
                other_token = str(row["claim_token"] or "")
                started = (
                    _parse_backfill_status_dt(row["claim_started_at"])
                    or _parse_backfill_status_dt(row["updated_at"])
                )
                age_min = None
                if started is not None:
                    age_min = max(0.0, (now_dt - started).total_seconds() / 60.0)

                # 自分以外の生きたCLAIMは絶対に奪わない。
                if other_token and other_token != claim_token:
                    if age_min is None or age_min < ttl_minutes:
                        owner = f"{row['claim_host'] or '?'} pid={row['claim_pid'] or '?'}"
                        conn.commit()
                        age_txt = "?" if age_min is None else f"{age_min:.1f}m"
                        return False, f"busy:{owner}, age={age_txt}"

                # tokenが無い旧running行も、TTL以内なら安全側でbusy。
                if not other_token and started is not None and age_min < ttl_minutes:
                    conn.commit()
                    return False, f"busy:legacy-running, age={age_min:.1f}m"

                # ここに来たrunningはstale。CLAIMを回収する。
                if age_min is not None:
                    print(
                        f"[backfill][STALE CLAIM] {code}: "
                        f"{age_min:.1f}分経過 → このプロセスが回収"
                    )

        conn.execute("""
            INSERT INTO forecast_backfill_status(
                コード,requested_years,saved_years,status,engine_version,
                started_at,completed_at,last_error,updated_at,
                claim_token,claim_pid,claim_host,claim_started_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(コード) DO UPDATE SET
                requested_years=MAX(forecast_backfill_status.requested_years, excluded.requested_years),
                status='running',
                engine_version=excluded.engine_version,
                started_at=excluded.started_at,
                completed_at=NULL,
                last_error=NULL,
                updated_at=excluded.updated_at,
                claim_token=excluded.claim_token,
                claim_pid=excluded.claim_pid,
                claim_host=excluded.claim_host,
                claim_started_at=excluded.claim_started_at
        """, (
            code,int(years),0,"running",_BACKFILL_ENGINE_VERSION,
            now_str,None,None,now_str,
            claim_token,pid,host,now_str
        ))
        conn.commit()
        return True, "claimed"

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def _finish_backfill_claim(
    conn: sqlite3.Connection,
    code: str,
    *,
    claim_token: str,
    years: int,
    saved_years: int,
    status: str,
    error: str|None=None,
) -> bool:
    """
    自分がCLAIMした行だけ完了/失敗に遷移する。
    他プロセスのCLAIMを誤って上書きしない。
    """
    now = now_iso()
    completed_at = now if status == "complete" else None

    cur = conn.execute("""
        UPDATE forecast_backfill_status
        SET requested_years=MAX(requested_years, ?),
            saved_years=?,
            status=?,
            engine_version=?,
            completed_at=?,
            last_error=?,
            updated_at=?,
            claim_token=NULL,
            claim_pid=NULL,
            claim_host=NULL,
            claim_started_at=NULL
        WHERE コード=? AND claim_token=?
    """, (
        int(years),int(saved_years),status,_BACKFILL_ENGINE_VERSION,
        completed_at,error,now,code,claim_token
    ))
    conn.commit()
    return int(cur.rowcount or 0) == 1


def _release_backfill_claim(
    conn: sqlite3.Connection,
    code: str,
    *,
    claim_token: str,
    reason: str="interrupted",
) -> bool:
    """
    Ctrl+C等の正常な中断時に自分のCLAIMを即時解放する。
    """
    cur = conn.execute("""
        UPDATE forecast_backfill_status
        SET status='pending',
            last_error=?,
            updated_at=?,
            claim_token=NULL,
            claim_pid=NULL,
            claim_host=NULL,
            claim_started_at=NULL
        WHERE コード=? AND claim_token=?
    """, (reason, now_iso(), code, claim_token))
    conn.commit()
    return int(cur.rowcount or 0) == 1

def _codes_from_screener(conn: sqlite3.Connection) -> list[str]:
    try:
        rows=conn.execute("SELECT DISTINCT CAST(コード AS TEXT) FROM screener WHERE コード IS NOT NULL").fetchall()
        return [_normalize_code(r[0]) for r in rows if _normalize_code(r[0])]
    except Exception:
        return []




def run_forecast_backfill(
    years: int=5,
    codes: list[str]|None=None,
    debug: bool=True,
    limit: int|None=None,
    offset: int=0,
    resume: bool=True,
    force: bool=False,
    claim_ttl_minutes: int=_BACKFILL_DEFAULT_CLAIM_TTL_MINUTES,
) -> int:
    global _FAILED_DISCLOSURE_URLS
    _FAILED_DISCLOSURE_URLS.clear()

    worker_token = _make_backfill_claim_token()
    worker_short = worker_token.split(":")[-2] if ":" in worker_token else str(os.getpid())

    print(
        "[backfill] v25 + SAFE MULTI-WINDOW CLAIM + WORKER HEARTBEAT + "
        "v23 verified PDF guard + resume: ON"
    )
    print(
        f"[backfill][WORKER] pid={os.getpid()} token={worker_token} "
        f"claim_ttl={claim_ttl_minutes}min"
    )

    conn=get_db_conn()
    try:
        conn.execute("PRAGMA busy_timeout=60000;")
    except Exception:
        pass

    ensure_tdnet_documents_schema(conn)
    ensure_forecast_history_schema(conn)
    ensure_forecast_achievement_schema(conn)
    ensure_tdnet_xbrl_metrics_schema(conn)
    ensure_forecast_backfill_status_schema(conn)
    ensure_forecast_backfill_workers_schema(conn)
    _register_backfill_worker(conn, worker_token, years)

    try:
        cc=[_normalize_code(c) for c in (codes or []) if _normalize_code(c)]
        if not cc:
            cc=_codes_from_screener(conn)

        cc=list(dict.fromkeys(cc))
        original_count=len(cc)

        resume_skipped=0
        legacy_bootstrapped=0

        # 完了済みだけ先に除外。runningはCLAIM時に原子的に判定する。
        if resume and not force:
            pending=[]
            for code in cc:
                done, reason, _saved = _backfill_is_complete(
                    conn, code, years, bootstrap_legacy=False
                )
                if done:
                    resume_skipped += 1
                    if reason.startswith("legacy bootstrap"):
                        legacy_bootstrapped += 1
                    continue
                pending.append(code)
            cc=pending
            print(
                f"[backfill] RESUME: 完了済み {resume_skipped}社をスキップ / "
                f"候補 {len(cc)}社"
            )
        elif force:
            print("[backfill] FORCE: 完了済み判定を無視。active CLAIMは尊重します")
        else:
            print("[backfill] RESUME OFF: 完了済みも候補。active CLAIMは尊重します")

        try:
            offset_n=max(0,int(offset or 0))
        except Exception:
            offset_n=0
        if offset_n:
            before=len(cc)
            cc=cc[offset_n:]
            print(
                f"[backfill] OFFSET={offset_n} "
                f"候補 {before}社 -> {len(cc)}社"
            )

        claim_limit = None
        if limit is not None:
            try:
                n=int(limit)
                claim_limit = n if n > 0 else None
            except Exception:
                claim_limit = None

        if claim_limit:
            print(
                f"[backfill] LIMIT={claim_limit}: "
                "候補を先に切らず、CLAIM成功銘柄をこの件数まで処理"
            )

        if not cc:
            print("[backfill] 対象銘柄がありません。")
            return 0

        total=0
        completed=0
        errors=0
        claimed=0
        busy_skipped=0
        race_complete_skipped=0
        scanned=0

        for code in cc:
            _heartbeat_backfill_worker(conn, worker_token)
            if claim_limit is not None and claimed >= claim_limit:
                break

            scanned += 1

            ok, reason = _try_claim_backfill_code(
                conn,
                code,
                years=years,
                claim_token=worker_token,
                ttl_minutes=claim_ttl_minutes,
                skip_complete=(resume and not force),
            )

            if not ok:
                if reason.startswith("busy:"):
                    busy_skipped += 1
                    print(f"[backfill][CLAIM BUSY] {code}: {reason[5:]}")
                elif reason.startswith("complete:"):
                    race_complete_skipped += 1
                continue

            claimed += 1
            print(
                f"\n=== BACKFILL CLAIM {claimed}"
                + (f"/{claim_limit}" if claim_limit else "")
                + f" {code} [worker pid={os.getpid()}] ==="
            )

            try:
                saved=_backfill_one_code(
                    conn,code,years=years,debug=debug
                )
                total+=saved
                completed+=1

                owned = _finish_backfill_claim(
                    conn,
                    code,
                    claim_token=worker_token,
                    years=years,
                    saved_years=saved,
                    status="complete",
                )
                if not owned:
                    print(
                        f"[backfill][CLAIM LOST][WARN] {code}: "
                        "完了時にCLAIM所有権がありません"
                    )
                else:
                    print(
                        f"[backfill][COMPLETE] {code}: "
                        f"{saved}年度保存 → COMPLETE"
                    )
                _heartbeat_backfill_worker(conn, worker_token)

            except KeyboardInterrupt:
                _release_backfill_claim(
                    conn,
                    code,
                    claim_token=worker_token,
                    reason="KeyboardInterrupt",
                )
                print(
                    f"[backfill][INTERRUPTED] {code}: "
                    "CLAIMを解放しました。次回すぐ再開できます"
                )
                raise

            except Exception as e:
                errors+=1
                _finish_backfill_claim(
                    conn,
                    code,
                    claim_token=worker_token,
                    years=years,
                    saved_years=0,
                    status="error",
                    error=str(e)[:1000],
                )
                print(f"[backfill][ERROR] {code}: {e}")
                _heartbeat_backfill_worker(conn, worker_token)

        print(
            "\n========== BACKFILL SUMMARY ==========\n"
            f"worker PID        {os.getpid()}\n"
            f"元候補銘柄        {original_count}\n"
            f"完了済みSKIP      {resume_skipped}\n"
            f"候補走査           {scanned}\n"
            f"CLAIM成功          {claimed}\n"
            f"他窓処理中SKIP     {busy_skipped}\n"
            f"競合後完了SKIP     {race_complete_skipped}\n"
            f"正常完了           {completed}\n"
            f"ERROR              {errors}\n"
            f"保存年度数         {total}\n"
            f"要求年数/社        {years}\n"
            "======================================"
        )
        return total
    finally:
        _unregister_backfill_worker(conn, worker_token, status="stopped")
        conn.close()


def ensure_offerings_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offerings_events(
        コード        TEXT NOT NULL,
        銘柄名        TEXT,
        タイトル      TEXT,
        リンク        TEXT,
        発表日時      TEXT,
        提出時刻      TEXT NOT NULL,
        種別          TEXT,
        参照ID        TEXT,
        センチメント TEXT,
        created_at    TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(コード, 提出時刻, タイトル)
    );
    """)
    conn.commit()

def _classify_offering_kind(title: str) -> str:
    t = title or ""
    if any(k in t for k in ["行使","行使状況","新株予約権の行使"]): return "行使"
    if any(k in t for k in ["公募増資","第三者割当","第三者割当増資","新株発行","募集新株予約権","新株予約権発行","MSワラント","ワラント","ライツ・オファリング"]): return "増資"
    if any(k in t for k in ["株式の売出","自己株式の処分"]): return "売出/処分"
    if any(k in t for k in ["転換社債","CB","EB債"]): return "CB/EB"
    return "その他"

def upsert_offerings_events(conn: sqlite3.Connection, tdnet_items: list[dict]):
    cur = conn.cursor()
    for it in tdnet_items or []:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not title or not any(kw in title for kw in OFFERING_KW): continue

        raw = (td.get("company_code") or td.get("code") or td.get("company_code_raw") or "").strip()
        code = raw[:4] if re.fullmatch(r"\d{5}", raw) else (raw.upper() if re.fullmatch(r"\d{3}[0-9a-zA-Z]", raw) else (td.get("ticker") or "").strip())
        code = code or "0000"

        name  = (td.get("company_name") or "").strip() or code
        link  = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        pub   = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","").replace("/","-").strip()
        tei   = pub or now_iso()
        kind  = _classify_offering_kind(title)
        refid = td.get("id") or link or title
        _, label, _ = _summarize_title_simple(title)

        cur.execute("""
            INSERT INTO offerings_events(コード,銘柄名,タイトル,リンク,発表日時,提出時刻,種別,参照ID,センチメント)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
              銘柄名=excluded.銘柄名, リンク=excluded.リンク, 発表日時=excluded.発表日時,
              種別=excluded.種別, 参照ID=excluded.参照ID, センチメント=excluded.センチメント
        """, (code,name,title,link,pub,tei,kind,refid,label))
    conn.commit()

def ensure_tob_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tob_events (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      コード            TEXT NOT NULL,
      銘柄名            TEXT,
      タイトル          TEXT NOT NULL,
      リンク            TEXT,
      発表日時          TEXT,
      提出時刻          TEXT NOT NULL,
      種別              TEXT DEFAULT 'TOB',
      買付価格          REAL,
      下限価格          REAL,
      上限価格          REAL,
      目標保有比率      REAL,
      最低応募株数      INTEGER,
      メモ              TEXT
    );
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_tob_unique ON tob_events(コード, 提出時刻, タイトル);")
    conn.commit()

def _to_number(s):
    try: return float(str(s).replace(",", ""))
    except Exception: return None

def parse_tob_details(text: str) -> dict:
    t = text or ""
    out = {"買付価格": None, "下限価格": None, "上限価格": None, "目標保有比率": None, "最低応募株数": None}
    _NUM = r"(?:\d{1,3}(?:,\d{3})*|\d+)"
    m = re.search(r"(買付(?:け)?価格)[：:\s]*(" + _NUM + r")\s*円", t)
    if m: out["買付価格"] = _to_number(m.group(2))
    m = re.search(r"(?:買付(?:け)?价格|買付(?:け)?価額|買付(?:け)?価格)[^0-9]*(" + _NUM + r")\s*円\s*[~〜－-]\s*(" + _NUM + r")\s*円", t)
    if m: out["下限価格"] = _to_number(m.group(1)); out["上限価格"] = _to_number(m.group(2))
    m = re.search(r"(目標保有比率|保有比率)[：:\s]*(" + _NUM + r"(?:\.\d+)?)\s*%", t)
    if m: out["目標保有比率"] = _to_number(m.group(2))
    m = re.search(r"(最低応募株数)[：:\s]*(" + _NUM + r")\s*株", t)
    if m: out["最低応募株数"] = int(_to_number(m.group(2)) or 0)
    return out

def tdnet_item_to_tob_row(it: dict) -> dict:
    td = it.get("Tdnet") or it
    title = html.unescape(td.get("title") or "")
    raw_code = (td.get("company_code") or td.get("code") or "").strip()
    code = raw_code[:4] if (len(raw_code) >= 4 and raw_code[:4].isdigit()) else raw_code
    
    name = td.get("company_name") or td.get("name") or ""
    link = td.get("document_url") or td.get("pdf_url") or td.get("url") or ""
    pub = td.get("publish_datetime") or td.get("pubdate") or td.get("time") or ""
    time_str = (pub or "").replace("T"," ").replace("+09:00","").replace("/","-")

    details = parse_tob_details(title)
    return {
        "コード": code,
        "銘柄名": name,
        "タイトル": title,
        "リンク": link,
        "発表日時": time_str,
        "提出時刻": time_str or now_iso(),
        "種別": "TOB",
        "買付価格": details.get("買付価格"),
        "下限価格": details.get("下限価格"),
        "上限価格": details.get("上限価格"),
        "目標保有比率": details.get("目標保有比率"),
        "最低応募株数": details.get("最低応募株数"),
        "メモ": None,
    }

def upsert_tob_events(conn: sqlite3.Connection, rows: List[dict]):
    if not rows: return
    sql = """
    INSERT INTO tob_events
      (コード, 銘柄名, タイトル, リンク, 発表日時, 提出時刻, 種別,
       買付価格, 下限価格, 上限価格, 目標保有比率, 最低応募株数, メモ)
    VALUES
      (:コード, :銘柄名, :タイトル, :リンク, :発表日時, :提出時刻, :種別,
       :買付価格, :下限価格, :上限価格, :目標保有比率, :最低応募株数, :メモ)
    ON CONFLICT(コード, 提出時刻, タイトル) DO NOTHING
    """
    with conn:
        conn.executemany(sql, rows)

# ------------------ メイン処理 (データ取得と保存のみ) ------------------
def _parse_ts_str(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s2 = s.replace("T"," ").replace("+09:00","").replace("/","-").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try: return datetime.strptime(s2, fmt).replace(tzinfo=JST)
        except Exception: continue
    return None

def _latest_teishutsu_ts(conn: sqlite3.Connection) -> Optional[datetime]:
    cur = conn.cursor()
    latest: Optional[datetime] = None
    for tbl in ("earnings_events", "offerings_events", "tob_events"):
        try:
            # table存在確認
            cur.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{tbl}'")
            if not cur.fetchone(): continue
            
            ts = cur.execute(f"SELECT MAX(提出時刻) FROM {tbl}").fetchone()[0]
            dt = _parse_ts_str(ts)
            if dt and (latest is None or dt > latest): latest = dt
        except Exception: continue
    return latest

def _tdnet_item_pub_dt(it: dict) -> Optional[datetime]:
    td = it.get("Tdnet") or it
    s = (td.get("pubdate") or td.get("publish_datetime") or td.get("time") or "")
    return _parse_ts_str(s)

def _earn_row_dt(row: dict) -> Optional[datetime]:
    return _parse_ts_str(row.get("time") or "")

def _active_forecast_backfill_claims(conn: sqlite3.Connection, ttl_minutes: int = 360) -> list[tuple[str, str]]:
    """生きているforecast backfill CLAIMを返す。通常LIVEとのwriter/意味競合を避ける。"""
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='forecast_backfill_status'").fetchone():
            return []
        rows = conn.execute("""
            SELECT コード, claim_started_at, updated_at
            FROM forecast_backfill_status
            WHERE status='running'
        """).fetchall()
    except Exception:
        return []
    now_dt = datetime.now()
    out = []
    for r in rows:
        code = str(r[0] or "")
        started = _parse_backfill_status_dt(r[1]) or _parse_backfill_status_dt(r[2])
        if started is None:
            out.append((code, "age=?"))
            continue
        age = max(0.0, (now_dt - started).total_seconds() / 60.0)
        if age < max(1, int(ttl_minutes)):
            out.append((code, f"age={age:.1f}m"))
    return out


def main() -> int:
    print("=== fetch_all: TDnetデータ収集開始 ===")

    core_errors: list[str] = []
    enrichment_errors: list[str] = []

    # DB接続・スキーマ保証
    try:
        conn = get_db_conn()
        ensure_earnings_schema(conn)
        ensure_offerings_schema(conn)
        ensure_tob_schema(conn)
        ensure_tdnet_documents_schema(conn)
        ensure_forecast_history_schema(conn)
        ensure_forecast_achievement_schema(conn)
        ensure_tdnet_xbrl_metrics_schema(conn)
    except Exception as e:
        print(f"[FATAL] DB/schema initialization failed: {e}")
        return 1

    # 手動の5年forecast backfillが生きている間は通常LIVEを重ねない。
    # stale claimはbackfill側TTL回収に任せる。
    active_backfills = _active_forecast_backfill_claims(conn, _BACKFILL_DEFAULT_CLAIM_TTL_MINUTES)
    if active_backfills:
        sample = ", ".join(f"{c}({age})" for c, age in active_backfills[:5])
        print(f"[BUSY] forecast backfill running: {len(active_backfills)} codes / {sample}")
        try:
            conn.close()
        except Exception:
            pass
        return 2

    try:
        # 増分取得のチェックポイント
        since_dt = _latest_teishutsu_ts(conn)
        now_jst = datetime.now(JST).replace(microsecond=0)

        def span_days(max_cap: int) -> int:
            if not since_dt:
                return max_cap
            delta = now_jst - since_dt
            return min(max(1, int(delta.total_seconds() // 86400) + 2), max_cap)

        # 1. 決算情報（TDnet）の取得と解析
        tdnet_items = []
        earnings_rows = []
        try:
            # 最低2日を重複取得し、DB側UPSERTで重複排除する。
            earn_days = max(2, span_days(4)) if since_dt else 2
            tdnet_items = fetch_earnings_tdnet_only(days=earn_days, per_day_limit=300)
            earnings_rows = tdnet_items_to_earnings_rows(tdnet_items)
        except Exception as e:
            core_errors.append(f"earnings fetch/parse: {e}")
            print("[earnings] 取得/整形で例外:", e)

        # 2. 増資レーン
        offer_items = []
        try:
            offer_days = max(2, span_days(4)) if since_dt else 2
            offer_items = fetch_tdnet_by_keywords(days=offer_days, keywords=OFFERING_KW, per_day_limit=300)
        except Exception as e:
            core_errors.append(f"offerings fetch: {e}")
            print("[offerings] fetch error:", e)

        # 3. TOBレーン
        tob_rows = []
        try:
            tob_days = max(2, span_days(4)) if since_dt else 2
            tob_rows = fetch_tdnet_tob(days=tob_days, per_day_limit=300)
        except Exception as e:
            core_errors.append(f"TOB fetch: {e}")
            print("[TOB] fetch error:", e)

        # 4. DB保存。取得に成功したレーンだけでも保存するが、保存失敗はcore failure。
        try:
            upsert_offerings_events(conn, offer_items)
        except Exception as e:
            core_errors.append(f"offerings upsert: {e}")
            print("[offerings] upsert error:", e)

        try:
            upsert_tob_events(conn, tob_rows)
        except Exception as e:
            core_errors.append(f"TOB upsert: {e}")
            print("[TOB] upsert error:", e)

        try:
            conn.execute("BEGIN IMMEDIATE;")
            upsert_earnings_rows(conn, earnings_rows)
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            core_errors.append(f"earnings upsert: {e}")
            print("[earnings] upsert error:", e)

        # シンデン用enrichment。ここだけ失敗してもTDnetイベント本体は利用可能。
        saved_docs = 0
        try:
            saved_docs = upsert_tdnet_document_texts(conn, tdnet_items, max_workers=4)
        except Exception as e:
            enrichment_errors.append(f"tdnet_documents: {e}")
            print("[tdnet_documents] save error:", e)

        forecast_stats = {
            "targets": 0,
            "initial_saved": 0,
            "revision_saved": 0,
            "achievement_updated": 0,
            "unresolved": 0,
        }
        try:
            forecast_stats = update_daily_forecast_history(
                conn,
                tdnet_items,
                finalize=True,
                debug=False,
                source="tdnet_daily_v20",
            )
        except Exception as e:
            enrichment_errors.append(f"forecast_history: {e}")
            print("[forecast_history] daily update error:", e)

        print(
            f"[OK] 決算:{len(earnings_rows)}件, 増資:{len(offer_items)}件, "
            f"TOB:{len(tob_rows)}件, 本文:{saved_docs}件, "
            f"予想初期:{forecast_stats.get('initial_saved',0)}件, "
            f"予想修正:{forecast_stats.get('revision_saved',0)}件, "
            f"達成履歴更新:{forecast_stats.get('achievement_updated',0)}件 "
            "をDBに保存しました。"
        )

        if core_errors:
            print(f"[FAILED] TDnet core errors={len(core_errors)}")
            for x in core_errors:
                print("  -", x)
            return 1
        if enrichment_errors:
            print(f"[PARTIAL] TDnet coreは成功 / enrichment errors={len(enrichment_errors)}")
            for x in enrichment_errors:
                print("  -", x)
            return 2

        print("=== fetch_all: 完全成功 ===")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ============================================================================
# v29: 決算後株価反応ラベル・バックフィル
# ----------------------------------------------------------------------------
# 目的:
#   「シンデンに似ているか」ではなく、実際の決算後株価反応を正解教師にする。
#   TDnetの決算短信を5年分拾い、Yahoo Financeの日足から
#   D1/D3/D5の反応を earnings_reaction_labels に保存する。
#
# 方針:
#   - 決算跨ぎの教師に使うため、デフォルトは「引け後発表」のみ。
#   - 2024-11-05以降は東証15:30引け、それ以前は15:00引けとして判定。
#   - 株式分割等の影響を抑えるため、Yahooのadjclose/close比でOHLCを調整。
#   - 5段階ラベル: D1終値 +5%以上=2, +2%以上=1, -2%超=0,
#                   -5%超=-1, -5%以下=-2
# ============================================================================


def ensure_earnings_reaction_labels_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS earnings_reaction_labels(
        コード TEXT NOT NULL,
        銘柄名 TEXT,
        発表日時 TEXT NOT NULL,
        タイトル TEXT NOT NULL,
        URL TEXT,
        決算種別 TEXT,
        引け後 INTEGER DEFAULT 0,
        reaction_mode TEXT,
        基準日 TEXT,
        基準終値 REAL,
        D1日付 TEXT,
        D1始値騰落率 REAL,
        D1高値騰落率 REAL,
        D1安値騰落率 REAL,
        D1終値騰落率 REAL,
        D1出来高比20日 REAL,
        D1始値比終値_pct REAL,
        D3最大高値騰落率 REAL,
        D3最大下値騰落率 REAL,
        D3終値騰落率 REAL,
        D5最大高値騰落率 REAL,
        D5最大下値騰落率 REAL,
        D5終値騰落率 REAL,
        反応ラベル INTEGER,
        翌日5pct上昇 INTEGER,
        翌日5pct下落 INTEGER,
        price_source TEXT DEFAULT 'yahoo_chart',
        updated_at TEXT,
        PRIMARY KEY(コード, 発表日時, タイトル)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_earnings_reaction_code_date ON earnings_reaction_labels(コード, 発表日時 DESC);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_earnings_reaction_label ON earnings_reaction_labels(反応ラベル, 引け後);")
    conn.commit()


def _reaction_period_type(title: str) -> str:
    t = str(title or "")
    if any(x in t for x in ("第1四半期","第１四半期")): return "1Q"
    if any(x in t for x in ("第2四半期","第２四半期","中間期","中間決算")): return "2Q/H1"
    if any(x in t for x in ("第3四半期","第３四半期")): return "3Q"
    return "FY"


def _is_reaction_earnings_title(title: str) -> bool:
    t = str(title or "")
    if "決算短信" not in t:
        return False
    # 訂正・説明資料は別イベントとして二重計上しない
    bad = ("訂正", "決算説明", "説明資料", "補足資料", "補足説明")
    return not any(x in t for x in bad)


def _tse_close_minutes(d: date) -> int:
    # 2024-11-05から現物市場の取引終了が15:30へ延長
    return 15 * 60 + (30 if d >= date(2024, 11, 5) else 0)


def _is_after_close_release(dt: datetime) -> bool:
    mins = dt.hour * 60 + dt.minute
    return mins >= _tse_close_minutes(dt.date())


def _yahoo_symbol(code: str) -> str:
    return f"{_normalize_code(code)}.T"


def _fetch_yahoo_daily(code: str, start_d: date, end_d: date) -> list[dict]:
    """Yahoo Finance chart APIから調整済み日足を取得。end_dは包含扱い。"""
    symbol = _yahoo_symbol(code)
    p1 = int(datetime(start_d.year, start_d.month, start_d.day, tzinfo=JST).timestamp())
    p2d = end_d + timedelta(days=2)
    p2 = int(datetime(p2d.year, p2d.month, p2d.day, tzinfo=JST).timestamp())
    params = f"period1={p1}&period2={p2}&interval=1d&events=history%2Cdiv%2Csplits&includeAdjustedClose=true"
    urls = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{params}",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?{params}",
    ]
    js = None
    last_err = None
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/142 Safari/537.36"}
    for u in urls:
        try:
            r = requests.get(u, headers=headers, timeout=12)
            if r.status_code == 200:
                js = r.json()
                break
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
    if not js:
        raise RuntimeError(f"Yahoo price fetch failed {code}: {last_err}")

    result = (((js or {}).get("chart") or {}).get("result") or [])
    if not result:
        err = ((js or {}).get("chart") or {}).get("error")
        raise RuntimeError(f"Yahoo no price result {code}: {err}")
    z = result[0]
    ts = z.get("timestamp") or []
    q = (((z.get("indicators") or {}).get("quote") or [{}])[0])
    ac = (((z.get("indicators") or {}).get("adjclose") or [{}])[0]).get("adjclose") or []
    op = q.get("open") or []; hi = q.get("high") or []; lo = q.get("low") or []
    cl = q.get("close") or []; vo = q.get("volume") or []

    rows = []
    n = len(ts)
    for i in range(n):
        try:
            c = cl[i] if i < len(cl) else None
            if c is None or float(c) <= 0:
                continue
            adj = ac[i] if i < len(ac) else None
            fac = (float(adj) / float(c)) if adj is not None and float(c) else 1.0
            def av(arr):
                v = arr[i] if i < len(arr) else None
                return None if v is None else float(v) * fac
            d = datetime.fromtimestamp(int(ts[i]), tz=timezone.utc).astimezone(JST).date()
            rows.append({
                "date": d,
                "open": av(op), "high": av(hi), "low": av(lo), "close": av(cl),
                "volume": float(vo[i]) if i < len(vo) and vo[i] is not None else None,
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["date"])
    return rows


def _pct(v: float|None, base: float|None) -> float|None:
    if v is None or base is None or base == 0:
        return None
    return (float(v) / float(base) - 1.0) * 100.0


def _reaction_label(p: float|None) -> int|None:
    if p is None: return None
    if p >= 5.0: return 2
    if p >= 2.0: return 1
    if p > -2.0: return 0
    if p > -5.0: return -1
    return -2


def _event_reaction_from_prices(pub_dt: datetime, prices: list[dict]) -> dict|None:
    if not prices:
        return None
    pd = pub_dt.date()
    after_close = _is_after_close_release(pub_dt)
    mins = pub_dt.hour * 60 + pub_dt.minute

    # 発表後の「最初に反応を含むセッション」を決める。
    # 引け後: 翌取引日 / 寄り前: 当日 / 場中: 当日
    if after_close:
        candidates = [i for i,r in enumerate(prices) if r["date"] > pd]
        mode = "next_session_after_close"
    else:
        candidates = [i for i,r in enumerate(prices) if r["date"] >= pd]
        mode = "same_session_preopen" if mins < 9*60 else "same_session_intraday"
    if not candidates:
        return None
    i1 = candidates[0]
    if i1 <= 0:
        return None

    # 基準は反応セッション直前の終値。
    base = prices[i1-1]
    d1 = prices[i1]
    base_close = base.get("close")
    if base_close is None or float(base_close) <= 0:
        return None

    def window(k: int) -> list[dict]:
        return prices[i1:min(len(prices), i1+k)]

    w3 = window(3); w5 = window(5)
    def max_high(w):
        xs=[r.get("high") for r in w if r.get("high") is not None]
        return max(xs) if xs else None
    def min_low(w):
        xs=[r.get("low") for r in w if r.get("low") is not None]
        return min(xs) if xs else None
    def end_close(w):
        return w[-1].get("close") if w else None

    # D1出来高 / 直前20営業日平均出来高
    prev20 = prices[max(0,i1-20):i1]
    pv = [r.get("volume") for r in prev20 if r.get("volume") is not None and r.get("volume") > 0]
    vavg = (sum(pv)/len(pv)) if pv else None
    vratio = (d1.get("volume")/vavg) if vavg and d1.get("volume") is not None else None

    d1_close_pct = _pct(d1.get("close"), base_close)
    d1_open_pct = _pct(d1.get("open"), base_close)
    d1_close_from_open = _pct(d1.get("close"), d1.get("open"))
    return {
        "引け後": 1 if after_close else 0,
        "reaction_mode": mode,
        "基準日": base["date"].isoformat(),
        "基準終値": float(base_close),
        "D1日付": d1["date"].isoformat(),
        "D1始値騰落率": d1_open_pct,
        "D1高値騰落率": _pct(d1.get("high"), base_close),
        "D1安値騰落率": _pct(d1.get("low"), base_close),
        "D1終値騰落率": d1_close_pct,
        "D1出来高比20日": vratio,
        "D1始値比終値_pct": d1_close_from_open,
        "D3最大高値騰落率": _pct(max_high(w3), base_close),
        "D3最大下値騰落率": _pct(min_low(w3), base_close),
        "D3終値騰落率": _pct(end_close(w3), base_close),
        "D5最大高値騰落率": _pct(max_high(w5), base_close),
        "D5最大下値騰落率": _pct(min_low(w5), base_close),
        "D5終値騰落率": _pct(end_close(w5), base_close),
        "反応ラベル": _reaction_label(d1_close_pct),
        "翌日5pct上昇": 1 if d1_close_pct is not None and d1_close_pct >= 5.0 else 0,
        "翌日5pct下落": 1 if d1_close_pct is not None and d1_close_pct <= -5.0 else 0,
    }


def _save_reaction_row(conn: sqlite3.Connection, row: dict):
    cols = [
        "コード","銘柄名","発表日時","タイトル","URL","決算種別","引け後","reaction_mode",
        "基準日","基準終値","D1日付","D1始値騰落率","D1高値騰落率","D1安値騰落率","D1終値騰落率",
        "D1出来高比20日","D1始値比終値_pct","D3最大高値騰落率","D3最大下値騰落率","D3終値騰落率",
        "D5最大高値騰落率","D5最大下値騰落率","D5終値騰落率","反応ラベル","翌日5pct上昇","翌日5pct下落",
        "price_source","updated_at"
    ]
    q = ",".join("?" for _ in cols)
    colsql = ",".join(f'"{c}"' for c in cols)
    updates = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c not in ("コード","発表日時","タイトル"))
    conn.execute(
        f'INSERT INTO earnings_reaction_labels({colsql}) VALUES({q}) '
        f'ON CONFLICT(コード,発表日時,タイトル) DO UPDATE SET {updates}',
        [row.get(c) for c in cols]
    )


def run_earnings_reaction_backfill(
    years: int=2,
    codes: list[str]|None=None,
    limit: int|None=None,
    offset: int=0,
    after_close_only: bool=True,
    fetch_docs: bool=False,
    skip_existing: bool=True,
) -> int:
    """
    TDnet決算短信×実株価反応の教師データを作る高速版。

    v30 FAST:
      - reaction用途ではPDF本文を既定で取得しない（最大の時間短縮）
      - DBに既にあるイベントは再計算しない
      - offset/limitで分割実行できる
      - Yahoo日足は「未保存イベントがある会社」だけ取得
    """
    conn = get_db_conn()
    ensure_tdnet_documents_schema(conn)
    ensure_earnings_reaction_labels_schema(conn)
    try:
        cc=[_normalize_code(c) for c in (codes or []) if _normalize_code(c)]
        if not cc:
            cc=_codes_from_screener(conn)
        cc=list(dict.fromkeys(cc))

        try:
            off=max(0,int(offset or 0))
        except Exception:
            off=0
        if off:
            cc=cc[off:]

        if limit is not None:
            try:
                lim=int(limit)
            except Exception:
                lim=0
            if lim > 0:
                cc=cc[:lim]

        print(
            f"[reaction] v30 FAST REAL MARKET LABELS: 対象 {len(cc)}社 / {years}年 / "
            f"引け後のみ={after_close_only} / docs={fetch_docs} / resume={skip_existing} / offset={off}"
        )

        cutoff = datetime.now(JST) - timedelta(days=max(1,int(years))*370 + 30)
        cutoff_s = cutoff.strftime("%Y-%m-%d")
        total=0; no_event=0; no_price=0; already_done=0

        for idx,code in enumerate(cc,1):
            print(f"\n=== REACTION {idx}/{len(cc)} {code} ===")
            try:
                items = fetch_tdnet_by_code(code, limit=600)
                events=[]
                for it in items:
                    c,n,t,u,p = _tdnet_fields(it)
                    if not _is_reaction_earnings_title(t):
                        continue
                    dt = _parse_ts_str(p)
                    if not dt or dt < cutoff:
                        continue
                    if after_close_only and not _is_after_close_release(dt):
                        continue
                    events.append((it,c or code,n,t,u,p,dt))
                events.sort(key=lambda x:x[-1])

                if not events:
                    print(f"[reaction] {code}: 対象決算なし")
                    no_event += 1
                    continue

                # 既保存イベントを除外。これで再実行がほぼ瞬時になる。
                if skip_existing:
                    existing = {
                        (str(r[0]), str(r[1]))
                        for r in conn.execute(
                            """
                            SELECT 発表日時, タイトル
                            FROM earnings_reaction_labels
                            WHERE コード=? AND substr(発表日時,1,10)>=?
                            """,
                            (code, cutoff_s),
                        ).fetchall()
                    }
                    before=len(events)
                    events=[e for e in events if (str(e[5]), str(e[3])) not in existing]
                    if not events:
                        already_done += 1
                        print(f"[reaction][RESUME] {code}: {before}件すべて保存済み -> SKIP")
                        continue
                    if before != len(events):
                        print(f"[reaction][RESUME] {code}: 保存済み {before-len(events)}件を除外 / 未保存 {len(events)}件")

                # 教師ラベル作成だけならPDF本文は不要。必要な時だけ明示取得。
                if fetch_docs:
                    try:
                        upsert_tdnet_document_texts(conn,[x[0] for x in events],forced_code=code)
                    except Exception as e:
                        print(f"[reaction][DOC WARN] {code}: {e}")

                min_d = min(x[-1].date() for x in events) - timedelta(days=35)
                max_d = max(x[-1].date() for x in events) + timedelta(days=15)
                prices = _fetch_yahoo_daily(code,min_d,max_d)
                if not prices:
                    print(f"[reaction] {code}: 株価なし")
                    no_price += 1
                    continue

                saved=0
                for _it,c,n,t,u,p,dt in events:
                    rr = _event_reaction_from_prices(dt,prices)
                    if not rr:
                        print(f"[reaction][SKIP] {code} {p} 反応セッション不足 {t[:35]}")
                        continue
                    row={
                        "コード":code,"銘柄名":n,"発表日時":p,"タイトル":t,"URL":u,
                        "決算種別":_reaction_period_type(t),
                        **rr,
                        "price_source":"yahoo_chart","updated_at":now_iso(),
                    }
                    _save_reaction_row(conn,row)
                    saved += 1; total += 1
                    print(
                        f"[reaction][SAVE] {code} {p[:16]} {_reaction_period_type(t)} "
                        f"D1={rr['D1終値騰落率']:+.2f}% label={rr['反応ラベル']} "
                        f"D3high={rr['D3最大高値騰落率']:+.2f}% "
                        f"vol={rr['D1出来高比20日'] if rr['D1出来高比20日'] is not None else '-'}"
                    )
                conn.commit()
                print(f"[reaction] {code}: {saved}件保存")
                time.sleep(0.05)
            except KeyboardInterrupt:
                print("\n[reaction] Ctrl+C: ここまでの保存分はDBに残っています。")
                conn.commit()
                raise
            except Exception as e:
                print(f"[reaction][ERROR] {code}: {type(e).__name__}: {e}")
                try: conn.rollback()
                except Exception: pass

        try:
            s=conn.execute("""
                SELECT COUNT(*),
                       SUM(CASE WHEN 反応ラベル=2 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN 反応ラベル=-2 THEN 1 ELSE 0 END),
                       AVG(D1終値騰落率)
                FROM earnings_reaction_labels
                WHERE 引け後=1
            """).fetchone()
            print(
                f"\n[reaction] 完了: 今回保存 {total}件 / DB引け後総数 {int(s[0] or 0)}件 / "
                f"+5%正例 {int(s[1] or 0)} / -5%負例 {int(s[2] or 0)} / "
                f"D1平均 {float(s[3] or 0):+.2f}% / 保存済み会社SKIP {already_done}"
            )
        except Exception:
            print(f"\n[reaction] 完了: 今回保存 {total}件")
        return total
    finally:
        conn.close()



# ============================================================================
# v31: 反応教師の極端例だけ決算本文を高速補完
# ============================================================================
def _is_stock_like_code_for_training(code: str) -> bool:
    c=str(code or "").strip().upper()
    return bool(re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", c))


def _reaction_training_group_key(title: str) -> tuple[str,str]:
    fk = None
    try:
        fk = _title_fiscal_key(title)
    except Exception:
        fk = None
    return (str(fk or ""), _reaction_period_type(title))


def hydrate_reaction_training_docs(
    years: int=2,
    per_class: int=800,
    workers: int=6,
) -> int:
    """
    反応ラベルのうち +5%以上(label=2) / -5%以下(label=-2) をバランス抽出し、
    「今回発表された決算短信本文」だけ tdnet_documents に補完する。

    決算アルゴ模倣では current release PDF は未来情報ではなく、まさに発表直後に読む入力。
    全23,000件を取らず、1Q/2Q/3Q/FYを均等にした極端例だけ取る。
    """
    conn=get_db_conn()
    ensure_tdnet_documents_schema(conn)
    ensure_earnings_reaction_labels_schema(conn)
    cutoff=(datetime.now(JST)-timedelta(days=max(1,int(years))*370+30)).strftime("%Y-%m-%d")
    try:
        rows=conn.execute("""
            SELECT コード,銘柄名,発表日時,タイトル,URL,決算種別,反応ラベル,D1終値騰落率
            FROM earnings_reaction_labels
            WHERE 引け後=1
              AND 反応ラベル IN (2,-2)
              AND substr(発表日時,1,10)>=?
              AND URL IS NOT NULL AND TRIM(URL)<>''
            ORDER BY 発表日時 DESC
        """,(cutoff,)).fetchall()

        # 非株式コード、訂正/再掲系を除外し、同一 fiscal×quarter の重複は最初の市場イベントだけ残す。
        cleaned=[]
        seen=set()
        for r in sorted(rows,key=lambda x:str(x[2] or "")):
            code=str(r[0] or "").strip().upper()
            title=str(r[3] or "")
            if not _is_stock_like_code_for_training(code):
                continue
            if any(k in title for k in ("訂正","再訂正")):
                continue
            fk,pt=_reaction_training_group_key(title)
            # fiscal keyが取れない場合は日付を含め、過剰dedupを避ける
            grp=(code,fk or str(r[2] or "")[:10],pt)
            if grp in seen:
                continue
            seen.add(grp)
            cleaned.append(r)

        # 決算種別を均等化して、銘柄偏重も軽減。
        period_order=("1Q","2Q/H1","3Q","FY")
        selected=[]
        for label in (2,-2):
            pool=[r for r in cleaned if int(r[6])==label]
            quota=max(1,int(per_class)//4)
            chosen=[]
            chosen_ids=set()
            for pt in period_order:
                cand=[r for r in pool if str(r[5] or _reaction_period_type(str(r[3] or "")))==pt]
                # 大きい反応だけに偏りすぎないよう、日付順に等間隔サンプリング
                cand=sorted(cand,key=lambda x:str(x[2] or ""))
                if len(cand)<=quota:
                    pick=cand
                else:
                    step=len(cand)/float(quota)
                    pick=[cand[min(len(cand)-1,int(i*step))] for i in range(quota)]
                for r in pick:
                    key=(str(r[0]),str(r[2]),str(r[3]))
                    if key not in chosen_ids:
                        chosen.append(r); chosen_ids.add(key)

            # quota不足を他periodから補充
            if len(chosen)<int(per_class):
                rest=[r for r in pool if (str(r[0]),str(r[2]),str(r[3])) not in chosen_ids]
                rest=sorted(rest,key=lambda x:str(x[2] or ""))
                need=int(per_class)-len(chosen)
                if rest and need>0:
                    if len(rest)<=need:
                        fill=rest
                    else:
                        step=len(rest)/float(need)
                        fill=[rest[min(len(rest)-1,int(i*step))] for i in range(need)]
                    chosen.extend(fill)
            selected.extend(chosen[:int(per_class)])

        # 既存本文キャッシュを除外
        missing=[]
        cached=0
        for r in selected:
            code,name,pub,title,url,ptype,label,d1=r
            c=conn.execute("""
                SELECT length(本文) FROM tdnet_documents
                WHERE コード=? AND 提出時刻=? AND タイトル=?
                LIMIT 1
            """,(str(code),str(pub),str(title))).fetchone()
            if c and int(c[0] or 0)>=50:
                cached+=1
                continue
            missing.append(r)

        print(
            f"[reaction-docs] v31 EXTREME PDF HYDRATE: "
            f"候補clean={len(cleaned)} / 選抜={len(selected)} "
            f"(正例<={per_class}, 負例<={per_class}) / cache={cached} / download={len(missing)} / workers={workers}"
        )
        if not missing:
            print("[reaction-docs] すべて本文取得済み")
            return 0

        def _worker(r):
            code,name,pub,title,url,ptype,label,d1=r
            item={"Tdnet":{
                "company_code":str(code),
                "company_name":str(name or ""),
                "title":str(title or ""),
                "document_url":str(url or ""),
                "pubdate":str(pub or ""),
            }}
            try:
                txt,resolved,kind=_download_disclosure_text(item,timeout=8)
                return r,txt,resolved,kind,None
            except Exception as e:
                return r,"","","",e

        saved=0; failed=0
        maxw=max(1,min(12,int(workers or 1)))
        with ThreadPoolExecutor(max_workers=maxw) as ex:
            futs=[ex.submit(_worker,r) for r in missing]
            for i,fut in enumerate(as_completed(futs),1):
                r,txt,resolved,kind,err=fut.result()
                code,name,pub,title,url,ptype,label,d1=r
                if err or not txt or len(str(txt).strip())<50:
                    failed+=1
                    if i<=20 or i%100==0:
                        print(f"[reaction-docs][FAIL] {code} {str(pub)[:10]} {title[:38]} {err or 'empty'}")
                    continue
                body=str(txt)
                final_url=str(resolved or url or "")
                h=hashlib.sha256(body.encode("utf-8","ignore")).hexdigest()
                conn.execute("""
                    INSERT INTO tdnet_documents(コード,銘柄名,タイトル,URL,提出時刻,document_type,本文,text_hash)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
                      銘柄名=excluded.銘柄名,URL=excluded.URL,document_type=excluded.document_type,
                      本文=excluded.本文,text_hash=excluded.text_hash
                """,(str(code),str(name or ""),str(title),final_url,str(pub),
                     _classify_shinden_doc(str(title)),body,h))
                saved+=1
                if saved%25==0:
                    conn.commit()
                if i%50==0 or i==len(missing):
                    print(f"[reaction-docs] {i}/{len(missing)} saved={saved} failed={failed}")

        conn.commit()
        print(
            f"[reaction-docs] 完了: 新規本文 {saved}件 / cache {cached}件 / failed {failed}件 / "
            f"選抜総数 {len(selected)}件"
        )
        return saved
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--backfill-forecast-history", action="store_true", help="過去の期初予想→修正→実績をTDnetから復元")
    parser.add_argument("--backfill-reaction-labels", action="store_true", help="過去の決算短信→決算後株価反応を正解教師として保存")
    parser.add_argument("--include-intraday-reactions", action="store_true", help="株価反応バックフィルで場中・寄り前発表も含める（既定は引け後のみ）")
    parser.add_argument("--reaction-fetch-docs", action="store_true", help="反応ラベル作成時にも決算PDF本文を取得する（遅い。既定OFF）")
    parser.add_argument("--no-reaction-resume", action="store_true", help="既保存の反応イベントも再計算する")
    parser.add_argument("--hydrate-reaction-docs", action="store_true", help="+5%%/-5%%極端反応の決算短信本文だけ教師用に補完")
    parser.add_argument("--reaction-docs-per-class", type=int, default=800, help="本文補完する正例/負例の最大件数（各クラス）")
    parser.add_argument("--reaction-doc-workers", type=int, default=6, help="本文ダウンロード並列数")
    parser.add_argument("--years", type=int, default=5, help="バックフィル年度数")
    parser.add_argument("--codes", nargs="*", default=None, help="対象コード。省略時はscreener全銘柄")
    parser.add_argument("--limit", type=int, default=None, help="未完了銘柄から今回処理する件数。例: --limit 20")
    parser.add_argument("--offset", type=int, default=0, help="未完了銘柄リストの先頭からN社飛ばす")
    parser.add_argument("--resume", dest="resume", action="store_true", help="完了済み会社を丸ごとスキップ（デフォルトON）")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="完了済み会社も再処理")
    parser.set_defaults(resume=True)
    parser.add_argument("--force-backfill", action="store_true", help="完了状態を無視して指定銘柄を強制再バックフィル")
    parser.add_argument("--claim-ttl-minutes", type=int, default=360, help="異常終了したrunning CLAIMを回収するまでの分数（デフォルト360分）")
    parser.add_argument("--no-backfill-debug", action="store_true", help="バックフィルの抽出DEBUG表示を抑制")
    args, _unknown = parser.parse_known_args()

    _special_ran = False
    if args.backfill_forecast_history:
        run_forecast_backfill(
            years=max(1,args.years),
            codes=args.codes,
            debug=not args.no_backfill_debug,
            limit=args.limit,
            offset=args.offset,
            resume=args.resume,
            force=args.force_backfill,
            claim_ttl_minutes=max(1,args.claim_ttl_minutes),
        )
        _special_ran = True

    if args.backfill_reaction_labels:
        run_earnings_reaction_backfill(
            years=max(1,args.years),
            codes=args.codes,
            limit=args.limit,
            offset=args.offset,
            after_close_only=not args.include_intraday_reactions,
            fetch_docs=args.reaction_fetch_docs,
            skip_existing=not args.no_reaction_resume,
        )
        _special_ran = True

    if args.hydrate_reaction_docs:
        hydrate_reaction_training_docs(
            years=max(1,args.years),
            per_class=max(50,args.reaction_docs_per_class),
            workers=max(1,args.reaction_doc_workers),
        )
        _special_ran = True

    if not _special_ran:
        raise SystemExit(main())
