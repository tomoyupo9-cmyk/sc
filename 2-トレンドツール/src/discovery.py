# -*- coding: utf-8 -*-
"""
discovery.py (hybrid-fast)
Yahoo掲示板ランキング + 株探ニュースで見つかった4桁コードの union をユニバースにし、
各コードに aliases/news_query を付与する。既存の build_universe(cfg) 互換。
"""

from __future__ import annotations
import re
import time
import html
from typing import Iterable, Optional, Set, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore
from bs4 import BeautifulSoup

HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36"
}
DEFAULT_TIMEOUT: int = 10
_SESSION: Optional[requests.Session] = None

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    try:
        retry = Retry(total=3, read=3, connect=3, backoff_factor=0.3,
                      status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=frozenset(["GET"]))
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=retry)
    except Exception:
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32)
    s.mount("http://", adpt); s.mount("https://", adpt)
    _SESSION = s
    return s

def _sleep_ms(ms: int) -> None:
    if ms and ms > 0:
        time.sleep(ms/1000.0)

def _safe_get(url: str, timeout: Optional[int] = None) -> Optional[requests.Response]:
    if timeout is None: timeout = DEFAULT_TIMEOUT
    try:
        return _get_session().get(url, headers=HEADERS, timeout=timeout)
    except Exception:
        return None

def _extract_codes_from_text(text: str) -> Set[str]:
    if not text: return set()
    return {m.group(1) for m in re.finditer(r"\b(\d{4})\b", text)}

# ===== Yahoo!掲示板ランキング =====
def _parse_yahoo_ranking_html(html_text: str) -> Set[str]:
    codes: Set[str] = set()
    soup = BeautifulSoup(html_text, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href", "") or ""
        m = re.search(r"/quote/(\d{4})(?:\.T)?(?:/bbs)?(?:[/?#]|$)", href)
        if m:
            codes.add(m.group(1))
    codes |= _extract_codes_from_text(soup.get_text(" ", strip=True))
    return codes

def discover_yahoo_bbs_hot(max_workers: int = 6) -> Set[str]:
    urls = [
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=daily&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=daily&page=2",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=weekly&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=weekly&page=2",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=monthly&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=monthly&page=2",
    ]
    found: Set[str] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_safe_get, u): u for u in urls}
        for fut in as_completed(futs):
            r = fut.result()
            if r and r.status_code == 200:
                try:
                    found |= _parse_yahoo_ranking_html(r.text)
                except Exception:
                    pass
    return found

# ===== 株探ニュース（一覧→記事本文）=====
def discover_kabutan_news(max_articles: int = 80, article_sleep_ms: int = 300, max_workers: int = 4) -> Set[str]:
    found: Set[str] = set()
    r = _safe_get("https://kabutan.jp/news/")
    if not r or r.status_code != 200:
        return found
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("a[href*='/stock/?code=']"):
        href = a.get("href", "")
        m = re.search(r"code=(\d{4})", href)
        if m: found.add(m.group(1))
    links = list(dict.fromkeys([a.get("href","") for a in soup.select("a[href^='/news/']")]))
    links = [p for p in links if p][:max_articles]

    def _fetch_article_codes(path: str) -> Set[str]:
        try:
            art = _safe_get(f"https://kabutan.jp{path}")
            if not art or art.status_code != 200:
                _sleep_ms(article_sleep_ms); return set()
            s = BeautifulSoup(art.text, "lxml")
            codes: Set[str] = set()
            for a in s.select("a[href*='/stock/?code=']"):
                href = a.get("href","")
                m = re.search(r"code=(\d{4})", href)
                if m: codes.add(m.group(1))
            codes |= _extract_codes_from_text(s.get_text(" ", strip=True))
            return codes
        finally:
            _sleep_ms(article_sleep_ms)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_fetch_article_codes, p) for p in links]
        for fut in as_completed(futs):
            try:
                found |= (fut.result() or set())
            except Exception:
                pass
    return found

# ===== 名称解決・バリデーション =====
_NAME_CACHE: Dict[str, str] = {}
TRAILING_NOISE = re.compile(
    r"(?:\s*[-|｜／/・|\|]\s*)?(?:株価|チャート|掲示板|ニュース|決算(?:速報)?|適時開示|基本情報|時価総額|配当|プロフィール|企業情報|業績|株主優待|投資情報)(?:.*)?$"
)

def _clean_name(name: str) -> str:
    if not name: return ""
    n = html.unescape(name).strip()
    n = TRAILING_NOISE.sub("", n).strip()
    n = re.sub(r"【\s*\d{4}\s*】(?:.*)?$", "", n).strip()
    n = re.sub(r"[（(][^（）()]{1,30}[)）]\s*$", "", n).strip()
    n = re.sub(r"\s*[｜|／/・]\s*", " ", n).strip()
    n = re.sub(r"^(?:株式会社|\(株\))\s*", "", n)
    return n

def lookup_name(code: str, interval_ms: int = 120) -> str:
    if code in _NAME_CACHE:
        _sleep_ms(interval_ms); return _NAME_CACHE[code]
    # 株探
    r = _safe_get(f"https://kabutan.jp/stock/?code={code}")
    if r and r.status_code == 200:
        try:
            s = BeautifulSoup(r.text, "lxml")
            for el in (s.find("title"), s.select_one("h1, h2")):
                if el and el.get_text(strip=True):
                    nm = _clean_name(el.get_text(strip=True))
                    if nm: _NAME_CACHE[code] = nm; _sleep_ms(interval_ms); return nm
        except Exception: pass
    # Yahoo
    r = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    if r and r.status_code == 200:
        try:
            s = BeautifulSoup(r.text, "lxml")
            for el in (s.find("title"), s.select_one("h1, h2")):
                if el and el.get_text(strip=True):
                    nm = _clean_name(el.get_text(strip=True))
                    if nm: _NAME_CACHE[code] = nm; _sleep_ms(interval_ms); return nm
        except Exception: pass
    _sleep_ms(interval_ms); _NAME_CACHE.setdefault(code,""); return ""

def validate_code_exists(code: str, interval_ms: int = 100) -> bool:
    r = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    ok = False
    if r and r.status_code == 200:
        try:
            s = BeautifulSoup(r.text, "lxml")
            t = s.find("title")
            if t and t.text and re.search(rf"\b{re.escape(code)}\b", t.text):
                ok = True
        except Exception: ok = False
    _sleep_ms(interval_ms); return ok

_NON_EQUITY_KEYWORDS = ["ETF","ＥＴＦ","ETN","ＥＴＮ","投資信託","上場投信","投資法人","投資証券","指数","先物","レバレッジ","ダブル","インバース"]
_NON_EQUITY_CODE_PREFIXES = ("13","15","16","20","25")
def _is_non_equity(code: str, name: str) -> bool:
    n = (name or "").upper()
    if any(k.upper() in n for k in _NON_EQUITY_KEYWORDS): return True
    return any(code.startswith(p) for p in _NON_EQUITY_CODE_PREFIXES)

def _print_codes_with_names(label: str, codes: Iterable[str], interval_ms: int = 120) -> None:
    codes = sorted({c for c in codes if re.fullmatch(r"\d{4}", c)})
    print(f"\n=== {label} ===")
    for c in codes:
        name = lookup_name(c, interval_ms=interval_ms)
        if _is_non_equity(c, name): continue
        print(f"{c}：{name or c}")

# ===== エントリ =====
def build_universe(cfg: Dict) -> List[Dict]:
    ad = cfg.get("auto_discover", {}) or {}
    http_cfg = cfg.get("http", {}) or {}
    # HTTP設定
    if http_cfg.get("user_agent"): HEADERS["User-Agent"] = str(http_cfg["user_agent"])
    global DEFAULT_TIMEOUT
    if "timeout" in http_cfg:
        try: DEFAULT_TIMEOUT = int(http_cfg.get("timeout", DEFAULT_TIMEOUT))
        except Exception: pass

    kabutan_max_articles = int(ad.get("kabutan_max_articles", 80))
    sm = (ad.get("sleep_ms") or {})
    sleep_lookup = int(sm.get("lookup_name", 120))
    sleep_validate = int(sm.get("validate_code", 100))
    sleep_kabutan_article = int(sm.get("kabutan_article", 300))

    max_workers_discovery = int(ad.get("max_workers", 6))
    max_workers_namecheck = max(2, min(6, int(ad.get("max_workers_namecheck", 4))))

    # === 収集（Google RSS は除外）===
    s_yahoo = s_kabutan = set()
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut1 = ex.submit(discover_yahoo_bbs_hot, max_workers=min(6, max_workers_discovery))
        fut2 = ex.submit(discover_kabutan_news, kabutan_max_articles, sleep_kabutan_article, min(4, max_workers_discovery))
        for _ in as_completed([fut1, fut2]):
            pass
        s_yahoo = fut1.result() or set()
        s_kabutan = fut2.result() or set()

    _print_codes_with_names("1) Yahoo掲示板ランキング（日/週/月）", s_yahoo, interval_ms=sleep_lookup)
    _print_codes_with_names("2) 株探ニュース（一覧→本文）", s_kabutan, interval_ms=sleep_lookup)

    codes = [c for c in sorted(set().union(s_yahoo, s_kabutan)) if re.fullmatch(r"\d{4}", c)]

    # 実在確認（任意、既定 True）
    if bool(ad.get("validate_codes", True)) and codes:
        def _val(c: str) -> Tuple[str,bool]: return c, validate_code_exists(c, interval_ms=sleep_validate)
        ok: Set[str] = set()
        with ThreadPoolExecutor(max_workers=max_workers_namecheck) as ex:
            futs = {ex.submit(_val, c): c for c in codes}
            for fut in as_completed(futs):
                try:
                    c, v = fut.result()
                    if v: ok.add(c)
                except Exception: pass
        codes = sorted(ok)

    # 名称付与 → 非株除外 → 整形
    universe: List[Dict] = []
    if codes:
        def _name(c: str) -> Tuple[str,str]: return c, lookup_name(c, interval_ms=sleep_lookup)
        names: Dict[str,str] = {}
        with ThreadPoolExecutor(max_workers=max_workers_namecheck) as ex:
            futs = {ex.submit(_name, c): c for c in codes}
            for fut in as_completed(futs):
                try:
                    c, nm = fut.result()
                    names[c] = nm
                except Exception:
                    names[codes[0]] = ""
        for code in codes:
            name = names.get(code, "")
            if _is_non_equity(code, name): continue
            aliases = [name, code] if name else [code]
            news_q = f"({name} OR {code}) 株" if name else f"{code} 株"
            universe.append({
                "ticker": code,
                "name": name or code,
                "aliases": aliases,
                "news_query": news_q,
                "bbs": {"yahoo_finance_code": code},
            })

    top_k = int(ad.get("top_k", 40))
    return universe[:top_k]
