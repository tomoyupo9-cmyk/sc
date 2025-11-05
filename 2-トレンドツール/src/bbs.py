# -*- coding: utf-8 -*-
"""
bbs.py (fast)
Yahoo!ファイナンス掲示板 高速化版
- 共有 Session（コネクションプール/リトライ）
- 検索ページの並列取得（stop_on_known=False で高速）
- sentiment の並列判定
- DB: WAL + executemany + 単一トランザクション
- BeautifulSoup 走査の回数最小化
"""

from __future__ import annotations
import re
import sqlite3
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import urljoin, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Unified SQLite connection helper ===
try:
    import sqlite3  # ensure available
    def _get_db_conn(db_path: str, *, timeout: float = 30.0):
        """
        Open a tuned SQLite connection.
        - WAL journaling, NORMAL sync (fast, durable enough)
        - temp_store=MEMORY, mmap_size ~128MB
        - cache_size ~256MB (negative => size in KiB)
        """
        conn = _get_db_conn(db_path, timeout=timeout)
        try:
            cur = conn.cursor()
            for pragma in [
                "PRAGMA journal_mode=WAL;",
                "PRAGMA synchronous=NORMAL;",
                "PRAGMA temp_store=MEMORY;",
                "PRAGMA mmap_size=134217728;",
                "PRAGMA cache_size=-262144;"
            ]:
                try:
                    cur.execute(pragma)
                except Exception:
                    pass
            conn.commit()
        except Exception:
            # even if pragmas fail, return a usable connection
            pass
        return conn
except Exception:
    # Fallback stub to avoid import errors in limited environments
    def _get_db_conn(db_path: str, *, timeout: float = 30.0):
        raise RuntimeError("sqlite3 not available in this environment")
# === end helper ===


import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore
from bs4 import BeautifulSoup, NavigableString
from dateutil import parser, tz

HEADERS = {"User-Agent": "Mozilla/5.0"}
YAHOO_BASE_URL = "https://finance.yahoo.co.jp"
_DEFAULT_TIMEOUT = 15
_SESSION: Optional[requests.Session] = None

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None: return _SESSION
    s = requests.Session()
    try:
        retry = Retry(total=3, connect=3, read=3, backoff_factor=0.3,
                      status_forcelist=[429,500,502,503,504],
                      allowed_methods=frozenset(["GET"]))
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=retry)
    except Exception:
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32)
    s.mount("http://", adpt); s.mount("https://", adpt)
    _SESSION = s
    return s

def _http_get(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[requests.Response]:
    try:
        r = _get_session().get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status(); r.encoding = r.apparent_encoding
        return r
    except Exception:
        return None

# ---------- DB ----------
def _get_table_name(query_key: str) -> str:
    return f"{query_key}_comments"

def _ensure_db_schema_for_query(db_path: str, query_key: str) -> None:
    table = _get_table_name(query_key)
    conn = _get_db_conn(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                link TEXT PRIMARY KEY,
                ticker TEXT,
                name TEXT,
                published TEXT,
                text TEXT,
                sentiment TEXT,
                sentiment_explain TEXT,
                sentiment_debug TEXT,
                query_key TEXT NOT NULL
            )
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{query_key}_ticker ON {table}(ticker)")
        conn.commit()
    finally:
        conn.close()

def _get_existing_links(db_path: str, query_key: str) -> Set[str]:
    table = _get_table_name(query_key)
    links: Set[str] = set()
    conn = _get_db_conn(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT link FROM {table}")
        for (lnk,) in cur.fetchall(): links.add(lnk)
    except Exception:
        pass
    finally:
        conn.close()
    return links

def _save_comments_to_db_bulk(db_path: str, query_key: str, comments: List[Dict[str, Any]]) -> None:
    if not comments: return
    table = _get_table_name(query_key)
    conn = _get_db_conn(db_path)
    try:
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA mmap_size=134217728;")
        except Exception:
            pass
        conn.commit()
        cur.executemany(
            f"""
            INSERT OR IGNORE INTO {table} (
                link, ticker, name, published, text, sentiment, sentiment_explain, sentiment_debug, query_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (c.get("link"), c.get("ticker"), c.get("name"), c.get("published"),
                 c.get("text"), c.get("sentiment"), c.get("sentiment_explain"),
                 c.get("sentiment_debug"), query_key)
                for c in comments
            ]
        )
        conn.commit()
    finally:
        conn.close()

# ---------- HTML 解析 ----------
def _parse_time(s: str):
    s = (s or "").strip()
    m = re.search(r"(\d+)分前", s)
    if m: return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(minutes=int(m.group(1)))
    m = re.search(r"(\d+)時間前", s)
    if m: return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)日前", s)
    if m: return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(days=int(m.group(1)))
    try:
        dt = parser.parse(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=tz.gettz('Asia/Tokyo'))
        return dt
    except Exception:
        return None

def _count_posts(soup: BeautifulSoup, look_threads: int = 50) -> Dict[str, int]:
    times = []
    for tag in soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})")):
        dt = _parse_time(tag.strip())
        if dt: times.append(dt)
        if len(times) >= look_threads: break
    now = datetime.now(tz=tz.gettz('Asia/Tokyo'))
    posts_24h = sum(1 for t in times if (now - t).total_seconds() <= 24*3600)
    posts_72h = sum(1 for t in times if (now - t).total_seconds() <= 72*3600)
    return {"posts_24h": posts_24h, "posts_72h": posts_72h, "threads_sampled": len(times)}

def _extract_samples(soup: BeautifulSoup, max_samples: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    time_nodes = soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})"))
    for node in time_nodes:
        if len(out) >= max_samples: break
        dt = _parse_time((node or "").strip()); 
        if not dt: continue
        block = None; cur = getattr(node, "parent", None)
        for _ in range(4):
            if not cur: break
            if cur.name in ("li","article","section","div"): block = cur; break
            cur = cur.parent
        if not block: continue
        text = " ".join(t.strip() for t in block.stripped_strings if isinstance(t,(str,NavigableString)))
        text = re.sub(r"(いいね！|返信|通報|ID:.*?|投稿日時:.*?)", "", text)
        text = re.sub(r"\s+"," ", text).strip()
        if not text: continue
        snip = text[:120]
        if snip in seen: continue
        seen.add(snip)
        out.append({"published": dt.isoformat(), "text": snip})
    return out

def _parse_search_page(html_text: str, encoded_query: str):
    soup = BeautifulSoup(html_text, "lxml")
    posts = soup.select(r"div.Fz\(12px\).C\(\$textDim\) > span.Fl\(end\)")
    if not posts: return [], True
    comments = []
    for post in posts:
        link_a = post.find_previous("a", class_=r"Maw\(600px\)")
        if not link_a: continue
        link = urljoin(YAHOO_BASE_URL, link_a.get("href","") or "")
        if not link: continue
        m = re.search(r"/quote/(\d{4})(?:\.T)?/bbs", link)
        ticker_code = m.group(1) if m else None
        meta_div = post.find_previous("div", class_=r"Lh\(1\.2\)")
        if not ticker_code and meta_div:
            m2 = re.search(r"(\d{4})", meta_div.text or "")
            ticker_code = m2.group(1) if m2 else None
        name = re.sub(r"（.+?）|【.+?】|\d{4}株|掲示板", "", meta_div.text if meta_div else "").strip() if meta_div else ""
        if not ticker_code: continue
        published = post.select_one("span:nth-child(2)")
        published = published.text if published else ""
        text = (link_a.text or "").strip()
        comments.append({"link": link, "ticker": ticker_code, "name": name, "published": published, "text": text})
    return comments, False

# ---------- 検索コア ----------

def fetch_search_comments(db_path: str, query: str, query_key: str,
                          max_pages: int = 100, stop_on_known: bool = False, max_workers: int = 6):
    try:
        from .sentiment import classify_sentiment
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    _ensure_db_schema_for_query(db_path, query_key)
    existing = _get_existing_links(db_path, query_key)
    enc = quote_plus(query, encoding='utf-8')

    page_urls = [f"{YAHOO_BASE_URL}/search/bbs?query={enc}&page={p}" for p in range(1, max_pages+1)]
    page_results: List[Tuple[int, List[Dict[str, Any]]]] = []

    if stop_on_known:
        for p, url in enumerate(page_urls, start=1):
            html = _http_get(url)
            if not html: continue
            soup = BeautifulSoup(html.text, "lxml")
            items = _extract_samples(soup, max_samples=999)
            known = sum(1 for it in items if it.get("link") in existing)
            page_results.append((p, items))
            if known >= 5:  # そのページの半分以上が既知なら打ち切り
                break
    else:
        # 並列取得
        def _fetch_one(p, url):
            try:
                html = _http_get(url)
                if not html: return (p, [])
                soup = BeautifulSoup(html.text, "lxml")
                return (p, _extract_samples(soup, max_samples=999))
            except Exception:
                return (p, [])
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_fetch_one, i+1, u) for i, u in enumerate(page_urls)]
            for fut in as_completed(futs):
                page_results.append(fut.result())

    # 古い→新しい順に整列してから既知リンクを除外、sentiment 付与（並列）
    page_results.sort(key=lambda t: t[0])
    targets: List[Dict[str, Any]] = []
    for _, items in page_results:
        for it in items:
            lnk = it.get("link")
            if not lnk or lnk in existing: continue
            targets.append(it)

    # sentiment 付与（軽量デバッガを含む）
    def _sent(c):
        txt = c.get("text","")
        try:
            label, dbg = classify_sentiment(txt, source_type="bbs", source_name="Yahoo検索", return_debug=True)
            c["sentiment"] = label
            c["sentiment_explain"] = dbg.get("explain_jp")
            c["sentiment_debug"] = dbg
        except Exception:
            c["sentiment"] = "neutral"
            c["sentiment_explain"] = "（判定エラーのため中立化）"
        return c

    all_comments: List[Dict[str, Any]] = []
    if targets:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(max_workers, len(targets))) as ex:
            for fut in as_completed([ex.submit(_sent, c) for c in targets]):
                try: all_comments.append(fut.result())
                except Exception: pass

    _save_comments_to_db_bulk(db_path, query_key, all_comments)

    table = _get_table_name(query_key)
    conn = _get_db_conn(db_path)
    try:
        cur = conn.cursor()
        # 集計
        cur.execute(
            f"""
            SELECT ticker, name, COUNT(link) FROM {table}
             WHERE ticker IS NOT NULL AND LENGTH(ticker)=4 AND ticker GLOB '[0-9][0-9][0-9][0-9]'
             GROUP BY ticker, name HAVING COUNT(link) > 0 ORDER BY COUNT(link) DESC
            """
        )
        counts = {r[0]: r[2] for r in cur.fetchall()}

        # 後方互換: sentiment_debug が無い古いテーブルでも動くようにする
        cols_base = "link, ticker, name, published, text, sentiment, sentiment_explain"
        try:
            cur.execute(f"PRAGMA table_info({table})")
            has_debug = any(r[1] == "sentiment_debug" for r in cur.fetchall())
        except Exception:
            has_debug = False
        cols_sel = cols_base + (", sentiment_debug" if has_debug else ", NULL AS sentiment_debug")
        cur.execute(
            f"""
            SELECT {cols_sel}
              FROM {table}
             WHERE ticker IS NOT NULL AND LENGTH(ticker)=4 AND ticker GLOB '[0-9][0-9][0-9][0-9]'
             ORDER BY published DESC
            """
        )
        cols = [c[0] for c in cur.description]
        rows_all = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    uniq = sorted(counts.keys(), key=lambda t: counts[t], reverse=True)
    rows = []
    seen: Set[str] = set()
    for tkr in uniq:
        cnt = counts[tkr]
        name = next((x.get("name") for x in rows_all if x.get("ticker")==tkr), "不明")
        samples = [x for x in rows_all if x.get("ticker")==tkr]
        # 重複リンクを除去
        uniq_samples = []
        for s in samples:
            l = s.get("link")
            if l in seen: continue
            seen.add(l); uniq_samples.append(s)
        rows.append({
            "ticker": tkr, "name": name, "count": cnt, "score": cnt**0.5,
            "comments": uniq_samples[:50]
        })
    rows.sort(key=lambda r: r["score"], reverse=True)
    return {
        "generated_at": datetime.now(tz=tz.gettz("Asia/Tokyo")).isoformat(timespec="seconds"),
        "total_comments": len(rows_all),
        "total_tickers": len(counts),
        "rows": rows,
    }

    def _sent(c):
        try:
            label, dbg = classify_sentiment(c["text"], source_type="bbs_search",
                                            source_name=f"Yahoo検索:{query}", return_debug=True)
        except Exception:
            label, dbg = "neutral", {"explain_jp": "（判定エラーのため中立化）"}
        txt = c["text"]
        if query_key == "tob":
            if re.search(r"(TOB|ＴＯＢ|公開買付|公開買い付け|MBO|完全子会社化|上場廃止予定|成立|応募|買付価格|プレミアム)", txt): label = "positive"
            if re.search(r"(不成立|撤回|中止|否決|見送り|買付価格引き下げ|買付期間短縮)", txt): label = "negative"
        if query_key == "leak":
            if re.search(r"(上方修正|増配|最高益|黒字転換|好材料|思惑|噂|ポジティブ)", txt): label = "positive"
            if re.search(r"(下方修正|減配|赤字|悪材料|リーク.*虚偽|デマ|ネガティブ)", txt): label = "negative"
        out = dict(c)
        out["sentiment"] = label
        out["sentiment_explain"] = dbg.get("explain_jp")
        out["sentiment_debug"] = str(dbg)
        return out

    targets = [c for _, cs in page_results for c in cs]
    all_comments: List[Dict[str, Any]] = []
    if targets:
        for c in targets: existing.add(c["link"])
        with ThreadPoolExecutor(max_workers=min(8, max_workers*2)) as ex:
            for fut in as_completed([ex.submit(_sent, c) for c in targets]):
                try: all_comments.append(fut.result())
                except Exception: pass

    _save_comments_to_db_bulk(db_path, query_key, all_comments)

    table = _get_table_name(query_key)
    conn = _get_db_conn(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT ticker, name, COUNT(link) FROM {table}
             WHERE ticker IS NOT NULL AND LENGTH(ticker)=4 AND ticker GLOB '[0-9][0-9][0-9][0-9]'
             GROUP BY ticker, name HAVING COUNT(link) > 0 ORDER BY COUNT(link) DESC
            """
        )
        counts = {r[0]: r[2] for r in cur.fetchall()}
        cur.execute(
            f"""
            SELECT link, ticker, name, published, text, sentiment, sentiment_explain, sentiment_debug
              FROM {table}
             WHERE ticker IS NOT NULL AND LENGTH(ticker)=4 AND ticker GLOB '[0-9][0-9][0-9][0-9]'
             ORDER BY published DESC
            """
        )
        cols = [c[0] for c in cur.description]
        rows_all = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    uniq = sorted(counts.keys(), key=lambda t: counts[t], reverse=True)
    rows = []
    for tkr in uniq:
        cnt = counts[tkr]
        name = next((x.get("name") for x in rows_all if x.get("ticker")==tkr), "不明")
        rows.append({
            "ticker": tkr, "name": name, "count": cnt, "score": cnt**0.5,
            "comments": [x for x in rows_all if x.get("ticker")==tkr][:6]
        })
    rows.sort(key=lambda r: r["score"], reverse=True)
    return {
        "generated_at": datetime.now(tz=tz.gettz("Asia/Tokyo")).isoformat(timespec="seconds"),
        "total_comments": len(rows_all),
        "total_tickers": len(counts),
        "rows": rows,
    }

def fetch_leak_comments(db_path: str, max_pages: int = 100):
    return fetch_search_comments(db_path, "漏れてる？", "leak", max_pages=max_pages, stop_on_known=False)

def fetch_tob_comments(db_path: str, max_pages: int = 100):
    return fetch_search_comments(db_path, "TOB", "tob", max_pages=max_pages, stop_on_known=False)

def fetch_bbs_stats(yahoo_finance_code: str, look_threads: int = 50,
                    include_samples: bool = False, max_samples: int = 20, query_key: str = "bbs"):
    url = f"{YAHOO_BASE_URL}/quote/{yahoo_finance_code}.T/bbs"
    r = _http_get(url)
    base = {"posts_24h":0,"posts_72h":0,"threads_sampled":0}
    if not r:
        return {**base, **({"samples":[] } if include_samples else {})}
    soup = BeautifulSoup(r.text, "lxml")
    counts = _count_posts(soup, look_threads=look_threads)
    if not include_samples: return counts

    try:
        from .sentiment import classify_sentiment
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    items = _extract_samples(soup, max_samples=max_samples)

    def _sent_item(it):
        txt = it.get("text","")
        try:
            label, dbg = classify_sentiment(txt, source_type="bbs", source_name=f"Yahoo個別:{yahoo_finance_code}", return_debug=True)
            if query_key == "tob":
                if re.search(r"(TOB|ＴＯＢ|公開買付|公開買い付け|MBO|完全子会社化|上場廃止予定|成立|応募|買付価格|プレミアム)", txt): label="positive"
                if re.search(r"(不成立|撤回|中止|否決|見送り|買付価格引き下げ|買付期間短縮)", txt): label="negative"
            if query_key == "leak":
                if re.search(r"(上方修正|増配|最高益|黒字転換|好材料|思惑|噂|ポジティブ)", txt): label="positive"
                if re.search(r"(下方修正|減配|赤字|悪材料|リーク.*虚偽|デマ|ネガティブ)", txt): label="negative"
            return {"published": it.get("published",""), "text": txt,
                    "sentiment": label, "sentiment_explain": dbg.get("explain_jp"), "sentiment_debug": dbg}
        except Exception:
            return {"published": it.get("published",""), "text": txt,
                    "sentiment": "neutral", "sentiment_explain": "（判定エラーのため中立化）"}

    out = []
    if items:
        with ThreadPoolExecutor(max_workers=min(8, len(items))) as ex:
            for fut in as_completed([ex.submit(_sent_item, x) for x in items]):
                try: out.append(fut.result())
                except Exception: pass
    counts["samples"] = out
    return counts
