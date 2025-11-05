# -*- coding: utf-8 -*-
"""
bbs.py (fast, fixed)
Yahoo!ファイナンス掲示板 収集/統計（高速・安定版）
- requests.Session 共有（コネクションプール/リトライ）
- 検索ページは _parse_search_page() で link/ticker/name/published/text を取得
- sentiment 並列付与
- DB は common._get_db_conn() を使用（WAL/PRAGMA は common.py に一元化）
"""

from __future__ import annotations
import re
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import urljoin, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==== 共通ユーティリティ/DB ====
try:
    # パッケージ形式でも相対/絶対の両対応
    from . import common  # type: ignore
except Exception:
    import common  # type: ignore

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


# ========== HTTP ==========
def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    try:
        retry = Retry(
            total=3, connect=3, read=3, backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"])
        )
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=retry)
    except Exception:
        adpt = HTTPAdapter(pool_connections=32, pool_maxsize=32)
    s.mount("http://", adpt)
    s.mount("https://", adpt)
    _SESSION = s
    return s


def _http_get(url: str, timeout: int = _DEFAULT_TIMEOUT) -> Optional[requests.Response]:
    try:
        r = _get_session().get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r
    except Exception:
        return None


# ========== DB ==========
def _use_db(db_path: str):
    """common のシングルトン接続に DB パスをセット（初回のみ反映）。"""
    common.set_db_path(db_path)  # 以降は common._get_db_conn() を使う


def _conn():
    return common._get_db_conn()


def _get_table_name(query_key: str) -> str:
    return f"{query_key}_comments"


def _ensure_db_schema_for_query(query_key: str) -> None:
    table = _get_table_name(query_key)
    conn = _conn()
    cur = conn.cursor()
    try:
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
        cur.close()


def _get_existing_links(query_key: str) -> Set[str]:
    table = _get_table_name(query_key)
    conn = _conn()
    cur = conn.cursor()
    out: Set[str] = set()
    try:
        cur.execute(f"SELECT link FROM {table}")
        for (lnk,) in cur.fetchall():
            if lnk:
                out.add(lnk)
    except Exception:
        pass
    finally:
        cur.close()
    return out


def _save_comments_to_db_bulk(query_key: str, comments: List[Dict[str, Any]]) -> None:
    if not comments:
        return
    table = _get_table_name(query_key)
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.executemany(
            f"""
            INSERT OR IGNORE INTO {table}
            (link, ticker, name, published, text, sentiment, sentiment_explain, sentiment_debug, query_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    c.get("link"),
                    c.get("ticker"),
                    c.get("name"),
                    c.get("published"),
                    c.get("text"),
                    c.get("sentiment"),
                    c.get("sentiment_explain"),
                    c.get("sentiment_debug"),
                    query_key,
                )
                for c in comments
                if c.get("link")  # PRIMARY KEY 必須
            ],
        )
        conn.commit()
    finally:
        cur.close()


# ========== HTML 解析 ==========
def _parse_time(s: str):
    s = (s or "").strip()
    m = re.search(r"(\d+)分前", s)
    if m:
        return datetime.now(tz=tz.gettz("Asia/Tokyo")) - timedelta(minutes=int(m.group(1)))
    m = re.search(r"(\d+)時間前", s)
    if m:
        return datetime.now(tz=tz.gettz("Asia/Tokyo")) - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)日前", s)
    if m:
        return datetime.now(tz=tz.gettz("Asia/Tokyo")) - timedelta(days=int(m.group(1)))
    try:
        dt = parser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.gettz("Asia/Tokyo"))
        return dt
    except Exception:
        return None


def _count_posts(soup: BeautifulSoup, look_threads: int = 50) -> Dict[str, int]:
    times = []
    for tag in soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})")):
        dt = _parse_time((tag or "").strip())
        if dt:
            times.append(dt)
        if len(times) >= look_threads:
            break
    now = datetime.now(tz=tz.gettz("Asia/Tokyo"))
    posts_24h = sum(1 for t in times if (now - t).total_seconds() <= 24 * 3600)
    posts_72h = sum(1 for t in times if (now - t).total_seconds() <= 72 * 3600)
    return {"posts_24h": posts_24h, "posts_72h": posts_72h, "threads_sampled": len(times)}


def _extract_samples(soup: BeautifulSoup, max_samples: int = 20) -> List[Dict[str, Any]]:
    """
    個別銘柄掲示板ページ用：テキストと時刻のサンプルだけ返す（linkは無し）。
    """
    out: List[Dict[str, Any]] = []
    seen = set()
    time_nodes = soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})"))
    for node in time_nodes:
        if len(out) >= max_samples:
            break
        dt = _parse_time((node or "").strip())
        if not dt:
            continue
        block = None
        cur = getattr(node, "parent", None)
        for _ in range(4):
            if not cur:
                break
            if cur.name in ("li", "article", "section", "div"):
                block = cur
                break
            cur = cur.parent
        if not block:
            continue
        text = " ".join(t.strip() for t in block.stripped_strings if isinstance(t, (str, NavigableString)))
        text = re.sub(r"(いいね！|返信|通報|ID:.*?|投稿日時:.*?)", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        snip = text[:120]
        if snip in seen:
            continue
        seen.add(snip)
        out.append({"published": dt.isoformat(), "text": snip})
    return out


def _parse_search_page(html_text: str) -> Tuple[List[Dict[str, Any]], bool]:
    """
    検索結果ページを解析し、link/ticker/name/published/text を返す。
    """
    soup = BeautifulSoup(html_text, "lxml")
    posts = soup.select(r"div.Fz\(12px\).C\(\$textDim\) > span.Fl\(end\)")
    if not posts:
        return [], True  # 解析失敗 or 0件
    comments = []
    for post in posts:
        link_a = post.find_previous("a", class_=r"Maw\(600px\)")
        if not link_a:
            continue
        link = urljoin(YAHOO_BASE_URL, link_a.get("href", "") or "")
        if not link:
            continue
        m = re.search(r"/quote/(\d{4})(?:\.T)?/bbs", link)
        ticker_code = m.group(1) if m else None
        meta_div = post.find_previous("div", class_=r"Lh\(1\.2\)")
        if not ticker_code and meta_div:
            m2 = re.search(r"(\d{4})", meta_div.text or "")
            ticker_code = m2.group(1) if m2 else None
        name = (
            re.sub(r"（.+?）|【.+?】|\d{4}株|掲示板", "", meta_div.text if meta_div else "").strip()
            if meta_div
            else ""
        )
        if not ticker_code:
            continue
        published = post.select_one("span:nth-child(2)")
        published = published.text if published else ""
        text = (link_a.text or "").strip()
        comments.append(
            {"link": link, "ticker": ticker_code, "name": name, "published": published, "text": text}
        )
    return comments, False


# ========== 検索系・コア ==========
def fetch_search_comments(
    db_path: str,
    query: str,
    query_key: str,
    max_pages: int = 100,
    stop_on_known: bool = False,
    max_workers: int = 6,
):
    """
    Yahooファイナンス掲示板 検索ページからコメントを拾い、DBへ蓄積。
    """
    # sentiment 分類子のインポート（相対/絶対の両対応）
    try:
        from .sentiment import classify_sentiment  # type: ignore
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    _use_db(db_path)
    _ensure_db_schema_for_query(query_key)
    existing = _get_existing_links(query_key)
    enc = quote_plus(query, encoding="utf-8")

    page_urls = [f"{YAHOO_BASE_URL}/search/bbs?query={enc}&page={p}" for p in range(1, max_pages + 1)]
    page_results: List[Tuple[int, List[Dict[str, Any]]]] = []

    if stop_on_known:
        # 順次取得し、既知リンクが多ければ早期終了
        for p, url in enumerate(page_urls, start=1):
            html = _http_get(url)
            if not html:
                continue
            items, _ = _parse_search_page(html.text)
            known = sum(1 for it in items if it.get("link") in existing)
            page_results.append((p, items))
            if known >= max(5, len(items) // 2):
                break
    else:
        # 並列取得
        def _fetch_one(p: int, url: str):
            try:
                html = _http_get(url)
                if not html:
                    return (p, [])
                items, _ = _parse_search_page(html.text)
                return (p, items)
            except Exception:
                return (p, [])

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_fetch_one, i + 1, u) for i, u in enumerate(page_urls)]
            for fut in as_completed(futs):
                page_results.append(fut.result())

    # 古い→新しい順
    page_results.sort(key=lambda t: t[0])
    targets: List[Dict[str, Any]] = []
    for _, items in page_results:
        for it in items:
            lnk = it.get("link")
            if not lnk or lnk in existing:
                continue
            targets.append(it)

    # sentiment 付与（並列）
    def _sent(c: Dict[str, Any]):
        txt = c.get("text", "")
        try:
            label, dbg = classify_sentiment(
                txt, source_type="bbs_search", source_name=f"Yahoo検索:{query}", return_debug=True
            )
        except Exception:
            label, dbg = "neutral", {"explain_jp": "（判定エラーのため中立化）"}
        out = dict(c)
        out["sentiment"] = label
        out["sentiment_explain"] = dbg.get("explain_jp")
        out["sentiment_debug"] = dbg
        return out

    enriched: List[Dict[str, Any]] = []
    if targets:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(targets))) as ex:
            for fut in as_completed([ex.submit(_sent, c) for c in targets]):
                try:
                    enriched.append(fut.result())
                except Exception:
                    pass

    _save_comments_to_db_bulk(query_key, enriched)

    # ---- 集計出力 ----
    table = _get_table_name(query_key)
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT ticker, name, COUNT(link)
              FROM {table}
             WHERE ticker IS NOT NULL AND LENGTH(ticker)=4 AND ticker GLOB '[0-9][0-9][0-9][0-9]'
             GROUP BY ticker, name
             HAVING COUNT(link) > 0
             ORDER BY COUNT(link) DESC
            """
        )
        counts = {r[0]: r[2] for r in cur.fetchall()}

        # sentiment_debug 有無互換
        cur.execute(f"PRAGMA table_info({table})")
        has_debug = any(r[1] == "sentiment_debug" for r in cur.fetchall())
        cols_sel = (
            "link, ticker, name, published, text, sentiment, sentiment_explain"
            + (", sentiment_debug" if has_debug else ", NULL AS sentiment_debug")
        )
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
        cur.close()

    uniq = sorted(counts.keys(), key=lambda t: counts[t], reverse=True)
    rows = []
    seen: Set[str] = set()
    for tkr in uniq:
        cnt = counts[tkr]
        name = next((x.get("name") for x in rows_all if x.get("ticker") == tkr), "不明")
        samples = [x for x in rows_all if x.get("ticker") == tkr]
        uniq_samples = []
        for s in samples:
            l = s.get("link")
            if not l or l in seen:
                continue
            seen.add(l)
            uniq_samples.append(s)
        rows.append(
            {"ticker": tkr, "name": name, "count": cnt, "score": cnt ** 0.5, "comments": uniq_samples[:50]}
        )
    rows.sort(key=lambda r: r["score"], reverse=True)
    return {
        "generated_at": common.now_iso(common.JST),
        "total_comments": len(rows_all),
        "total_tickers": len(counts),
        "rows": rows,
    }


# ========== プリセット ==========
def fetch_leak_comments(db_path: str, max_pages: int = 100):
    return fetch_search_comments(db_path, "漏れてる？", "leak", max_pages=max_pages, stop_on_known=False)


def fetch_tob_comments(db_path: str, max_pages: int = 100):
    return fetch_search_comments(db_path, "TOB", "tob", max_pages=max_pages, stop_on_known=False)


# ========== 個別掲示板の統計 ==========
def fetch_bbs_stats(
    yahoo_finance_code: str,
    look_threads: int = 50,
    include_samples: bool = False,
    max_samples: int = 20,
    query_key: str = "bbs",
):
    """
    個別銘柄掲示板ページの直近投稿数などを概算。DBには書き込まない。
    """
    url = f"{YAHOO_BASE_URL}/quote/{yahoo_finance_code}.T/bbs"
    r = _http_get(url)
    base = {"posts_24h": 0, "posts_72h": 0, "threads_sampled": 0}
    if not r:
        return {**base, **({"samples": []} if include_samples else {})}
    soup = BeautifulSoup(r.text, "lxml")
    counts = _count_posts(soup, look_threads=look_threads)
    if not include_samples:
        return counts

    # sentiment
    try:
        from .sentiment import classify_sentiment  # type: ignore
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    items = _extract_samples(soup, max_samples=max_samples)

    def _sent_item(it: Dict[str, Any]):
        txt = it.get("text", "")
        try:
            label, dbg = classify_sentiment(
                txt, source_type="bbs", source_name=f"Yahoo個別:{yahoo_finance_code}", return_debug=True
            )
            # オプションのキーワード補正
            if query_key == "tob":
                if re.search(r"(TOB|ＴＯＢ|公開買付|公開買い付け|MBO|完全子会社化|上場廃止予定|成立|応募|買付価格|プレミアム)", txt):
                    label = "positive"
                if re.search(r"(不成立|撤回|中止|否決|見送り|買付価格引き下げ|買付期間短縮)", txt):
                    label = "negative"
            if query_key == "leak":
                if re.search(r"(上方修正|増配|最高益|黒字転換|好材料|思惑|噂|ポジティブ)", txt):
                    label = "positive"
                if re.search(r"(下方修正|減配|赤字|悪材料|リーク.*虚偽|デマ|ネガティブ)", txt):
                    label = "negative"
            return {
                "published": it.get("published", ""),
                "text": txt,
                "sentiment": label,
                "sentiment_explain": dbg.get("explain_jp"),
                "sentiment_debug": dbg,
            }
        except Exception:
            return {
                "published": it.get("published", ""),
                "text": txt,
                "sentiment": "neutral",
                "sentiment_explain": "（判定エラーのため中立化）",
            }

    out = []
    if items:
        with ThreadPoolExecutor(max_workers=min(8, len(items))) as ex:
            for fut in as_completed([ex.submit(_sent_item, x) for x in items]):
                try:
                    out.append(fut.result())
                except Exception:
                    pass
    counts["samples"] = out
    return counts
