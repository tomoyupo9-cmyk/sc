
# -*- coding: utf-8 -*-
"""
bbs.py
Yahoo!ファイナンス掲示板
"""

import re
import sqlite3
from typing import List, Dict, Any, Set, Optional
from datetime import datetime, timedelta
from urllib.parse import urljoin, quote_plus

import requests
from bs4 import BeautifulSoup, NavigableString
from dateutil import parser, tz

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE = "https://finance.yahoo.co.jp/quote/{code}.T/bbs"
YAHOO_BASE_URL = "https://finance.yahoo.co.jp"

# ============================================================
# DB helpers (generic by query_key)
# ============================================================

def _get_table_name(query_key: str) -> str:
    # 内部で管理するキー（例：'leak', 'tob'）を想定
    return f"{query_key}_comments"


def _ensure_db_schema_for_query(db_path: str, query_key: str) -> None:
    table = _get_table_name(query_key)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
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
        # ticker のインデックスは必須
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{query_key}_ticker ON {table}(ticker)")
        conn.commit()
    finally:
        if conn: conn.close()


def _get_existing_links(db_path: str, query_key: str) -> Set[str]:
    table = _get_table_name(query_key)
    conn = None
    links: Set[str] = set()
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT link FROM {table}")
        for r in cur.fetchall():
            links.add(r[0])
    except Exception:
        pass
    finally:
        if conn: conn.close()
    return links


def _save_comments_to_db(db_path: str, query_key: str, comments: List[Dict[str, Any]]) -> None:
    table = _get_table_name(query_key)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        for c in comments:
            cur.execute(
                f"""
                INSERT OR IGNORE INTO {table} (
                    link, ticker, name, published, text, sentiment, sentiment_explain, sentiment_debug, query_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    c.get("link"), c.get("ticker"), c.get("name"), c.get("published"), c.get("text"),
                    c.get("sentiment"), c.get("sentiment_explain"), c.get("sentiment_debug"), query_key
                )
            )
        conn.commit()
    except Exception as e:
        print(f"[DB] Error saving to {table}: {e}")
    finally:
        if conn: conn.close()

# ------------------------------------------------------------
# ============================================================
# Core Crawler
# ============================================================

def fetch_search_comments(
    db_path: str, query: str, query_key: str, max_pages: int = 10, stop_on_known: bool = True
) -> Dict[str, Any]:
    """
    Yahoo!掲示板を検索し、DBに保存。結果をコード別件数で集計して返す。
    :param db_path: SQLite DBのパス
    :param query: 検索キーワード (例: "漏れてる？", "TOB")
    :param query_key: DBテーブル名用のキー (例: "leak", "tob")
    :param max_pages: クロールする最大ページ数
    :param stop_on_known: 既知のリンクに遭遇したらクロールを停止するか
    """
    # sentiment.py のインポート（このファイルでは相対インポート）
    try:
        from .sentiment import classify_sentiment
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    _ensure_db_schema_for_query(db_path, query_key)
    existing_links = _get_existing_links(db_path, query_key)

    encoded_query = quote_plus(query, encoding='utf-8')
    all_comments: List[Dict[str, Any]] = []

    print(f"[BBS_SEARCH] Starting search for '{query}' (key: {query_key}, max_pages: {max_pages})")

    for page in range(1, max_pages + 1):
        # 検索URL: https://finance.yahoo.co.jp/search/bbs?query=XXX&page=Y
        url = f"{YAHOO_BASE_URL}/search/bbs?query={encoded_query}&page={page}"
        print(f"  -> Fetching page {page} ({url})")

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
        except Exception as e:
            print(f"  [WARN] Failed to fetch page {page}: {e}")
            break

        soup = BeautifulSoup(r.text, "lxml")
        posts = soup.select(r"div.Fz\(12px\).C\(\$textDim\) > span.Fl\(end\)")

        found_on_page = 0
        is_end_of_results = True

        for post in posts:
            is_end_of_results = False

            # 投稿リンクを特定 (aタグの親の親など)
            link_a = post.find_previous("a", class_=r"Maw\(600px\)")
            if not link_a:
                continue

            link = urljoin(YAHOO_BASE_URL, link_a.get("href", ""))
            if not link:
                continue

            if link in existing_links:
                print(f"    [SKIP] Already known link: {link}")
                if stop_on_known:
                    print("    [STOP] Reached known links (stop_on_known=True). Stopping crawl.")
                    is_end_of_results = True # 既知リンクに達したら次のページもスキップ
                    break
                continue # 次のコメントへ


            # 銘柄名とコードを抽出（URL優先）
            # 例: /quote/1234.T/bbs → 1234 を取得
            m_link = re.search(r"/quote/(\d{4})(?:\.T)?/bbs", link)
            ticker_code = m_link.group(1) if m_link else None

            meta_div = post.find_previous("div", class_=r"Lh\(1\.2\)")
            if not ticker_code and meta_div:
                m_meta = re.search(r"(\d{4})", meta_div.text or "")
                ticker_code = m_meta.group(1) if m_meta else None

            name = re.sub(r"（.+?）|【.+?】|\d{4}株|掲示板", "", meta_div.text if meta_div else "").strip() if meta_div else ""

            # 4桁コードが取れない投稿は保存対象外（ダミー禁止）
            if not ticker_code:
                continue

            # 投稿日時
            published = post.select_one("span:nth-child(2)").text if post.select_one("span:nth-child(2)") else ""

            # 投稿本文 (リンク先のタイトル)
            text = link_a.text.strip()

            # センチメント判定（例外時は neutral にフォールバック）
            try:
                label, dbg = classify_sentiment(
                    text, source_type="bbs_search", source_name=f"Yahoo検索:{query}", return_debug=True
                )
            except Exception:
                label, dbg = "neutral", {"explain_jp": "（判定エラーのため中立化）"}

            comment = {
                "link": link,
                "ticker": ticker_code,
                "name": name,
                "published": published,
                "text": text,
                "sentiment": label,
                "sentiment_explain": dbg.get("explain_jp"),
                "sentiment_debug": str(dbg), # JSONとしてDBに保存するため文字列化
            }
            all_comments.append(comment)
            found_on_page += 1
            existing_links.add(link) # 重複チェック用に即座に追加

        # ページ単位でDBに保存
        if all_comments:
            new_comments = all_comments[-found_on_page:]
            _save_comments_to_db(db_path, query_key, new_comments)
            print(f"    [SAVE] Saved {len(new_comments)} new comments to DB.")

        if is_end_of_results:
            break

        if not posts and page > 1:
            # 1ページ目で結果なしはありえるが、途中ページでいきなり空は終了と見なす
            break

    # 4. DBから全てのコメントを再取得（既に保存済みのもの全てを含める）
    table = _get_table_name(query_key)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # 最新の300件（または過去2週間分など、任意の絞り込みが可能）
        # ここではシンプルに全件取得し、メモリで絞り込む
        cur.execute(
            f"""
            SELECT ticker, name, COUNT(link) as count
            FROM {table}
            WHERE ticker IS NOT NULL
              AND LENGTH(ticker)=4
              AND ticker GLOB '[0-9][0-9][0-9][0-9]'
            GROUP BY ticker, name
            HAVING count > 0
            ORDER BY count DESC
            """
        )

        counts = {r[0]: r[2] for r in cur.fetchall()} # ticker -> count

        cur.execute(
            f"""
            SELECT * FROM {table}
            WHERE ticker IS NOT NULL
              AND LENGTH(ticker)=4
              AND ticker GLOB '[0-9][0-9][0-9][0-9]'
            ORDER BY published DESC
            """
        )
        all_comments = [dict(zip([col[0] for col in cur.description], r)) for r in cur.fetchall()]


    except Exception as e:
        print(f"[DB] Error retrieving aggregated results for {query_key}: {e}")
        counts = {}
        all_comments = []
    finally:
        if conn: conn.close()

    # 5. 結果を集計して整形
    unique_tickers = sorted(counts.keys(), key=lambda t: counts[t], reverse=True)
    rows: List[Dict[str, Any]] = []

    for tkr in unique_tickers:
        cnt = counts[tkr]
        # 銘柄名をコメントから取得（全件取得しているので安全）
        name = next((x.get("name") for x in all_comments if x.get("ticker") == tkr), "不明")
        # スコア (件数の平方根など、UIで使うための指標)
        score = cnt ** 0.5
        rows.append(
            {
                "ticker": tkr,
                "name": name,
                "count": cnt,
                "score": score,
                # all_comments は published DESC で取得済 → 先頭から6件を採用
                "comments": [x for x in all_comments if x.get("ticker") == tkr][:6],
            }
        )

    rows.sort(key=lambda r: r["score"], reverse=True)

    return {
        "generated_at": datetime.now(tz=tz.gettz("Asia/Tokyo")).isoformat(timespec="seconds"),
        "total_comments": len(all_comments),
        "total_tickers": len(counts),
        "rows": rows,
    }

# ------------------------------------------------------------

# ============================================================
# Wrappers
# ============================================================

def fetch_leak_comments(db_path: str, max_pages: int = 100) -> Dict[str, Any]:
    """
    「漏れてる？」検索結果を取得しDBに保存するラッパー
    """
    return fetch_search_comments(db_path, "漏れてる？", "leak", max_pages=max_pages, stop_on_known=False)


def fetch_tob_comments(db_path: str, max_pages: int = 100) -> Dict[str, Any]:
    """
    「TOB」検索結果を取得しDBに保存するラッパー
    """
    return fetch_search_comments(db_path, "TOB", "tob", max_pages=max_pages, stop_on_known=False)


# ------------------------------------------------------------
# 時刻パース
# ------------------------------------------------------------
def _parse_time(s: str) -> Optional[datetime]:
    s = s.strip()
    m = re.search(r"(\d+)分前", s)
    if m:
        return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(minutes=int(m.group(1)))
    m = re.search(r"(\d+)時間前", s)
    if m:
        return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)日前", s)
    if m:
        return datetime.now(tz=tz.gettz('Asia/Tokyo')) - timedelta(days=int(m.group(1)))
    try:
        dt = parser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.gettz('Asia/Tokyo'))
        return dt
    except Exception:
        return None

# ------------------------------------------------------------
# 既存の集計（そのまま維持）
# ------------------------------------------------------------
def _count_posts(soup: BeautifulSoup, look_threads: int = 50) -> Dict[str, int]:
    candidates: List[datetime] = []
    for tag in soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})")):
        dt = _parse_time(tag.strip())
        if dt:
            candidates.append(dt)
        if len(candidates) >= look_threads:
            break
    now = datetime.now(tz=tz.gettz('Asia/Tokyo'))
    posts_24h = sum(1 for t in candidates if (now - t).total_seconds() <= 24*3600)
    posts_72h = sum(1 for t in candidates if (now - t).total_seconds() <= 72*3600)
    return {"posts_24h": posts_24h, "posts_72h": posts_72h, "threads_sampled": len(candidates)}

# ------------------------------------------------------------
# 本文スニペット抽出（ベストエフォート）
#   - Yahoo!側のDOMは頻繁に変わるため、堅いセレクタではなく「時間表示の親近傍」を取得
#   - 文字数は短くトリミングし、同一っぽいものは重複排除
# ------------------------------------------------------------
def _extract_samples(soup: BeautifulSoup, max_samples: int = 20) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    seen_texts = set()

    # 時刻語を持つテキストノードを起点に、近傍の投稿本文を拾う
    time_nodes = soup.find_all(text=re.compile(r"(分前|時間前|日前|\d{1,2}/\d{1,2})"))
    for node in time_nodes:
        if len(samples) >= max_samples:
            break
        dt = _parse_time(node.strip())
        if not dt:
            continue

        # 候補: その親→親…と辿ってブロック要素を取得
        block = None
        cur = node.parent
        for _ in range(4):
            if not cur:
                break
            # 投稿本文らしい大きめのブロック
            if cur.name in ("li", "article", "section", "div"):
                block = cur
                break
            cur = cur.parent

        if not block:
            continue

        # ブロックからテキストを抽出してノイズを落とす
        text = " ".join(
            t.strip() for t in block.stripped_strings
            if isinstance(t, (str, NavigableString))
        )
        # 明らかに不要なUIラベルを落とす（ゆるめ）
        text = re.sub(r"(いいね！|返信|通報|ID:.*?|投稿日時:.*?)", "", text)
        text = re.sub(r"\s+", " ", text).strip()

        if not text:
            continue

        # 短く要約（掲示板は長いので一旦80〜120字程度）
        snippet = text[:120]
        if snippet in seen_texts:
            continue
        seen_texts.add(snippet)

        samples.append({
            "published": dt.isoformat(),
            "text": snippet,
        })

    return samples

# ------------------------------------------------------------
# 公開関数
#   include_samples=True で、本文サンプル＋感情判定を返す
# ------------------------------------------------------------
def fetch_bbs_stats(
    yahoo_finance_code: str,
    look_threads: int = 50,
    include_samples: bool = False,
    max_samples: int = 20,
    query_key: str = "bbs",
) -> Dict[str, Any]:
    url = BASE.format(code=yahoo_finance_code)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception:
        base: Dict[str, Any] = {"posts_24h": 0, "posts_72h": 0, "threads_sampled": 0}
        if include_samples:
            base["samples"] = []
        return base

    soup = BeautifulSoup(r.text, "lxml")
    counts = _count_posts(soup, look_threads=look_threads)

    if not include_samples:
        return counts

    # sentiment 判定器（ニュースと共通）
    try:
        from .sentiment import classify_sentiment
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    # 本文サンプルを抽出して、掲示板モードで sentiment を付与
    items = _extract_samples(soup, max_samples=max_samples)
    out_samples: List[Dict[str, Any]] = []
    for it in items:
        txt = it.get("text", "")
        try:
            label, dbg = classify_sentiment(
                txt, source_type="bbs", source_name=f"Yahoo個別:{yahoo_finance_code}", return_debug=True
            )

            # --- 追加：語彙ルール補正（任意） ---
            if query_key == "tob":
                # 強いポジ語
                if re.search(r"(TOB|ＴＯＢ|公開買付|公開買い付け|MBO|完全子会社化|上場廃止予定|成立|応募|買付価格|プレミアム)", txt):
                    label = "positive"
                # 明確なネガは最優先で上書き
                if re.search(r"(不成立|撤回|中止|否決|見送り|買付価格引き下げ|買付期間短縮)", txt):
                    label = "negative"

            if query_key == "leak":
                if re.search(r"(上方修正|増配|最高益|黒字転換|好材料|思惑|噂|ポジティブ)", txt):
                    label = "positive"
                if re.search(r"(下方修正|減配|赤字|悪材料|リーク.*虚偽|デマ|ネガティブ)", txt):
                    label = "negative"

            out_samples.append({
                "published": it.get("published", ""),
                "text": txt,
                "sentiment": label,
                "sentiment_explain": dbg.get("explain_jp"),
                "sentiment_debug": dbg,  # 必要なら外してOK
            })
        except Exception:
            out_samples.append({
                "published": it.get("published", ""),
                "text": txt,
                "sentiment": "neutral",
                "sentiment_explain": "（判定エラーのため中立化）",
            })

    counts["samples"] = out_samples
    return counts
