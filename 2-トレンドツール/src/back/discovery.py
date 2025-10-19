# -*- coding: utf-8 -*-
"""
discovery.py
-------------
「最近話題になっている日本株の4桁コード」を自動で見つけ、fetch_all.py へ渡せる
ユニバース（銘柄配列）を生成するユーティリティ。

## 主な機能
- Yahoo!ファイナンス掲示板ランキング（日/週/月, 各page=1..2）から4桁コード抽出
- 代表銘柄（9984/7203/6758）の掲示板からリンク辿りで4桁抽出
- 株探ニュース一覧→各記事本文から4桁抽出（件数上限/スリープはconfigで調整可能）
- Googleニュース 日本語RSSから4桁抽出（軽量）
- 4桁コードの実在確認（Yahoo銘柄ページの<title>確認）※任意
- 銘柄名の取得（株探→Yahooの順で推定し、ノイズ語・コード・読みを除去）
- 非株（ETF/ETN/指数/為替/投資法人 等）を除外するフィルタ
- 最終的に top_k 件にスライスして返す
- 各ソース(1〜4)で取得した銘柄を「コード：銘柄名」でコンソールに出力（非株は表示除外）

## 設定（config）の例
auto_discover:
  enabled: true
  top_k: 40
  validate_codes: true
  kabutan_max_articles: 80
  sleep_ms:
    lookup_name: 150
    validate_code: 120
    kabutan_article: 300
http:
  timeout: 10
  user_agent: "Mozilla/5.0 (...)"

依存: requests, bs4(lxml), re, html, time
"""
from __future__ import annotations

import re
import time
import html
from typing import Iterable, Optional, Set, Dict, List

import requests
from bs4 import BeautifulSoup

# ======================
# HTTP 設定（configで上書き可）
# ======================
HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"
}
# configから変更可能な共通タイムアウト（秒）
DEFAULT_TIMEOUT: int = 10

# 名前キャッシュ（重複呼び出しのAPI負荷軽減）
_NAME_CACHE: Dict[str, str] = {}

# ======================
# 正規表現・共通ユーティリティ
# ======================
# 社名末尾からよくあるノイズ語を落とす（「株価/掲示板/ニュース/基本情報/株の…」など）
TRAILING_NOISE = re.compile(
    r"(?:\s*[-|｜／/・|\|]\s*)?"
    r"(?:株価|チャート|掲示板|ニュース|決算(?:速報)?|適時開示|基本情報|時価総額|配当|プロフィール|企業情報|業績|株主優待|投資情報"
    r"|株の(?:掲示板)?|株式情報)"
    r"(?:.*)?$"
)

def _sleep_ms(ms: int) -> None:
    if ms and ms > 0:
        time.sleep(ms / 1000.0)

def _safe_get(url: str, timeout: Optional[int] = None) -> Optional[requests.Response]:
    """
    例外を握りつぶして None を返す GET ラッパー。
    timeout 未指定時は DEFAULT_TIMEOUT を採用。HEADERS はモジュール変数を使用。
    """
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout)
    except Exception:
        return None

def _extract_codes_from_text(text: str) -> Set[str]:
    """本文テキストから「4桁」だけを抜きだす。"""
    found: Set[str] = set()
    if not text:
        return found
    for m in re.finditer(r"\b(\d{4})\b", text):
        found.add(m.group(1))
    return found

# ======================
# 収集(1) Yahoo!掲示板ランキング（日/週/月）
# ======================
def discover_yahoo_bbs_hot() -> Set[str]:
    """
    掲示板ランキング（daily/weekly/monthly, page=1..2）から 4桁コードを抽出。
    - /quote/XXXX(.T)/bbs へのリンクやテキスト中の4桁を広めに拾う
    """
    found: Set[str] = set()
    urls = [
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=daily&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=daily&page=2",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=weekly&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=weekly&page=2",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=monthly&page=1",
        "https://finance.yahoo.co.jp/stocks/ranking/bbs?market=all&term=monthly&page=2",
    ]
    for u in urls:
        res = _safe_get(u)
        if not res or res.status_code != 200:
            continue
        soup = BeautifulSoup(res.text, "lxml")

        # 1) aタグのhrefから抽出（.T有無、末尾/bbs、クエリ付きも許容）
        for a in soup.select("a[href]"):
            href = a.get("href", "") or ""
            m = re.search(r"/quote/(\d{4})(?:\.T)?(?:/bbs)?(?:[/?#]|$)", href)
            if m:
                found.add(m.group(1))

        # 2) 念のためテキスト全体から4桁抽出
        found |= _extract_codes_from_text(soup.get_text(" ", strip=True))

    return found

# ======================
# 収集(2) Yahoo!代表銘柄掲示板（9984/7203/6758）
# ======================
def discover_yahoo_bbs_seed() -> Set[str]:
    """
    代表的な大型株の掲示板からリンクを辿って 4桁抽出
    """
    found: Set[str] = set()
    seeds = ["9984", "7203", "6758"]
    for code in seeds:
        url = f"https://finance.yahoo.co.jp/quote/{code}.T/bbs"
        res = _safe_get(url)
        if not res or res.status_code != 200:
            continue
        soup = BeautifulSoup(res.text, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            m = re.search(r"/quote/(\d{4})\.T", href)
            if m:
                found.add(m.group(1))
    return found

# ======================
# 収集(3) 株探ニュース（一覧→記事本文）
# ======================
def discover_kabutan_news(max_articles: int = 80, article_sleep_ms: int = 300) -> Set[str]:
    """
    株探ニュース一覧 -> 記事本文をたどって 4桁コードを抽出
    :param max_articles: 本文へ辿る記事の最大件数
    :param article_sleep_ms: 記事1本ごとの処理後に入れる待機（ミリ秒）
    """
    found: Set[str] = set()
    res = _safe_get("https://kabutan.jp/news/")
    if not res or res.status_code != 200:
        return found

    soup = BeautifulSoup(res.text, "lxml")

    # 一覧ページ上にある個別銘柄リンク（/stock/?code=XXXX）を優先抽出
    for a in soup.select("a[href*='/stock/?code=']"):
        href = a.get("href", "")
        m = re.search(r"code=(\d{4})", href)
        if m:
            found.add(m.group(1))

    # ニュース記事ページのURL一覧（重複排除）→ 最大 max_articles 件に制限
    links = list(dict.fromkeys([a.get("href", "") for a in soup.select("a[href^='/news/']")]))
    links = [p for p in links if p][:max_articles]

    # 各記事本文から 4桁抽出（リンク優先＋保険として本文テキスト）
    for p in links:
        art = _safe_get(f"https://kabutan.jp{p}")
        if not art or art.status_code != 200:
            _sleep_ms(article_sleep_ms)
            continue
        s = BeautifulSoup(art.text, "lxml")
        for a in s.select("a[href*='/stock/?code=']"):
            href = a.get("href", "")
            m = re.search(r"code=(\d{4})", href)
            if m:
                found.add(m.group(1))
        text = s.get_text(" ", strip=True)
        found |= _extract_codes_from_text(text)
        _sleep_ms(article_sleep_ms)  # サイト負荷軽減
    return found

# ======================
# 収集(4) Googleニュース 日本語RSS
# ======================
def discover_gtrends_japan() -> Set[str]:
    """
    GoogleニュースRSSの見出し等から 4桁をざっくり抽出（軽量）
    """
    found: Set[str] = set()
    url = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"
    res = _safe_get(url)
    if not res or res.status_code != 200:
        return found
    text = res.text
    return _extract_codes_from_text(text)

# ======================
# 銘柄名の取得・整形
# ======================
def _clean_name(name: str) -> str:
    if not name:
        return ""
    n = html.unescape(name).strip()
    # 1) 末尾の汎用ノイズを除去（株価/掲示板/ニュース…）
    n = TRAILING_NOISE.sub("", n).strip()
    # 2) 【1357】 などのコード表記を除去（続く「株の…」ごと削る）
    n = re.sub(r"【\s*\d{4}\s*】(?:.*)?$", "", n).strip()
    # 3) 読み仮名や略称など末尾の括弧書き（全角/半角）を除去
    n = re.sub(r"[（(][^（）()]{1,30}[)）]\s*$", "", n).strip()
    # 4) 区切り記号の連続をスペースに正規化
    n = re.sub(r"\s*[｜|／/・]\s*", " ", n).strip()
    # 5) 先頭の「株式会社」「(株)」を簡易に剥がす
    n = re.sub(r"^(?:株式会社|\(株\))\s*", "", n)
    return n

def lookup_name(code: str, interval_ms: int = 150) -> str:
    """
    まず株探、その後Yahooで銘柄名を推定し、末尾ノイズを除去
    """
    # キャッシュ
    if code in _NAME_CACHE:
        _sleep_ms(interval_ms)
        return _NAME_CACHE[code]

    # 1) 株探
    res = _safe_get(f"https://kabutan.jp/stock/?code={code}")
    if res and res.status_code == 200:
        s = BeautifulSoup(res.text, "lxml")
        t = s.find("title")
        if t and t.text:
            nm = _clean_name(t.text)
            if nm:
                _NAME_CACHE[code] = nm
                _sleep_ms(interval_ms)
                return nm
        h = s.select_one("h1, h2")
        if h and h.get_text(strip=True):
            nm = _clean_name(h.get_text(strip=True))
            if nm:
                _NAME_CACHE[code] = nm
                _sleep_ms(interval_ms)
                return nm

    # 2) Yahoo
    res = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    if res and res.status_code == 200:
        s = BeautifulSoup(res.text, "lxml")
        t = s.find("title")
        if t and t.text:
            nm = _clean_name(t.text)
            if nm:
                _NAME_CACHE[code] = nm
                _sleep_ms(interval_ms)
                return nm
        h = s.select_one("h1, h2")
        if h and h.get_text(strip=True):
            nm = _clean_name(h.get_text(strip=True))
            if nm:
                _NAME_CACHE[code] = nm
                _sleep_ms(interval_ms)
                return nm

    _sleep_ms(interval_ms)
    _NAME_CACHE.setdefault(code, "")
    return ""

# ======================
# 実在確認（Yahoo<title>で確認）
# ======================
def validate_code_exists(code: str, interval_ms: int = 120) -> bool:
    """
    Yahoo銘柄ページの<title>に【コード】が入るかで実在確認
    """
    res = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    ok = False
    if res and res.status_code == 200:
        s = BeautifulSoup(res.text, "lxml")
        t = s.find("title")
        if t and t.text:
            txt = t.text
            if re.search(rf"\b{re.escape(code)}\b", txt):
                ok = True
    _sleep_ms(interval_ms)
    return ok

# ======================
# 非株（ETF/ETN/指数/為替/投資信託/投資法人 など）判定
# ======================
_NON_EQUITY_KEYWORDS = [
    "ETF", "ＥＴＦ", "ETN", "ＥＴＮ",
    "投資信託", "上場投信", "上場投資信託",
    "投資法人", "投資証券",
    "指数", "インデックス", "先物", "レバレッジ", "ダブル", "インバース",
    "純金", "原油", "金先物", "商品", "コモディティ",
    "iPath", "ＮＯＴＥＳ", "NEXT NOTES", "One ETF", "iシェアーズ", "ＳＭＴＡＭ",
    "レート", "為替",
]
_NON_EQUITY_CODE_PREFIXES = ("13", "15", "16", "20", "25")  # 保険的に除外するレンジ

def _is_non_equity(code: str, name: str) -> bool:
    """ETF/ETN/指数・為替・投資法人などをざっくり除外する。"""
    n = (name or "").upper()
    if any(k.upper() in n for k in _NON_EQUITY_KEYWORDS):
        return True
    return any(code.startswith(p) for p in _NON_EQUITY_CODE_PREFIXES)

# ======================
# 補助：コンソール出力（コード：銘柄名）※非株は表示スキップ
# ======================
def _print_codes_with_names(label: str, codes: Iterable[str], interval_ms: int = 150) -> None:
    codes = sorted({c for c in codes if re.fullmatch(r"\d{4}", c)})
    print(f"\n=== {label} ===")
    for c in codes:
        name = lookup_name(c, interval_ms=interval_ms)
        if _is_non_equity(c, name):
            continue
        print(f"{c}：{name or c}")

# ======================
# ユニバース生成のエントリポイント
# ======================
def build_universe(cfg: Dict) -> List[Dict]:
    """
    4つの発見関数の union -> 4桁化 -> （任意）実在確認 -> 名前解決 -> 非株除外 -> top_k で整形
    ※ 各ソースごとの取得結果を「コード：銘柄名」でコンソール出力
    :param cfg: 外部の設定dict（YAML読込結果など）
    :return: fetch_all.py へ渡せる銘柄配列
    """
    ad = cfg.get("auto_discover", {}) or {}
    http_cfg = cfg.get("http", {}) or {}

    # === HTTP 設定（User-Agent / timeout）を反映 ===
    ua = http_cfg.get("user_agent")
    if ua:
        HEADERS["User-Agent"] = str(ua)
    global DEFAULT_TIMEOUT
    if "timeout" in http_cfg:
        try:
            DEFAULT_TIMEOUT = int(http_cfg.get("timeout", DEFAULT_TIMEOUT))
        except Exception:
            DEFAULT_TIMEOUT = DEFAULT_TIMEOUT

    # === kabutan の本文辿り件数・sleep を config から ===
    kabutan_max_articles = int(ad.get("kabutan_max_articles", 80))
    sm = (ad.get("sleep_ms") or {})
    sleep_lookup = int(sm.get("lookup_name", 150))
    sleep_validate = int(sm.get("validate_code", 120))
    sleep_kabutan_article = int(sm.get("kabutan_article", 300))

    found: Set[str] = set()

    # 1) Yahoo!掲示板ランキング（日/週/月）
    s1 = discover_yahoo_bbs_hot()
    _print_codes_with_names("1) Yahoo掲示板ランキング（日/週/月）", s1, interval_ms=sleep_lookup)
    found |= s1

    # 2) 代表銘柄掲示板（9984/7203/6758）
    s2 = discover_yahoo_bbs_seed()
    _print_codes_with_names("2) 代表3銘柄掲示板シード", s2, interval_ms=sleep_lookup)
    found |= s2

    # 3) 株探ニュース（一覧 -> 記事本文）
    s3 = discover_kabutan_news(
        max_articles=kabutan_max_articles,
        article_sleep_ms=sleep_kabutan_article
    )
    _print_codes_with_names("3) 株探ニュース（一覧→本文）", s3, interval_ms=sleep_lookup)
    found |= s3

    # 4) GoogleニュースRSS（日本語）
    s4 = discover_gtrends_japan()
    _print_codes_with_names("4) GoogleニュースRSS(日本語)", s4, interval_ms=sleep_lookup)
    found |= s4

    # 4桁のみ残す（ETF等5桁は除外）& ソート
    codes = [c for c in sorted(found) if re.fullmatch(r"\d{4}", c)]

    # 実在確認（任意）
    validate = bool(ad.get("validate_codes", True))
    if validate:
        codes = [c for c in codes if validate_code_exists(c, interval_ms=sleep_validate)]

    # 銘柄名を取得 → 非株を排除 → 整形
    universe: List[Dict] = []
    for code in codes:
        name = lookup_name(code, interval_ms=sleep_lookup)
        if _is_non_equity(code, name):
            continue

        if not name:
            aliases = [code]
            news_q = f"{code} 株"
        else:
            aliases = [name, code]
            news_q = f"({name} OR {code}) 株"

        universe.append({
            "ticker": code,
            "name": name or code,
            "aliases": aliases,
            "news_query": news_q,
            "bbs": {"yahoo_finance_code": code},
        })

    top_k = int(ad.get("top_k", 40))
    return universe[:top_k]
