"""
discovery.py
 - 銘柄名の取得 (Kabutan優先 → Yahoo補助)
 - 自動発見：掲示板/ニュース等から直近で話題の4桁コードを収集して universe を構築
"""

from __future__ import annotations
import re, time, html, requests
from typing import Dict, List, Set, Optional
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
NOISE_NAMES = {"ポートフォリオ", "マイポートフォリオ", "Portfolio"}
CODE_RE = re.compile(r"\b(\d{4})\b")

# 余計な語を削るためのパターン（タイトルや見出しに混ざる）
TRAILING_NOISE = re.compile(
    r"(?:株価.*|基本情報.*|チャート.*|時系列.*|ニュース.*|決算.*|掲示板.*|企業情報.*|"
    r"テクニカル.*|指数.*|見通し.*|株探.*|Yahoo!?.*|ヤフー.*)$"
)

# ------------------------------------------------------------
# 共通
# ------------------------------------------------------------
def _safe_get(url: str, timeout=10) -> Optional[str]:
    """URLのHTMLを返す（失敗時はNone）"""
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except Exception:
        return None

def _sleep_ms(ms:int):
    if ms > 0:
        time.sleep(ms / 1000.0)

def _clean_name(text: str) -> str:
    """社名候補からノイズを徹底除去（(1234)、基本情報/株価 等も削る）"""
    if not text:
        return ""
    t = html.unescape(text)

    # よく混ざる表記を除去
    t = t.replace("(株)", "").replace("（株）", "")

    # 末尾のノイズ語を丸ごと削る
    t = TRAILING_NOISE.sub("", t)

    # 「(1234)」「（1234）」などの4桁コード表記を削除
    t = re.sub(r"[\(（]\s*\d{4}\s*[\)）]", "", t)

    # 記号・空白の整理
    t = t.strip()
    t = t.strip(" -｜|、,　:/：・")

    # 先頭/末尾の記号を追加クリーン
    t = re.sub(r"^[\-\|｜・:：\s]+|[\-\|｜・:：\s]+$", "", t)

    return t

# ------------------------------------------------------------
# 銘柄名: Kabutan → Yahoo
# ------------------------------------------------------------
def _lookup_name_from_kabutan(code: str, html_text: Optional[str] = None) -> Optional[str]:
    if html_text is None:
        html_text = _safe_get(f"https://kabutan.jp/stock/?code={code}")
    if not html_text:
        return None
    soup = BeautifulSoup(html_text, "lxml")

    # <title> 例: "プラス（2424）株価｜基本情報 - 株探"
    title = (soup.title.string if soup.title else "") or ""
    m = re.search(r"^(.+?)（\s*\d{4}\s*）", title)
    if m:
        name = _clean_name(m.group(1))
        if name and name not in NOISE_NAMES and not name.isdigit():
            return name

    # h1/h2 など（念のため同様にクリーンアップ）
    for sel in ["h1", "h2", ".company_name"]:
        h = soup.select_one(sel)
        if h:
            name = _clean_name(h.get_text(" ", strip=True))
            if name and name not in NOISE_NAMES and not name.isdigit():
                return name
    return None

def _lookup_name_from_yahoo(code: str, html_text: Optional[str] = None) -> Optional[str]:
    if html_text is None:
        html_text = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    if not html_text:
        return None
    soup = BeautifulSoup(html_text, "lxml")

    # <title> 例: "プラス(株)【2424】：株価 - Yahoo!ファイナンス"
    title = (soup.title.string if soup.title else "") or ""
    m = re.search(r"^(.+?)【\s*\d{4}\s*】", title)
    if m:
        name = _clean_name(m.group(1))
        if name and name not in NOISE_NAMES and not name.isdigit():
            return name

    for tag in ["h1", "h2"]:
        h = soup.find(tag)
        if h:
            name = _clean_name(h.get_text(" ", strip=True))
            if name and name not in NOISE_NAMES and not name.isdigit():
                return name
    return None

def lookup_name(code: str, interval_ms: int = 200) -> str:
    """銘柄名を取得 (Kabutan→Yahooの順)。失敗時はcodeを返す"""
    name = _lookup_name_from_kabutan(code) or _lookup_name_from_yahoo(code)
    if not name or name in NOISE_NAMES or name.isdigit():
        name = code
    _sleep_ms(interval_ms)
    return name

# ------------------------------------------------------------
# コード存在チェック（Yahooタイトルに【code】が入るか）
# ------------------------------------------------------------
def validate_code_exists(code: str, interval_ms:int=120) -> bool:
    html_text = _safe_get(f"https://finance.yahoo.co.jp/quote/{code}.T")
    _sleep_ms(interval_ms)
    if not html_text:
        return False
    soup = BeautifulSoup(html_text, "lxml")
    title = (soup.title.string if soup.title else "") or ""
    return (f"【{code}】" in title) or (f"{code}.T" in title)

# ------------------------------------------------------------
# 自動発見ソース
# ------------------------------------------------------------
def discover_yahoo_bbs_hot() -> Set[str]:
    """Yahoo!掲示板/マーケットから 4 桁コードらしきリンクを拾う"""
    out: Set[str] = set()
    for url in [
        "https://finance.yahoo.co.jp/cm/message",
        "https://finance.yahoo.co.jp/markets",
    ]:
        html_text = _safe_get(url)
        if not html_text:
            continue
        for a in BeautifulSoup(html_text, "lxml").find_all("a", href=True):
            m = re.search(r"/quote/(\d{4})\.T", a["href"])
            if m:
                out.add(m.group(1))
    return out

def discover_yahoo_bbs_seed() -> Set[str]:
    """いくつかの代表銘柄掲示板からリンクを辿ってコードを拾う"""
    out: Set[str] = set()
    for url in [
        "https://finance.yahoo.co.jp/quote/9984.T/bbs",
        "https://finance.yahoo.co.jp/quote/7203.T/bbs",
        "https://finance.yahoo.co.jp/quote/6758.T/bbs",
    ]:
        html_text = _safe_get(url)
        if not html_text:
            continue
        for a in BeautifulSoup(html_text, "lxml").find_all("a", href=True):
            m = re.search(r"/quote/(\d{4})\.T", a["href"])
            if m:
                out.add(m.group(1))
    return out

def discover_kabutan_news(max_articles:int=80) -> Set[str]:
    """株探ニュースの一覧と本文から 4 桁コードを抽出"""
    out: Set[str] = set()
    base = "https://kabutan.jp/news/"
    html_text = _safe_get(base)
    if not html_text:
        return out
    soup = BeautifulSoup(html_text, "lxml")

    # 個別銘柄リンク優先
    for a in soup.select("a[href*='/stock/?code=']"):
        m = re.search(r"code=(\d{4})", a.get("href",""))
        if m:
            out.add(m.group(1))

    # 記事本文を少し辿る
    links = []
    for a in soup.select("a[href^='/news/']"):
        links.append("https://kabutan.jp" + a.get("href",""))
    links = list(dict.fromkeys(links))[:max_articles]

    for u in links:
        art = _safe_get(u)
        if not art:
            continue
        s = BeautifulSoup(art, "lxml")
        for a in s.select("a[href*='/stock/?code=']"):
            m = re.search(r"code=(\d{4})", a.get("href",""))
            if m:
                out.add(m.group(1))
        # テキストから4桁拾い（保険）
        out.update(re.findall(CODE_RE, s.get_text(" ")))
        _sleep_ms(300)

    return out

def discover_gtrends_japan() -> Set[str]:
    """軽量：GoogleニュースRSSの見出しから4桁抽出（近似の代替）"""
    out: Set[str] = set()
    url = "https://news.google.com/rss?hl=ja&gl=JP&ceid=JP:ja"
    xml = _safe_get(url)
    if not xml:
        return out
    out.update(re.findall(CODE_RE, xml))
    return out

# ------------------------------------------------------------
# universe 構築
# ------------------------------------------------------------
def build_universe(cfg: Dict) -> List[Dict]:
    """
    自動発見した銘柄を fetch_all でそのまま使える形に変換
    config.yaml:
      auto_discover:
        enabled: true
        top_k: 40
        validate_codes: true
    """
    ad = cfg.get("auto_discover", {}) or {}
    if not ad.get("enabled", True):
        return []

    top_k = int(ad.get("top_k", 40))
    validate = bool(ad.get("validate_codes", True))

    found: Set[str] = set()
    found |= discover_yahoo_bbs_hot()
    found |= discover_yahoo_bbs_seed()
    found |= discover_kabutan_news()
    found |= discover_gtrends_japan()

    # 4桁に限定
    codes = [c for c in sorted(found) if re.fullmatch(r"\d{4}", c)]

    # 検証（存在しないコードは落とす）
    if validate:
        codes = [c for c in codes if validate_code_exists(c)]

    codes = codes[:top_k]

    # fetch_all.py がそのまま使える形に整形
    rows: List[Dict] = []
    for code in codes:
        name = lookup_name(code, interval_ms=150)
        aliases = [name, code] if name != code else [code]
        rows.append({
            "ticker": code,
            "name": name,
            "aliases": aliases,
            "news_query": f"({name} OR {code}) 株",
            "bbs": {"yahoo_finance_code": code},
        })
    return rows
