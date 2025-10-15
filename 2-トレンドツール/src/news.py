# -*- coding: utf-8 -*-
"""
news.py (better dedupe)
Google News RSS を使ってクエリのニュースを取得し、sentiment を付与して返す。
- タイトル・URL の正規化、同一記事の重複除去
- ドメイン頻度の偏り抑制（同ドメイン連続を緩く間引き）
- 既存の fetch_news(query, max_items, include_debug) 互換
"""

from __future__ import annotations
import re
import html
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlparse

import requests
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime

HEADERS = {"User-Agent": "Mozilla/5.0"}
JST = timezone(timedelta(hours=9))

def _normalize_text(s: str) -> str:
    if not s: return ""
    s = html.unescape(s).strip()
    s = s.replace("\u3000"," ").replace("\xa0"," ")
    s = re.sub(r"\s+"," ", s).strip()
    return s

def _canonical_title(s: str) -> str:
    t = _normalize_text(s)
    t = re.sub(r"[-–—｜\|•]\s*.*$", "", t)  # サイト名など末尾の区切りを落とす（弱め）
    t = re.sub(r"【\d{4}】", "", t)
    return t.strip().lower()

def _domain(u: str) -> str:
    try: return urlparse(u).netloc.lower()
    except Exception: return ""

def _parse_rss(xml_text: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(xml_text, "xml")
    items = []
    for it in soup.find_all("item"):
        title = _normalize_text(it.title.text if it.title else "")
        link = (it.link.text if it.link else "") or (it.guid.text if it.guid else "")
        pub = it.pubDate.text if it.pubDate else ""
        try:
            dt = parsedate_to_datetime(pub)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            pub_iso = dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pub_iso = ""
        items.append({"title": title, "link": link, "published": pub_iso})
    return items

def _google_news_rss(query: str) -> str:
    q = quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"

def fetch_news(query: str, max_items: int = 30, include_debug: bool = False) -> Dict[str, Any]:
    try:
        from .sentiment import classify_sentiment
    except Exception:
        from sentiment import classify_sentiment  # type: ignore

    url = _google_news_rss(query)
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
    except Exception as e:
        return {"query": query, "items": [], "count_24h": 0, "error": str(e)}

    raw_items = _parse_rss(r.text)
    # 重複除去（タイトル正規化 + 同URL/ドメイン近傍の緩間引き）
    seen_title = set()
    seen_url = set()
    domain_count = {}
    items = []
    for it in raw_items:
        if not it.get("title") or not it.get("link"):
            continue
        tnorm = _canonical_title(it["title"])
        if tnorm in seen_title:
            continue
        if it["link"] in seen_url:
            continue
        dom = _domain(it["link"])
        # 同ドメインが連続しすぎる場合はゆるく間引き（同じ媒体ばかりの片寄りを減らす）
        cnt = domain_count.get(dom, 0)
        if cnt >= 5:
            continue
        seen_title.add(tnorm); seen_url.add(it["link"]); domain_count[dom] = cnt + 1
        items.append(it)
        if len(items) >= max_items:
            break

    # sentiment 付与
    out_items = []
    now = datetime.now(JST)
    count_24h = 0
    for it in items:
        label, dbg = classify_sentiment(it["title"], source_type="news", source_name=dom, return_debug=True)
        if it.get("published"):
            try:
                dt = datetime.strptime(it["published"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST)
                if (now - dt).total_seconds() <= 24*3600:
                    count_24h += 1
            except Exception:
                pass
        ent = dict(it)
        ent["sentiment"] = label
        if include_debug:
            ent["debug"] = dbg
        out_items.append(ent)

    return {"query": query, "items": out_items, "count_24h": count_24h}
