# -*- coding: utf-8 -*-
"""
news.py
GoogleニュースRSSからニュース取得し、各アイテムに sentiment を付与
- include_debug=True の場合、判定の日本語説明と詳細デバッグを items に埋め込む
"""

from __future__ import annotations
import feedparser
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta
from .sentiment import classify_sentiment

def _parse_published(p) -> str:
    try:
        if hasattr(p, "tm_year"):  # struct_time
            dt = datetime(*p[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        return str(p)
    except Exception:
        return ""

def fetch_news(query: str, max_items: int = 50, include_debug: bool = False) -> Dict[str, Any]:
    import urllib.parse
    q = urllib.parse.quote(query, safe="")
    url = f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"
    feed = feedparser.parse(url)

    items: List[Dict[str, Any]] = []
    for entry in feed.entries[:max_items]:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        published = _parse_published(entry.get("published_parsed") or entry.get("published") or "")
        source = ""
        try:
            source = (entry.get("source") or {}).get("title", "") or ""
        except Exception:
            source = ""

        if include_debug:
            label, dbg = classify_sentiment(title, source_type="news", source_name=source, return_debug=True)
            sentiment = label
            sentiment_explain = dbg.get("explain_jp")
            sentiment_debug = dbg
        else:
            sentiment = classify_sentiment(title, source_type="news", source_name=source)
            sentiment_explain = None
            sentiment_debug = None

        item = {
            "title": title,
            "link": link,
            "published": published,
            "source": source,
            "sentiment": sentiment,
        }
        if include_debug:
            item["sentiment_explain"] = sentiment_explain
            item["sentiment_debug"] = sentiment_debug
        items.append(item)

    now = datetime.now(timezone.utc)
    def _in_last_hours(published_iso: str, hours: int) -> bool:
        try:
            dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
            return (now - dt) <= timedelta(hours=hours)
        except Exception:
            return False

    count_24h = sum(1 for it in items if _in_last_hours(it["published"], 24))
    count_72h = sum(1 for it in items if _in_last_hours(it["published"], 72))

    return {"count_24h": count_24h, "count_72h": count_72h, "items": items}
