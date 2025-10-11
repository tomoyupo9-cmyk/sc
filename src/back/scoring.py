# -*- coding: utf-8 -*-
"""
scoring.py (revived style)
- 旧版に近い配点: 0.40 News, 0.30 BBS, 0.20 RVOL, 0.10 Trends
- 各項目は 0..1 に正規化して合成。値が欠損のときは 0。
"""
from __future__ import annotations
from typing import Dict, Any, List
import math, json

def _clip01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    return 0.0 if x < 0 else (1.0 if x > 1 else x)

def _score_news(row: Dict[str, Any]) -> float:
    n = int(((row.get("news") or {}).get("count_24h")) or 0)
    items = ((row.get("news") or {}).get("items")) or []
    # ユニーク媒体数
    sources = set()
    kabu_hits = 0
    for it in items:
        src = (it.get("source") or "").strip().lower()
        if src:
            sources.add(src)
        title = (it.get("title") or "")
        if "kabutan" in (it.get("link") or "").lower() or "株探" in title:
            kabu_hits += 1
    raw = n + 0.5 * len(sources) + 1.5 * kabu_hits
    return _clip01(raw / 5.0)  # 5点で飽和

def _score_bbs(row: Dict[str, Any]) -> float:
    b = (row.get("bbs") or {})
    p24 = float(b.get("posts_24h") or 0)
    p72 = float(b.get("posts_72h") or 0)
    prev48 = max(0.0, p72 - p24)
    if prev48 <= 0:
        ratio = 0.0 if p24 == 0 else 1.0  # 初回増でも1.0寄与
    else:
        ratio = p24 / prev48
    # 10倍で ~1.0
    val = math.log1p(max(0.0, ratio)) / math.log1p(10.0)
    return _clip01(val)

def _score_rvol(row: Dict[str, Any]) -> float:
    rvol = float(((row.get("market") or {}).get("rvol")) or 0.0)
    return _clip01(rvol / 5.0)  # 5倍で満点

def _score_trends(row: Dict[str, Any]) -> float:
    t = row.get("trends")
    if isinstance(t, dict):
        latest = float(t.get("latest") or 0.0)
        base = float(t.get("median_base") or 0.0)
        # 無ければ latest をそのまま 0..1 として扱う
        if base <= 0: 
            return _clip01(latest)
        spike = max(0.0, latest - base)
        return _clip01(1.5 * spike)
    elif isinstance(t, (int, float)):
        return _clip01(float(t))
    return 0.0

def build_scores(rows: List[Dict[str, Any]], weights: Dict[str, float]) -> None:
    w_news   = float(weights.get("news", 0.40))
    w_bbs    = float(weights.get("bbs", 0.30))
    w_rvol   = float(weights.get("rvol", 0.20))
    w_trends = float(weights.get("trends", 0.10))

    for r in rows:
        s_news   = _score_news(r)
        s_bbs    = _score_bbs(r)
        s_rvol   = _score_rvol(r)
        s_trends = _score_trends(r)
        total = (w_news * s_news) + (w_bbs * s_bbs) + (w_rvol * s_rvol) + (w_trends * s_trends)
        r["score"] = round(total, 6)
        r["score_components"] = {
            "weights": {"news": w_news, "bbs": w_bbs, "rvol": w_rvol, "trends": w_trends},
            "news": s_news, "bbs": s_bbs, "rvol": s_rvol, "trends": s_trends, "total": total
        }
