# -*- coding: utf-8 -*-
"""
scoring.py
「仕手化・演出銘柄」を早期発見するための総合スコアリング。

総合スコア(0~1) = 
  0.4 * ニュース強度 +
  0.3 * BBS増加率 +
  0.2 * 出来高RVOL +
  0.1 * GTrends急増

※ RVOLはデータが無い環境では0として扱う（将来拡張用）。
※ 各コンポーネントは0~1に正規化。
"""

from __future__ import annotations
from typing import Dict, List, Any, Tuple
import math
import statistics

# --- ユーティリティ ----------------------------------------------------------

def _clip01(x: float) -> float:
    if x < 0: return 0.0
    if x > 1: return 1.0
    return float(x)

def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    try:
        if den == 0: return default
        return float(num) / float(den)
    except Exception:
        return default

def _median_or(x: List[float], default: float = 0.0) -> float:
    try:
        return float(statistics.median(x)) if x else default
    except Exception:
        return default

# --- コンポーネント別スコア --------------------------------------------------

def score_news(row: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """
    ニュース強度：
      - 直近24hの件数（count_24h）
      - ユニーク媒体数（同一媒体の連投を割り引く）
      - 株探(Kabutan)等の“演出に直結しやすい媒体”ボーナス
    """
    news = row.get("news") or {}
    items = news.get("items") or []
    count_24h = float(news.get("count_24h") or 0)

    # ユニーク媒体数
    srcs = []
    kabutan_hits = 0
    for it in items:
        src = (it.get("source") or "").strip()
        if src:
            srcs.append(src)
            if "株探" in src or "Kabutan" in src:
                kabutan_hits += 1
    uniq_src = len(set(srcs))

    # 原点：件数 + 0.5*ユニーク媒体 + 1.5*株探
    raw = count_24h + 0.5 * uniq_src + 1.5 * kabutan_hits

    # 5点で満点に近づく飽和（多すぎる記事は1.0で打ち止め）
    norm = _clip01(raw / 5.0)

    return norm, {
        "count_24h": count_24h,
        "uniq_sources": uniq_src,
        "kabutan_hits": kabutan_hits,
        "raw": raw,
        "norm": norm,
    }

def score_bbs(row: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """
    BBS増加率：
      - 直近24hの投稿数 / 直前48hの投稿数
      - 絶対数が小さくても「比率の急増」を拾う
      - ログ圧縮で外れ値を抑制（ratio=5で ~0.7、10で ~0.85 程度）
    """
    bbs = row.get("bbs") or {}
    p24 = float(bbs.get("posts_24h") or 0)
    p72 = float(bbs.get("posts_72h") or 0)

    prev48 = max(0.0, p72 - p24)
    denom = max(1.0, prev48)  # 0除算回避、小さすぎるときは1扱い
    ratio = _safe_div(p24, denom, 0.0)

    # ログ圧縮 → 5倍で0.7前後、10倍で0.85前後、無限大で1に漸近
    norm = _clip01(math.log1p(ratio) / math.log1p(10.0))  # 基準10倍

    return norm, {
        "posts_24h": p24,
        "posts_72h": p72,
        "prev48": prev48,
        "ratio": ratio,
        "norm": norm,
    }

def score_trends(row: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """
    GTrends急増：
      - latest（0~1）と、“直近以前のベース”との差分を見る
      - series があれば直近7を除いた直前~30の中央値をベースに。
      - seriesが無ければ latest をそのまま採用。
    """
    trends = row.get("trends") or {}
    latest = float(trends.get("latest") or 0.0)  # 0~1 の想定
    series = trends.get("series") or []

    base = 0.0
    if series and isinstance(series, list):
        vals = [float(p.get("score") or 0.0) / 100.0 for p in series]  # 0~100→0~1
        if len(vals) > 8:
            base = _median_or(vals[:-7], 0.0)  # 直近7ポイントを除いた中央値
        elif vals:
            base = _median_or(vals, 0.0)

    spike = max(0.0, latest - base)  # ベースよりどれだけ上回ったか
    # ベースが極小の時の偶発点灯を抑えるため軽く圧縮
    norm = _clip01(spike * 1.5)  # 1.5倍しても上限クリップ

    return norm, {
        "latest": latest,
        "base": base,
        "spike": spike,
        "norm": norm,
    }

def score_rvol(row: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """
    出来高RVOL（20日平均比）：
      - row.get('market', {}).get('rvol') を想定。
      - 無ければ 0 とする（既存環境は価格系を未取得のため）。
      - RVOL=3→0.6, 5→1.0（上限）程度のスケーリング。
    """
    market = row.get("market") or {}
    rvol = float(market.get("rvol") or 0.0)
    # 5倍で飽和（1.0）。それ以下は線形。
    norm = _clip01(rvol / 5.0)
    return norm, {"rvol": rvol, "norm": norm}

# --- メイン: スコア合成 ------------------------------------------------------

DEFAULT_WEIGHTS = {
    "news": 0.40,
    "bbs":  0.30,
    "rvol": 0.20,
    "trends": 0.10,
}

def build_scores(rows: List[Dict[str, Any]], weights: Dict[str, float] | None = None) -> List[float]:
    """
    各rowに score と score_components を付与し、score配列を返す。
    weights が無ければ DEFAULT_WEIGHTS を使用。
    """
    w = dict(DEFAULT_WEIGHTS)
    if weights:
        w.update({k: float(v) for k, v in weights.items() if k in w})

    out_scores: List[float] = []

    for r in rows:
        n_val, n_dbg = score_news(r)
        b_val, b_dbg = score_bbs(r)
        v_val, v_dbg = score_rvol(r)
        t_val, t_dbg = score_trends(r)

        score = (
            w["news"]   * n_val +
            w["bbs"]    * b_val +
            w["rvol"]   * v_val +
            w["trends"] * t_val
        )
        score = _clip01(score)

        r["score_components"] = {
            "news":   n_dbg,
            "bbs":    b_dbg,
            "rvol":   v_dbg,
            "trends": t_dbg,
            "weights": w,
            "total": score,
        }

        out_scores.append(score)

    return out_scores
