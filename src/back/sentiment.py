# -*- coding: utf-8 -*-
"""
sentiment.py 強化版（キーワード重み付け対応）
- 既存のBERT等のモデル出力（score, label, debug）に「確定キーワード」スコアを加算して補強
- ここでは model_infer(text) という既存推論関数がある前提で呼び出します。
  （無い場合はダミーの中立スコア(0.5)で動作）
"""
from __future__ import annotations
from typing import Dict, Tuple, Optional, Any
import re
from pathlib import Path

# --- 確定キーワード重み ---
DEFINITE_POS_KWS = {
    "最高益": 2.0, "過去最高": 2.0, 
    "大幅上方修正": 1.5, "大幅増益": 1.5, "ストップ高": 1.5,
    "上方修正": 1.0, "増配": 1.0, "黒字転換": 1.0, "TOB": 1.0, "公開買い付け": 1.0,
    "増益": 0.5, "上振れ": 0.5, "好決算": 0.5,
}
DEFINITE_NEG_KWS = {
    "下方修正": -1.0, "大幅下方修正": -1.5, "ストップ安": -1.5,
    "減益": -0.5, "赤字転落": -1.0, "債務超過": -2.0, "特損": -0.5,
}

POS_THRESHOLD = 0.6
NEG_THRESHOLD = 0.4

def _get_definite_score(text: str) -> float:
    """テキストに含まれる確定キーワードのスコアを計算する"""
    t = text.lower()
    s = 0.0
    for kw, w in DEFINITE_POS_KWS.items():
        if kw in t:
            s += w
    for kw, w in DEFINITE_NEG_KWS.items():
        if kw in t:
            s += w
    return s

def _safe_model_infer(text: str) -> tuple[float, str, Dict[str, Any]]:
    """
    既存のモデル推論を呼ぶための薄いラッパ。（感情分析モデルが別途提供されていない場合）
    - 🚨 外部の感情分析モデルが提供されていない場合、中立スコア(0.5)でフォールバックします。
    戻り値: (score[0..1], label, debug)
    """
    try:
        # 💡 [実装が必要な場合] 外部モデルをインポートし、推論を呼び出す
        # from your_model_package import infer as model_infer
        # score, label, dbg = model_infer(text)
        # return float(score), str(label), dict(dbg)
        
        # 🚨 実装がないため、そのままフォールバックへ移行
        pass
    except Exception:
        pass
        
    # フォールバック：中立相当
    return 0.5, "neutral", {"explain_jp": "モデル推論なし (フォールバック)"}

def classify_sentiment(
    text: str,
    source_type: str = "news",
    source_name: Optional[str] = None,
    return_debug: bool = False,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    テキストの感情を分類するメイン関数。
    モデル推論結果を確定キーワードで補正する。
    """
    base_score, base_label, base_dbg = _safe_model_infer(text)

    debug: Dict[str, Any] = dict(base_dbg or {})
    debug["base_score"] = base_score
    debug["base_label"] = base_label

    # 1) 確定キーワードの加点
    definite = _get_definite_score(text or "")
    debug["definite_score"] = definite
    
    # 最終スコア：確定スコアを加算
    final_score = base_score + definite
    debug["final_score"] = final_score

    # 2) 最終的なラベル決定
    final_label = ""
    explain_jp = ""
    
    # 判定ロジック
    if final_score >= POS_THRESHOLD and definite >= 1.0:
        final_label = "positive"
        explain_jp = "確定キーワードによる強いポジティブ（例: 最高益、大幅上方修正）"
    elif final_score >= POS_THRESHOLD and definite > 0:
        final_label = "positive"
        explain_jp = "基本スコアがポジティブ ＋ 確定キーワードによる補強"
    elif final_score >= POS_THRESHOLD:
        final_label = "positive"
        explain_jp = "基本スコアがポジティブ判定"
        
    elif final_score <= NEG_THRESHOLD and definite <= -1.0:
        final_label = "negative"
        explain_jp = "確定キーワードによる強いネガティブ（例: 大幅下方修正、赤字転落）"
    elif final_score <= NEG_THRESHOLD and definite < 0:
        final_label = "negative"
        explain_jp = "基本スコアがネガティブ ＋ 確定キーワードによる補強"
    elif final_score <= NEG_THRESHOLD:
        final_label = "negative"
        explain_jp = "基本スコアがネガティブ判定"
        
    else:
        final_label = "neutral"
        explain_jp = debug.get("explain_jp", "中立と判定")

    debug["label"] = final_label
    debug["definite_pos_kws"] = [kw for kw in DEFINITE_POS_KWS if kw in text]
    debug["definite_neg_kws"] = [kw for kw in DEFINITE_NEG_KWS if kw in text]
    debug["explain_jp"] = explain_jp
    debug["definite_score_norm"] = final_score
    
    if return_debug:
        return final_label, debug
    else:
        return final_label, None