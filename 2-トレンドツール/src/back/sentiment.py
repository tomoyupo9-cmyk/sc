# -*- coding: utf-8 -*-
"""
sentiment.py 強化版（日本語最適化＋可視化）
- ① 金融辞書/テンプレで確定ラベル（AIより優先）
- ② 否定/逆接で中和（neutral）
- ③ 媒体別スコア補正（株探/日経/Reuters/Bloomberg/TDnet/PRTIMES など）
- ④ 掲示板の煽り語で positive→neutral 補正
- ⑤ config.yaml からモデル＆しきい値を自動読込
- ⑥ return_debug=True で「判定の日本語説明」を含む詳細を返す

依存（日本語モデル利用時）:
  pip install transformers fugashi ipadic sentencepiece
"""

from __future__ import annotations
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple, Optional

# -------------------------------
# 設定ファイルのロード
# -------------------------------
def _find_config_path() -> Optional[Path]:
    p = Path(__file__).resolve()
    for parent in [p.parent, *p.parents]:
        cand = parent / "config.yaml"
        if cand.exists():
            return cand
    return None

@lru_cache(maxsize=1)
def _load_settings() -> Dict:
    defaults = {
        "model": "daigo/bert-base-japanese-sentiment",  # 日本語特化モデル
        "threshold": {"news": 0.65, "bbs": 0.70},
    }
    try:
        import yaml  # type: ignore
        cfg_path = _find_config_path()
        if cfg_path:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            if isinstance(cfg.get("sentiment"), dict):
                s = cfg["sentiment"]
                if "model" in s:
                    defaults["model"] = s["model"]
                if "threshold" in s and isinstance(s["threshold"], dict):
                    defaults["threshold"].update(s["threshold"])
    except Exception:
        pass
    return defaults

# -------------------------------
# 前処理
# -------------------------------
Z2H = str.maketrans({"（":"(", "）":")", "：":":", "！":"!", "？":"?", "　":" "})
def _normalize(text: str) -> str:
    if not text:
        return ""
    t = text.strip().translate(Z2H)
    t = re.sub(r"https?://\S+", "", t)            # URL除去
    t = re.sub(r"[#＠@]\S+", "", t)               # ハッシュ/メンション除去
    t = re.sub(r"\s+", " ", t)                    # 連続空白
    t = t.replace("ｗｗｗ", "笑").replace("w", "")
    return t

# -------------------------------
# 金融辞書・テンプレ
# -------------------------------
POS_WORDS = (
    "上方修正","増配","黒字","黒字転換","好調","受注","受賞",
    "提携","上積み","上昇","最高益","増益","過去最高益","大幅増益",
    "好決算","上振れ","計画超過","通期上方","通期増額","増収増益",
    "特別利益","契約獲得","大型受注","量産化","自社株買い",
    "復配","上場維持","監理解除","特設注意解除","指定替え承認",
    "増資中止","業務再開","行政処分解除","承認","販売開始",
)

NEG_WORDS = (
    "下方修正","減配","赤字","不調","不振","偽装","粉飾",
    "下落","ストップ安","業績悪化","減益","不正","大幅減益",
    "希薄化","公募","PO","売出","第三者割当","MSワラント","ＭＳワラント",
    "劣後","監理","特設注意","注意喚起","上場廃止","粉飾決算",
    "不正会計","業績未達","業務停止","行政処分","業務改善命令",
)

POS_PATTERNS = [
    re.compile(r"通期(見通し|予想).*(上方修正)"),
    re.compile(r"(大型|新規)受注"),
    re.compile(r"自社株買い"),
]
NEG_PATTERNS = [
    re.compile(r"通期(見通し|予想).*(下方修正)"),
    re.compile(r"公募(増資|売出)"),
    re.compile(r"(監理|特設注意)指定"),
    re.compile(r"希薄化率?\s*\d+%"),
]

NEGATE_PATTERNS = [
    re.compile(r"ではない"), re.compile(r"見込みはない"),
    re.compile(r"しかし"), re.compile(r"一方"), re.compile(r"だが"),
]

BBS_NOISE = ["草","情弱","養分","オワタ","逝った","仕手","煽り"]

def _keyword_or_pattern(text: str) -> str:
    if any(w in text for w in POS_WORDS): return "positive"
    if any(w in text for w in NEG_WORDS): return "negative"
    for p in POS_PATTERNS:
        if p.search(text): return "positive"
    for p in NEG_PATTERNS:
        if p.search(text): return "negative"
    return "neutral"

def _has_negation(text: str) -> bool:
    return any(p.search(text) for p in NEGATE_PATTERNS)

def _has_bbs_noise(text: str) -> bool:
    return any(w in text for w in BBS_NOISE)

# -------------------------------
# モデル実行（Hugging Face）
# -------------------------------
@lru_cache(maxsize=1)
def _get_pipe(model_name: str):
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline  # type: ignore
    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForSequenceClassification.from_pretrained(model_name)
    return pipeline("text-classification", model=mdl, tokenizer=tok, top_k=None)

def _run_model(text: str, model_name: str) -> Tuple[str, float]:
    clf = _get_pipe(model_name)
    out = clf(text, truncation=True, max_length=256)
    scores = out if isinstance(out, list) else [out]
    norm: Dict[str, float] = {}
    for s in scores:
        label = (s.get("label") or "").lower()
        sc = float(s.get("score") or 0.0)
        if "neg" in label:   norm["negative"] = sc
        elif "neu" in label: norm["neutral"]  = sc
        elif "pos" in label: norm["positive"] = sc
    if not norm:
        return "neutral", 0.0
    return max(norm.items(), key=lambda kv: kv[1])

# -------------------------------
# 日本語説明の生成
# -------------------------------
def _explain_jp(dbg: dict) -> str:
    path = " > ".join(dbg.get("decision_path", []))
    src = dbg.get("source_name") or ""
    base = []
    if dbg.get("keyword_hit"): base.append("辞書ヒット")
    if dbg.get("pattern_hit"): base.append("テンプレ一致")
    if dbg.get("negation"):    base.append("否定/逆接→中立")
    if src:
        adj = dbg.get("media_adjustment") or 0.0
        if adj > 0: base.append(f"媒体補正(+{adj:.2f}):{src}")
        elif adj < 0: base.append(f"媒体補正({adj:.2f}):{src}")
    if dbg.get("bbs_noise"): base.append("掲示板の煽り語→中立")
    th = dbg.get("threshold")
    best = dbg.get("best_label")
    sc  = dbg.get("best_score")
    fin = dbg.get("final_label")
    bits = []
    if base: bits.append(" / ".join(base))
    if sc is not None and best is not None: bits.append(f"モデル={best}:{sc:.2f}")
    if th is not None: bits.append(f"閾値={th}")
    if path: bits.append(f"[流れ:{path}]")
    bits.append(f"→ 最終:{fin}")
    return " | ".join(bits)

# -------------------------------
# 公開API
# -------------------------------
def classify_sentiment(
    text: str,
    source_type: str = "news",
    source_name: str = "",
    return_debug: bool = False,
):
    """
    戻り値:
      return_debug=False … 'positive' | 'neutral' | 'negative'
      return_debug=True  … (label, debug_dict)
    """
    debug = {
        "input": text,
        "source_type": source_type,
        "source_name": source_name,
        "normalized": None,
        "keyword_hit": None,
        "pattern_hit": None,
        "negation": None,
        "model": None,
        "raw_scores": None,
        "best_label": None,
        "best_score": None,
        "threshold": None,
        "media_adjustment": 0.0,
        "bbs_noise": None,
        "final_label": None,
        "decision_path": [],
        "explain_jp": None,
    }

    if not text or not text.strip():
        if return_debug:
            debug["final_label"] = "neutral"; debug["decision_path"].append("empty->neutral")
            debug["explain_jp"] = _explain_jp(debug)
            return "neutral", debug
        return "neutral"

    t = _normalize(text)
    debug["normalized"] = t

    # 1) 辞書/テンプレ優先
    kw = _keyword_or_pattern(t)
    debug["keyword_hit"]  = (kw == "positive") or (kw == "negative") and any(w in t for w in POS_WORDS)
    debug["pattern_hit"]  = (kw != "neutral" and not debug["keyword_hit"])
    if kw != "neutral":
        debug["final_label"] = kw
        debug["decision_path"].append("rule->kw/pattern")
        if return_debug:
            debug["explain_jp"] = _explain_jp(debug)
            return kw, debug
        return kw

    # 2) モデル判定
    settings = _load_settings()
    model_name = settings["model"]
    th_news = float(settings["threshold"]["news"]); th_bbs = float(settings["threshold"]["bbs"])
    debug["model"] = model_name
    try:
        label, score = _run_model(t, model_name=model_name)
        debug["raw_scores"] = {"best": [label, score]}
        debug["best_label"] = label
        debug["best_score"] = score
        debug["decision_path"].append("model")
    except Exception:
        debug["final_label"] = "neutral"; debug["decision_path"].append("model_fail->neutral")
        if return_debug:
            debug["explain_jp"] = _explain_jp(debug)
            return "neutral", debug
        return "neutral"

    # 3) 否定/逆接 → 中和
    neg = _has_negation(t)
    debug["negation"] = neg
    if neg:
        debug["final_label"] = "neutral"; debug["decision_path"].append("negation->neutral")
        if return_debug:
            debug["explain_jp"] = _explain_jp(debug)
            return "neutral", debug
        return "neutral"

    # 4) 媒体補正
    adj = 0.0
    if source_name:
        good = ("株探","Kabutan","日経","REUTERS","Bloomberg","TDnet","PRTIMES")
        bad  = ("ブログ","まとめ","Yahoo知恵袋")
        if any(s in source_name for s in good):
            adj += 0.05
        elif any(s in source_name for s in bad):
            adj -= 0.05
    score += adj
    debug["media_adjustment"] = adj

    # 5) 掲示板の煽り語 → positiveをneutral
    bbs_noise = (_has_bbs_noise(t) if source_type == "bbs" else False)
    debug["bbs_noise"] = bbs_noise
    if source_type == "bbs" and label == "positive" and bbs_noise:
        debug["final_label"] = "neutral"; debug["decision_path"].append("bbs_noise->neutral")
        if return_debug:
            debug["explain_jp"] = _explain_jp(debug)
            return "neutral", debug
        return "neutral"

    # 6) しきい値
    th = th_news if source_type == "news" else th_bbs
    debug["threshold"] = th
    final = label if score >= th else "neutral"
    debug["final_label"] = final
    debug["decision_path"].append(f"threshold({th})->{final}")
    if return_debug:
        debug["explain_jp"] = _explain_jp(debug)
        return final, debug
    return final
