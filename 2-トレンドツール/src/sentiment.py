# -*- coding: utf-8 -*-
"""
sentiment.py (fast)
- 互換 API: classify_sentiment(..., return_debug) を維持
- 早期ルール判定 → モデル回避、LRUキャッシュ、遅延ロード共有パイプライン
- 追加: classify_sentiment_batch, warmup
"""

from __future__ import annotations
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Iterable

def _find_config_path() -> Optional[Path]:
    p = Path(__file__).resolve()
    for parent in [p.parent, *p.parents]:
        cand = parent / "config.yaml"
        if cand.exists(): return cand
    return None

@lru_cache(maxsize=1)
def _load_settings() -> Dict:
    defaults = {
        "model": "daigo/bert-base-japanese-sentiment",
        "threshold": {"news": 0.65, "bbs": 0.70},
        "hf": {"device": None, "batch_size": 16, "threads": 1}
    }
    try:
        import yaml  # type: ignore
        cfg_path = _find_config_path()
        if cfg_path:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            s = (cfg.get("sentiment") or {}) if isinstance(cfg.get("sentiment"), dict) else {}
            if "model" in s: defaults["model"] = s["model"]
            if "threshold" in s and isinstance(s["threshold"], dict):
                defaults["threshold"].update(s["threshold"])
            if isinstance(s.get("hf"), dict):
                defaults["hf"].update({k:v for k,v in s["hf"].items() if k in defaults["hf"]})
    except Exception:
        pass
    return defaults

Z2H = str.maketrans({"（":"(", "）":")", "：":":", "！":"!", "？":"?", "　":" "})
def _normalize(text: str) -> str:
    if not text: return ""
    t = text.strip().translate(Z2H)
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[#＠@]\S+", "", t)
    t = re.sub(r"\s+", " ", t)
    t = t.replace("ｗｗｗ", "笑").replace("w","")
    return t

POS_WORDS = ("上方修正","増配","黒字","黒字転換","好調","受注","受賞","提携","上積み","上昇","最高益","増益","過去最高益","大幅増益","好決算","上振れ","計画超過","通期上方","通期増額","増収増益","特別利益","契約獲得","大型受注","量産化","自社株買い","復配","上場維持","監理解除","特設注意解除","指定替え承認","増資中止","業務再開","行政処分解除","承認","販売開始")
NEG_WORDS = ("下方修正","減配","赤字","不調","不振","偽装","粉飾","下落","ストップ安","業績悪化","減益","不正","大幅減益","希薄化","公募","PO","売出","第三者割当","MSワラント","ＭＳワラント","劣後","監理","特設注意","注意喚起","上場廃止","粉飾決算","不正会計","業績未達","業務停止","行政処分","業務改善命令")
POS_PATTERNS = [re.compile(r"通期(見通し|予想).*(上方修正)"), re.compile(r"(大型|新規)受注"), re.compile(r"自社株買い")]
NEG_PATTERNS = [re.compile(r"通期(見通し|予想).*(下方修正)"), re.compile(r"公募(増資|売出)"), re.compile(r"(監理|特設注意)指定"), re.compile(r"希薄化率?\s*\d+%")]
NEGATE_PATTERNS = [re.compile(r"ではない"), re.compile(r"見込みはない"), re.compile(r"しかし"), re.compile(r"一方"), re.compile(r"だが")]
BBS_NOISE = ["草","情弱","養分","オワタ","逝った","仕手","煽り"]

def _keyword_or_pattern(t: str) -> str:
    if any(w in t for w in POS_WORDS): return "positive"
    if any(w in t for w in NEG_WORDS): return "negative"
    for p in POS_PATTERNS:
        if p.search(t): return "positive"
    for p in NEG_PATTERNS:
        if p.search(t): return "negative"
    return "neutral"

def _has_negation(t: str) -> bool:
    return any(p.search(t) for p in NEGATE_PATTERNS)

def _has_bbs_noise(t: str) -> bool:
    return any(w in t for w in BBS_NOISE)

_PIPELINE = None
def _init_threads(threads: int):
    try:
        import torch  # type: ignore
        torch.set_num_threads(max(1,int(threads)))
        torch.set_num_interop_threads(max(1,int(threads)))
    except Exception:
        pass

def _resolve_device(device_cfg: Optional[str]) -> int:
    if device_cfg is None:
        try:
            import torch  # type: ignore
            return 0 if torch.cuda.is_available() else -1
        except Exception:
            return -1
    if device_cfg == "cpu": return -1
    if str(device_cfg).startswith("cuda"): return 0
    return -1

def _get_pipe(model_name: str):
    global _PIPELINE
    if _PIPELINE is not None: return _PIPELINE
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline  # type: ignore
    s = _load_settings(); hf = s.get("hf", {})
    _init_threads(int(hf.get("threads",1)))
    device_idx = _resolve_device(hf.get("device"))
    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForSequenceClassification.from_pretrained(model_name)
    _PIPELINE = pipeline("text-classification", model=mdl, tokenizer=tok, top_k=None, device=device_idx)
    return _PIPELINE

def warmup():
    s = _load_settings(); _get_pipe(s["model"])

@lru_cache(maxsize=8192)
def _run_model_cached(norm_text: str, model_name: str) -> Tuple[str, float]:
    clf = _get_pipe(model_name)
    out = clf(norm_text, truncation=True, max_length=256)
    scores = out if isinstance(out, list) else [out]
    norm: Dict[str, float] = {}
    for s in scores:
        label = (s.get("label") or "").lower()
        sc = float(s.get("score") or 0.0)
        if "neg" in label:   norm["negative"] = sc
        elif "neu" in label: norm["neutral"]  = sc
        elif "pos" in label: norm["positive"] = sc
    if not norm: return "neutral", 0.0
    return max(norm.items(), key=lambda kv: kv[1])

def _looks_uninformative(t: str) -> bool:
    if not t: return True
    if len(t) < 6: return True
    punct = sum(1 for ch in t if ch in ".,!?;:()[]{}「」『』【】…—-‐・/\\|~^=+*#@$%")
    if punct / max(1,len(t)) >= 0.35: return True
    if re.fullmatch(r"[0-9\s\-\+\.%,/]+", t): return True
    return False

def _explain_jp(dbg: dict) -> str:
    path = " > ".join(dbg.get("decision_path", []))
    src = dbg.get("source_name") or ""
    base = []
    if dbg.get("keyword_hit"): base.append("辞書ヒット")
    if dbg.get("pattern_hit"): base.append("テンプレ一致")
    if dbg.get("negation"): base.append("否定/逆接→中立")
    if dbg.get("bbs_noise"): base.append("掲示板の煽り語→中立")
    th = dbg.get("threshold"); best = dbg.get("best_label"); sc = dbg.get("best_score")
    bits = []
    if base: bits.append(" / ".join(base))
    if sc is not None and best is not None: bits.append(f"モデル={best}:{sc:.2f}")
    if th is not None: bits.append(f"閾値={th}")
    if path: bits.append(f"[流れ:{path}]")
    bits.append(f"→ 最終:{dbg.get('final_label')}")
    return " | ".join(bits)

def classify_sentiment(text: str, source_type: str = "news", source_name: str = "", return_debug: bool = False):
    dbg = {"input": text, "source_type": source_type, "source_name": source_name,
           "normalized": None, "keyword_hit": None, "pattern_hit": None, "negation": None,
           "model": None, "raw_scores": None, "best_label": None, "best_score": None,
           "threshold": None, "media_adjustment": 0.0, "bbs_noise": None,
           "final_label": None, "decision_path": [], "explain_jp": None}
    if not text or not str(text).strip():
        dbg["final_label"]="neutral"; dbg["decision_path"].append("empty->neutral")
        return ("neutral", dbg) if return_debug else "neutral"
    t = _normalize(str(text)); dbg["normalized"]=t

    if _looks_uninformative(t):
        dbg["final_label"]="neutral"; dbg["decision_path"].append("uninformative->neutral")
        return ("neutral", dbg) if return_debug else "neutral"

    kw = _keyword_or_pattern(t)
    dbg["keyword_hit"] = any(w in t for w in POS_WORDS) or any(w in t for w in NEG_WORDS)
    dbg["pattern_hit"] = (kw!="neutral" and not dbg["keyword_hit"])
    if kw!="neutral":
        dbg["final_label"]=kw; dbg["decision_path"].append("rule->kw/pattern")
        return (kw, dbg) if return_debug else kw

    s = _load_settings(); model_name = s["model"]
    dbg["model"]=model_name
    try:
        label, score = _run_model_cached(t, model_name)
        dbg["raw_scores"]={"best":[label, score]}; dbg["best_label"]=label; dbg["best_score"]=score
        dbg["decision_path"].append("model")
    except Exception:
        dbg["final_label"]="neutral"; dbg["decision_path"].append("model_fail->neutral")
        return ("neutral", dbg) if return_debug else "neutral"

    if _has_negation(t):
        dbg["final_label"]="neutral"; dbg["negation"]=True; dbg["decision_path"].append("negation->neutral")
        dbg["explain_jp"]=_explain_jp(dbg)
        return ("neutral", dbg) if return_debug else "neutral"

    adj = 0.0
    if source_name:
        good=("株探","Kabutan","日経","REUTERS","Bloomberg","TDnet","PRTIMES")
        bad =("ブログ","まとめ","Yahoo知恵袋")
        if any(s in source_name for s in good): adj += 0.05
        elif any(s in source_name for s in bad): adj -= 0.05
    score += adj

    bbs_noise = (_has_bbs_noise(t) if source_type=="bbs" else False); dbg["bbs_noise"]=bbs_noise
    if source_type=="bbs" and label=="positive" and bbs_noise:
        dbg["final_label"]="neutral"; dbg["decision_path"].append("bbs_noise->neutral")
        dbg["explain_jp"]=_explain_jp(dbg)
        return ("neutral", dbg) if return_debug else "neutral"

    th = float(s["threshold"]["news"] if source_type=="news" else s["threshold"]["bbs"]); dbg["threshold"]=th
    final = label if score >= th else "neutral"
    dbg["final_label"]=final; dbg["decision_path"].append(f"threshold({th})->{final}")
    dbg["explain_jp"]=_explain_jp(dbg)
    return (final, dbg) if return_debug else final

def classify_sentiment_batch(texts: Iterable[str], source_type: str = "news", source_name: str = "", return_debug: bool = False) -> List:
    s = _load_settings(); model_name = s["model"]; hf = s.get("hf", {})
    th_news = float(s["threshold"]["news"]); th_bbs = float(s["threshold"]["bbs"])
    items = list(texts or [])
    outs: List = [None]*len(items)
    need_idx, need_texts = [], []

    for i, raw in enumerate(items):
        dbg = {"input": raw, "source_type": source_type, "source_name": source_name,
               "normalized": None, "keyword_hit": None, "pattern_hit": None, "negation": None,
               "model": model_name, "raw_scores": None, "best_label": None, "best_score": None,
               "threshold": None, "media_adjustment": 0.0, "bbs_noise": None,
               "final_label": None, "decision_path": [], "explain_jp": None}
        if not raw or not str(raw).strip():
            lab="neutral"; dbg["final_label"]=lab; dbg["decision_path"].append("empty->neutral")
            outs[i]=(lab,dbg) if return_debug else lab; continue
        t=_normalize(str(raw)); dbg["normalized"]=t
        if _looks_uninformative(t):
            lab="neutral"; dbg["final_label"]=lab; dbg["decision_path"].append("uninformative->neutral")
            outs[i]=(lab,dbg) if return_debug else lab; continue
        kw = _keyword_or_pattern(t)
        dbg["keyword_hit"]=any(w in t for w in POS_WORDS) or any(w in t for w in NEG_WORDS)
        dbg["pattern_hit"]=(kw!="neutral" and not dbg["keyword_hit"])
        if kw!="neutral":
            dbg["final_label"]=kw; dbg["decision_path"].append("rule->kw/pattern")
            outs[i]=(kw,dbg) if return_debug else kw; continue
        need_idx.append(i); need_texts.append(t)

    if need_texts:
        pipe = _get_pipe(model_name)
        bs = int(hf.get("batch_size",16))
        preds: List[Tuple[str,float]] = []
        # キャッシュ反映
        to_run_idx, to_run_texts = [], []
        for t in need_texts:
            try:
                preds.append(_run_model_cached(t, model_name))  # キャッシュヒット時
            except Exception:
                preds.append(("__PENDING__", 0.0))
                to_run_idx.append(len(preds)-1); to_run_texts.append(t)
        # バッチ推論
        for st in range(0, len(to_run_texts), bs):
            batch = to_run_texts[st:st+bs]
            res = pipe(batch, truncation=True, max_length=256)
            if isinstance(res, dict): res=[res]
            for j, out in enumerate(res):
                scores = out if isinstance(out,list) else [out]
                norm: Dict[str,float] = {}
                for s_ in scores:
                    label=(s_.get("label") or "").lower(); sc=float(s_.get("score") or 0.0)
                    if "neg" in label: norm["negative"]=sc
                    elif "neu" in label: norm["neutral"]=sc
                    elif "pos" in label: norm["positive"]=sc
                lab, sc = ("neutral",0.0) if not norm else max(norm.items(), key=lambda kv: kv[1])
                preds[to_run_idx[st+j]] = (lab, sc)

        for pos, (lab, sc) in zip(need_idx, preds):
            raw = items[pos]; t=_normalize(str(raw))
            dbg = {"input": raw, "source_type": source_type, "source_name": source_name, "normalized": t,
                   "keyword_hit": False, "pattern_hit": False, "negation": None, "model": model_name,
                   "raw_scores": {"best":[lab,sc]}, "best_label": lab, "best_score": sc, "threshold": None,
                   "media_adjustment": 0.0, "bbs_noise": None, "final_label": None, "decision_path": ["model"], "explain_jp": None}
            if _has_negation(t):
                final="neutral"; dbg["negation"]=True; dbg["decision_path"].append("negation->neutral"); dbg["final_label"]=final; dbg["explain_jp"]=_explain_jp(dbg)
                outs[pos]=(final,dbg) if return_debug else final; continue
            adj=0.0
            if source_name:
                good=("株探","Kabutan","日経","REUTERS","Bloomberg","TDnet","PRTIMES")
                bad=("ブログ","まとめ","Yahoo知恵袋")
                if any(s in source_name for s in good): adj+=0.05
                elif any(s in source_name for s in bad): adj-=0.05
            sc2=sc+adj; dbg["media_adjustment"]=adj
            if source_type=="bbs" and lab=="positive" and _has_bbs_noise(t):
                final="neutral"; dbg["bbs_noise"]=True; dbg["decision_path"].append("bbs_noise->neutral"); dbg["final_label"]=final; dbg["threshold"]=th_bbs; dbg["explain_jp"]=_explain_jp(dbg)
                outs[pos]=(final,dbg) if return_debug else final; continue
            th = th_news if source_type=="news" else th_bbs
            final = lab if sc2 >= th else "neutral"; dbg["threshold"]=th; dbg["final_label"]=final; dbg["decision_path"].append(f"threshold({th})->{final}"); dbg["explain_jp"]=_explain_jp(dbg)
            outs[pos]=(final,dbg) if return_debug else final
    return outs
