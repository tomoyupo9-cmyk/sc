#!/usr/bin/env python
# -*- coding: utf-8 -*-

# fetch_all.py — TDnetデータクローラー（決算・TOB・増資取得＆DB保存専用スリム版）
# ※ダッシュボードHTML出力機能やBBS取得機能は自動スクリーニング.pyに委譲し削除済み。
# ※common.pyやsentiment.pyなどの外部ファイルに依存せず、単独で動作します。

import os
import io
import re
import html
import json
import time
import requests
import sqlite3
import warnings
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

# --- ログ抑制 ---
logging.getLogger("PyPDF2").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)
warnings.filterwarnings("ignore", category=UserWarning)

# --- 環境設定・DBパス ---
JST = timezone(timedelta(hours=9))
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

def get_db_conn() -> sqlite3.Connection:
    """独立して動くためのシンプルなDB接続関数"""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

def now_iso() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

# ------------------ 簡易センチメント ------------------
POS_KW = ["上方修正","増益","最高益","過去最高","増配","黒字転換","上振れ","好調","好決算","上乗せ","大幅増","公開買付け","公開買い付け","TOB","ＴＯＢ"]
NEG_KW = ["下方修正","減益","赤字","減配","未達","下振れ","特損","不適正","監理","大幅減","業績悪化"]

def _judge_sentiment(title: str) -> tuple[str, int, str]:
    t = title or ""
    for kw in POS_KW:
        if kw in t: return ("positive", +2, kw)
    for kw in NEG_KW:
        if kw in t: return ("negative", -2, kw)
    return ("neutral", 0, "")

def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def summarize_title_simple(title: str) -> str:
    t = str(title or "")
    t = re.sub(r"【.*?】|\[.*?\]|\(.*?\)|（.*?）|＜.*?＞|〈.*?〉", "", t)
    t = re.sub(r"^\s*適時開示\s*[:：-]?\s*", "", t)
    t = re.sub(r"\s*のお知らせ\s*$", "", t)
    t = _normalize_spaces(t)
    return (t[:80] + "…") if len(t) > 80 else t

def _summarize_title_simple(title: str):
    summary = summarize_title_simple(title)
    label, score, hit_kw = _judge_sentiment(title) 
    return summary, label, hit_kw

# ------------------ TDnet: API取得基盤 ------------------
EARNINGS_KW = ["決算","短信","四半期","通期","上方修正","下方修正","業績","配当","進捗"]
TOB_KW = ["公開買付け", "公開買い付け", "公開買付", "公開買い付", "TOB", "ＴＯＢ"]
OFFERING_KW = [
    "公募増資","第三者割当","第三者割当増資","新株発行","自己株式の処分",
    "株式の売出","募集新株予約権","新株予約権発行","MSワラント","ワラント",
    "転換社債","CB","EB債","ライツ・オファリング","行使", "行使状況", "新株予約権の行使"
]

def _unescape_text(s: str) -> str:
    if not s: return ""
    try:
        if r"\u" in s:
            s = s.encode("utf-8").decode("unicode_escape")
    except Exception: pass
    return html.unescape(s).strip()

from urllib.parse import quote
def _http_get_json(url: str, retries: int = 3, sleep_sec: float = 0.8):
    global _HTTP
    try:
        _HTTP  # type: ignore
    except NameError:
        _HTTP = requests.Session()
        try:
            _RETRY = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
            _ADPT  = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=_RETRY)
        except Exception:
            _ADPT  = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        _HTTP.mount("http://", _ADPT); _HTTP.mount("https://", _ADPT)

    for i in range(retries):
        try:
            r = _HTTP.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/json" not in ctype and not r.text.strip().startswith(("{","[")):
                if i == retries - 1:
                    print(f"[_http_get_json] non-json {ctype or '(unknown)'} url={url}")
                else:
                    time.sleep(sleep_sec)
                continue
            return r.json()
        except Exception as e:
            if i == retries - 1:
                msg = str(e).splitlines()[0]
                print(f"[_http_get_json] error: {msg} url={url}")
                return None
            time.sleep(sleep_sec)
    return None

def fetch_tdnet_by_keywords(
    days: int = 90,
    keywords: list[str] | None = None,
    per_day_limit: int = 300,
    slice_escalation=(12, 6, 3, 1, 0.5)
) -> List[Dict[str, Any]]:
    time_capable = False
    KEY_RE_LOCAL = re.compile("|".join(map(re.escape, keywords or []))) if (keywords and len(keywords) > 0) else None

    def _fetch_tdnet_by_range(dt_from: datetime, dt_to: datetime) -> list[dict]:
        nonlocal time_capable
        s_day  = dt_from.strftime("%Y%m%d"); e_day  = dt_to.strftime("%Y%m%d")
        s_isoT = dt_from.strftime("%Y-%m-%dT%H:%M:%S"); e_isoT = dt_to.strftime("%Y-%m-%dT%H:%M:%S")
        s_nosep= dt_from.strftime("%Y%m%d%H%M%S");      e_nosep= dt_to.strftime("%Y%m%d%H%M%S")
        s_spc  = dt_from.strftime("%Y%m%d %H:%M:%S");   e_spc  = dt_to.strftime("%Y%m%d %H:%M:%S")

        url_day = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json"
        js_day = _http_get_json(url_day)
        items_day = js_day.get("items") if isinstance(js_day, dict) else (js_day if isinstance(js_day, list) else None)
        if isinstance(items_day, list):
            return items_day or []

        variants = [
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json",     "list/day",  False),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_nosep}-{e_nosep}.json", "list/nosep", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_isoT}-{e_isoT}.json",   "list/isoT",  True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{quote(s_spc)}-{quote(e_spc)}.json", "list/space", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list_time/{s_nosep}-{e_nosep}.json", "list_time/nosep", True),
        ]
        for url, tag, is_time in variants:
            js = _http_get_json(url)
            items = js.get("items") if isinstance(js, dict) else (js if isinstance(js, list) else None)
            if isinstance(items, list):
                if is_time: time_capable = True
                return items or []
        return []

    def _fetch_day_extra_pages(s_day: str, e_day: str, page_try: int = 15) -> list[dict]:
        collected = []
        base = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json"
        patterns = ["?page={p}", "?p={p}"]
        for p in range(2, 2 + max(1, page_try)):
            hit = 0
            for fmt in patterns:
                url = base + fmt.format(p=p)
                js = _http_get_json(url)
                items = js.get("items") if isinstance(js, dict) else (js if isinstance(js, list) else None)
                if isinstance(items, list) and items:
                    collected.extend(items); hit += len(items)
                    break
            if hit == 0: break
            time.sleep(0.15)
        return collected

    def _dedup_key(it: dict) -> str:
        td = it.get("Tdnet") or it
        return (td.get("id") or td.get("document_url") or td.get("title") or "") + "|" + (td.get("pubdate") or "")

    def _slice_fetch(dt_from: datetime, dt_to: datetime, level: int = 0) -> list[dict]:
        items = _fetch_tdnet_by_range(dt_from, dt_to)
        if len(items) < per_day_limit:
            return items
        if not time_capable:
            if (dt_from.hour, dt_from.minute, dt_from.second) == (0, 0, 0) and (dt_to.hour, dt_to.minute, dt_to.second) == (23, 59, 59) and dt_from.date() == dt_to.date():
                s_day = dt_from.strftime("%Y%m%d"); e_day = dt_to.strftime("%Y%m%d")
                extras = _fetch_day_extra_pages(s_day, e_day)
                if extras: return items + extras
            return items
        if level >= len(slice_escalation): return items
        
        width_h = slice_escalation[level]
        step_sec = int(max(width_h * 3600, 1800))
        parts: list[dict] = []
        cur = dt_from
        while cur < dt_to:
            nxt = min(cur + timedelta(seconds=step_sec), dt_to)
            parts.extend(_slice_fetch(cur, nxt, level + 1))
            cur = nxt
            time.sleep(0.12)
        return parts

    def _ts(x: dict) -> float:
        td = x.get("Tdnet") or {}
        s = (td.get("pubdate") or "").replace("/", "-")
        try: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception: return 0.0

    all_items: list[dict] = []
    seen: set[str] = set()
    end = datetime.now(JST).replace(microsecond=0)

    for i in range(days):
        day_to   = (end - timedelta(days=i)).replace(hour=23, minute=59, second=59)
        day_from = (end - timedelta(days=i)).replace(hour=0,  minute=0,  second=0)
        day_items = _slice_fetch(day_from, day_to)

        for it in day_items:
            td = it.get("Tdnet") or it
            title = _unescape_text(td.get("title") or "")
            if KEY_RE_LOCAL and title and not KEY_RE_LOCAL.search(title):
                continue
            td["title"] = title
            k = _dedup_key(it)
            if not k or k in seen: continue
            seen.add(k)
            all_items.append(it)
        time.sleep(0.2)

    all_items.sort(key=_ts, reverse=True)
    return all_items

def fetch_earnings_tdnet_only(days: int = 90, per_day_limit: int = 300) -> List[Dict[str, Any]]:
    return fetch_tdnet_by_keywords(days=days, keywords=EARNINGS_KW, per_day_limit=per_day_limit)

def fetch_tdnet_tob(days: int = 90, per_day_limit: int = 300) -> List[dict]:
    items = fetch_tdnet_by_keywords(days=days, keywords=TOB_KW, per_day_limit=per_day_limit)
    rows = []
    for it in items:
        td = it.get("Tdnet") or it
        title = (td.get("title") or "")
        if any(k in title for k in TOB_KW):
            rows.append(tdnet_item_to_tob_row(it))
    return rows

# --------- PDF解析 / 決算採点処理 ---------
def _download_pdf_bytes(url: str, timeout: int = 25) -> bytes:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
        r.raise_for_status()
        return r.content or b""
    except Exception: return b""

def _extract_text_pdfminer(pdf_bytes: bytes) -> str:
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        with io.BytesIO(pdf_bytes) as bio:
            return (extract_text(bio) or "").strip()
    except Exception: return ""

def _extract_text_pypdf2(pdf_bytes: bytes) -> str:
    try:
        import PyPDF2  # type: ignore
        out = []
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            try: out.append(page.extract_text() or "")
            except Exception: out.append("")
        return "\n".join(out).strip()
    except Exception: return ""

def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if not pdf_bytes: return ""
    text = _extract_text_pdfminer(pdf_bytes)
    if text and len(text) > 30: return text
    return _extract_text_pypdf2(pdf_bytes)

def _safe_extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        txt = _extract_text_from_pdf(pdf_bytes)
        if isinstance(txt, str) and txt.strip(): return txt
    except Exception: pass
    return ""

def _parse_earnings_metrics(text: str) -> Dict[str, Any]:
    if not text: return {"metrics": {}, "progress": None}

    def _num(s):
        s = s.replace(",", "").replace("，", "").replace("％", "%").replace("▲", "-").replace("△", "-")
        s = re.sub(r"[^\d\.\-\+%]", "", s)
        try:
            if s.endswith("%"): return float(s[:-1])
            return float(s)
        except: return None

    UNIT_PAT = r"(百万円|億円|万円|円)?"
    fields = {
        "売上高": r"(売上高)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "営業利益": r"(営業利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "経常利益": r"(経常利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "純利益": r"(当期純利益|親会社株主に帰属する当期純利益|純利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "EPS": r"(EPS|1株当たり当期純利益)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*" + UNIT_PAT,
        "進捗率": r"(進捗率)[^0-9\-＋\+\,\.％%\(\)]*([0-9,\.\-\+]+)\s*%",
    }

    metrics = {}
    yoy = {}

    def _scan_yoy_near(pos: int, text: str):
        if pos < 0: return None
        s = text[max(0, pos-120): pos+220].replace('％','%').replace('▲','-').replace('△','-')
        m = re.search(r'([\-+]?\d+(?:\.\d+)?)\s*%', s)
        if not m: return None
        try: val = float(m.group(1))
        except Exception: return None
        around = s[max(0, m.start()-10): m.end()+10]
        if ('-' not in m.group(1) and '+' not in m.group(1)):
            if re.search(r'(減|悪化|縮小)', around): val = -val
        return val

    for key, pat in fields.items():
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            raw = m.group(2) if len(m.groups()) >= 2 else None
            val = _num(raw or "")
            if val is not None:
                unit = m.group(3) if len(m.groups()) >= 3 else ""
                if key != "EPS" and key != "進捗率" and unit and "百万円" in unit:
                    val = val / 100.0
                metrics[key] = val
            try:
                y = _scan_yoy_near(text.find(key), text)
                if y is not None: yoy[key] = y
            except Exception: pass

    progress = float(metrics["進捗率"]) if "進捗率" in metrics else None
    return {"metrics": metrics, "progress": progress, "yoy": yoy}

def _summarize_earnings_text(text: str, max_chars: int = 240) -> str:
    if not text: return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    KEY = ["上方修正", "下方修正", "通期", "四半期", "増益", "減益", "増収", "減収", "進捗率", "配当", "業績予想", "修正"]
    hits = [l for l in lines if any(k in l for k in KEY)]
    base = " / ".join(hits[:4]) if hits else " ".join(lines[:6])
    return re.sub(r"\s+", " ", base).strip()[:max_chars]

def _grade_earnings(title: str, text: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    reasons: list[str] = []
    score = 0.0

    T = title or ""
    X = text or ""
    met = (parsed or {}).get("metrics", {})
    prog = (parsed or {}).get("progress", None)
    yoy = (parsed or {}).get("yoy", {})

    def _hit(ws): return any((w in T) or (w in X) for w in ws)

    if _hit(["上方修正", "通期上方", "通期増額", "上期予想修正（増額）", "業績予想の修正（増額）", "通期予想修正（増額）"]):
        score += 2; reasons.append("上方修正（増額）で+2")
    if _hit(["増配", "復配", "自社株買い", "配当予想の修正（増額）"]):
        score += 1; reasons.append("配当増額/復配/自社株買いで+1")
    if _hit(["最高益", "過去最高益"]): score += 1; reasons.append("最高益で+1")
    if _hit(["黒字転換"]): score += 1; reasons.append("黒字転換で+1")
    if _hit(["下方修正", "通期予想修正（減額）"]): score -= 3; reasons.append("下方修正（減額）で-3")
    if _hit(["減配", "配当予想の修正（減額）"]): score -= 2; reasons.append("減配で-2")
    if _hit(["特損", "特別損失"]): score -= 2; reasons.append("特損で-2")

    if isinstance(prog, (int, float)):
        if prog >= 70: score += 1; reasons.append(f"進捗率{prog:.0f}%（高進捗）")
        elif prog <= 30: score -= 1; reasons.append(f"進捗率{prog:.0f}%（低進捗）")

    neg_profit_yoy = 0
    pos_profit_yoy = 0
    for k in ("売上高", "営業利益", "経常利益", "純利益"):
        y = yoy.get(k)
        if not isinstance(y, (int, float)) or not (-120.0 <= y <= 120.0): continue
        if k in ("営業利益", "経常利益", "純利益"):
            if y <= -5: neg_profit_yoy += 1
            if y >= 1:  pos_profit_yoy += 1
        if y >= 5: score += 1; reasons.append(f"{k}YoY+{y:.1f}%")
        elif y >= 1: score += 0.5; reasons.append(f"{k}YoY+{y:.1f}%")
        elif y <= -5: score -= 1; reasons.append(f"{k}YoY{y:.1f}%")
        elif y <= -1: score -= 0.5; reasons.append(f"{k}YoY{y:.1f}%")

    if neg_profit_yoy >= 2:
        score -= 1.0; reasons.append("利益YoYが複数項目で減少（総じて減益傾向）")

    for k in ("営業利益", "経常利益", "純利益"):
        v = met.get(k, None)
        if isinstance(v, (int, float)):
            if v > 0:
                if pos_profit_yoy > 0: score += 0.3; reasons.append(f"{k}が黒字（増益傾向と整合）")
                else: reasons.append(f"{k}が黒字（単独要素のため加点なし）")
            if v < 0: score -= 0.5; reasons.append(f"{k}が赤字")

    verdict = "good" if score >= 1.5 else ("bad" if score <= -1.5 else "neutral")
    return {"verdict": verdict, "score": score, "reasons": reasons}

def _summarize_one_pdf_row(row: dict) -> dict:
    link = (row or {}).get("link") or ""
    if not link: return row
    try:
        b = _download_pdf_bytes(link)
        if not b: return row
        text = _safe_extract_pdf_text(b)
        if not text: return row

        parsed = _parse_earnings_metrics(text) or {}
        sum2 = _summarize_earnings_text(text)
        judge = _grade_earnings(row.get("title",""), text, parsed)

        out = dict(row)
        if sum2: out["summary"] = sum2
        out["metrics"]  = parsed.get("metrics", {})
        out["progress"] = parsed.get("progress")
        out["verdict"]  = judge.get("verdict")
        out["score_judge"] = judge.get("score")
        out["reasons"]  = judge.get("reasons", [])
        
        if out["verdict"] == "good": out["sentiment"] = "positive"
        elif out["verdict"] == "bad": out["sentiment"] = "negative"
        return out
    except Exception: return row

EARNINGS_SUMMARY_MAX = 40
def summarize_earnings_rows_parallel(rows: list, max_items: int = 40, max_workers: int = 8):
    if not rows: return rows
    targets = [(i, r) for i, r in enumerate(rows[:max_items]) if r.get("link")]
    if not targets: return rows
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut2idx = {ex.submit(_summarize_one_pdf_row, r): i for i, r in targets}
            for fut in as_completed(fut2idx):
                try: rows[fut2idx[fut]] = fut.result()
                except Exception: pass
    except Exception: pass
    return rows

def tdnet_items_to_earnings_rows(tdnet_items):
    rows = []
    POS_KEYS = ("決算短信","四半期","通期","決算")
    EXCLUDE  = ("動画","説明資料")

    for it in tdnet_items:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not any(k in title for k in POS_KEYS) or any(k in title for k in EXCLUDE):
            continue

        code_raw = str(td.get("company_code") or td.get("code") or "").strip()
        if re.fullmatch(r"\d{5}", code_raw): ticker = code_raw[:4]
        elif re.fullmatch(r"\d{3}[0-9a-zA-Z]", code_raw): ticker = code_raw.upper()
        else: ticker = ""

        link = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        time_str = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","")

        s_t, label_guess, why_t = _summarize_title_simple(title)
        
        row = {
            "ticker": ticker or "0000",
            "name":   (td.get("company_name") or "").strip() or ticker,
            "score":  0,
            "sentiment": td.get("sentiment") or label_guess or "neutral",
            "title": title,
            "link":  link,
            "time":  time_str or "",
            "summary": s_t,
            "reasons": [why_t] if why_t else [],
            "verdict": "neutral",
            "score_judge": 0,
            "metrics": {},
            "progress": None,
        }
        rows.append(row)

    def _ts(s):
        try: return datetime.strptime((s or "").replace("/","-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except: return 0
    rows.sort(key=lambda r: _ts(r["time"]), reverse=True)
    return summarize_earnings_rows_parallel(rows, max_items=EARNINGS_SUMMARY_MAX, max_workers=8)


# ------------------ DB スキーマ定義・保存処理 ------------------
def ensure_earnings_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS earnings_events(
        コード        TEXT NOT NULL,
        銘柄名        TEXT,
        タイトル      TEXT,
        リンク        TEXT,
        発表日時      TEXT,
        提出時刻      TEXT NOT NULL,
        要約          TEXT,
        判定          TEXT,
        判定スコア    INTEGER,
        理由JSON      TEXT,
        指標JSON      TEXT,
        進捗率        REAL,
        センチメント TEXT,
        素点          INTEGER,
        created_at    TEXT DEFAULT (datetime('now','localtime'))
    );
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_earn_code_teishutsu ON earnings_events(コード, 提出時刻);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_earn_teishutsu_desc ON earnings_events(提出時刻 DESC);")
    conn.commit()

def upsert_earnings_rows(conn: sqlite3.Connection, rows: list[dict]):
    import json
    cur = conn.cursor()
    for r in rows:
        jp_vals = {
            "コード": r.get("ticker", "0000"),
            "銘柄名": r.get("name", ""),
            "タイトル": r.get("title", ""),
            "リンク": r.get("link", ""),
            "発表日時": r.get("time", ""),
            "提出時刻": r.get("time", "") or now_iso(),
            "要約": r.get("summary", ""),
            "判定": r.get("verdict", "").strip(),
            "判定スコア": int(r.get("score_judge") or 0),
            "理由JSON": json.dumps(r.get("reasons") or [], ensure_ascii=False),
            "指標JSON": json.dumps(r.get("metrics") or {}, ensure_ascii=False),
            "進捗率": r.get("progress"),
            "センチメント": r.get("sentiment", ""),
            "素点": int(r.get("score") or 0),
        }
        
        insert_cols = list(jp_vals.keys())
        insert_vals = list(jp_vals.values())
        placeholders = ",".join(["?"] * len(insert_cols))
        
        cur.execute(f'INSERT OR IGNORE INTO earnings_events ({", ".join(insert_cols)}) VALUES ({placeholders})', insert_vals)
        if cur.rowcount == 0:
            set_clause = ", ".join([f'"{k}"=?' for k in insert_cols if k not in ("コード", "提出時刻")])
            update_vals = [jp_vals[k] for k in insert_cols if k not in ("コード", "提出時刻")]
            update_vals.extend([jp_vals["コード"], jp_vals["提出時刻"]])
            cur.execute(f'UPDATE earnings_events SET {set_clause} WHERE "コード"=? AND "提出時刻"=?', update_vals)

def ensure_offerings_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offerings_events(
        コード        TEXT NOT NULL,
        銘柄名        TEXT,
        タイトル      TEXT,
        リンク        TEXT,
        発表日時      TEXT,
        提出時刻      TEXT NOT NULL,
        種別          TEXT,
        参照ID        TEXT,
        センチメント TEXT,
        created_at    TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(コード, 提出時刻, タイトル)
    );
    """)
    conn.commit()

def _classify_offering_kind(title: str) -> str:
    t = title or ""
    if any(k in t for k in ["行使","行使状況","新株予約権の行使"]): return "行使"
    if any(k in t for k in ["公募増資","第三者割当","第三者割当増資","新株発行","募集新株予約権","新株予約権発行","MSワラント","ワラント","ライツ・オファリング"]): return "増資"
    if any(k in t for k in ["株式の売出","自己株式の処分"]): return "売出/処分"
    if any(k in t for k in ["転換社債","CB","EB債"]): return "CB/EB"
    return "その他"

def upsert_offerings_events(conn: sqlite3.Connection, tdnet_items: list[dict]):
    cur = conn.cursor()
    for it in tdnet_items or []:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not title or not any(kw in title for kw in OFFERING_KW): continue

        raw = (td.get("company_code") or td.get("code") or td.get("company_code_raw") or "").strip()
        code = raw[:4] if re.fullmatch(r"\d{5}", raw) else (raw.upper() if re.fullmatch(r"\d{3}[0-9a-zA-Z]", raw) else (td.get("ticker") or "").strip())
        code = code or "0000"

        name  = (td.get("company_name") or "").strip() or code
        link  = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        pub   = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","").replace("/","-").strip()
        tei   = pub or now_iso()
        kind  = _classify_offering_kind(title)
        refid = td.get("id") or link or title
        _, label, _ = _summarize_title_simple(title)

        cur.execute("""
            INSERT INTO offerings_events(コード,銘柄名,タイトル,リンク,発表日時,提出時刻,種別,参照ID,センチメント)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
              銘柄名=excluded.銘柄名, リンク=excluded.リンク, 発表日時=excluded.発表日時,
              種別=excluded.種別, 参照ID=excluded.参照ID, センチメント=excluded.センチメント
        """, (code,name,title,link,pub,tei,kind,refid,label))
    conn.commit()

def ensure_tob_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tob_events (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      コード            TEXT NOT NULL,
      銘柄名            TEXT,
      タイトル          TEXT NOT NULL,
      リンク            TEXT,
      発表日時          TEXT,
      提出時刻          TEXT NOT NULL,
      種別              TEXT DEFAULT 'TOB',
      買付価格          REAL,
      下限価格          REAL,
      上限価格          REAL,
      目標保有比率      REAL,
      最低応募株数      INTEGER,
      メモ              TEXT
    );
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_tob_unique ON tob_events(コード, 提出時刻, タイトル);")
    conn.commit()

def _to_number(s):
    try: return float(str(s).replace(",", ""))
    except Exception: return None

def parse_tob_details(text: str) -> dict:
    t = text or ""
    out = {"買付価格": None, "下限価格": None, "上限価格": None, "目標保有比率": None, "最低応募株数": None}
    _NUM = r"(?:\d{1,3}(?:,\d{3})*|\d+)"
    m = re.search(r"(買付(?:け)?価格)[：:\s]*(" + _NUM + r")\s*円", t)
    if m: out["買付価格"] = _to_number(m.group(2))
    m = re.search(r"(?:買付(?:け)?价格|買付(?:け)?価額|買付(?:け)?価格)[^0-9]*(" + _NUM + r")\s*円\s*[~〜－-]\s*(" + _NUM + r")\s*円", t)
    if m: out["下限価格"] = _to_number(m.group(1)); out["上限価格"] = _to_number(m.group(2))
    m = re.search(r"(目標保有比率|保有比率)[：:\s]*(" + _NUM + r"(?:\.\d+)?)\s*%", t)
    if m: out["目標保有比率"] = _to_number(m.group(2))
    m = re.search(r"(最低応募株数)[：:\s]*(" + _NUM + r")\s*株", t)
    if m: out["最低応募株数"] = int(_to_number(m.group(2)) or 0)
    return out

def tdnet_item_to_tob_row(it: dict) -> dict:
    td = it.get("Tdnet") or it
    title = html.unescape(td.get("title") or "")
    raw_code = (td.get("company_code") or td.get("code") or "").strip()
    code = raw_code[:4] if (len(raw_code) >= 4 and raw_code[:4].isdigit()) else raw_code
    
    name = td.get("company_name") or td.get("name") or ""
    link = td.get("document_url") or td.get("pdf_url") or td.get("url") or ""
    pub = td.get("publish_datetime") or td.get("pubdate") or td.get("time") or ""
    time_str = (pub or "").replace("T"," ").replace("+09:00","").replace("/","-")

    details = parse_tob_details(title)
    return {
        "コード": code,
        "銘柄名": name,
        "タイトル": title,
        "リンク": link,
        "発表日時": time_str,
        "提出時刻": time_str or now_iso(),
        "種別": "TOB",
        "買付価格": details.get("買付価格"),
        "下限価格": details.get("下限価格"),
        "上限価格": details.get("上限価格"),
        "目標保有比率": details.get("目標保有比率"),
        "最低応募株数": details.get("最低応募株数"),
        "メモ": None,
    }

def upsert_tob_events(conn: sqlite3.Connection, rows: List[dict]):
    if not rows: return
    sql = """
    INSERT INTO tob_events
      (コード, 銘柄名, タイトル, リンク, 発表日時, 提出時刻, 種別,
       買付価格, 下限価格, 上限価格, 目標保有比率, 最低応募株数, メモ)
    VALUES
      (:コード, :銘柄名, :タイトル, :リンク, :発表日時, :提出時刻, :種別,
       :買付価格, :下限価格, :上限価格, :目標保有比率, :最低応募株数, :メモ)
    ON CONFLICT(コード, 提出時刻, タイトル) DO NOTHING
    """
    with conn:
        conn.executemany(sql, rows)

# ------------------ メイン処理 (データ取得と保存のみ) ------------------
def _parse_ts_str(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s2 = s.replace("T"," ").replace("+09:00","").replace("/","-").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try: return datetime.strptime(s2, fmt).replace(tzinfo=JST)
        except Exception: continue
    return None

def _latest_teishutsu_ts(conn: sqlite3.Connection) -> Optional[datetime]:
    cur = conn.cursor()
    latest: Optional[datetime] = None
    for tbl in ("earnings_events", "offerings_events", "tob_events"):
        try:
            # table存在確認
            cur.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{tbl}'")
            if not cur.fetchone(): continue
            
            ts = cur.execute(f"SELECT MAX(提出時刻) FROM {tbl}").fetchone()[0]
            dt = _parse_ts_str(ts)
            if dt and (latest is None or dt > latest): latest = dt
        except Exception: continue
    return latest

def _tdnet_item_pub_dt(it: dict) -> Optional[datetime]:
    td = it.get("Tdnet") or it
    s = (td.get("pubdate") or td.get("publish_datetime") or td.get("time") or "")
    return _parse_ts_str(s)

def _earn_row_dt(row: dict) -> Optional[datetime]:
    return _parse_ts_str(row.get("time") or "")

def main():
    print("=== fetch_all: TDnetデータ収集開始 ===")
    
    # DB接続・スキーマ保証
    conn = get_db_conn()
    ensure_earnings_schema(conn)
    ensure_offerings_schema(conn)
    ensure_tob_schema(conn)

    # 増分取得のチェックポイント
    since_dt = _latest_teishutsu_ts(conn)
    now_jst = datetime.now(JST).replace(microsecond=0)

    def span_days(max_cap: int) -> int:
        if not since_dt: return max_cap
        delta = now_jst - since_dt
        return min(max(1, int(delta.total_seconds() // 86400) + 2), max_cap)

    # 1. 決算情報（TDnet）の取得と解析
    try:
        earn_days = span_days(4) if since_dt else 1
        tdnet_items = fetch_earnings_tdnet_only(days=earn_days, per_day_limit=300)
        if since_dt:
            tdnet_items = [it for it in tdnet_items if (_tdnet_item_pub_dt(it) or now_jst) > since_dt]
        
        earnings_rows = tdnet_items_to_earnings_rows(tdnet_items)
        if since_dt:
            earnings_rows = [r for r in earnings_rows if (_earn_row_dt(r) or now_jst) > since_dt]

    except Exception as e:
        print("[earnings] 取得/整形で例外:", e)
        earnings_rows = []
        
    # 2. 増資レーンの取得
    offer_items = []
    try:
        offer_days = span_days(4) if since_dt else 1
        offer_items = fetch_tdnet_by_keywords(days=offer_days, keywords=OFFERING_KW, per_day_limit=300)
        if since_dt:
            offer_items = [it for it in offer_items if (_tdnet_item_pub_dt(it) or now_jst) > since_dt]
    except Exception as e:
        print("[offerings] fetch error:", e)

    # 3. TOBレーンの取得
    tob_rows = []
    try:
        tob_days = span_days(4) if since_dt else 1
        tob_src = fetch_tdnet_tob(days=tob_days, per_day_limit=300)
        if since_dt:
            def _row_dt(r): return _parse_ts_str(r.get("提出時刻") or r.get("発表日時") or "")
            tob_src = [r for r in tob_src if (_row_dt(r) or now_jst) > since_dt]
        tob_rows = tob_src
    except Exception as e:
        print("[TOB] fetch error:", e)

    # 4. データベースへの保存 (UPSERT)
    try:
        upsert_offerings_events(conn, offer_items)
    except Exception as e:
        print("[offerings] upsert error:", e)

    try:
        upsert_tob_events(conn, tob_rows)
    except Exception as e:
        print("[TOB] upsert error:", e)

    try:
        conn.execute("BEGIN IMMEDIATE;")
        upsert_earnings_rows(conn, earnings_rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("[earnings] upsert error:", e)

    print(f"[OK] 決算:{len(earnings_rows)}件, 増資:{len(offer_items)}件, TOB:{len(tob_rows)}件 をDBに保存しました。")
    print("=== fetch_all: 完了 ===")

if __name__ == "__main__":
    try:
        main()
    finally:
        pass