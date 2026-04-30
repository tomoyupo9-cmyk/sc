#!/usr/bin/env python
# -*- coding: utf-8 -*-

# fetch_all.py — TDnetのみで決算取得 + PDF自動サマリ + 良否判定（判定理由をUI表示）
# 日別整理のUI改善を含む完全版
# 2025-10-14: TDnet API安定化パッチ
#  - 日単位エンドポイント(list/day)を最優先
#  - JSON以外(text/html等)の場合は静かに再試行/最終試行のみログ
# 2025-10-17: ★TOB専用レーン追加（tob_events）、ダッシュボードに「TOB(TDNET)」タブを追加
# 2025-10-17: ★TOB(掲示板)タブに TDNET決定済み除外フィルタを適用
# 2025-11-06: ★DB接続を common.py のシングルトンに統一（PRAGMAチューンは common 側）
# 2025-11-06: ★★★ 増分取得対応（earnings/offerings/tob の提出時刻MAX以降だけ取得）

# --- add local src/ to import path ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
SRC_DIR = HERE / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# set_db_path が無い版でも動くように try/except
try:
    from common import _get_db_conn, _close_db_conn, set_db_path  # type: ignore
except Exception:
    from common import _get_db_conn, _close_db_conn  # type: ignore
    set_db_path = None  # type: ignore
# --- end ---

from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import io
import re
import html
import json
import time
import requests
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

# === DB接続は common.py に一本化 ===
try:
    from common import _get_db_conn, _close_db_conn, set_db_path  # set_db_path が無い環境も想定
except Exception:
    from common import _get_db_conn, _close_db_conn  # type: ignore
    def set_db_path(*args, **kwargs):
        pass

# --- 既存モジュール（環境のまま） ---
from src.trends import fetch_trends
from src.news import fetch_news
from src.bbs import fetch_bbs_stats, fetch_leak_comments, fetch_tob_comments
from src.discovery import build_universe
from src.common import ensure_dir, dump_json, now_iso, load_config
# --- ログ抑制パッチ（ここから追加） ---
import logging

# PyPDF2 や pdfminer の内部ログが stderr に流れるのを抑制する
logging.getLogger("PyPDF2").setLevel(logging.ERROR)
# 他のライブラリ（pdfminerなど）が原因の場合も考慮して全体的に警告以下を抑制
logging.basicConfig(level=logging.ERROR)

# 標準エラー出力に直接書かれるライブラリ用の対策（必要に応じて）
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
# --- ログ抑制パッチ（ここまで） ---


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"
OUT_BBS_DIR = ROOT / "out_bbs"
DASH_DIR = ROOT / "dashboard"
JST = timezone(timedelta(hours=9))

# あなたの実環境の DB を指定（common 側にも伝播させる）
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
try:
    set_db_path(DB_PATH)
except Exception:
    pass

# ------------------ 簡易センチメント ------------------
POS_KW = ["上方修正","増益","最高益","過去最高","増配","黒字転換","上振れ","好調","好決算","上乗せ","大幅増","公開買付け","公開買い付け","TOB","ＴＯＢ"]
NEG_KW = ["下方修正","減益","赤字","減配","未達","下振れ","特損","不適正","監理","大幅減","業績悪化"]

def _judge_sentiment(title: str) -> (str, int, str):
    t = title or ""
    for kw in POS_KW:
        if kw in t: return ("positive", +2, kw)
    for kw in NEG_KW:
        if kw in t: return ("negative", -2, kw)
    return ("neutral", 0, "")

# ---- title summarizer (backward-compat) ------------------------------------
def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def summarize_title_simple(title: str) -> str:
    """
    TDnet等のタイトルからノイズを軽く落として要約（最大80文字）。
    - 各種括弧（全角含む）内をざっくり除去
    - 「適時開示:」「〜のお知らせ」等の定型を整理
    - 連続空白を1つに
    """
    t = str(title or "")
    t = re.sub(r"【.*?】|\[.*?\]|\(.*?\)|（.*?）|＜.*?＞|〈.*?〉", "", t)
    t = re.sub(r"^\s*適時開示\s*[:：-]?\s*", "", t)
    t = re.sub(r"\s*のお知らせ\s*$", "", t)
    t = _normalize_spaces(t)
    return (t[:80] + "…") if len(t) > 80 else t

def _summarize_title_simple(title: str):
    """
    旧コード互換の戻り値（三つ組）を返す:
      (summary:str, sentiment_label:str, hit_keyword:str)
    """
    summary = summarize_title_simple(title)
    label, score, hit_kw = _judge_sentiment(title)  # label: "positive"/"neutral"/"negative"
    return summary, label, hit_kw
# ---------------------------------------------------------------------------
def load_tob_from_bbs_lightweight(rows_to_check):
    import requests
    import time
    import random
    from datetime import datetime

    # セッションを作成し、ブラウザに近いヘッダーを設定
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        "Referer": "https://finance.yahoo.co.jp/",
    })

    keywords = ["TOB", "買収", "公開買付", "親会社", "子会社化"]
    results_rows = []
    
    # 100件は多いため、安全のために件数を絞ることを推奨（例: [:50]）
    target_list = rows_to_check[:50] 
    print(f"[Board-Check] 開始: {len(target_list)} 銘柄をスキャンします...")

    for r in target_list:
        code = str(r.get("ticker", ""))
        if not code: continue
        
        url = f"https://finance.yahoo.co.jp/quote/{code}.T/bbs"
        try:
            # セッション経由でリクエスト
            res = session.get(url, timeout=7)
            res.raise_for_status()
            
            # --- サイレントブロック検知 ---
            # 200 OKでも中身が「掲示板」でない（認証ページ等）場合は即座に中止
            if "掲示板" not in res.text:
                print(f"  [!] Yahoo掲示板からブロックされた可能性があります。処理を中断します。")
                break

            # キーワード判定
            found = [kw for kw in keywords if kw in res.text]
            if found:
                results_rows.append({
                    "ticker": code,
                    "name": r.get("name", ""),
                    "count": "検知", 
                    "comments": [{"text": f"掲示板にてキーワード検知: {', '.join(found)}", "link": url, "sentiment": "neutral", "published": "最新"}]
                })
                print(f"  [Board-Check] Detected: {code} ({', '.join(found)})")
            
            # --- 修正案Bの核心: 間隔のランダム化 ---
            # 2.0秒〜5.0秒の間でランダムに待機し、Bot判定を回避
            wait_time = random.uniform(2.0, 5.0)
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"  [Error] {code}: {str(e)[:50]}")
            # エラーが連続する場合はIP制限の可能性があるため、少し長めに休む
            time.sleep(10)
            continue
            
    return {
        "rows": results_rows, 
        "total_tickers": len(results_rows), 
        "generated_at": datetime.now().isoformat()
    }
# ------------------ TDnet: 取得 ------------------
TDNET_LIST_JSON = "https://webapi.yanoshin.jp/webapi/tdnet/list/{date_from}-{date_to}.json"

# 用途別キーワードに分離
EARNINGS_KW = ["決算","短信","四半期","通期","上方修正","下方修正","業績","配当","進捗"]

# ★TOBキーワード
TOB_KW = [
    "公開買付け", "公開買い付け", "公開買付", "公開買い付",
    "TOB", "ＴＯＢ"
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
    """HTTP GET JSON with pooled session + retry."""
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
            # 非JSON応答(例: HTMLエラーページ)はリトライ/最後だけ簡潔ログ
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

def fetch_earnings_tdnet_only(days: int = 90, per_day_limit: int = 300,
                              slice_escalation=(12, 6, 3, 1, 0.5)) -> List[Dict[str, Any]]:
    # 汎用関数に EARNINGS_KW を渡すだけ
    return fetch_tdnet_by_keywords(
        days=days,
        keywords=EARNINGS_KW,
        per_day_limit=per_day_limit,
        slice_escalation=slice_escalation
    )

def ensure_earnings_schema(conn: sqlite3.Connection):
    """
    earnings_events を日本語カラムのみのスキーマに統一。
    主キーは持たず、UNIQUE(コード, 提出時刻) をインデックスで保証。
    """
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS earnings_events(
        コード       TEXT NOT NULL,
        銘柄名       TEXT,
        タイトル     TEXT,
        リンク       TEXT,
        発表日時     TEXT,
        提出時刻     TEXT NOT NULL,
        要約         TEXT,
        判定         TEXT,
        判定スコア   INTEGER,
        理由JSON     TEXT,
        指標JSON     TEXT,
        進捗率       REAL,
        センチメント TEXT,
        素点         INTEGER,
        created_at   TEXT DEFAULT (datetime('now','localtime'))
    );
    """)
    cur.execute("PRAGMA table_info(earnings_events);")
    existing = {r[1] for r in cur.fetchall()}
    def _add(col, typ):
        if col not in existing:
            cur.execute(f'ALTER TABLE earnings_events ADD COLUMN "{col}" {typ};')
    for col, typ in [
        ("コード","TEXT NOT NULL"),
        ("銘柄名","TEXT"),
        ("タイトル","TEXT"),
        ("リンク","TEXT"),
        ("発表日時","TEXT"),
        ("提出時刻","TEXT NOT NULL"),
        ("要約","TEXT"),
        ("判定","TEXT"),
        ("判定スコア","INTEGER"),
        ("理由JSON","TEXT"),
        ("指標JSON","TEXT"),
        ("進捗率","REAL"),
        ("センチメント","TEXT"),
        ("素点","INTEGER"),
        ("created_at","TEXT"),
    ]:
        _add(col, typ)

    cur.execute("DROP INDEX IF EXISTS idx_earn_time;")
    cur.execute("DROP INDEX IF EXISTS idx_earn_announced;")
    cur.execute("DROP INDEX IF EXISTS idx_earn_code_time;")
    cur.execute("""
      CREATE UNIQUE INDEX IF NOT EXISTS idx_earn_code_teishutsu
      ON earnings_events(コード, 提出時刻);
    """)
    cur.execute("""
      CREATE INDEX IF NOT EXISTS idx_earn_teishutsu_desc
      ON earnings_events(提出時刻 DESC);
    """)
    conn.commit()

# ========= offerings_events: 増資/行使 系イベント保存 =========

OFFERING_KW = [
    "公募増資","第三者割当","第三者割当増資","新株発行","自己株式の処分",
    "株式の売出","募集新株予約権","新株予約権発行","MSワラント","ワラント",
    "転換社債","CB","EB債","ライツ・オファリング",
    # 行使系
    "行使", "行使状況", "新株予約権の行使"
]

def ensure_offerings_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offerings_events(
        コード       TEXT NOT NULL,
        銘柄名       TEXT,
        タイトル     TEXT,
        リンク       TEXT,
        発表日時     TEXT,
        提出時刻     TEXT NOT NULL,
        種別         TEXT,
        参照ID       TEXT,
        センチメント TEXT,
        created_at   TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(コード, 提出時刻, タイトル)
    );
    """)
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_offerings_date ON offerings_events(substr(提出時刻,1,10));""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_offerings_code_date ON offerings_events(コード, substr(提出時刻,1,10));""")
    conn.commit()

def load_offerings_recent_map(days: int = 365*3) -> dict[str, bool]:
    out: dict[str, bool] = {}
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='offerings_events'")
        if not cur.fetchone():
            return out
        # 修正: CAST AS INTEGER をやめ、文字列として先頭4文字を取得するロジックに変更
        rows = cur.execute("""
            SELECT DISTINCT
                   CASE
                     WHEN length(コード) >= 4 THEN substr(コード, 1, 4)
                     ELSE コード
                   END AS code4
            FROM offerings_events
            WHERE date(提出時刻) >= date('now', ?)
        """, (f"-{days} day",)).fetchall()
        for (c,) in rows:
            if c:
                out[str(c).zfill(4)] = True
    except Exception as e:
        print("[offerings] load_offerings_recent_map error:", e)
    return out

def _classify_offering_kind(title: str) -> str:
    t = title or ""
    if any(k in t for k in ["行使","行使状況","新株予約権の行使"]): return "行使"
    if any(k in t for k in ["公募増資","第三者割当","第三者割当増資","新株発行","募集新株予約権","新株予約権発行","MSワラント","ワラント","ライツ・オファリング"]): return "増資"
    if any(k in t for k in ["株式の売出","自己株式の処分"]): return "売出/処分"
    if any(k in t for k in ["転換社債","CB","EB債"]): return "CB/EB"
    return "その他"

def upsert_offerings_events(conn: sqlite3.Connection, tdnet_items: list[dict]):
    import re, json
    cur = conn.cursor()

    def norm_code(it: dict) -> str:
        td = it.get("Tdnet", it) or {}
        raw = (td.get("company_code") or td.get("code") or td.get("company_code_raw") or "").strip()
        if re.fullmatch(r"\d{5}", raw): return raw[:4]
        # 修正: 英字入り4桁に対応
        if re.fullmatch(r"\d{3}[0-9a-zA-Z]", raw): return raw.upper()
        return (td.get("ticker") or "").strip()

    def norm_time(s: str) -> str:
        s = (s or "").replace("T"," ").replace("+09:00","").replace("/","-").strip()
        return s

    count_upd = 0
    for it in tdnet_items or []:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not title: 
            continue
        if not any(kw in title for kw in OFFERING_KW):
            continue

        code  = norm_code(it) or "0000"
        name  = (td.get("company_name") or "").strip() or code
        link  = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        pub   = norm_time(td.get("pubdate") or td.get("publish_datetime") or "")
        tei   = pub or now_iso()
        kind  = _classify_offering_kind(title)
        refid = td.get("id") or link or title
        _, label, _ = _summarize_title_simple(title)

        cur.execute("""
            INSERT INTO offerings_events(コード,銘柄名,タイトル,リンク,発表日時,提出時刻,種別,参照ID,センチメント)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(コード,提出時刻,タイトル) DO UPDATE SET
              銘柄名=excluded.銘柄名,
              リンク=excluded.リンク,
              発表日時=excluded.発表日時,
              種別=excluded.種別,
              参照ID=excluded.参照ID,
              センチメント=excluded.センチメント
        """, (code,name,title,link,pub,tei,kind,refid,label))
        count_upd += 1

    conn.commit()
    if count_upd:
        print(f"[offerings] upsert rows: {count_upd}")

# ========= ★TOB専用レーン（tob_events） =========

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
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_tob_unique
                   ON tob_events(コード, 提出時刻, タイトル);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_tob_code_date
                   ON tob_events(コード, substr(提出時刻,1,10));""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_tob_date_only
                   ON tob_events(substr(提出時刻,1,10));""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_tob_teishutsu_desc
                   ON tob_events(提出時刻 DESC);""")
    conn.commit()

_NUM = r"(?:\d{1,3}(?:,\d{3})*|\d+)"
def _to_number(s):
    try:
        return float(str(s).replace(",", ""))
    except Exception:
        return None

def parse_tob_details(text: str) -> dict:
    """タイトルやPDFテキストからTOBの主要数値っぽいものをゆるく抽出"""
    t = text or ""
    out = {"買付価格": None, "下限価格": None, "上限価格": None,
           "目標保有比率": None, "最低応募株数": None}

    m = re.search(r"(買付(?:け)?価格)[：:\s]*(" + _NUM + r")\s*円", t)
    if m: out["買付価格"] = _to_number(m.group(2))

    m = re.search(r"(?:買付(?:け)?价格|買付(?:け)?価額|買付(?:け)?価格)[^0-9]*(" + _NUM + r")\s*円\s*[~〜－-]\s*(" + _NUM + r")\s*円", t)
    if m:
        out["下限価格"] = _to_number(m.group(1))
        out["上限価格"] = _to_number(m.group(2))

    m = re.search(r"(目標保有比率|保有比率)[：:\s]*(" + _NUM + r"(?:\.\d+)?)\s*%", t)
    if m: out["目標保有比率"] = _to_number(m.group(2))

    m = re.search(r"(最低応募株数)[：:\s]*(" + _NUM + r")\s*株", t)
    if m: out["最低応募株数"] = int(_to_number(m.group(2)) or 0)

    return out

from html import unescape
def _norm_code(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 4 and s[:4].isdigit():
        return s[:4]
    return s

def tdnet_item_to_tob_row(it: dict) -> dict:
    td = it.get("Tdnet") or it  # フォールバック
    title = unescape(td.get("title") or "")
    code = _norm_code(td.get("company_code") or td.get("code") or "")
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

def fetch_tdnet_tob(days: int = 90, per_day_limit: int = 300) -> List[dict]:
    """Yanoshin TDNET list API から TOBキーワードだけ抽出して返す。"""
    items = fetch_tdnet_by_keywords(days=days, keywords=TOB_KW, per_day_limit=per_day_limit)
    rows = []
    for it in items:
        td = it.get("Tdnet") or it
        title = (td.get("title") or "")
        if any(k in title for k in TOB_KW):
            rows.append(tdnet_item_to_tob_row(it))
    return rows

def upsert_tob_events(conn: sqlite3.Connection, rows: List[dict]) -> int:
    """tob_events へバルクUPSERT。"""
    if not rows:
        return 0
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
    print(f"[TOB] upsert rows: {len(rows)}")
    return len(rows)

def load_tob_for_dashboard_from_db(days: int = 120) -> list[dict]:
    """ダッシュボード用にtob_eventsを読み出し"""
    conn = _get_db_conn()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT コード,銘柄名,タイトル,リンク,発表日時,提出時刻,種別,
               買付価格,下限価格,上限価格,目標保有比率,最低応募株数
        FROM tob_events
        WHERE date(提出時刻) >= date('now', ?)
        ORDER BY 提出時刻 DESC
        """,
        (f"-{days} day",)
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "ticker": (r[0] or "0000"),
            "name": r[1] or "",
            "title": r[2] or "",
            "link": r[3] or "",
            "announced": (r[4] or ""),
            "submitted": (r[5] or ""),
            "kind": r[6] or "TOB",
            "buy_price": r[7],
            "range_low": r[8],
            "range_high": r[9],
            "target_ratio": r[10],
            "min_shares": r[11],
        })
    return out

# ========= ★掲示板TOBタブの除外に使う：TDNET決定済みコード集合 =========
def load_tob_codes_recent(days: int = 180) -> set[str]:
    """
    tob_events に登録済み（＝TDNETでTOBが公表済み）の銘柄コード（4桁）を直近days日分だけ返す。
    掲示板TOBタブでの除外に利用。
    """
    out: set[str] = set()
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        # テーブル存在確認
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tob_events'")
        if not cur.fetchone():
            return out
        cur.execute("""
            SELECT DISTINCT
                   CASE
                     WHEN length(コード)=4 THEN コード
                     WHEN length(コード)=5 AND substr(コード,1,1) BETWEEN '0' AND '9' THEN substr(コード,1,4)
                     ELSE printf('%04d', CAST(コード AS INTEGER))
                   END AS code4
            FROM tob_events
            WHERE date(提出時刻) >= date('now', ?)
        """, (f"-{days} day",))
        for (c,) in cur.fetchall():
            if c:
                out.add(str(c).zfill(4))
    except Exception as e:
        print("[TOB] load_tob_codes_recent error:", e)
    return out

# ========= 決算UPSERT/ロード =========

def upsert_earnings_rows(conn: sqlite3.Connection, rows: list[dict]):
    """
    TDnet抽出 rows を日本語スキーマ earnings_events にUPSERT。
    - 取り込み対象：『決算短信／四半期／通期／決算』を含み、『動画／説明資料』は除外
    - キー： (コード, 提出時刻)
    - UPDATE 0件なら INSERT
    """
    import json, re
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(earnings_events);")
    cols = {r[1] for r in cur.fetchall()}

    POS_KEYS = ("決算短信","四半期","通期","決算")
    EXCLUDE  = ("動画","説明資料")

    def norm_code(r: dict) -> str:
        cc = (r.get("company_code") or r.get("company_code_raw") or "").strip()
        # 5桁数字なら先頭4桁 (例: 13010 -> 1301)
        if cc.isdigit() and len(cc) == 5:
            return cc[:4]
        # 修正: 既に英字入りで抽出済みの ticker があればそれを優先
        tk = str(r.get("ticker") or "").strip().upper()
        if len(tk) == 4:
            return tk
        return tk.zfill(4)

    for r in rows:
        title = (r.get("title") or "").strip()
        if not any(k in title for k in POS_KEYS):
            continue
        if any(k in title for k in EXCLUDE):
            continue

        code = norm_code(r)
        announced = (r.get("time") or "").replace("T"," ").replace("+09:00","").strip()
        teishutsu = announced or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        jp_vals_all = {
            "コード": code,
            "銘柄名": r.get("name") or "",
            "タイトル": title,
            "リンク": r.get("link") or "",
            "発表日時": announced,
            "提出時刻": teishutsu,
            "要約": r.get("summary") or "",
            "判定": (r.get("verdict") or "").strip(),
            "判定スコア": int(r.get("score_judge") or 0),
            "理由JSON": json.dumps(r.get("reasons") or [], ensure_ascii=False),
            "指標JSON": json.dumps(r.get("metrics") or {}, ensure_ascii=False),
            "進捗率": r.get("progress"),
            "センチメント": r.get("sentiment") or "",
            "素点": int(r.get("score") or 0),
        }

        set_cols, set_vals = [], []
        for k, v in jp_vals_all.items():
            if k in cols and k not in ("コード","提出時刻") and v is not None:
                set_cols.append(f'"{k}"=?'); set_vals.append(v)

        updated = 0
        if set_cols:
            sql = (
                f'UPDATE earnings_events SET {", ".join(set_cols)} '
                f'WHERE "コード"=? AND "提出時刻"=?'
            )
            cur.execute(sql, set_vals + [jp_vals_all["コード"], jp_vals_all["提出時刻"]])
            updated = cur.rowcount

        if updated == 0:
            insert_cols, insert_vals = [], []
            for k, v in jp_vals_all.items():
                if k in cols and v is not None:
                    insert_cols.append(f'"{k}"'); insert_vals.append(v)
            placeholders = ",".join(["?"] * len(insert_cols))
            sql = f'INSERT OR IGNORE INTO earnings_events ({", ".join(insert_cols)}) VALUES ({placeholders})'
            cur.execute(sql, insert_vals)

    conn.commit()

def load_earnings_for_dashboard_from_db(days: int = 30, filter_mode: str = "nonnegative") -> list[dict]:
    import json as _json
    conn = _get_db_conn()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
          "コード","銘柄名","タイトル","リンク","発表日時","提出時刻",
          "要約","判定","判定スコア","理由JSON","指標JSON","進捗率","センチメント","素点"
        FROM "v_events_dedup"
        WHERE date("提出時刻") >= date('now', ?)
        ORDER BY "提出時刻" DESC
        """,
        (f"-{days} day",)
    ).fetchall()

    def _to_row(r):
        try:
            reasons = _json.loads(r[9]) if r[9] else []
        except Exception:
            reasons = []
        try:
            metrics = _json.loads(r[10]) if r[10] else {}
        except Exception:
            metrics = {}
        return {
            "ticker": (r[0] or "0000"),
            "name":   r[1] or (r[0] or "TDnet"),
            "title":  r[2] or "",
            "link":   r[3] or "",
            "time":   (r[5] or "").replace("T"," ").replace("+09:00",""),
            "summary": r[6] or "",
            "verdict": (r[7] or "").strip(),
            "score_judge": r[8],
            "reasons": reasons if isinstance(reasons, list) else [str(reasons)],
            "metrics": metrics if isinstance(metrics, dict) else {},
            "progress": r[11],
            "sentiment": r[12] or "neutral",
            "score": r[13] or 0,
        }

    items = [_to_row(r) for r in rows]

    if filter_mode == "pos_good":
        items = [x for x in items if (x.get("sentiment") == "positive") or ((x.get("verdict") or "") == "good")]
    elif filter_mode == "nonnegative":
        items = [x for x in items if (x.get("sentiment") != "negative") or ((x.get("verdict") or "") == "good")]

    try:
        codes = list({str(x.get("ticker") or "0000").zfill(4) for x in items})
        quotes = load_quotes_from_price_history(conn, codes)
        for x in items:
            q = quotes.get(str(x["ticker"]).zfill(4))
            if q:
                x["quote"] = q
    except Exception as e:
        print("[earnings] quotes attach error:", e)

    def _ts(s: str) -> float:
        try:
            return datetime.strptime((s or "").replace("/", "-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            return 0.0
    items.sort(key=lambda r: _ts(r.get("time","")), reverse=True)
    return items[:300]

# ------------------ HTML生成（日別タブ込み + ★TOB(TDNET)タブ） ------------------
# ------------------ HTML生成（日別タブ込み + ★TOB(TDNET)タブ） ------------------
def render_dashboard_html(payload: dict, api_base: str = "") -> str:
    data_json = json.dumps(payload, ensure_ascii=False)

    tpl = r"""<!doctype html>
<meta charset="utf-8" />
<title>注目度ダッシュボード</title>
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/>
<meta http-equiv="Pragma" content="no-cache"/>
<meta http-equiv="Expires" content="0"/>
<style>
  :root { --br:#e5e7eb; --fg:#0f172a; --muted:#64748b; --bg:#fff; --pos:#16a34a; --neu:#6b7280; --neg:#dc2626; --tab:#3b82f6; }
  html,body{background:#fff}
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;margin:18px;color:var(--fg)}
  h1{font-size:26px;margin:0 0 10px}
  .toolbar{display:flex;gap:10px;align-items:center;margin:6px 0 14px}
  .tabs{display:flex;gap:8px}
  .tab{padding:8px 12px;border:1px solid var(--br);border-radius:999px;background:#f8fafc;color:#111;font-size:13px;cursor:pointer}
  .tab.active{background:#e6f0ff;border-color:#bfdbfe;color:#1d4ed8;font-weight:600}
  .stamp{font-size:12px;color:var(--muted);margin-left:8px}
  .card{border:1px solid var(--br);border-radius:12px;padding:12px;margin:14px 0;background:#fff}
  table{width:100%;border-collapse:collapse}
  th,td{border-bottom:1px solid #f1f5f9;padding:8px 10px;text-align:left;vertical-align:top}
  th{background:#f8fafc;color:#334155;font-weight:600;position:sticky;top:0; z-index:10;}
  .codechip{display:inline-flex;align-items:center;gap:6px}
  .code{display:inline-block;background:#f1f5f9;border-radius:999px;padding:2px 8px;font-size:12px;color:#334155}
  .badge{display:inline-block;padding:2px 10px;border-radius:999px;color:#fff;font-size:12px;white-space:nowrap}
  .pos{background:var(--pos)} .neu{background:var(--neu)} .neg{background:var(--neg)}
  .newslist{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;max-width:1100px}
  .newsitem{border:1px solid var(--br);border-radius:10px;padding:8px;background:#fff}
  .src{color:#64748b;font-size:12px}
  .btn{padding:8px 12px;border:1px solid var(--br);border-radius:10px;background:#f8fafc;cursor:pointer}
  .btn:hover{filter:brightness(0.98)}
  .small{font-size:12px;color:#64748b}
  a{color:#1d4ed8;text-decoration:none} a:hover{text-decoration:underline}
  .day-head{font-weight:700;margin:18px 0 8px; background:#f1f5f9; color:#334155;}
  .muted{opacity:.85}
  td.num{text-align:right}
  td.diff.plus{color:#16a34a;font-weight:600}
  td.diff.minus{color:#dc2626;font-weight:600}
  .copybtn{cursor:pointer;text-decoration:none}
  .judge-row{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:6px; }
  .jdg{ display:inline-flex; align-items:center; gap:6px; padding:2px 10px; border-radius:999px; font-weight:700; border:1px solid var(--br); line-height:1.9; }
  .jdg .score{ font-weight:600; opacity:.8; }
  .jdg.positive{ color:#065f46; background:#ecfdf5; border-color:#a7f3d0; }
  .jdg.neutral { color:#374151; background:#f3f4f6; border-color:#e5e7eb; }
  .jdg.negative{ color:#7f1d1d; background:#fef2f2; border-color:#fecaca; }
  .ev{ padding:4px 10px; border-radius:12px; line-height:1.9; background:#f0f9ff; color:#0c4a6e; border-left:4px solid #38bdf8; }
  .ev .chip{ display:inline-block; margin:2px 6px 2px 0; padding:2px 8px; border-radius:999px; background:#e0f2fe; border:1px solid #bae6fd; font-weight:600; }
  /* Filter Input Style */
  .filter-input { width: 90%; font-size: 11px; padding: 4px; margin-top: 4px; border: 1px solid #cbd5e1; border-radius: 4px; box-sizing: border-box; }
  .filter-input:focus { outline: 2px solid #bfdbfe; border-color: #3b82f6; }
</style>

<h1>注目度ダッシュボード</h1>
<div class="toolbar">
  <div class="tabs">
    <button class="tab" data-mode="earnings">決算</button>
    <button class="tab" data-mode="earnings_day">決算（日別）</button>
    <button class="tab" data-mode="leak">漏れ？</button>
    <button class="tab" data-mode="tob">TOB</button>
    <button class="tab" data-mode="tob_tdnet">TOB(TDNET)</button>
  </div>
  <span id="stamp" class="stamp"></span>
  <button class="btn" id="btn-rerender">再描画</button>
</div>

<div class="card">
  <table id="tbl">
    <thead id="thead"></thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
  window.__DATA__ = __DATA_JSON__;
  const API_BASE = "";
  let MODE = "earnings";

  function esc(s){return (s??"").toString().replace(/[&<>\"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m]));}
  function fmt(v){const n=Number(v||0);return isFinite(n)?n.toFixed(3):"0.000";}
  function badge(label){const cls=label==="positive"?"pos":(label==="negative"?"neg":"neu");return `<span class="badge ${cls}">${label||"neutral"}</span>`;}
  function parseJST(s){
    if(!s) return 0;
    const t = String(s).replace("T"," ").replace("+09:00","").trim();
    const m = t.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
    if(m){
      const y=+m[1], mo=+m[2]-1, d=+m[3], h=+m[4], mi=+m[5], se=+m[6];
      const utcMs = Date.UTC(y, mo, d, h-9, mi, se);
      return isFinite(utcMs) ? utcMs : 0;
    }
    const dt = Date.parse(t+" +09:00");
    return isNaN(dt) ? 0 : dt;
  }
  function yQuoteUrl(code){const c=String(code||"").padStart(4,"0");return `https://finance.yahoo.co.jp/quote/${c}.T`;}
  function copyCode(code){
    navigator.clipboard?.writeText(String(code||""))?.then(()=>{})
  }
  function toNewsCards(items){
    if(!items||!items.length) return "";
    const html = items.slice(0,3).map(it=>{
      const t=esc(it.title||""); const l=esc(it.link||"#"); const s=esc(it.source||""); const p=esc(it.published||"");
      return `<div class="newsitem"><a href="${l}" target="_blank" rel="noopener">${t}</a><div class="src">${s}　${p}</div></div>`;
    }).join("");
    return `<div class="newslist">${html}</div>`;
  }
  function stockCell(code,name){
    const c=String(code||"").padStart(4,"0"); const n=esc(name||c);
    return `<span class="codechip"><a class="copybtn" title="コードをコピー" href="javascript:void(0)" onclick="copyCode('${c}')">📋</a><span class="code">${c}</span><span>${n}</span></span>`;
  }

  function offeringCell(f){ return f ? `<span class="code">増資経歴アリ</span>` : ``; }
  function verdictToLabel(v){
    if(v==="good") return "positive";
    if(v==="bad")  return "negative";
    return "neutral";
  }
  function judgeHtml(verdict, score, reasons){
    const cls = verdictToLabel(verdict||"");
    const s = (score ?? null) !== null ? `（score ${esc(score)}）` : "";
    const chips = (Array.isArray(reasons)?reasons:[])
      .filter(x=>x!=null && String(x).trim()!=="")
      .map(x=>`<span class="chip">${esc(String(x))}</span>`).join("");
    const ev = chips ? `<span class="ev">根拠: ${chips}</span>` : "";
    return `<div class="judge-row"><span class="jdg ${cls}">判定: ${esc(verdict||"")}<span class="score">${s}</span></span>${ev}</div>`;
  }

  // --- Filtering Logic ---
  function applyFilters(){
    const inputs = document.querySelectorAll(".filter-input");
    const rows = document.querySelectorAll("#tbody tr");
    
    // 現在のフィルタ条件を抽出 [{col: 1, val: "7203"}, ...]
    const activeFilters = [];
    inputs.forEach(inp => {
        const val = inp.value.toLowerCase().trim();
        if(val) activeFilters.push({ col: parseInt(inp.dataset.col), val: val });
    });

    rows.forEach(tr => {
        // "earnings_day" の日付ヘッダー行は特別扱い（一旦表示、後でクリーンアップ）
        if(tr.classList.contains("day-head")) {
            tr.style.display = ""; 
            return;
        }
        
        // データ行の判定
        let isMatch = true;
        if(activeFilters.length > 0) {
            activeFilters.forEach(f => {
                const td = tr.children[f.col];
                if(!td) return;
                const txt = td.textContent.toLowerCase();
                // 数値カラムなどでカンマが入っていても部分一致で検索可能
                if(!txt.includes(f.val)) isMatch = false;
            });
        }
        tr.style.display = isMatch ? "" : "none";
    });

    // "earnings_day" の場合、子要素がすべて消えた日付ヘッダーを隠す処理
    if(MODE === "earnings_day"){
        const dayHeads = document.querySelectorAll("tr.day-head");
        dayHeads.forEach(head => {
            let next = head.nextElementSibling;
            let hasVisibleChild = false;
            // 次のヘッダーまたは末尾まで走査
            while(next && !next.classList.contains("day-head")){
                if(next.style.display !== "none") {
                    hasVisibleChild = true; 
                    break;
                }
                next = next.nextElementSibling;
            }
            head.style.display = hasVisibleChild ? "" : "none";
        });
    }
  }

  // Helper for generating TH with filter input
  function thFilter(label, colIdx, width){
    return `<th>${label}<br><input type="text" class="filter-input" data-col="${colIdx}" oninput="applyFilters()" placeholder="絞込..."></th>`;
  }
  function thSimple(label){ return `<th>${label}</th>`; }


function render(j){
  document.getElementById("stamp").textContent = "生成時刻: " + (j.generated_at || "");
  const thead=document.getElementById("thead");
  const tbody=document.getElementById("tbody");
  tbody.innerHTML="";

  // --- 決算（日別） ---
  if(MODE==="earnings_day"){
    const rows=(j.earnings||[])
      .slice()
      .sort((a,b)=>{const tb=parseJST(b.time),ta=parseJST(a.time);return tb!==ta?tb-ta:(Number(b.score_judge||0)-Number(a.score_judge||0));})
      .slice(0, 300);
    
    // 絞り込み可能な列: コード(1), 銘柄(2), スコア(8), Sentiment(9), 内容(10)
    thead.innerHTML = `<tr>`
      + thSimple("#")
      + thFilter("コード", 1)
      + thFilter("銘柄", 2)
      + thSimple("増資経歴")
      + thSimple("Yahoo")
      + thSimple("現在値") + thSimple("前日終値") + thSimple("前日比")
      + thFilter("判定スコア", 8)
      + thFilter("Sentiment", 9)
      + thFilter("書類名 / サマリ / 判定理由", 10)
      + thSimple("時刻")
      + `</tr>`;

    if(!rows.length){const tr=document.createElement("tr");tr.innerHTML=`<td colspan="12" class="small">直近では見つかりませんでした。</td>`;tbody.appendChild(tr);return;}
    let day=""; rows.forEach((r,idx)=>{
      const d=(r.time||"").slice(0,10);
      if(d!==day){day=d; const trh=document.createElement("tr"); trh.className="day-head"; trh.innerHTML=`<td colspan="12">${esc(day)}</td>`; tbody.appendChild(trh);}
      const q=r.quote||{};
      const cur = q.current!=null ? Number(q.current).toLocaleString() : "";
      const prev= q.prev_close!=null ? Number(q.prev_close).toLocaleString() : "";
      const diff= q.diff_yen!=null ? Number(q.diff_yen) : null;
      const diffStr = diff!=null ? ((diff>=0?"+":"")+diff.toLocaleString()) : "";
      const diffCls = diff==null ? "" : (diff>=0?"plus":"minus");

      const tr=document.createElement("tr"); const t=(h,c)=>{const el=document.createElement("td"); if(c) el.className=c; el.innerHTML=h; tr.appendChild(el);};
      t(String(idx+1));
      t(`<a class="copybtn" title="コピー" href="javascript:void(0)" onclick="copyCode('${esc(r.ticker)}')">📋</a> ${esc(r.ticker)}`);
      t(stockCell(r.ticker,r.name)); t(offeringCell(r.has_offering_history)); t(`<a href="${yQuoteUrl(r.ticker)}" target="_blank" rel="noopener">Yahoo</a>`);
      t(cur, "num"); t(prev, "num"); t(diffStr, "num diff "+diffCls);
      t(String(r.score_judge??0)); t(badge(r.sentiment||"neutral"));
      const titleHtml=r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener">${esc(r.title||"(無題)")}</a>`:esc(r.title||"(無題)");
      const summaryHtml=r.summary?`<div class="small muted" style="margin-top:6px; white-space:pre-line;">${esc(r.summary)}</div>`:"";
      const judgeBlock = judgeHtml(r.verdict||"", r.score_judge, r.reasons||[]);
      t(`${titleHtml}${summaryHtml}${judgeBlock}`);
      t(esc((r.time||"").replace("T"," ").replace("+09:00","")));
      tbody.appendChild(tr);
    });
    return;
  }

  // --- 決算（一覧） ---
  if(MODE==="earnings"){
    const rows=(j.earnings||[])
      .slice()
      .sort((a,b)=>{const tb=parseJST(b.time),ta=parseJST(a.time);return tb!==ta?tb-ta:(Number(b.score_judge||0)-Number(a.score_judge||0));})
      .slice(0, 300);
    
    thead.innerHTML = `<tr>`
      + thSimple("#")
      + thFilter("コード", 1)
      + thFilter("銘柄", 2)
      + thSimple("増資経歴")
      + thSimple("Yahoo")
      + thSimple("現在値") + thSimple("前日終値") + thSimple("前日比")
      + thFilter("判定スコア", 8)
      + thFilter("Sentiment", 9)
      + thFilter("書類名 / サマリ / 判定理由", 10)
      + thSimple("時刻")
      + `</tr>`;

    if(!rows.length){const tr=document.createElement("tr");tr.innerHTML=`<td colspan="12" class="small">直近では見つかりませんでした。</td>`;tbody.appendChild(tr);return;}
    rows.forEach((r,idx)=>{
      const q=r.quote||{};
      const cur = q.current!=null ? Number(q.current).toLocaleString() : "";
      const prev= q.prev_close!=null ? Number(q.prev_close).toLocaleString() : "";
      const diff= q.diff_yen!=null ? Number(q.diff_yen) : null;
      const diffStr = diff!=null ? ((diff>=0?"+":"")+diff.toLocaleString()) : "";
      const diffCls = diff==null ? "" : (diff>=0?"plus":"minus");

      const tr=document.createElement("tr"); const t=(h,c)=>{const el=document.createElement("td"); if(c) el.className=c; el.innerHTML=h; tr.appendChild(el);};
      t(String(idx+1));
      t(`<a class="copybtn" title="コピー" href="javascript:void(0)" onclick="copyCode('${esc(r.ticker)}')">📋</a> ${esc(r.ticker)}`);
      t(stockCell(r.ticker,r.name)); t(offeringCell(r.has_offering_history)); t(`<a href="${yQuoteUrl(r.ticker)}" target="_blank" rel="noopener">Yahoo</a>`);
      t(cur, "num"); t(prev, "num"); t(diffStr, "num diff "+diffCls);
      t(String(r.score_judge??0)); t(badge(r.sentiment||"neutral"));
      const titleHtml=r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener">${esc(r.title||"(無題)")}</a>`:esc(r.title||"(無題)");
      const summaryHtml=r.summary?`<div class="small muted" style="margin-top:6px; white-space:pre-line;">${esc(r.summary)}</div>`:"";
      const judgeBlock = judgeHtml(r.verdict||"", r.score_judge, r.reasons||[]);
      t(`${titleHtml}${summaryHtml}${judgeBlock}`);
      t(esc((r.time||"").replace("T"," ").replace("+09:00","")));
      tbody.appendChild(tr);
    });
    return;
  }

  // --- 掲示板 ---
  if(MODE==="bbs"){
    const rows=(j.rows||[]).slice().sort((a,b)=>{
      const a24=a?.bbs?.posts_24h||0, b24=b?.bbs?.posts_24h||0; if(b24!==a24) return b24-a24;
      const a72=a?.bbs?.posts_72h||0, b72=b?.bbs?.posts_72h||0; if(b72!==a72) return b72-a72;
      return String(a.name||a.ticker).localeCompare(String(b.name||b.ticker));
    });
    
    thead.innerHTML = `<tr>`
      + thSimple("#")
      + thFilter("銘柄", 1)
      + thSimple("増資経歴")
      + thSimple("BBS(24h)")
      + thSimple("BBS(72h)")
      + thSimple("増加率")
      + thFilter("Sentiment(News)", 6)
      + thFilter("最新ニュース", 7)
      + `</tr>`;

    rows.forEach((r,idx)=>{
      const tr=document.createElement("tr"); const t=(h)=>{const el=document.createElement("td"); el.innerHTML=h; tr.appendChild(el);};
      const b24=r?.bbs?.posts_24h||0, b72=r?.bbs?.posts_72h||0, growth=(b24/Math.max(1,b72)).toFixed(2);
      const items=(r.news&&r.news.items)?r.news.items:[]; const label=(items[0]?.sentiment)||"neutral";
      t(String(idx+1));
      t(`${esc(r.name||r.ticker)} <span class="code">(${esc(r.ticker)})</span>`);
      t(offeringCell(r.has_offering_history));
      t(String(b24));
      t(String(b72));
      t(String(growth));
      t(badge(label));
      t(toNewsCards(items));
      tbody.appendChild(tr);
    });
    return;
  }

  // 共通: 検索系（漏れてる？ / TOB[掲示板]）レンダラ
  function renderAggRows(agg){
    const rows=(agg?.rows||[]).slice().sort((a,b)=> (b.count||0)-(a.count||0));
    thead.innerHTML = `<tr>
      <th>#</th><th>銘柄</th><th>件数</th><th>サンプル（最大6件）</th>
    </tr>`;

    if(!rows.length){
      const tr=document.createElement("tr");
      tr.innerHTML=`<td colspan="4" class="small">該当がありません。</td>`;
      tbody.appendChild(tr);
      return;
    }
    rows.forEach((r,idx)=>{
      const tr=document.createElement("tr");
      const t=(h)=>{const el=document.createElement("td"); el.innerHTML=h; tr.appendChild(el);};
      const samples=(r.comments||[]).slice(0,6).map(c=>{
        const lab = c.sentiment||"neutral";
        const badgeCls = lab==="positive" ? "pos" : (lab==="negative"?"neg":"neu");
        const text = esc(c.text||"");
        const link = esc(c.link||"#");
        const pub  = esc((c.published||"").toString());
        return `<div class="newsitem">
                  <a href="${link}" target="_blank" rel="noopener">${text}</a>
                  <div class="src"><span class="badge ${badgeCls}">${lab}</span>　${pub}</div>
                </div>`;
      }).join("");
      t(String(idx+1));
      t(`${esc(r.name||r.ticker)} <span class="code">(${esc(r.ticker)})</span>`);
      t(String(r.count||0));
      t(`<div class="newslist">${samples}</div>`);
      tbody.appendChild(tr);
    });
  }

  if(MODE==="leak"){
    renderAggRows((j.leak||{}));
    return;
  }
  if(MODE==="tob"){
    renderAggRows((j.tob||{}));
    return;
  }

  // ★TDNET由来のTOB表示
  if(MODE==="tob_tdnet"){
    const rows=(j.tob_tdnet||[]).slice();
    thead.innerHTML=`<tr>
      <th>#</th><th>コード</th><th>銘柄</th><th>書類名</th>
      <th class="num">買付価格(円)</th><th class="num">レンジ</th><th class="num">目標比率(%)</th>
      <th class="num">最低応募株数</th><th>提出時刻</th>
    </tr>`;
    if(!rows.length){
      const tr=document.createElement("tr");
      tr.innerHTML=`<td colspan="9" class="small">直近ではTOB( TDNET )は見つかりませんでした。</td>`;
      tbody.appendChild(tr);
      return;
    }
    rows.forEach((r,idx)=>{
      const tr=document.createElement("tr");
      const t=(h,c)=>{const el=document.createElement("td"); if(c) el.className=c; el.innerHTML=h; tr.appendChild(el);};
      t(String(idx+1));
      t(`<a class="copybtn" title="コピー" href="javascript:void(0)" onclick="copyCode('${esc(r.ticker)}')">📋</a> ${esc(r.ticker)}`);
      t(stockCell(r.ticker, r.name));
      const titleHtml=r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener">${esc(r.title||"(無題)")}</a>`:esc(r.title||"(無題)");
      t(titleHtml);
      t(r.buy_price!=null?Number(r.buy_price).toLocaleString():"", "num");
      const rg = (r.range_low!=null||r.range_high!=null) ? `${r.range_low!=null?Number(r.range_low).toLocaleString():""}〜${r.range_high!=null?Number(r.range_high).toLocaleString():""}` : "";
      t(rg, "num");
      t(r.target_ratio!=null?Number(r.target_ratio).toLocaleString(): "", "num");
      t(r.min_shares!=null?Number(r.min_shares).toLocaleString():"", "num");
      t(esc((r.submitted||"").replace("T"," ").replace("+09:00","")));
      tbody.appendChild(tr);
    });
    return;
  }

  // 総合
  const rows=(j.rows||[]).slice().sort((a,b)=>(b.score||0)-(a.score||0));
  
  thead.innerHTML = `<tr>`
    + thSimple("#")
    + thFilter("銘柄", 1)
    + thSimple("増資経歴")
    + thFilter("スコア", 3)
    + thSimple("Trends")
    + thSimple("BBS")
    + thSimple("News(24h)")
    + thFilter("Sentiment", 7)
    + thFilter("最新ニュース", 8)
    + `</tr>`;

  rows.forEach((r,idx)=>{
    const tr=document.createElement("tr"); const t=(h)=>{const el=document.createElement("td"); el.innerHTML=h; tr.appendChild(el);};
    const items=(r.news&&r.news.items)?r.news.items:[]; const label=(items[0]?.sentiment)||"neutral";
    t(String(idx+1));
    t(`${esc(r.name||r.ticker)} <span class="code">(${esc(r.ticker)})</span>`);
    t(offeringCell(r.has_offering_history));
    t(fmt(r.score));
    t(fmt(r.trends?.latest));
    t(String(r.bbs?.posts_24h||0));
    t(String(r.news?.count_24h||0));
    t(badge(label));
    t(toNewsCards(items));
    tbody.appendChild(tr);
  });
}

  document.addEventListener("DOMContentLoaded", ()=>{
    document.querySelectorAll(".tab").forEach(btn=>{
      btn.addEventListener("click", ()=> { MODE=btn.dataset.mode; render(window.__DATA__); document.querySelectorAll(".tab").forEach(b=>b.classList.toggle("active", b===btn)); });
    });
    document.getElementById("btn-rerender").addEventListener("click", ()=> render(window.__DATA__));
    render(window.__DATA__);
  });
</script>
    """

    return tpl.replace("__DATA_JSON__", data_json)

# -------- PDF ユーティリティ --------
def _download_pdf_bytes(url: str, timeout: int = 25) -> bytes:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
        r.raise_for_status()
        return r.content or b""
    except Exception as e:
        print(f"[pdf] download error: {e} url={url}")
        return b""

def _extract_text_pdfminer(pdf_bytes: bytes) -> str:
    try:
        from pdfminer_high_level import extract_text  # type: ignore
    except Exception:
        try:
            from pdfminer.high_level import extract_text  # type: ignore
        except Exception:
            extract_text = None
    if extract_text is None:
        return ""
    try:
        with io.BytesIO(pdf_bytes) as bio:
            return (extract_text(bio) or "").strip()
    except Exception as e:
        print(f"[pdfminer] extract error: {e}")
        return ""

def _extract_text_pypdf2(pdf_bytes: bytes) -> str:
    try:
        import PyPDF2  # type: ignore
        out = []
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            try:
                out.append(page.extract_text() or "")
            except Exception:
                out.append("")
        return "\n".join(out).strip()
    except Exception as e:
        print(f"[PyPDF2] extract error: {e}")
        return ""

def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    # 先に pdfminer を試す（比較的静かで高精度）
    text = _extract_text_pdfminer(pdf_bytes)
    if text and len(text) > 30:
        return text
    
    # ここで PyPDF2 が呼ばれる際にログが出る。
    # 前述の logging.getLogger("PyPDF2").setLevel(logging.ERROR) が効いていれば静かになります。
    text = _extract_text_pypdf2(pdf_bytes)
    return text
    
def _safe_extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        txt = _extract_text_from_pdf(pdf_bytes)
        if isinstance(txt, str) and txt.strip():
            return txt
    except Exception:
        pass
    return ""

# -------- 決算テキストの解析/採点 --------
def _parse_earnings_metrics(text: str) -> Dict[str, Any]:
    if not text:
        return {"metrics": {}, "progress": None}

    def _num(s):
        s = s.replace(",", "").replace("，", "").replace("％", "%").replace("▲", "-").replace("△", "-")
        s = re.sub(r"[^\d\.\-\+%]", "", s)
        try:
            if s.endswith("%"):
                return float(s[:-1])
            return float(s)
        except:
            return None

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
        s = text[max(0, pos-120): pos+220]
        s = s.replace('％','%').replace('▲','-').replace('△','-')
        m = re.search(r'([\-+]?\d+(?:\.\d+)?)\s*%', s)
        if not m: return None
        try:
            val = float(m.group(1))
        except Exception:
            return None
        around = s[max(0, m.start()-10): m.end()+10]
        if ('-' not in m.group(1) and '+' not in m.group(1)):
            if re.search(r'(減|悪化|縮小)', around):
                val = -val
        return val

    for key, pat in fields.items():
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            raw = m.group(2) if len(m.groups()) >= 2 else None
            val = _num(raw or "")
            if val is not None:
                unit = m.group(3) if len(m.groups()) >= 3 else ""
                if key != "EPS" and key != "進捗率" and unit:
                    u = unit
                    if "百万円" in u:
                        val = val / 100.0
                metrics[key] = val

            try:
                pos = text.find(key)
                y = _scan_yoy_near(pos, text)
                if y is not None:
                    yoy[key] = y
            except Exception:
                pass

    progress = None
    if "進捗率" in metrics:
        progress = float(metrics["進捗率"])
    return {"metrics": metrics, "progress": progress, "yoy": yoy}

def _summarize_earnings_text(text: str, max_chars: int = 240) -> str:
    if not text:
        return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    KEY = [
        "上方修正", "下方修正", "通期", "四半期", "増益", "減益", "増収", "減収",
        "進捗率", "配当", "業績予想", "修正"
    ]
    hits = [l for l in lines if any(k in l for k in KEY)]
    base = " / ".join(hits[:4]) if hits else " ".join(lines[:6])
    base = re.sub(r"\s+", " ", base).strip()
    return base[:max_chars]

def _grade_earnings(title: str, text: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    reasons: list[str] = []
    score = 0.0

    T = title or ""
    X = text or ""
    met = (parsed or {}).get("metrics", {})
    prog = (parsed or {}).get("progress", None)
    yoy = (parsed or {}).get("yoy", {})

    def _hit(ws):
        return any((w in T) or (w in X) for w in ws)

    if _hit(["上方修正", "通期上方", "通期増額", "上期予想修正（増額）",
             "業績予想の修正（増額）", "通期予想修正（増額）"]):
        score += 2; reasons.append("上方修正（増額）で+2")

    if _hit(["増配", "復配", "自社株買い", "配当予想の修正（増額）"]):
        score += 1; reasons.append("配当増額/復配/自社株買いで+1")

    if _hit(["最高益", "過去最高益"]):
        score += 1; reasons.append("最高益で+1")

    if _hit(["黒字転換"]):
        score += 1; reasons.append("黒字転換で+1")

    if _hit(["下方修正", "通期予想修正（減額）"]):
        score -= 3; reasons.append("下方修正（減額）で-3")

    if _hit(["減配", "配当予想の修正（減額）"]):
        score -= 2; reasons.append("減配で-2")

    if _hit(["特損", "特別損失"]):
        score -= 2; reasons.append("特損で-2")

    if isinstance(prog, (int, float)):
        if prog >= 70:
            score += 1; reasons.append(f"進捗率{prog:.0f}%（高進捗）")
        elif prog <= 30:
            score -= 1; reasons.append(f"進捗率{prog:.0f}%（低進捗）")

    def _sane(y):
        return y if isinstance(y, (int, float)) and -120.0 <= y <= 120.0 else None

    neg_profit_yoy = 0
    pos_profit_yoy = 0
    for k in ("売上高", "営業利益", "経常利益", "純利益"):
        y = _sane(yoy.get(k))
        if y is None:
            continue
        if k in ("営業利益", "経常利益", "純利益"):
            if y <= -5: neg_profit_yoy += 1
            if y >= 1:  pos_profit_yoy += 1
        if y >= 5:
            score += 1;   reasons.append(f"{k}YoY+{y:.1f}%")
        elif y >= 1:
            score += 0.5; reasons.append(f"{k}YoY+{y:.1f}%")
        elif y <= -5:
            score -= 1;   reasons.append(f"{k}YoY{y:.1f}%")
        elif y <= -1:
            score -= 0.5; reasons.append(f"{k}YoY{y:.1f}%")

    if neg_profit_yoy >= 2:
        score -= 1.0; reasons.append("利益YoYが複数項目で減少（総じて減益傾向）")

    for k in ("営業利益", "経常利益", "純利益"):
        v = met.get(k, None)
        if isinstance(v, (int, float)):
            if v > 0:
                if pos_profit_yoy > 0:
                    score += 0.3; reasons.append(f"{k}が黒字（増益傾向と整合）")
                else:
                    reasons.append(f"{k}が黒字（単独要素のため加点なし）")
            if v < 0:
                score -= 0.5; reasons.append(f"{k}が赤字")

    verdict = "good" if score >= 1.5 else ("bad" if score <= -1.5 else "neutral")
    return {"verdict": verdict, "score": score, "reasons": reasons}

# ========= TDNET取得の核 =========
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

        # まずは常に安定する日単位API
        url_day = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json"
        js_day = _http_get_json(url_day)
        items_day = js_day.get("items") if isinstance(js_day, dict) else (js_day if isinstance(js_day, list) else None)
        if isinstance(items_day, list):
            print(f"[tdnet] {dt_from:%Y-%m-%d %H:%M}~{dt_to:%H:%M} : api={len(items_day)} (fmt=list/day)")
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
                if is_time:
                    time_capable = True
                print(f"[tdnet] {dt_from:%Y-%m-%d %H:%M}~{dt_to:%H:%M} : api={len(items)} (fmt={tag})")
                return items or []
        print(f"[tdnet] {dt_from:%Y-%m-%d %H:%M}~{dt_to:%H:%M} : api=None (all time-formats failed)")
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
                    print(f"[tdnet]   extra page p={p} via '{fmt[1:4]}' -> {len(items)}")
                    break
            if hit == 0:
                break
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
                if extras:
                    print(f"[tdnet] day extra merged: base=300 + extras={len(extras)}")
                    return items + extras
            print(f"[tdnet] time-capability=NO → stop slicing ({dt_from:%F} {dt_from:%H:%M}~{dt_to:%H:%M}), got={len(items)}")
            return items
        if level >= len(slice_escalation):
            print(f"[tdnet] slice exhausted ({dt_from:%F} {dt_from:%H:%M}~{dt_to:%H:%M}), got={len(items)} >= limit={per_day_limit}")
            return items
        width_h = slice_escalation[level]
        step_sec = int(max(width_h * 3600, 1800))
        parts: list[dict] = []
        cur = dt_from
        while cur < dt_to:
            nxt = min(cur + timedelta(seconds=step_sec), dt_to)
            parts.extend(_slice_fetch(cur, nxt, level + 1))
            cur = nxt
            time.sleep(0.12)
        print(f"[tdnet] api-slice level={level} w={width_h}h -> merged={len(parts)}")
        return parts

    def _ts(x: dict) -> float:
        td = x.get("Tdnet") or {}
        s = (td.get("pubdate") or "").replace("/", "-")
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            return 0.0

    all_items: list[dict] = []
    seen: set[str] = set()
    end = datetime.now(JST).replace(microsecond=0)

    for i in range(days):
        day_to   = (end - timedelta(days=i)).replace(hour=23, minute=59, second=59)
        day_from = (end - timedelta(days=i)).replace(hour=0,  minute=0,  second=0)
        day_items = _slice_fetch(day_from, day_to)

        kept = 0
        for it in day_items:
            td = it.get("Tdnet") or it
            title = _unescape_text(td.get("title") or "")
            if KEY_RE_LOCAL and title and not KEY_RE_LOCAL.search(title):
                continue
            td["title"] = title
            k = _dedup_key(it)
            if not k or k in seen:
                continue
            seen.add(k)
            all_items.append(it); kept += 1

        print(f"[tdnet] {day_from:%Y-%m-%d} kept={kept} / raw={len(day_items)}")
        time.sleep(0.2)

    all_items.sort(key=_ts, reverse=True)
    return all_items

def tdnet_items_to_earnings_rows(tdnet_items):
    rows = []
    summarized = 0

    for it in tdnet_items:
        td = it.get("Tdnet", it) or {}

        title = (td.get("title") or "").strip()
        name  = (td.get("company_name") or "").strip()
        code_raw = str(td.get("company_code") or td.get("code") or "").strip()
        # 修正: 4桁目がアルファベットのパターン (例: 130A) に対応
        if re.fullmatch(r"\d{5}", code_raw):
            ticker = code_raw[:4]
        elif re.fullmatch(r"\d{3}[0-9a-zA-Z]", code_raw):
            ticker = code_raw.upper()  # アルファベットは大文字に統一
        else:
            ticker = ""

        link = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        time_str = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","")

        summary = (td.get("summary") or "").strip()
        reason  = (td.get("reason")  or "").strip()
        label_guess = "neutral"

        s_t, label_from_title, why_t = _summarize_title_simple(title)
        if label_from_title in ("positive", "negative"):
            label_guess = label_from_title
        if not summary and s_t:
            summary = s_t
        if not reason and why_t:
            reason = why_t

        verdict = (td.get("verdict") or "").strip()
        score_judge = td.get("score_judge")
        reasons = td.get("reasons")
        metrics = td.get("metrics")
        progress = td.get("progress")

        should_try_pdf = bool(link)
        if should_try_pdf and summarized < EARNINGS_SUMMARY_MAX:
            try:
                pdf_bytes = _download_pdf_bytes(link)
                if pdf_bytes:
                    text = _extract_text_from_pdf(pdf_bytes)
                    if text:
                        parsed = _parse_earnings_metrics(text)
                        metrics = parsed.get("metrics", {})
                        progress = parsed.get("progress")
                        sum2 = _summarize_earnings_text(text)
                        if sum2:
                            summary = sum2
                        judge = _grade_earnings(title, text, parsed)
                        verdict = judge["verdict"]
                        score_judge = judge["score"]
                        reasons = judge["reasons"]
                        if verdict == "good":
                            label_guess = "positive"
                        elif verdict == "bad":
                            label_guess = "negative"
                        summarized += 1
                time.sleep(0.15)
            except Exception as e:
                print(f"[earnings] PDF summarize error: {e}")

        if isinstance(reasons, str) and reasons:
            reasons = [reasons]
        if reasons is None:
            reasons = ([reason] if reason else [])

        sentiment = td.get("sentiment") or label_guess or "neutral"

        row = {
            "ticker": ticker or "0000",
            "name":   name or (ticker or "TDnet"),
            "score":  0,
            "sentiment": sentiment,
            "title": title,
            "link":  link,
            "time":  time_str or "",
            "summary": summary,
            "reasons": reasons,
            "verdict": verdict,
            "score_judge": score_judge,
            "metrics": metrics or {},
            "progress": progress,
        }
        rows.append(row)

    def _ts(s):
        try:
            return datetime.strptime((s or "").replace("/","-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except:
            return 0
    rows.sort(key=lambda r: _ts(r["time"]), reverse=True)

    try:
        rows = summarize_earnings_rows_parallel(rows, max_items=EARNINGS_SUMMARY_MAX, max_workers=8)
    except Exception as _e:
        print('[parallel] skipped:', _e)
    return rows

# ------------------ スコア計算（総合/掲示板） ------------------
def build_scores(rows, weights):
    wt = float(weights.get("trends", 1.0))
    wb = float(weights.get("bbs_growth", 1.0))
    wn = float(weights.get("news", 1.0))

    scores = []
    for r in rows:
        t = 0.0
        try:
            t = float((r.get("trends") or {}).get("latest", 0.0) or 0.0)
        except Exception:
            t = 0.0

        b24 = int((r.get("bbs") or {}).get("posts_24h", 0) or 0)
        b72 = int((r.get("bbs") or {}).get("posts_72h", 0) or 0)
        bbs_growth = b24 / max(1, b72)
        n = int((r.get("news") or {}).get("count_24h", 0) or 0)

        score = wt * t + wb * bbs_growth + wn * n
        scores.append(score)
    return scores

def build_bbs_scores(rows):
    out = []
    for r in rows:
        b24 = int((r.get("bbs") or {}).get("posts_24h", 0) or 0)
        b72 = int((r.get("bbs") or {}).get("posts_72h", 0) or 0)
        out.append(b24 / max(1, b72))
    return out

# === Parallel PDF summarization for earnings rows ===
EARNINGS_SUMMARY_MAX = 40

def _summarize_one_pdf_row(row: dict) -> dict:
    link = (row or {}).get("link") or (row or {}).get("pdf") or ""
    if not link:
        return row
    try:
        b = _download_pdf_bytes(link)
        if not b:
            return row
        try:
            text = _safe_extract_pdf_text(b)
        except NameError:
            return row
        if not text:
            return row

        parsed = {}
        if "_parse_earnings_metrics" in globals():
            try:
                parsed = _parse_earnings_metrics(text) or {}
            except Exception as e:
                print("[earnings] _parse_earnings_metrics err:", e)

        sum2 = None
        if "_summarize_earnings_text" in globals():
            try:
                sum2 = _summarize_earnings_text(text)
            except Exception as e:
                print("[earnings] _summarize_earnings_text err:", e)

        judge = {"verdict": None, "score": None, "reasons": []}
        if "_grade_earnings" in globals():
            try:
                judge = _grade_earnings(row.get("title",""), text, parsed)
            except Exception as e:
                print("[earnings] _grade_earnings err:", e)

        out = dict(row)
        if sum2: out["summary"] = sum2
        if isinstance(parsed, dict):
            out["metrics"]  = parsed.get("metrics", parsed)
            out["progress"] = parsed.get("progress", out.get("progress"))
        if isinstance(judge, dict):
            out["verdict"]     = judge.get("verdict", out.get("verdict"))
            out["score_judge"] = judge.get("score", out.get("score_judge"))
            out["reasons"]     = judge.get("reasons", out.get("reasons", []))
            if out.get("verdict") == "good":
                out["sentiment"] = "positive"
            elif out.get("verdict") == "bad":
                out["sentiment"] = "negative"
        return out
    except Exception as e:
        print(f"[earnings] summarize error: {e} url={link}")
        return row

def summarize_earnings_rows_parallel(rows: list, max_items: int = None, max_workers: int = 8):
    if not isinstance(rows, list) or not rows:
        return rows
    try:
        N = int(EARNINGS_SUMMARY_MAX)
    except Exception:
        N = 20
    if isinstance(max_items, int) and max_items > 0:
        N = max_items

    targets = [(i, r) for i, r in enumerate(rows[:N]) if isinstance(r, dict) and (r.get("link") or r.get("pdf"))]
    if not targets:
        return rows
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut2idx = {ex.submit(_summarize_one_pdf_row, r): i for i, r in targets}
            for fut in as_completed(fut2idx):
                i = fut2idx[fut]
                try:
                    rows[i] = fut.result()
                except Exception as e:
                    print("[earnings] future err:", e)
    except Exception as e:
        print("[earnings] parallel block err:", e)
    return rows

def upsert_earnings_rows_fast(conn, rows):
    """Fast path: WAL + single transaction. PRAGMAは common 側で設定済みだが念のため軽い設定のみ。"""
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass
    conn.commit()

    conn.execute("BEGIN IMMEDIATE;")
    try:
        upsert_earnings_rows(conn, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return None

# ========= 追加インデックス / ビュー / ヘルスチェック =========
def _table_exists(conn, name: str) -> bool:
    try:
        cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False

def ensure_misc_objects(conn):
    """
    追加インデックス / トリガ / ビューの作成（存在チェック付き）
    """
    cur = conn.cursor()

    if _table_exists(conn, "signals_log"):
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_log_type_date
            ON signals_log(種別, substr(日時,1,10));
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_log_code_date
            ON signals_log(コード, substr(日時,1,10));
        """)

    if _table_exists(conn, "earnings_events"):
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earn_date_only
            ON earnings_events(substr(提出時刻,1,10));
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earn_code_date
            ON earnings_events(コード, substr(提出時刻,1,10));
        """)

    if _table_exists(conn, "price_history"):
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(price_history)").fetchall()]
            price_date_col = "日付" if "日付" in cols else ("date" if "date" in cols else None)
        except Exception:
            price_date_col = None

        if price_date_col:
            cur.execute("DROP TRIGGER IF EXISTS trg_price_history_date_norm_ins;")
            cur.execute("DROP TRIGGER IF EXISTS trg_price_history_date_norm_upd;")
            cur.execute(f"""
                CREATE TRIGGER IF NOT EXISTS trg_price_history_date_norm_ins
                AFTER INSERT ON price_history
                BEGIN
                  UPDATE price_history
                     SET "{price_date_col}" =
                         COALESCE(
                           date(replace(replace(NEW."{price_date_col}",'/','-'),'.','-')),
                           substr(replace(replace(NEW."{price_date_col}",'/','-'),'.','-'),1,10)
                         )
                   WHERE rowid = NEW.rowid;
                END;
            """)
            cur.execute(f"""
                CREATE TRIGGER IF NOT EXISTS trg_price_history_date_norm_upd
                AFTER UPDATE OF "{price_date_col}" ON price_history
                BEGIN
                  UPDATE price_history
                     SET "{price_date_col}" =
                         COALESCE(
                           date(replace(replace(NEW."{price_date_col}",'/','-'),'.','-')),
                           substr(replace(replace(NEW."{price_date_col}",'/','-'),'.','-'),1,10)
                         )
                   WHERE rowid = NEW.rowid;
                END;
            """)

    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_events_dedup AS
        WITH max_ts AS (
          SELECT
            コード,
            substr(提出時刻,1,10) AS 提出日,
            MAX(提出時刻) AS max_提出時刻
          FROM earnings_events
          GROUP BY コード, 提出日
        )
        SELECT e.*
          FROM earnings_events e
          JOIN max_ts m
            ON e.コード = m.コード
           AND substr(e.提出時刻,1,10) = m.提出日
           AND e.提出時刻 = m.max_提出時刻;
    """)
    conn.commit()

def run_light_healthcheck(conn):
    result = {"ok": True}
    try:
        last_date = None
        dcol = None
        if _table_exists(conn, "price_history"):
            cols = [r[1] for r in conn.execute("PRAGMA table_info(price_history)").fetchall()]
            dcol = "日付" if "日付" in cols else ("date" if "date" in cols else None)
            if dcol:
                last_date = conn.execute(f'SELECT MAX("{dcol}") FROM price_history').fetchone()[0]

        cov = None
        if last_date and dcol:
            cov = conn.execute(
                f'SELECT COUNT(DISTINCT "コード") FROM price_history WHERE "{dcol}"=?',
                (last_date,)
            ).fetchone()[0]

        last_event_ts = None
        if _table_exists(conn, "earnings_events"):
            last_event_ts = conn.execute("SELECT MAX(提出時刻) FROM earnings_events").fetchone()[0]
        result.update({
            "price_last_date": last_date,
            "price_coverage_codes": cov,
            "earnings_last_ts": last_event_ts
        })
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)

    if _table_exists(conn, "signals_log"):
        try:
            conn.execute(
                "INSERT INTO signals_log(コード, 種別, 日時, 詳細) VALUES(?, ?, ?, ?)",
                ("", "healthcheck",
                 datetime.utcnow().isoformat(),
                 json.dumps(result, ensure_ascii=False))
            )
            conn.commit()
        except Exception:
            pass

    return result

def _detect_price_cols(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(price_history);")
    cols = [r[1] for r in cur.fetchall()]
    if not cols:
        return None, None, None

    def pick(*cands):
        for c in cands:
            if c in cols:
                return c
        return None

    date_col = pick("日付", "date")
    code_col = pick("コード", "code", "ticker")
    price_col = pick("終値", "終値調整後", "close", "Close", "終値(円)", "終値_円")

    return date_col, code_col, price_col

def load_quotes_from_price_history(conn: sqlite3.Connection, codes: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not codes:
        return out

    date_col, code_col, price_col = _detect_price_cols(conn)
    if not all([date_col, code_col, price_col]):
        return out

    qmarks = ",".join("?" for _ in codes)
    cur = conn.cursor()

    latest = dict(cur.execute(
        f'SELECT "{code_col}", MAX("{date_col}") '
        f'FROM price_history WHERE "{code_col}" IN ({qmarks}) GROUP BY "{code_col}"',
        codes
    ).fetchall())

    for code, max_d in latest.items():
        rows = cur.execute(
            f'''SELECT "{price_col}" FROM price_history
                WHERE "{code_col}"=? AND "{date_col}"<=?
                ORDER BY "{date_col}" DESC LIMIT 2''',
            (code, max_d)
        ).fetchall()
        if not rows:
            continue
        cur_px = rows[0][0]
        prev_px = rows[1][0] if len(rows) >= 2 else None

        try:
            cur_px = float(cur_px) if cur_px is not None else None
        except Exception:
            cur_px = None
        try:
            prev_px = float(prev_px) if prev_px is not None else None
        except Exception:
            prev_px = None

        diff = None
        if cur_px is not None and prev_px is not None:
            diff = cur_px - prev_px

        out[str(code).zfill(4)] = {
            "current": cur_px,
            "prev_close": prev_px,
            "diff_yen": diff,
        }
    return out

# ======== ★★★ 増分取得ユーティリティ（ここだけ追加） ========
def _parse_ts_str(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s2 = s.replace("T"," ").replace("+09:00","").replace("/","-").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s2, fmt).replace(tzinfo=JST)
        except Exception:
            continue
    return None

def _latest_teishutsu_ts(conn: sqlite3.Connection) -> Optional[datetime]:
    """3テーブルの提出時刻の最新（最大）を返す。テーブルが無くても安全。"""
    cur = conn.cursor()
    latest: Optional[datetime] = None
    for tbl in ("earnings_events", "offerings_events", "tob_events"):
        try:
            if not _table_exists(conn, tbl):
                continue
            ts = cur.execute(f"SELECT MAX(提出時刻) FROM {tbl}").fetchone()[0]
            dt = _parse_ts_str(ts)
            if dt and (latest is None or dt > latest):
                latest = dt
        except Exception:
            continue
    return latest

def _tdnet_item_pub_dt(it: dict) -> Optional[datetime]:
    td = it.get("Tdnet") or it
    s = (td.get("pubdate") or td.get("publish_datetime") or td.get("time") or "")
    return _parse_ts_str(s)

def _earn_row_dt(row: dict) -> Optional[datetime]:
    return _parse_ts_str(row.get("time") or "")

# ------------------ main ------------------
def main():
    cfg = load_config(str(ROOT / "config.yaml"))

    include_dbg = bool((cfg.get("debug", {}) or {}).get("sentiment", False))
    include_bbs_samples = bool((cfg.get("debug", {}) or {}).get("bbs_samples", False))
    bbs_max_samples = int((cfg.get("bbs", {}) or {}).get("max_samples", 20))

    symbols = build_universe(cfg)

    # ---- 総合/掲示板タブ ----
    rows = []

    # ---- ★★★ 増分のチェックポイントを決定 ----
    conn = _get_db_conn()
    ensure_earnings_schema(conn)
    ensure_offerings_schema(conn)
    ensure_tob_schema(conn)
    ensure_misc_objects(conn)

    since_dt = _latest_teishutsu_ts(conn)  # Noneならフル取得
    now_jst = datetime.now(JST).replace(microsecond=0)

    # days の見積り（上限を決めて取り過ぎない）
    def span_days(max_cap: int) -> int:
        if not since_dt:
            return max_cap
        delta = now_jst - since_dt
        # 端数切上げ + バッファ2日
        days_est = max(1, int(delta.total_seconds() // 86400) + 2)
        return min(days_est, max_cap)

    # ---- 決算（TDnet）----
    try:
        earn_days = span_days(4) if since_dt else 14  # 既存挙動：初回は14日、増分時は最大90日まで動的
        tdnet_items = fetch_earnings_tdnet_only(days=earn_days, per_day_limit=300)
        print(f"[earnings] raw tdnet items = {len(tdnet_items)}")

        # since 以降だけ残す（> since：同一秒は除外）
        if since_dt:
            before = len(tdnet_items)
            tdnet_items = [it for it in tdnet_items if (_tdnet_item_pub_dt(it) or now_jst) > since_dt]
            print(f"[earnings] filter by since ({since_dt}): {before} -> {len(tdnet_items)}")

        earnings_rows = tdnet_items_to_earnings_rows(tdnet_items)
        print(f"[earnings] rows after shaping = {len(earnings_rows)}")

        # rows 側でも二重防御
        if since_dt:
            b2 = len(earnings_rows)
            earnings_rows = [r for r in earnings_rows if (_earn_row_dt(r) or now_jst) > since_dt]
            print(f"[earnings] shaped filter by since: {b2} -> {len(earnings_rows)}")

        for it in earnings_rows[:10]:
            print("  -", it.get("time",""), it.get("ticker",""), it.get("title","")[:60])

        if not earnings_rows:
            print("[earnings][fallback] 直近7日で再取得します…")
            tdnet_items_fallback = fetch_earnings_tdnet_only(days=5, per_day_limit=300)
            print(f"[earnings][fallback] raw items = {len(tdnet_items_fallback)}")
            if since_dt:
                tdnet_items_fallback = [it for it in tdnet_items_fallback if (_tdnet_item_pub_dt(it) or now_jst) > since_dt]
            earnings_rows = tdnet_items_to_earnings_rows(tdnet_items_fallback)
            if since_dt:
                earnings_rows = [r for r in earnings_rows if (_earn_row_dt(r) or now_jst) > since_dt]
            print(f"[earnings][fallback] shaped rows = {len(earnings_rows)}")

        if not earnings_rows:
            ensure_dir(OUT_DIR)
            dump_json(tdnet_items, str(OUT_DIR / "tdnet_raw_debug.json"))
            print("[earnings][debug] tdnet_raw_debug.json を出力しました（API応答の中身を確認できます）")

    except Exception as e:
        print("[earnings] 取得/整形で例外:", e)
        earnings_rows = []
        
    # ---- 増資レーン ----
    offer_items = []
    try:
        offer_days = span_days(4) if since_dt else 3
        offer_items = fetch_tdnet_by_keywords(days=offer_days, keywords=OFFERING_KW, per_day_limit=300)
        print(f"[offerings] raw tdnet items (by KW) = {len(offer_items)}")
        if since_dt:
            b = len(offer_items)
            offer_items = [it for it in offer_items if (_tdnet_item_pub_dt(it) or now_jst) > since_dt]
            print(f"[offerings] filter by since ({since_dt}): {b} -> {len(offer_items)}")
    except Exception as e:
        print("[offerings] fetch error:", e)

    # ---- ★TOBレーン（TDNET）----
    tob_rows = []
    try:
        tob_days = span_days(4) if since_dt else 3
        tob_src = fetch_tdnet_tob(days=tob_days, per_day_limit=300)
        if since_dt:
            b = len(tob_src)
            def _row_dt(r):
                return _parse_ts_str(r.get("提出時刻") or r.get("発表日時") or "")
            tob_src = [r for r in tob_src if (_row_dt(r) or now_jst) > since_dt]
            print(f"[TOB] filter by since ({since_dt}): {b} -> {len(tob_src)}")
        tob_rows = tob_src
        print(f"[TOB] tdnet rows (by KW) = {len(tob_rows)}")
    except Exception as e:
        print("[TOB] fetch error:", e)

    # ---- DB保存 ----
    try:
        upsert_offerings_events(conn, offer_items)
    except Exception as e:
        print("[offerings] upsert error:", e)

    try:
        upsert_tob_events(conn, tob_rows)  # ★TOB UPSERT
    except Exception as e:
        print("[TOB] upsert error:", e)

    upsert_earnings_rows_fast(conn, earnings_rows)
    run_light_healthcheck(conn)

    # ---- 出力（JSON/HTML）----
    earnings_for_dash = load_earnings_for_dashboard_from_db(days=30, filter_mode="nonnegative")

    # --- 増資経歴フラグ付与（直近3年） ---
    try:
        offerings_map = load_offerings_recent_map(days=365*3)
    except Exception as e:
        print("[offerings] map build error:", e)
        offerings_map = {}

    try:
        for r in rows:
            code4 = str(r.get("ticker") or "").zfill(4)
            r["has_offering_history"] = bool(offerings_map.get(code4, False))
    except Exception as e:
        print("[offerings] annotate rows error:", e)

    try:
        for r in earnings_for_dash:
            code4 = str(r.get("ticker") or "").zfill(4)
            r["has_offering_history"] = bool(offerings_map.get(code4, False))
    except Exception as e:
        print("[offerings] annotate earnings error:", e)
    print(f"[earnings][dash] from DB last 30d = {len(earnings_for_dash)}")
    
    # --- 掲示板検索：漏れてる？ / TOB(掲示板) ---
    try:
        leak = fetch_leak_comments(DB_PATH, max_pages=60)
    except Exception as e:
        print("[bbs_search][leak] error:", e)
        leak = {"rows": [], "total_comments": 0, "total_tickers": 0, "generated_at": now_iso()}
    # --- 掲示板検索：軽量モード（全銘柄の最新だけサッと見る） ---
    try:
        # 収集を止めた rows の代わりに、大元の symbols を渡す
        tob_bbs = load_tob_from_bbs_lightweight(symbols)
    except Exception as e:
        print("[bbs_search][tob-light] error:", e)
        tob_bbs = {"rows": [], "total_comments": 0, "total_tickers": 0, "generated_at": now_iso()}

    # ▼▼ 追加：TDNETで既にTOBが決まっている銘柄を掲示板TOBタブから除外 ▼▼
    try:
        decided_codes = load_tob_codes_recent(days=180)  # 期間は必要に応じて
        if isinstance(tob_bbs, dict) and "rows" in tob_bbs and isinstance(tob_bbs["rows"], list):
            before = len(tob_bbs["rows"])
            tob_bbs["rows"] = [
                r for r in tob_bbs["rows"]
                if str(r.get("ticker") or "").zfill(4) not in decided_codes
            ]
            # 件数を更新
            tob_bbs["total_tickers"] = len(tob_bbs["rows"])
            print(f"[TOB][filter] decided={len(decided_codes)} codes, rows {before} -> {len(tob_bbs['rows'])}")
    except Exception as e:
        print("[TOB][filter] error:", e)
    # ▲▲ 追加ここまで ▲▲

    # --- ★TDNET TOBのダッシュボード用データ ---
    tob_for_dash = []
    try:
        tob_for_dash = load_tob_for_dashboard_from_db(days=365)
    except Exception as e:
        print("[TOB] load dash error:", e)

    output = {
        "generated_at": now_iso(),
        "rows": [],
        "earnings": earnings_for_dash,
        "leak": leak,
        "tob": tob_bbs,         # 掲示板集計（TDNET決定済みを除外済み）
        "tob_tdnet": tob_for_dash  # ★TDNET由来
    }

    ensure_dir(OUT_DIR); ensure_dir(OUT_DIR / "history")
    ensure_dir(OUT_BBS_DIR); ensure_dir(OUT_BBS_DIR / "history")
    dump_json(output, str(OUT_DIR / "data.json"))
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    dump_json(output, str(OUT_DIR / "history" / f"{ts}.json"))
    dump_json(output, str(OUT_BBS_DIR / "data.json"))
    dump_json(output, str(OUT_BBS_DIR / "history" / f"{ts}.json"))
    print("[write]", (OUT_DIR / "data.json").resolve())

    ensure_dir(DASH_DIR)
    html_out = render_dashboard_html(output, api_base="")
    (DASH_DIR / "index.html").write_text(html_out, encoding="utf-8")
    print("[write]", (DASH_DIR / "index.html").resolve())
    print(f"[OK] {len(rows)} symbols + earnings {len(earnings_for_dash)} (filtered) + TOB(TDNET) {len(tob_for_dash)} → data.json / index.html 更新完了")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            _close_db_conn()
        except Exception:
            pass
