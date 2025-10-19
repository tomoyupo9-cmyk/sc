#!/usr/bin/env python
# -*- coding: utf-8 -*-

# fetch_all.py — TDnetのみで決算取得 + PDF自動サマリ + 良否判定（判定理由をUI表示）
# 日別整理のUI改善を含む完全版

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import io
import re
import html
import requests
import sqlite3

# --- 既存モジュール（環境のまま） ---
from src.trends import fetch_trends
from src.news import fetch_news
from src.bbs import fetch_bbs_stats, fetch_leak_comments, fetch_tob_comments
from src.discovery import build_universe
from src.common import ensure_dir, dump_json, now_iso, load_config

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"
OUT_BBS_DIR = ROOT / "out_bbs"
DASH_DIR = ROOT / "dashboard"
JST = timezone(timedelta(hours=9))
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\kani2.db"

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

# ------------------ TDnet: 取得 ------------------
TDNET_LIST_JSON = "https://webapi.yanoshin.jp/webapi/tdnet/list/{date_from}-{date_to}.json"

# 用途別キーワードに分離
EARNINGS_KW = ["決算","短信","四半期","通期","上方修正","下方修正","業績","配当","進捗"]
# （増資系は既に下の OFFERING_KW が定義済み）


def _unescape_text(s: str) -> str:
    if not s: return ""
    try:
        if r"\u" in s:
            s = s.encode("utf-8").decode("unicode_escape")
    except Exception: pass
    return html.unescape(s).strip()


from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
import time



from urllib.parse import quote

def _http_get_json(url: str, retries: int = 3, sleep_sec: float = 0.8):
    import requests, time
    for i in range(retries):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == retries - 1:
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



def ensure_earnings_schema(conn):
    """
    earnings_events を日本語カラムのみのスキーマに統一。
    主キーは持たず、UNIQUE(コード, 提出時刻) をインデックスで保証。
    """
    cur = conn.cursor()
    # 新規 or 既存でも流用できる形で作成
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
    # 既存列に不足があれば追加（安全策）
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

    # 古い英語系インデックスは念のため削除
    cur.execute("DROP INDEX IF EXISTS idx_earn_time;")
    cur.execute("DROP INDEX IF EXISTS idx_earn_announced;")
    cur.execute("DROP INDEX IF EXISTS idx_earn_code_time;")

    # 日本語カラムに合わせたインデックス（重複防止は UNIQUE）
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
        種別         TEXT,         -- '増資','行使','売出','CB' などのざっくり分類
        参照ID       TEXT,         -- TDnetのidやURL等（あるもの）
        センチメント TEXT,         -- タイトルからの簡易推定
        created_at   TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(コード, 提出時刻, タイトル)
    );
    """)
    # よく使う索引
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_offerings_date ON offerings_events(substr(提出時刻,1,10));""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_offerings_code_date ON offerings_events(コード, substr(提出時刻,1,10));""")
    conn.commit()

def load_offerings_recent_map(db_path: str, days: int = 365*3) -> dict[str, bool]:
    """
    offerings_events から直近days日内に増資/行使/売出/CBなどの履歴があったコード集合を返す。
    """
    out: dict[str, bool] = {}
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # offerings_events が存在しない場合は空
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='offerings_events'")
        if not cur.fetchone():
            conn.close()
            return out
        rows = cur.execute("""
            SELECT DISTINCT
                   CASE
                     WHEN length(コード)=4 THEN コード
                     WHEN length(コード)=5 AND substr(コード,1,1) BETWEEN '0' AND '9' THEN substr(コード,1,4)
                     ELSE printf('%04d', CAST(コード AS INTEGER))
                   END AS code4
            FROM offerings_events
            WHERE date(提出時刻) >= date('now', ?)
        """, (f"-{days} day",)).fetchall()
        conn.close()
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
    """
    TDnet items から「増資/行使/売出/CB 等」だけを抽出し、offerings_events にUPSERT保存。
    """
    import re, json
    cur = conn.cursor()

    def norm_code(it: dict) -> str:
        td = it.get("Tdnet", it) or {}
        raw = (td.get("company_code") or td.get("code") or td.get("company_code_raw") or "").strip()
        if re.fullmatch(r"\d{5}", raw): return raw[:4]
        if re.fullmatch(r"\d{4}", raw): return raw
        return (td.get("ticker") or "").strip()

    def norm_time(s: str) -> str:
        s = (s or "").replace("T"," ").replace("+09:00","").replace("/","-").strip()
        # 例外時は空で返す（NOT NULL列は後で補完）
        return s

    count_upd = 0
    for it in tdnet_items or []:
        td = it.get("Tdnet", it) or {}
        title = (td.get("title") or "").strip()
        if not title: 
            continue
        # キーワードヒット判定
        if not any(kw in title for kw in OFFERING_KW):
            continue

        code  = norm_code(it) or "0000"
        name  = (td.get("company_name") or "").strip() or code
        link  = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        pub   = norm_time(td.get("pubdate") or td.get("publish_datetime") or "")
        tei   = pub or now_iso()  # 提出時刻が空なら現在時刻を入れておく
        kind  = _classify_offering_kind(title)
        refid = td.get("id") or link or title
        # タイトル簡易ルールで sentiment
        _, label, _ = _summarize_title_simple(title)

        # UPDATE（無ければINSERT）— UNIQUE(コード,提出時刻,タイトル)
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


def upsert_earnings_rows(conn: sqlite3.Connection, rows: list[dict]):
    """
    TDnet抽出 rows を日本語スキーマ earnings_events にUPSERT。
    - 取り込み対象：『決算短信／四半期／通期／決算』を含み、『動画／説明資料』は除外
    - キー： (コード, 提出時刻)
    - UPDATE 0件なら INSERT
    """
    import json, re
    cur = conn.cursor()

    # 既存列（存在する列だけ詰めるため）
    cur.execute("PRAGMA table_info(earnings_events);")
    cols = {r[1] for r in cur.fetchall()}

    # ヒット条件
    POS_KEYS = ("決算短信","四半期","通期","決算")
    EXCLUDE  = ("動画","説明資料")

    def norm_code(r: dict) -> str:
        cc = (r.get("company_code") or r.get("company_code_raw") or "").strip()
        if cc.isdigit() and len(cc) == 5:
            return cc[:4]
        # それ以外は ticker を4桁ゼロ埋め
        return str(r.get("ticker") or "").zfill(4)

    for r in rows:
        title = (r.get("title") or "").strip()
        if not any(k in title for k in POS_KEYS):    # 決算系のみ
            continue
        if any(k in title for k in EXCLUDE):         # 動画/説明資料はスキップ
            continue

        code = norm_code(r)
        announced = (r.get("time") or "").replace("T"," ").replace("+09:00","").strip()
        # DBの運用に合わせ「提出時刻」をキーにする。無ければ発表日時で補完
        teishutsu = announced or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 行データ（日本語のみ）
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

        # === UPDATE ===
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

        # === INSERT（なければ） ===
        if updated == 0:
            insert_cols, insert_vals = [], []
            for k, v in jp_vals_all.items():
                if k in cols and v is not None:
                    insert_cols.append(f'"{k}"'); insert_vals.append(v)
            placeholders = ",".join(["?"] * len(insert_cols))
            sql = f'INSERT OR IGNORE INTO earnings_events ({", ".join(insert_cols)}) VALUES ({placeholders})'
            cur.execute(sql, insert_vals)

    conn.commit()



def load_earnings_for_dashboard_from_db(db_path: str, days: int = 30, filter_mode: str = "nonnegative") -> list[dict]:
    """
    HTMLダッシュボード用の 'earnings' を DB から直近N日分で再構築する。
    - filter_mode: "pos_good" | "nonnegative" | "all"
    さらに price_history から 現在値/前日終値/前日比(円) を付与する。
    """
    import json as _json
    conn = sqlite3.connect(db_path)
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

    # フィルタ
    if filter_mode == "pos_good":
        items = [x for x in items if (x.get("sentiment") == "positive") or ((x.get("verdict") or "") == "good")]
    elif filter_mode == "nonnegative":
        items = [x for x in items if (x.get("sentiment") != "negative") or ((x.get("verdict") or "") == "good")]

    # 価格データ付与（price_history から）
    try:
        codes = list({str(x.get("ticker") or "0000").zfill(4) for x in items})
        quotes = load_quotes_from_price_history(conn, codes)
        for x in items:
            q = quotes.get(str(x["ticker"]).zfill(4))
            if q:
                x["quote"] = q
    except Exception as e:
        print("[earnings] quotes attach error:", e)

    conn.close()

    # 並べ替え＆制限
    def _ts(s: str) -> float:
        from datetime import datetime
        try:
            return datetime.strptime((s or "").replace("/", "-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            return 0.0
    items.sort(key=lambda r: _ts(r.get("time","")), reverse=True)
    return items[:300]


# ------------------ HTML生成（日別タブ込み） ------------------
def render_dashboard_html(payload: dict, api_base: str = "") -> str:
    import json
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
  th{background:#f8fafc;color:#334155;font-weight:600;position:sticky;top:0}
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
  .day-head{font-weight:700;margin:18px 0 8px}
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
</style>

<h1>注目度ダッシュボード</h1>
<div class="toolbar">
  <div class="tabs">
    <button class="tab active" data-mode="total">総合</button>
    <button class="tab" data-mode="bbs">掲示板</button>
    <button class="tab" data-mode="earnings">決算</button>
    <button class="tab" data-mode="earnings_day">決算（日別）</button>
    <button class="tab" data-mode="leak">漏れ？</button>
    <button class="tab" data-mode="tob">TOB</button>
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
  let MODE = "total";

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

  function render(j){
    document.getElementById("stamp").textContent = "生成時刻: " + (j.generated_at || "");
    const thead=document.getElementById("thead");
    const tbody=document.getElementById("tbody");
    tbody.innerHTML="";

    if(MODE==="earnings_day"){
      const rows=(j.earnings||[])
        .slice()
        .sort((a,b)=>{const tb=parseJST(b.time),ta=parseJST(a.time);return tb!==ta?tb-ta:(Number(b.score||0)-Number(a.score||0));})
        .slice(0, 300);
      thead.innerHTML=`<tr>
        <th>#</th><th>コード</th><th>銘柄</th><th>増資経歴</th><th>Yahoo</th>
        <th class="num">現在値</th><th class="num">前日終値</th><th class="num">前日比(円)</th>
        <th>スコア</th><th>Sentiment</th><th>書類名 / サマリ / 判定理由</th><th>時刻</th></tr>`;
      if(!rows.length){const tr=document.createElement("tr");tr.innerHTML=`<td colspan="11" class="small">直近では見つかりませんでした。</td>`;tbody.appendChild(tr);return;}
      let day=""; rows.forEach((r,idx)=>{
        const d=(r.time||"").slice(0,10);
        if(d!==day){day=d; const trh=document.createElement("tr"); trh.innerHTML=`<td colspan="11" class="day-head">${esc(day)}</td>`; tbody.appendChild(trh);}
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
        t(String(r.score??0)); t(badge(r.sentiment||"neutral"));
        const titleHtml=r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener">${esc(r.title||"(無題)")}</a>`:esc(r.title||"(無題)");
        const summaryHtml=r.summary?`<div class="small muted" style="margin-top:6px; white-space:pre-line;">${esc(r.summary)}</div>`:"";
        const judgeBlock = judgeHtml(r.verdict||"", r.score_judge, r.reasons||[]);
        t(`${titleHtml}${summaryHtml}${judgeBlock}`);
        t(esc((r.time||"").replace("T"," ").replace("+09:00","")));
        tbody.appendChild(tr);
      });
      return;
    }

    if(MODE==="earnings"){
      const rows=(j.earnings||[])
        .slice()
        .sort((a,b)=>{const tb=parseJST(b.time),ta=parseJST(a.time);return tb!==ta?tb-ta:(Number(b.score||0)-Number(a.score||0));})
        .slice(0, 300);
      thead.innerHTML=`<tr>
        <th>#</th><th>コード</th><th>銘柄</th><th>増資経歴</th><th>Yahoo</th>
        <th class="num">現在値</th><th class="num">前日終値</th><th class="num">前日比(円)</th>
        <th>スコア</th><th>Sentiment</th><th>書類名 / サマリ / 判定理由</th><th>時刻</th></tr>`;
      if(!rows.length){const tr=document.createElement("tr");tr.innerHTML=`<td colspan="11" class="small">直近では見つかりませんでした。</td>`;tbody.appendChild(tr);return;}
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
        t(String(r.score??0)); t(badge(r.sentiment||"neutral"));
        const titleHtml=r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener">${esc(r.title||"(無題)")}</a>`:esc(r.title||"(無題)");
        const summaryHtml=r.summary?`<div class="small muted" style="margin-top:6px; white-space:pre-line;">${esc(r.summary)}</div>`:"";
        const judgeBlock = judgeHtml(r.verdict||"", r.score_judge, r.reasons||[]);
        t(`${titleHtml}${summaryHtml}${judgeBlock}`);
        t(esc((r.time||"").replace("T"," ").replace("+09:00","")));
        tbody.appendChild(tr);
      });
      return;
    }

    if(MODE==="bbs"){
      const rows=(j.rows||[]).slice().sort((a,b)=>{
        const a24=a?.bbs?.posts_24h||0, b24=b?.bbs?.posts_24h||0; if(b24!==a24) return b24-a24;
        const a72=a?.bbs?.posts_72h||0, b72=b?.bbs?.posts_72h||0; if(b72!==a72) return b72-a72;
        return String(a.name||a.ticker).localeCompare(String(b.name||b.ticker));
      });
      thead.innerHTML=`<tr><th>#</th><th>銘柄</th><th>増資経歴</th><th>BBS(24h)</th><th>BBS(72h)</th><th>増加率</th><th>Sentiment(News)</th><th>最新ニュース</th></tr>`;
      rows.forEach((r,idx)=>{
        const tr=document.createElement("tr"); const t=(h)=>{const el=document.createElement("td"); el.innerHTML=h; tr.appendChild(el);};
        const b24=r?.bbs?.posts_24h||0, b72=r?.bbs?.posts_72h||0, growth=(b24/Math.max(1,b72)).toFixed(2);
        const items=(r.news&&r.news.items)?r.news.items:[]; const label=(items[0]?.sentiment)||"neutral";
        t(String(idx+1)); t(`${esc(r.name||r.ticker)} <span class="code">(${esc(r.ticker)})</span>`); t(offeringCell(r.has_offering_history)); t(offeringCell(r.has_offering_history)); t(String(b24)); t(String(b72)); t(String(growth)); t(badge(label)); t(toNewsCards(items));
        tbody.appendChild(tr);
      });
      return;
    }

    // 共通: 検索系（漏れ？ / TOB）レンダラ
    function renderAggRows(agg){
      const rows=(agg?.rows||[]).slice().sort((a,b)=> (b.count||0)-(a.count||0));
      thead.innerHTML = `<tr>
        <th>#</th><th>銘柄</th><th>件数</th><th>サンプル（最大50件）</th>
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
        const samples=(r.comments||[]).slice(0,50).map(c=>{
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

    // 総合
    const rows=(j.rows||[]).slice().sort((a,b)=>(b.score||0)-(a.score||0));
    thead.innerHTML=`<tr><th>#</th><th>銘柄</th><th>増資経歴</th><th>スコア</th><th>Trends</th><th>BBS</th><th>News(24h)</th><th>Sentiment</th><th>最新ニュース</th></tr>`;
    rows.forEach((r,idx)=>{
      const tr=document.createElement("tr"); const t=(h)=>{const el=document.createElement("td"); el.innerHTML=h; tr.appendChild(el);};
      const items=(r.news&&r.news.items)?r.news.items:[]; const label=(items[0]?.sentiment)||"neutral";
      t(String(idx+1)); t(`${esc(r.name||r.ticker)} <span class="code">(${esc(r.ticker)})</span>`); t(fmt(r.score)); t(fmt(r.trends?.latest)); t(String(r.bbs?.posts_24h||0)); t(String(r.news?.count_24h||0)); t(badge(label)); t(toNewsCards(items));
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


import re
from datetime import datetime

def _summarize_title_simple(title: str):
    t = title or ""
    POS = [
        (r"(増配|配当予想の修正.*増配)", "増配", "タイトルに『増配』を検出"),
        (r"(上方修正|上方要因|増額修正|上振れ)", "上方修正", "タイトルに『上方修正/増額』を検出"),
        (r"(公開買付け|公開買い付け|TOB|ＴＯＢ)", "TOB/公開買付け", "TOB/公開買付けを検出"),
    ]
    NEG = [
        (r"(減配|無配|下方修正|下方要因|減額修正|下振れ)", "下方修正/減配", "タイトルに『下方/減配』を検出"),
    ]
    for pat, summ, why in POS:
        if re.search(pat, t): return summ, "positive", why
    for pat, summ, why in NEG:
        if re.search(pat, t): return summ, "negative", why
    if re.search(r"(決算短信|決算補足|決算説明資料|決算概要|決算短信.*訂正)", t):
        return "決算短信/資料", "neutral", "決算資料系（極端なポジ/ネガ語なし）"
    if re.search(r"(配当予想の修正|配当方針|中間配当予定|期末配当予定)", t):
        return "配当関連", "neutral", "配当関連（増配/減配の語は無し）"
    if re.search(r"(業績予想の修正|通期業績予想|四半期業績予想)", t):
        return "業績予想", "neutral", "業績予想（上方/下方の語は無し）"
    return "", "neutral", ""

# ========= PDF 要約＆判定ヘルパー 復活版 =========
import io, re, time, requests
from typing import Dict, Any, List

# 一度の実行で要約する最大件数（負荷対策）
EARNINGS_SUMMARY_MAX = 40

def _download_pdf_bytes(url: str, timeout: int = 25) -> bytes:
    """
    PDFをダウンロード。Yanoshin→TDnet→PDF直リンクの3xxを辿ることがあるため allow_redirects=True。
    """
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
        r.raise_for_status()
        # 一部で Content-Type が text/html のときもPDFが入っているので content優先
        return r.content or b""
    except Exception as e:
        print(f"[pdf] download error: {e} url={url}")
        return b""

def _extract_text_pdfminer(pdf_bytes: bytes) -> str:
    try:
        from pdfminer.high_level import extract_text
        with io.BytesIO(pdf_bytes) as bio:
            return (extract_text(bio) or "").strip()
    except Exception as e:
        print(f"[pdfminer] extract error: {e}")
        return ""

def _extract_text_pypdf2(pdf_bytes: bytes) -> str:
    try:
        import PyPDF2
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
    """
    pdfminer → PyPDF2 の順で試す。両方ダメなら空文字。
    """
    if not pdf_bytes:
        return ""
    text = _extract_text_pdfminer(pdf_bytes)
    if text and len(text) > 30:
        return text
    text = _extract_text_pypdf2(pdf_bytes)
    return text

def _parse_earnings_metrics(text: str) -> Dict[str, Any]:
    """
    PDF本文からシンプルな指標を抽出（売上高/営業利益/経常利益/純利益/EPS/進捗率）。
    数字は億円/百万円/円などの単位に大雑把対応。見つからなければ空。
    """
    if not text:
        return {"metrics": {}, "progress": None}

    def _num(s):
        # カンマ/全角/円や%除去→float
        s = s.replace(",", "").replace("，", "").replace("％", "%").replace("▲", "-").replace("△", "-")
        s = re.sub(r"[^\d\.\-\+%]", "", s)
        try:
            if s.endswith("%"):
                return float(s[:-1])
            return float(s)
        except:
            return None

    # 単位のざっくり補正（百万円→億円換算などは必要に応じて）
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

    # 前後100〜200文字を見てYoY%を拾うヘルパ
    def _scan_yoy_near(pos: int, text: str):
        if pos < 0: return None
        s = text[max(0, pos-120): pos+220]
        s = s.replace('％','%').replace('▲','-').replace('△','-')
        # まず % を拾う
        m = re.search(r'([\-+]?\d+(?:\.\d+)?)\s*%', s)
        if not m: return None
        try:
            val = float(m.group(1))
        except Exception:
            return None
        # 近傍に「減/悪化」があって明示符号が無ければ負号に
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
                # 単位が「百万円」の場合は “億円” 換算（/100）しておく
                unit = m.group(3) if len(m.groups()) >= 3 else ""
                if key != "EPS" and key != "進捗率" and unit:
                    u = unit
                    if "百万円" in u:
                        val = val / 100.0  # 百万円→億円
                metrics[key] = val

            # YoYを近傍テキストから推定
            try:
                # 指標名の最初の出現位置
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
    """
    重要語を優先して短い要約を作る。無ければ先頭数行を抜粋。
    """
    if not text:
        return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    # 優先語が含まれる行を拾う
    KEY = [
        "上方修正", "下方修正", "通期", "四半期", "増益", "減益", "増収", "減収",
        "進捗率", "配当", "業績予想", "修正"
    ]
    hits = [l for l in lines if any(k in l for k in KEY)]
    base = " / ".join(hits[:4]) if hits else " ".join(lines[:6])
    base = re.sub(r"\s+", " ", base).strip()
    return base[:max_chars]

def _grade_earnings(title: str, text: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    タイトル＋本文＋抽出指標から “good/bad/neutral” とスコア＆根拠を返す。
    - 減益/下方などは強ネガ優先
    - 「黒字」単独は加点しない（黒字“転換”のみ加点）
    - YoY は外れ値（±120%超）は無視し、複数利益項目の減少を強く減点
    """
    reasons: list[str] = []
    score = 0.0

    T = title or ""
    X = text or ""
    met = (parsed or {}).get("metrics", {})
    prog = (parsed or {}).get("progress", None)
    yoy = (parsed or {}).get("yoy", {})

    # ---- ここから個別採点ルール（バンドル加点を廃止）----
    # ルールは「見出し(T)」と「本文(X)」の両方を見る
    def _hit(ws):  # 任意の語が含まれれば True
        return any((w in T) or (w in X) for w in ws)

    # ▼ ポジ要因
    # 上方修正系は +2（通期・上期の“増額/上方”も含む）
    if _hit(["上方修正", "通期上方", "通期増額", "上期予想修正（増額）",
             "業績予想の修正（増額）", "通期予想修正（増額）"]):
        score += 2
        reasons.append("上方修正（増額）で+2")

    # 配当/自社株買いは +1
    if _hit(["増配", "復配", "自社株買い", "配当予想の修正（増額）"]):
        score += 1
        reasons.append("配当増額/復配/自社株買いで+1")

    # 最高益は +1
    if _hit(["最高益", "過去最高益"]):
        score += 1
        reasons.append("最高益で+1")

    # 黒字化は単独+0、ただし“黒字転換”は +1（本文/タイトルに明示）
    if _hit(["黒字転換"]):
        score += 1
        reasons.append("黒字転換で+1")

    # ▼ ネガ要因（強めに減点）
    # 下方修正は -3（強ネガ）
    if _hit(["下方修正", "通期予想修正（減額）"]):
        score -= 3
        reasons.append("下方修正（減額）で-3")

    # 減配は -2
    if _hit(["減配", "配当予想の修正（減額）"]):
        score -= 2
        reasons.append("減配で-2")

    # 特損は -2（“特別損失”など）
    if _hit(["特損", "特別損失"]):
        score -= 2
        reasons.append("特損で-2")
    # ---- 個別採点ここまで ----


    # 進捗率ヒューリスティクス
    if isinstance(prog, (int, float)):
        if prog >= 70:
            score += 1
            reasons.append(f"進捗率{prog:.0f}%（高進捗）")
        elif prog <= 30:
            score -= 1
            reasons.append(f"進捗率{prog:.0f}%（低進捗）")

    # YoY（前年同期比）：外れ値を無視し、利益系の複数マイナスを強く評価
    def _sane(y):
        return y if isinstance(y, (int, float)) and -120.0 <= y <= 120.0 else None

    neg_profit_yoy = 0
    pos_profit_yoy = 0
    for k in ("売上高", "営業利益", "経常利益", "純利益"):
        y = _sane(yoy.get(k))
        if y is None:
            continue
        if k in ("営業利益", "経常利益", "純利益"):
            if y <= -5:
                neg_profit_yoy += 1
            if y >= 1:
                pos_profit_yoy += 1
        # 個別の加点減点（緩め）
        if y >= 5:
            score += 1
            reasons.append(f"{k}YoY+{y:.1f}%")
        elif y >= 1:
            score += 0.5
            reasons.append(f"{k}YoY+{y:.1f}%")
        elif y <= -5:
            score -= 1
            reasons.append(f"{k}YoY{y:.1f}%")
        elif y <= -1:
            score -= 0.5
            reasons.append(f"{k}YoY{y:.1f}%")

    if neg_profit_yoy >= 2:
        score -= 1.0
        reasons.append("利益YoYが複数項目で減少（総じて減益傾向）")

    # 指標の符号：黒字は弱加点。YoYがマイナスのときは黒字加点を無効化
    for k in ("営業利益", "経常利益", "純利益"):
        v = met.get(k, None)
        if isinstance(v, (int, float)):
            if v > 0:
                if pos_profit_yoy > 0:  # 増益の裏付けがあるときのみ微加点
                    score += 0.3
                    reasons.append(f"{k}が黒字（増益傾向と整合）")
                else:
                    reasons.append(f"{k}が黒字（単独要素のため加点なし）")
            if v < 0:
                score -= 0.5
                reasons.append(f"{k}が赤字")

    # 「黒字」単独ワードは無視、黒字転換のみ別途ポジティブ（既にPOSで加点済み）

    # 最終判定のしきい値をやや広げて誤判定を減らす
    verdict = "good" if score >= 1.5 else ("bad" if score <= -1.5 else "neutral")
    return {"verdict": verdict, "score": score, "reasons": reasons}

# ========= ここまでヘルパー =========
def fetch_tdnet_by_keywords(
    days: int = 90,
    keywords: list[str] | None = None,
    per_day_limit: int = 300,
    slice_escalation=(12, 6, 3, 1, 0.5)
) -> List[Dict[str, Any]]:
    """
    Yanoshin TDnet 一覧APIを使って、与えたキーワード（OR条件）でタイトルをフィルタして取得。
    - list_time / list の時間範囲フォーマットが通れば段階スライス、ダメなら日単位+追加ページ探査。
    - keywords が None/空なら『フィルタなし（全件）』で返す。
    """
    import time
    from datetime import datetime, timedelta

    time_capable = False
    KEY_RE_LOCAL = re.compile("|".join(map(re.escape, keywords or []))) if (keywords and len(keywords)>0) else None

    def _fetch_tdnet_by_range(dt_from: datetime, dt_to: datetime) -> list[dict]:
        nonlocal time_capable
        s_day  = dt_from.strftime("%Y%m%d"); e_day  = dt_to.strftime("%Y%m%d")
        s_isoT = dt_from.strftime("%Y-%m-%dT%H:%M:%S"); e_isoT = dt_to.strftime("%Y-%m-%dT%H:%M:%S")
        s_nosep= dt_from.strftime("%Y%m%d%H%M%S");      e_nosep= dt_to.strftime("%Y%m%d%H%M%S")
        s_spc  = dt_from.strftime("%Y%m%d %H:%M:%S");   e_spc  = dt_to.strftime("%Y%m%d %H:%M:%S")

        variants = [
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_nosep}-{e_nosep}.json", "list/nosep", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_isoT}-{e_isoT}.json",   "list/isoT",  True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{quote(s_spc)}-{quote(e_spc)}.json", "list/space", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list_time/{s_nosep}-{e_nosep}.json", "list_time/nosep", True),
            (f"https://webapi.yanoshin.jp/webapi/tdnet/list/{s_day}-{e_day}.json",     "list/day",  False),
        ]
        for url, tag, is_time in variants:
            js = _http_get_json(url)
            items = js.get("items") if isinstance(js, dict) else (js if isinstance(js, list) else None)
            if isinstance(items, list):
                if is_time: time_capable = True
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
            if (dt_from.hour, dt_from.minute, dt_from.second) == (0,0,0) and (dt_to.hour, dt_to.minute, dt_to.second) == (23,59,59) and dt_from.date()==dt_to.date():
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
            # ← キーワード指定があるときだけフィルタ。無指定なら全件通す
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
    """
    fetch_earnings_tdnet_only() の返り値（Tdnet配列）→ ダッシュボードが読む行形式へ整形
    + PDF本文の要約/判定を復活（最大 EARNINGS_SUMMARY_MAX 件）
    """
    rows = []
    summarized = 0

    for it in tdnet_items:
        td = it.get("Tdnet", it) or {}

        title = (td.get("title") or "").strip()
        name  = (td.get("company_name") or "").strip()
        code_raw = str(td.get("company_code") or td.get("code") or "").strip()
        # 4桁コードに丸め
        if re.fullmatch(r"\d{5}", code_raw):
            ticker = code_raw[:4]
        elif re.fullmatch(r"\d{4}", code_raw):
            ticker = code_raw
        else:
            ticker = ""

        link = (td.get("document_url") or td.get("pdf_url") or td.get("url") or "").strip()
        time_str = (td.get("pubdate") or td.get("publish_datetime") or "").replace("T"," ").replace("+09:00","")

        # 既存の簡易summary/reason（タイトルルール）を尊重しつつ、
        # summary の有無に関係なくタイトルから sentiment を常に推定して使う
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

        # PDF本文の要約＆判定（上限 EARNINGS_SUMMARY_MAX 件）
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

                        # 本文要約を優先（タイトル要約が空/弱い場合上書き）
                        sum2 = _summarize_earnings_text(text)
                        if sum2:
                            summary = sum2

                        # 良否判定
                        judge = _grade_earnings(title, text, parsed)
                        verdict = judge["verdict"]
                        score_judge = judge["score"]
                        reasons = judge["reasons"]

                        # センチメント推定を verdict で上書き（good/bad のときだけ）
                        if verdict == "good":
                            label_guess = "positive"
                        elif verdict == "bad":
                            label_guess = "negative"

                        summarized += 1
                # 軽いクールダウン（TDnet側/中継CDNの優しさ）
                time.sleep(0.15)
            except Exception as e:
                print(f"[earnings] PDF summarize error: {e}")

        # reasons は配列
        if isinstance(reasons, str) and reasons:
            reasons = [reasons]
        if reasons is None:
            reasons = ([reason] if reason else [])

        # sentiment は既存があれば優先。なければタイトル/判定からの推定を反映
        sentiment = td.get("sentiment") or label_guess or "neutral"

        row = {
            "ticker": ticker or "0000",
            "name":   name or (ticker or "TDnet"),
            "score":  0,
            "sentiment": sentiment,          # 'positive' | 'negative' | 'neutral'
            "title": title,
            "link":  link,
            "time":  time_str or "",
            "summary": summary,              # ← フロントが読む
            "reasons": reasons,              # ← フロントが読む（配列）
            "verdict": verdict,              # 'good'/'bad'/…
            "score_judge": score_judge,      # 数値 or None
            "metrics": metrics or {},        # 解析した数値
            "progress": progress,            # 進捗率(%)
        }
        rows.append(row)

    # 時刻降順に整列（保険）
    def _ts(s):
        try:
            return datetime.strptime((s or "").replace("/","-"), "%Y-%m-%d %H:%M:%S").timestamp()
        except:
            return 0
    rows.sort(key=lambda r: _ts(r["time"]), reverse=True)
    return rows


# ------------------ スコア計算（総合/掲示板） ------------------
def build_scores(rows, weights):
    """
    総合スコア:
      trends.latest をそのまま
      bbs_growth = posts_24h / max(1, posts_72h)
      news = count_24h
      score = w_trends * trends + w_bbs_growth * bbs_growth + w_news * news
    """
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
    """
    掲示板用の単純スコア（増加率 = posts_24h / max(1, posts_72h)）
    """
    out = []
    for r in rows:
        b24 = int((r.get("bbs") or {}).get("posts_24h", 0) or 0)
        b72 = int((r.get("bbs") or {}).get("posts_72h", 0) or 0)
        out.append(b24 / max(1, b72))
    return out


# ------------------ main ------------------
# ==== ここから main() 完全置き換え ====

def _table_exists(conn, name: str) -> bool:
    try:
        cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False

def ensure_misc_objects(conn):
    """
    追加インデックス / トリガ / ビューの作成（存在チェック付き）
    - signals_log(種別, DATE(日時)), signals_log(コード, DATE(日時))
    - earnings_events(DATE(提出時刻)), earnings_events(コード, DATE(提出時刻))
    - price_history.date の YYYY-MM-DD 正規化トリガ
    - v_events_dedup（同コード×同日で最新の提出時刻を採用）
    """
    cur = conn.cursor()

    # --- signals_log の式インデックス（テーブルがある場合のみ） ---
    if _table_exists(conn, "signals_log"):
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_log_type_date
            ON signals_log(種別, substr(日時,1,10));
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_log_code_date
            ON signals_log(コード, substr(日時,1,10));
        """)

    # --- earnings_events の日付系インデックス ---
    if _table_exists(conn, "earnings_events"):
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earn_date_only
            ON earnings_events(substr(提出時刻,1,10));
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earn_code_date
            ON earnings_events(コード, substr(提出時刻,1,10));
        """)

        # --- price_history: 「日付」or「date」を自動判定して正規化トリガ（YYYY-MM-DD） ---
    if _table_exists(conn, "price_history"):
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(price_history)").fetchall()]
            price_date_col = "日付" if "日付" in cols else ("date" if "date" in cols else None)
        except Exception:
            price_date_col = None

        if price_date_col:
            # 旧トリガ（列名固定）を落として作り直し
            cur.execute("DROP TRIGGER IF EXISTS trg_price_history_date_norm_ins;")
            cur.execute("DROP TRIGGER IF EXISTS trg_price_history_date_norm_upd;")

            # INSERT 時の正規化：'/'や'.'を'-'に直し、date()でYYYY-MM-DD化（ダメなら先頭10文字）
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

            # UPDATE 時の正規化
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

    # --- v_events_dedup: 同コード×同日で最新の提出時刻のみ ---
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
    """
    軽量ヘルスチェック：
      - price_history の最新日付と coverage（該当日の distinct(コード) 件数）
      - earnings_events の最新提出時刻
    signals_log がある場合は 'healthcheck' として詳細JSONを書き込み
    """
    result = {"ok": True}
    try:
        # 置き換え後
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
            from datetime import datetime, timezone, timedelta
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
    """
    price_history の列名を動的検出する。
    - 日付: 「日付」優先、なければ「date」
    - コード: 「コード」優先、なければ「code」
    - 終値: 候補のうち最初に見つかった列（'終値', '終値調整後', 'close', 'Close' など）
    """
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
    """
    price_history から各コードの 最新終値 と 直前終値 を取り出し、
    {"2792": {"current": 1234.0, "prev_close": 1200.0, "diff_yen": 34.0}, ...}
    を返す。列名は動的検出。
    """
    out: dict[str, dict] = {}
    if not codes:
        return out

    date_col, code_col, price_col = _detect_price_cols(conn)
    if not all([date_col, code_col, price_col]):
        # 必要列が見つからない場合は空で返す
        return out

    qmarks = ",".join("?" for _ in codes)
    cur = conn.cursor()

    # 各コードの最新日付を取る
    latest = dict(cur.execute(
        f'SELECT "{code_col}", MAX("{date_col}") '
        f'FROM price_history WHERE "{code_col}" IN ({qmarks}) GROUP BY "{code_col}"',
        codes
    ).fetchall())

    # 最新日付とその前日（2件）を取得して current / prev を決める
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



def main():
    cfg = load_config(str(ROOT / "config.yaml"))

    # デバッグオプション（既存のキーを尊重）
    include_dbg = bool((cfg.get("debug", {}) or {}).get("sentiment", False))
    include_bbs_samples = bool((cfg.get("debug", {}) or {}).get("bbs_samples", False))
    bbs_max_samples = int((cfg.get("bbs", {}) or {}).get("max_samples", 20))

    # ユニバース作成
    symbols = build_universe(cfg)

    # ---- 総合/掲示板タブ用 rows を構築（ニュース/掲示板/トレンド収集 → スコア付与）----
    rows = []
    for sym in symbols:
        ticker = sym.get("ticker")
        aliases = sym.get("aliases", [])
        yf_code = sym.get("bbs", {}).get("yahoo_finance_code", ticker)

        trends = fetch_trends(aliases, timeframe="now 7-d", geo="JP")
        news = fetch_news(
            sym.get("news_query"),
            max_items=cfg.get("news", {}).get("max_items_per_symbol", 30),
            include_debug=include_dbg,
        )
        bbs = fetch_bbs_stats(
            yf_code,
            look_threads=cfg.get("bbs", {}).get("look_threads", 50),
            include_samples=include_bbs_samples,
            max_samples=bbs_max_samples,
        )

        if include_dbg:
            pos = sum(1 for it in news["items"] if it.get("sentiment") == "positive")
            neg = sum(1 for it in news["items"] if it.get("sentiment") == "negative")
            neu = sum(1 for it in news["items"] if it.get("sentiment") == "neutral")
            print(f"[{ticker}] news pos/neu/neg = {pos}/{neu}/{neg} (24h={news['count_24h']})")

        rows.append({
            "ticker": ticker,
            "name": sym.get("name"),
            "trends": trends,
            "news": news,
            "bbs": bbs,
        })

    # 既存ロジックでスコア付与（総合タブ＆掲示板タブ）
    scores = build_scores(rows, cfg.get("weights", {"trends": 1.0, "bbs_growth": 1.0, "news": 1.0}))
    for i, s in enumerate(scores):
        rows[i]["score"] = s
    bbs_scores = build_bbs_scores(rows)
    for i, s in enumerate(bbs_scores):
        rows[i]["score_bbs"] = s

    # ---- 決算タブ（TDnetを日割りで取得 → 行形式へ整形）----
    try:
        # まず 3日分（日割り 1日=最大300件で確実に拾う）
        tdnet_items = fetch_earnings_tdnet_only(days=3, per_day_limit=300)
        print(f"[earnings] raw tdnet items = {len(tdnet_items)}")

        # 行形式へ整形（ここで summary/reasons/verdict/metrics を詰める）
        earnings_rows = tdnet_items_to_earnings_rows(tdnet_items)
        print(f"[earnings] rows after shaping = {len(earnings_rows)}")

        # デバッグ：直近10件のタイトルだけ出す
        for it in earnings_rows[:10]:
            print("  -", it.get("time",""), it.get("ticker",""), it.get("title","")[:60])

        # 万一0件なら、フォールバックで直近3日を再取得（API不調対策）
        if not earnings_rows:
            print("[earnings][fallback] 直近7日で再取得します…")
            tdnet_items_fallback = fetch_earnings_tdnet_only(days=7, per_day_limit=300)
            print(f"[earnings][fallback] raw items = {len(tdnet_items_fallback)}")
            earnings_rows = tdnet_items_to_earnings_rows(tdnet_items_fallback)
            print(f"[earnings][fallback] shaped rows = {len(earnings_rows)}")

        # さらに0件なら、原因切り分け用に raw をファイルへ
        if not earnings_rows:
            
            ensure_dir(OUT_DIR)
            dump_json(tdnet_items, str(OUT_DIR / "tdnet_raw_debug.json"))
            print("[earnings][debug] tdnet_raw_debug.json を出力しました（API応答の中身を確認できます）")

    except Exception as e:
        print("[earnings] 取得/整形で例外:", e)
        earnings_rows = []
        
    # ---- 増資レーン：決算とは別に取りに行く ----
    offer_items = []
    try:
        offer_items = fetch_tdnet_by_keywords(days=7, keywords=OFFERING_KW, per_day_limit=300)
        print(f"[offerings] raw tdnet items (by KW) = {len(offer_items)}")
    except Exception as e:
        print("[offerings] fetch error:", e)


    # ---- DB保存（日本語スキーマ earnings_events）----
    conn = sqlite3.connect(DB_PATH)
    ensure_earnings_schema(conn)
    ensure_offerings_schema(conn)
    ensure_misc_objects(conn)
    # 増資/行使/売出は “増資レーン”の raw のみを保存
    try:
        upsert_offerings_events(conn, offer_items)
    except Exception as e:
        print("[offerings] upsert error:", e)

    
    
    upsert_earnings_rows(conn, earnings_rows)
    run_light_healthcheck(conn)
    conn.close()

    # ---- 出力（JSON/HTML）----
    # 表示用の決算データは DB から直近30日分（pos/goodのみ）を再構築
    earnings_for_dash = load_earnings_for_dashboard_from_db(DB_PATH, days=30, filter_mode="nonnegative")

    # --- 増資経歴フラグ付与（直近3年） ---
    try:
        offerings_map = load_offerings_recent_map(DB_PATH, days=365*3)
    except Exception as e:
        print("[offerings] map build error:", e)
        offerings_map = {}

    # 総合/掲示板タブ rows（ユニバース）へ付与
    try:
        for r in rows:
            code4 = str(r.get("ticker") or "").zfill(4)
            r["has_offering_history"] = bool(offerings_map.get(code4, False))
    except Exception as e:
        print("[offerings] annotate rows error:", e)

    # 決算タブ earnings へ付与
    try:
        for r in earnings_for_dash:
            code4 = str(r.get("ticker") or "").zfill(4)
            r["has_offering_history"] = bool(offerings_map.get(code4, False))
    except Exception as e:
        print("[offerings] annotate earnings error:", e)
    print(f"[earnings][dash] from DB last 30d = {len(earnings_for_dash)}")
    
    # --- 掲示板検索：漏れてる？ / TOB ---
    try:
        leak = fetch_leak_comments(DB_PATH, max_pages=60)
    except Exception as e:
        print("[bbs_search][leak] error:", e)
        leak = {"rows": [], "total_comments": 0, "total_tickers": 0, "generated_at": now_iso()}
    try:
        tob = fetch_tob_comments(DB_PATH, max_pages=60)
    except Exception as e:
        print("[bbs_search][tob] error:", e)
        tob = {"rows": [], "total_comments": 0, "total_tickers": 0, "generated_at": now_iso()}
    output = {
        "generated_at": now_iso(),
        "rows": rows,                    # 総合/掲示板用（元のまま）
        "earnings": earnings_for_dash,   # 決算タブ／日別タブ用：positive のみ・最大300件
        "leak": leak,
        "tob": tob
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
    print(f"[OK] {len(rows)} symbols + earnings {len(earnings_for_dash)} (filtered) → data.json / index.html 更新完了")

    
    
# ==== ここまで main() 完全置き換え ====

if __name__=="__main__":
    main()
