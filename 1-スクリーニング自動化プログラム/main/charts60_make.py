# -*- coding: utf-8 -*-
"""
charts60_make_v3.py — ローソク足＋出来高＋MA/BB/一目雲(+26) + クロスヘア＆ツールチップ
- 上げ足=赤/その他=緑（切替可）
- 雲はピンク帯 or A>B緑/A<B赤を選択式
- 三役好転は“完成日のみ”マーク（◆）
- GC（MA5↑MA25クロス）は▲マーク
- 右下ブラシで範囲ズーム（枠内ドラッグ=移動／左右端ドラッグ=拡大縮小／最小本数制約）
- マウスホバーでクロスヘア＆ツールチップ（open/close/low/high、MA5/25/75、BB0/±1/±2、SpanA/B）
- 【追加】凡例バー（▲=GC、◆=三役好転）とツールチップへのマーク説明
"""

from __future__ import annotations
import sqlite3, math, json, argparse, sys, os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Iterable

# ===== 設定（既定値。CLIで上書き可） =====
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
OUT_DIR = Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data\charts60")
JST = timezone(timedelta(hours=9))

# ===== スタイル集中管理 =====
STYLE = {
    # ローソク足
    "candle_up_is": "green",      # "green"（上げ=緑） or "red"（上げ=赤）
    "candle_body_w_ratio": 0.55,  # 実体の太さ(横幅比)

    # 出来高
    "volume_mode": "match_candle",  # "single_green" or "match_candle"

    # 雲の塗り
    "cloud_mode": "twotone",         # "pink"（帯をピンク） or "twotone"（A>B緑/A<B赤）

    # カラー
    "col_red":  "#ef4444",
    "col_red_wick": "#b91c1c",
    "col_green":"#16a34a",
    "col_green_wick":"#166534",
    "col_grid": "#f1f5f9",
    "col_axis": "#6b7280",
    "bb": "#9ca3af",
    "ma5": "#1f4aff", "ma25": "#16a34a", "ma75": "#b45309",
    "cloud_pink": "rgba(236,72,153,0.12)",
    "vol_ma": "#eab308",  # 出来高移動平均線（20日）の色（黄色）

    # クロスヘア/ツールチップ
    "crosshair": "#94a3b8",
    "tip_bg": "rgba(255,255,255,0.97)",
    "tip_border": "#e5e7eb",
    "tip_shadow": "0 4px 14px rgba(0,0,0,0.08)"
}

# ===== ユーティリティ =====
def jst_today_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d")

def jst_now_iso() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def safe_float(x) -> Optional[float]:
    try:
        if x is None: return None
        f=float(str(x).replace(',',''))
        return f if math.isfinite(f) else None
    except Exception:
        return None

def sma(arr: List[Optional[float]], n: int) -> List[Optional[float]]:
    out=[None]*len(arr); s=0.0; q=[]; fill=0
    for i,v in enumerate(arr):
        q.append(v)
        if v is not None: s+=v; fill+=1
        if len(q)>n:
            x=q.pop(0)
            if x is not None: s-=x; fill-=1
        out[i]=(s/n) if len(q)==n and fill==n else None
    return out

def rolling_std(arr: List[Optional[float]], n: int) -> List[Optional[float]]:
    out=[None]*len(arr); q=[]
    for i,v in enumerate(arr):
        q.append(v)
        if len(q)>n: q.pop(0)
        vals=[x for x in q if x is not None]
        if len(vals)==n:
            m=sum(vals)/n
            var=sum((x-m)*(x-m) for x in vals)/n
            out[i]=math.sqrt(var)
        else:
            out[i]=None
    return out

def ichimoku(highs, lows, closes):
    n1,n2,n3=9,26,52
    L=len(closes)
    tenkan=[None]*L; kijun=[None]*L; span_a=[None]*L; span_b=[None]*L
    for i in range(L):
        if i+1>=n1 and all(v is not None for v in highs[i-n1+1:i+1]+lows[i-n1+1:i+1]):
            hi=max(highs[i-n1+1:i+1]); lo=min(lows[i-n1+1:i+1]); tenkan[i]=(hi+lo)/2.0
        if i+1>=n2 and all(v is not None for v in highs[i-n2+1:i+1]+lows[i-n2+1:i+1]):
            hi=max(highs[i-n2+1:i+1]); lo=min(lows[i-n2+1:i+1]); kijun[i]=(hi+lo)/2.0
        if tenkan[i] is not None and kijun[i] is not None:
            span_a[i]=(tenkan[i]+kijun[i])/2.0
        if i+1>=n3 and all(v is not None for v in highs[i-n3+1:i+1]+lows[i-n3+1:i+1]):
            hi=max(highs[i-n3+1:i+1]); lo=min(lows[i-n3+1:i+1]); span_b[i]=(hi+lo)/2.0
    def chikou_bullish(idx:int)->Optional[bool]:
        past=idx-26
        if past<0: return None
        c=closes[idx]; pc=closes[past]
        if c is None or pc is None: return None
        return c>pc
    return tenkan,kijun,span_a,span_b,chikou_bullish

# ===== DB I/O =====
def connect_db(db_path:str)->sqlite3.Connection:
    conn=sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")
    return conn

def ensure_flags_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "chart_flags" (
          "コード"       TEXT PRIMARY KEY,
          "銘柄名"       TEXT,
          "GCフラグ"     INTEGER,
          "三役好転フラグ" INTEGER,
          "ボリバンm2"   INTEGER,
          "ボリバンm1"   INTEGER,
          "ボリバン0"    INTEGER,
          "ボリバンp1"   INTEGER,
          "ボリバンp2"   INTEGER,
          "5日線上"      INTEGER,
          "25日線上"     INTEGER,
          "75日線上"     INTEGER,
          "作成日時"     TEXT NOT NULL
        )
        """
    )

def load_codes_and_names(cur: sqlite3.Cursor, only_codes: Optional[Iterable[str]]=None):
    if only_codes:
        codes = tuple({str(c).strip() for c in only_codes if str(c).strip()})
        if len(codes)==1:
            cur.execute(
                """
                SELECT p.コード, COALESCE(s.銘柄名, NULL)
                FROM (SELECT DISTINCT コード FROM price_history WHERE コード=?) p
                LEFT JOIN screener s ON s.コード = p.コード
                """,
                (codes[0],)
            )
        else:
            placeholders=",".join(["?"]*len(codes))
            cur.execute(
                f"""
                SELECT p.コード, COALESCE(s.銘柄名, NULL)
                FROM (SELECT DISTINCT コード FROM price_history WHERE コード IN ({placeholders})) p
                LEFT JOIN screener s ON s.コード = p.コード
                ORDER BY p.コード
                """,
                codes
            )
    else:
        cur.execute(
            """
          SELECT p.コード, COALESCE(s.銘柄名, NULL)
          FROM (SELECT DISTINCT コード FROM price_history) p
          LEFT JOIN screener s ON s.コード = p.コード
          ORDER BY p.コード
        """
        )
    return [(r[0], r[1]) for r in cur.fetchall()]

def fetch_series_with_today(cur: sqlite3.Cursor, code: str):
    cur.execute(
        """
      SELECT 日付, 始値, 高値, 安値, 終値, 出来高
      FROM price_history
      WHERE コード = ?
      ORDER BY 日付 ASC
    """,
        (code,)
    )
    rows=cur.fetchall()
    dates=[r[0] for r in rows]
    opens=[safe_float(r[1]) for r in rows]
    highs=[safe_float(r[2]) for r in rows]
    lows =[safe_float(r[3]) for r in rows]
    closes=[safe_float(r[4]) for r in rows]
    vols=[int(r[5]) if r[5] is not None else 0 for r in rows]
    # 当日補完
    today=jst_today_str()
    need_append = (not dates) or dates[-1] < today
    if need_append:
        price=None
        try:
            cur.execute(
                "SELECT 現値, 終値, 日付 FROM latest_prices WHERE コード=? ORDER BY 日付 DESC LIMIT 1",
                (code,),
            )
            r=cur.fetchone()
            if r and r[2]==today:
                price = safe_float(r[0]) or safe_float(r[1])
        except sqlite3.Error:
            pass
        if price is None:
            try:
                cur.execute("SELECT 現在値 FROM screener WHERE コード=? LIMIT 1",(code,))
                r=cur.fetchone()
                if r: price=safe_float(r[0])
            except sqlite3.Error:
                pass
        if price is not None:
            dates.append(today)
            opens.append(price); highs.append(price); lows.append(price); closes.append(price)
            vols.append(0)
    return dates, opens, highs, lows, closes, vols

# ===== 指標・フラグ =====
# ===== 指標・フラグ =====
def compute_indicators(opens, highs, lows, closes, vols=None):
    sma5  = sma(closes,5)
    sma25 = sma(closes,25)
    sma75 = sma(closes,75)
    sma20 = sma(closes,20)
    sd20  = rolling_std(closes,20)
    up1=[(m+1.0*s) if (m is not None and s is not None) else None for m,s in zip(sma20,sd20)]
    lo1=[(m-1.0*s) if (m is not None and s is not None) else None for m,s in zip(sma20,sd20)]
    up2=[(m+2.0*s) if (m is not None and s is not None) else None for m,s in zip(sma20,sd20)]
    lo2=[(m-2.0*s) if (m is not None and s is not None) else None for m,s in zip(sma20,sd20)]
    tenkan,kijun,span_a,span_b,chikou_bullish=ichimoku(highs,lows,closes)
    # 先行Spanは+26日へシフト（未来）
    shift=26
    span_a_f=[None]*(len(span_a)+shift)
    span_b_f=[None]*(len(span_b)+shift)
    for i,val in enumerate(span_a):
        j=i+shift
        if j<len(span_a_f): span_a_f[j]=val
    for i,val in enumerate(span_b):
        j=i+shift
        if j<len(span_b_f): span_b_f[j]=val

    # 出来高の移動平均（25日）
    if vols is not None:
        vma25 = sma(vols, 25)
    else:
        vma25 = [None] * len(closes)

    return dict(
        sma5=sma5,sma25=sma25,sma75=sma75,
        bb_mid=sma20, bb_p1=up1, bb_m1=lo1, bb_p2=up2, bb_m2=lo2,
        tenkan=tenkan,kijun=kijun,span_a=span_a_f,span_b=span_b_f,
        chikou_bullish=chikou_bullish,
        vma25=vma25,
    )


def compute_event_marks_and_flags(dates, opens, highs, lows, closes, ind):
    L=len(dates)
    sma5,sma25,sma75=ind["sma5"], ind["sma25"], ind["sma75"]
    bb_mid,bb_p1,bb_m1,bb_p2,bb_m2 = ind["bb_mid"], ind["bb_p1"], ind["bb_m1"], ind["bb_p2"], ind["bb_m2"]
    tenkan,kijun,span_a,span_b,ch_bull = ind["tenkan"], ind["kijun"], ind["span_a"], ind["span_b"], ind["chikou_bullish"]

    marks_gc=[0]*L
    marks_tri=[0]*L

    tri_on_prev = False
    for i in range(1, L):
        # GC
        a0,a1=sma5[i],sma5[i-1]; b0,b1=sma25[i],sma25[i-1]
        if None not in (a0,a1,b0,b1) and a1<=b1 and a0>b0:
            marks_gc[i]=1

        # 三役好転（完成日のみ）
        tk, kj = tenkan[i], kijun[i]
        sa = span_a[i] if i < len(span_a) else None
        sb = span_b[i] if i < len(span_b) else None
        cl = closes[i]
        cond1 = (tk is not None and kj is not None and tk > kj)
        cond2 = (sa is not None and sb is not None and cl is not None and cl > max(sa, sb) and sa > sb)
        cond3 = (ch_bull(i) is True)
        tri_on = (cond1 and cond2 and cond3)
        if tri_on and not tri_on_prev:
            marks_tri[i]=1
        tri_on_prev = tri_on

    # 今日時点のフラグ
    i=L-1
    close=closes[i]
    def above(ma): return None if (ma is None or close is None) else (1 if close>ma else 0)
    def bb_bucket():
        m=bb_mid[i]; u1=bb_p1[i]; l1=bb_m1[i]; u2=bb_p2[i]; l2=bb_m2[i]
        if None in (m,u1,l1,u2,l2,close): return (None,)*5
        if close<=l2: return (1,0,0,0,0)
        elif close<=l1: return (0,1,0,0,0)
        elif close<=m: return (0,0,1,0,0)
        elif close<=u1: return (0,0,0,1,0)
        else: return (0,0,0,0,1)
    b_m2,b_m1,b0,b_p1,b_p2=bb_bucket()

    tk, kj = tenkan[i], kijun[i]
    sa = span_a[i] if i < len(span_a) else None
    sb = span_b[i] if i < len(span_b) else None
    tri_today = (tk is not None and kj is not None and tk>kj
                 and sa is not None and sb is not None and close is not None
                 and close>max(sa,sb) and sa>sb and ch_bull(i) is True)

    flags=dict(
        gc=1 if marks_gc[i] else 0,
        tri=1 if tri_today else 0,
        b_m2=b_m2, b_m1=b_m1, b0=b0, b_p1=b_p1, b_p2=b_p2,
        above5=above(sma5[i]), above25=above(sma25[i]), above75=above(sma75[i])
    )
    return marks_gc,marks_tri,flags

def upsert_flags(conn, code, name, flags):
    conn.execute(
        """
        INSERT INTO "chart_flags"
          ("コード","銘柄名","GCフラグ","三役好転フラグ",
           "ボリバンm2","ボリバンm1","ボリバン0","ボリバンp1","ボリバンp2",
           "5日線上","25日線上","75日線上","作成日時")
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT("コード") DO UPDATE SET
           "銘柄名"=excluded."銘柄名",
           "GCフラグ"=excluded."GCフラグ",
           "三役好転フラグ"=excluded."三役好転フラグ",
           "ボリバンm2"=excluded."ボリバンm2",
           "ボリバンm1"=excluded."ボリバンm1",
           "ボリバン0"=excluded."ボリバン0",
           "ボリバンp1"=excluded."ボリバンp1",
           "ボリバンp2"=excluded."ボリバンp2",
           "5日線上"=excluded."5日線上",
           "25日線上"=excluded."25日線上",
           "75日線上"=excluded."75日線上",
           "作成日時"=excluded."作成日時"
        """,
        (code, name, flags.get("gc"), flags.get("tri"),
         flags.get("b_m2"), flags.get("b_m1"), flags.get("b0"), flags.get("b_p1"), flags.get("b_p2"),
         flags.get("above5"), flags.get("above25"), flags.get("above75"), jst_now_iso())
    )

# ===== HTMLテンプレ（凡例バーとマーク説明を追加） =====
_HTML = r"""<!doctype html>
<meta charset="utf-8">
<title>__CODE__ __NAME__</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;margin:12px;color:#111827}
  header{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
  .chip{background:#f3f4f6;border:1px solid #e5e7eb;border-radius:12px;padding:2px 8px;font-size:12px}
  .wrap{position:relative;border:1px solid #e5e7eb;border-radius:10px;padding:8px}
  #cv{width:100%;height:420px;display:block}
  #cvb{width:100%;height:90px;margin-top:6px;display:block}
  /* ツールチップ */
  .tip{
    position:absolute; top:10px; left:10px; pointer-events:none;
    background: __TIP_BG__; border:1px solid __TIP_BORDER__; border-radius:8px;
    box-shadow: __TIP_SHADOW__; padding:10px 12px; font-size:12px; line-height:1.6; color:#111827; min-width:180px;
  }
  .hidden{display:none}
  .tip .row{display:flex;justify-content:space-between;gap:10px}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}
  .legend{display:flex;align-items:center;gap:6px}
  .muted{color:#6b7280}
  /* 追加：凡例バー */
  .legend-bar{display:flex;align-items:center;gap:16px;margin:4px 0 8px 0;font-size:12px;color:#111827}
  .legend-icon{display:inline-flex;align-items:center;gap:6px}
  .tri{width:0;height:0;border-left:6px solid transparent;border-right:6px solid transparent;border-bottom:10px solid #2563eb}
  .dia{width:10px;height:10px;background:#10b981;transform:rotate(45deg)}
</style>
<header>
  <h1 style="font-size:16px;margin:0">__CODE__ __NAME__</h1>
  <span class="chip">__SPAN_FROM__ ～ __SPAN_TO__（__N__本）</span>
  <span class="chip">生成:__GEN_TIME__</span>
  <span class="chip">GC:__GC__</span>
  <span class="chip">三役好転:__TRI__</span>
  <span class="chip">MA5:__UP5__ / MA25:__UP25__ / MA75:__UP75__</span>
</header>
<div class="legend-bar">
  <span class="legend-icon"><span class="tri"></span><span>GC（MA5↑MA25クロス）</span></span>
  <span class="legend-icon"><span class="dia"></span><span>三役好転（完成日のみ）</span></span>
</div>
<div class="wrap">
  <canvas id="cv"></canvas>
  <div id="tip" class="tip hidden"></div>
  <canvas id="cvb"></canvas>
</div>
<script>
(function(){
  const labels = __LABELS__;
  const O = __OPENS__, Hi = __HIGHS__, Lo = __LOWS__, C = __CLOSES__, V = __VOLS__;
  const IND = __IND__;
  const MARK_GC = __MARK_GC__;
  const MARK_TRI = __MARK_TRI__;
  const main = document.getElementById('cv');
  const brush = document.getElementById('cvb');
  const tip = document.getElementById('tip');
  const ctx = main.getContext('2d');
  const ctxb = brush.getContext('2d');

  // 初期表示範囲
  let view = {a: Math.max(0, labels.length-60), b: labels.length-1};
  // ブラシの選択範囲（インデックス）
  let selA = view.a, selB = view.b;

  // クロスヘア対象
  let hover_i = null;

  // ブラシ操作状態
  let isBrushing = false;       // ドラッグ中はツールチップを隠す
  let dragMode = null;          // 'inside' | 'left' | 'right'
  let dragStartX = 0;           // clientX
  let dragStartSelA = 0, dragStartSelB = 0;

  const MIN_BARS = 20;          // ブラシ最小幅（本数）
  const HANDLE_PX = 8;          // 端のつかみ許容幅(px)

  // ====== ユーティリティ ======
  function clamp(v,min,max){return Math.max(min, Math.min(max, v));}
  function pxX(i, W){ const n=view.b-view.a; return 40 + (W-60) * (i-view.a) / (n||1); }
  function idxFromX(x, W){
    const t = (x-40)/(W-60);
    const i = Math.round(view.a + (view.b-view.a)*t);
    return clamp(i, view.a, view.b);
  }
  function calcYRange(){
    let lo=Infinity, hi=-Infinity;
    for(let i=view.a;i<=view.b;i++){
      const arr=[Lo[i],Hi[i],IND.sma5[i],IND.sma25[i],IND.sma75[i],IND.bb_p2[i],IND.bb_m2[i]];
      for(const v of arr){ if(v!=null && isFinite(v)){ lo=Math.min(lo,v); hi=Math.max(hi,v);} }
      if(IND.span_a[i]!=null){ lo=Math.min(lo,IND.span_a[i]); hi=Math.max(hi,IND.span_a[i]); }
      if(IND.span_b[i]!=null){ lo=Math.min(lo,IND.span_b[i]); hi=Math.max(hi,IND.span_b[i]); }
    }
    if(!isFinite(lo)||!isFinite(hi)){ lo=0; hi=1; }
    if(hi===lo){ hi=lo+1; }
    return {lo,hi};
  }
  function yOf(v,Hc,lo,hi){ return 20 + (Hc-140) * (1 - (v-lo)/(hi-lo)); }
  function yVol(v, yTop, yBottom, vmax){
    const h = Math.max(1, yBottom - yTop);
    return yBottom - h * (v/(vmax||1));
  }
  // 数値整形（小数は最大3桁・末尾0は削除）
  function fmt(v){
    if(v==null || !isFinite(v)) return '';
    const s = Number(v).toFixed(3);
    const s2 = s.replace(/\.?0+$/,'');
    return s2.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  // ====== 雲描画 ======
  function drawCloud(Hc, lo, hi, W){
    const mode = "__CLOUD_MODE__";
    if (mode === "pink"){
      ctx.save(); ctx.fillStyle="__CLOUD_PINK__";
      ctx.beginPath(); let started=false;
      for(let i=view.a;i<=view.b;i++){
        const a=IND.span_a[i], b=IND.span_b[i]; if(a==null||b==null) continue;
        const x=pxX(i,W), ya=yOf(a,Hc,lo,hi);
        if(!started){ ctx.moveTo(x,ya); started=true;} else ctx.lineTo(x,ya);
      }
      for(let i=view.b;i>=view.a;i--){
        const a=IND.span_a[i], b=IND.span_b[i]; if(a==null||b==null) continue;
        const x=pxX(i,W), yb=yOf(b,Hc,lo,hi); ctx.lineTo(x,yb);
      }
      if(started){ ctx.closePath(); ctx.fill(); }
      ctx.restore();
    }else{
      ctx.save(); ctx.globalAlpha = 0.12;
      for(let i=Math.max(view.a,1); i<=view.b; i++){
        const sa0=IND.span_a[i-1], sb0=IND.span_b[i-1], sa1=IND.span_a[i], sb1=IND.span_b[i];
        if(sa0==null||sb0==null||sa1==null||sb1==null) continue;
        const bullish=(sa0>sb0)&&(sa1>sb1);
        ctx.fillStyle = bullish ? "__GREEN__" : "__RED__";
        const x0=pxX(i-1,W), x1=pxX(i,W);
        const ya0=yOf(sa0,Hc,lo,hi), ya1=yOf(sa1,Hc,lo,hi);
        const yb0=yOf(sb0,Hc,lo,hi), yb1=yOf(sb1,Hc,lo,hi);
        ctx.beginPath();
        ctx.moveTo(x0,ya0); ctx.lineTo(x1,ya1); ctx.lineTo(x1,yb1); ctx.lineTo(x0,yb0);
        ctx.closePath(); ctx.fill();
      }
      ctx.restore();
    }
  }

  // ====== メイン描画 ======
  function draw(){
    const W=main.width=main.clientWidth*devicePixelRatio;
    const Hc=main.height=main.clientHeight*devicePixelRatio;
    ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
    ctx.clearRect(0,0,W,Hc);

    // 枠
    ctx.lineWidth=1; ctx.strokeStyle="__GRID__";
    ctx.beginPath(); ctx.moveTo(40,20); ctx.lineTo(40,Hc-120); ctx.lineTo(W-20,Hc-120); ctx.stroke();

    const {lo,hi}=calcYRange();

    // 目盛＆日付
    ctx.fillStyle="__AXIS__"; ctx.font='12px ui-sans-serif,system-ui';
    for(let g=0; g<=4; g++){
      const v=lo+(hi-lo)*g/4, y=yOf(v,Hc,lo,hi);
      ctx.strokeStyle="__GRID__"; ctx.beginPath(); ctx.moveTo(40,y); ctx.lineTo(W-20,y); ctx.stroke();
      ctx.fillText(String(Math.round(v)),5,y+4);
    }
    const n= Math.max(1, view.b-view.a);
    const step=Math.max(1,Math.round(n/6));
    for(let i=view.a;i<=view.b;i+=step){
      const x=pxX(i,W); ctx.fillText(labels[i], x-20, Hc-100);
    }

    // 雲
    drawCloud(Hc, lo, hi, W);

    // 移動平均・BB
    function drawLine(arr,color,dash){
      ctx.strokeStyle=color; ctx.setLineDash(dash||[]);
      ctx.beginPath(); let first=true;
      for(let i=view.a;i<=view.b;i++){
        const v=arr[i]; if(v==null) continue;
        const x=pxX(i,W), y=yOf(v,Hc,lo,hi);
        if(first){ ctx.moveTo(x,y); first=false;} else ctx.lineTo(x,y);
      }
      ctx.stroke(); ctx.setLineDash([]);
    }
    drawLine(IND.bb_p2,"__BB__", [4,4]);
    drawLine(IND.bb_p1,"__BB__", [4,4]);
    drawLine(IND.bb_mid,"__BB__", [4,4]);
    drawLine(IND.bb_m1,"__BB__", [4,4]);
    drawLine(IND.bb_m2,"__BB__", [4,4]);
    drawLine(IND.sma5,"__MA5__");
    drawLine(IND.sma25,"__MA25__");
    drawLine(IND.sma75,"__MA75__");

    // ローソク
    const cw=Math.max(2,(W-80)/Math.max(10,n+1)*__BODYW__);
    for(let i=view.a;i<=view.b;i++){
      const x=pxX(i,W), o=O[i], h=Hi[i], l=Lo[i], c=C[i];
      if([o,h,l,c].some(v=>v==null)) continue;
      const yo=yOf(o,Hc,lo,hi), yh=yOf(h,Hc,lo,hi), yl=yOf(l,Hc,lo,hi), yc=yOf(c,Hc,lo,hi);
      const up = (c>o);
      let body, wick;
      if("__UP_IS__"==="green"){ body = up?"__GREEN__":"__RED__"; wick=up?"__GREENW__":"__REDW__"; }
      else{ body = up?"__RED__":"__GREEN__"; wick=up?"__REDW__":"__GREENW__"; }
      // wick
      ctx.strokeStyle=wick; ctx.beginPath(); ctx.moveTo(x,yl); ctx.lineTo(x,yh); ctx.stroke();
      // body
      const top=Math.min(yo,yc), ht=Math.max(2,Math.abs(yo-yc));
      ctx.fillStyle=body; ctx.fillRect(x-cw/2, top, cw, ht);

      // バッジ
      if(MARK_GC[i]){ ctx.fillStyle='#2563eb'; ctx.beginPath(); ctx.moveTo(x, top-10); ctx.lineTo(x-5, top-2); ctx.lineTo(x+5, top-2); ctx.closePath(); ctx.fill(); }
      if(MARK_TRI[i]){ ctx.fillStyle='#10b981'; ctx.save(); ctx.translate(x, top-6); ctx.rotate(Math.PI/4); ctx.fillRect(-4,-4,8,8); ctx.restore(); }
    }

    // 出来高
    const yTop = Hc - 110, yBottom = Hc - 20;
    ctx.strokeStyle="__GRID__"; ctx.beginPath(); ctx.moveTo(40,yTop); ctx.lineTo(W-20,yTop); ctx.stroke();
    const vmax = Math.max(1, ...V.slice(view.a, view.b+1).map(x => x || 0));
    ctx.save(); ctx.beginPath(); ctx.rect(40, yTop, (W-60), yBottom - yTop); ctx.clip();
    for (let i=view.a; i<=view.b; i++) {
      const x = pxX(i, W), vol = V[i] || 0, y = yBottom - Math.max(1, (yBottom - yTop) * (vol/(vmax || 1)));
      let col;
      if("__VOLMODE__"==="single_green"){ col="__GREEN__"; }
      else { col = ((C[i]||0) > (O[i]||0)) ? "__RED__" : "__GREEN__"; }
      ctx.fillStyle=col; ctx.fillRect(x - cw/2, y, cw, yBottom - y);
    }

    // 出来高移動平均線（25日）
    if (IND.vma25){
      ctx.strokeStyle = "#facc15";      // 黄色の線
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      let firstV = true;
      for (let i=view.a; i<=view.b; i++){
        const vma = IND.vma25[i];
        if (vma == null || !isFinite(vma)) continue;
        const x = pxX(i, W);
        const y = yBottom - Math.max(1, (yBottom - yTop) * (vma/(vmax || 1)));
        if (firstV){ ctx.moveTo(x, y); firstV = false; }
        else       { ctx.lineTo(x, y); }
      }
      ctx.stroke();
    }

    ctx.restore();


    // クロスヘア
    if(hover_i!=null){
      const x = pxX(hover_i, W);
      ctx.strokeStyle="__CROSS__"; ctx.setLineDash([4,4]);
      ctx.beginPath(); ctx.moveTo(x,20); ctx.lineTo(x,Hc-20); ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  // ====== ブラシ ======
  function indexToBrushX(i, W){
    return 40 + (W-60) * (i/(labels.length-1));
  }
  function brushXToIndex(px, W){
    const i = Math.round( (px-40) / (W-60) * (labels.length-1) );
    return Math.max(0, Math.min(labels.length-1, i));
  }
  function brushHitTest(mx, W){
    const aX = indexToBrushX(selA, W);
    const bX = indexToBrushX(selB, W);
    const L = Math.min(aX, bX), R = Math.max(aX, bX);
    if (Math.abs(mx - L) <= 8) return 'left';
    if (Math.abs(mx - R) <= 8) return 'right';
    if (mx >= L && mx <= R) return 'inside';
    return null;
  }

  function drawBrush(){
    const W=brush.width=brush.clientWidth*devicePixelRatio;
    const Hb=brush.height=brush.clientHeight*devicePixelRatio;
    ctxb.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
    ctxb.clearRect(0,0,W,Hb);

    // 全体の終値ライン
    const xs=C.filter(v=>v!=null);
    const cmin=Math.min.apply(null,xs), cmax=Math.max.apply(null,xs);
    function y(v){ return 10 + (Hb/2-20) * (1 - (v-cmin)/(cmax-cmin||1)); }
    ctxb.strokeStyle="__BB__";
    ctxb.beginPath(); let first=true;
    for(let i=0;i<C.length;i++){
      const x=indexToBrushX(i,W); const v=C[i]; if(v==null) continue;
      const yy=y(v); if(first){ ctxb.moveTo(x,yy); first=false;} else ctxb.lineTo(x,yy);
    }
    ctxb.stroke();

    // 選択範囲
    const aX=indexToBrushX(selA,W);
    const bX=indexToBrushX(selB,W);
    const L=Math.min(aX,bX), R=Math.max(aX,bX);

    ctxb.fillStyle='rgba(37,99,235,0.20)';
    ctxb.fillRect(L,0,Math.max(1,R-L),Hb);
    ctxb.strokeStyle='#2563eb';
    ctxb.lineWidth=1;
    ctxb.strokeRect(L+0.5,0.5,Math.max(1,R-L),Hb-1);

    // ハンドル（薄青の縦棒）
    ctxb.fillStyle='rgba(37,99,235,0.55)';
    ctxb.fillRect(L-1.5,0,Hb*0.02,Hb);
    ctxb.fillRect(R-1.5,0,Hb*0.02,Hb);
  }

  // ====== ツールチップ ======
  function updateTip(i, clientX, clientY){
    if(i==null){ tip.classList.add('hidden'); return; }
    const o=O[i],c=C[i],h=Hi[i],l=Lo[i];
    const bb0=IND.bb_mid[i], bbp1=IND.bb_p1[i], bbm1=IND.bb_m1[i], bbp2=IND.bb_p2[i], bbm2=IND.bb_m2[i];
    const ma5=IND.sma5[i], ma25=IND.sma25[i], ma75=IND.sma75[i];
    const sa=IND.span_a[i], sb=IND.span_b[i];

    let lines = [];
    lines.push(`<div class="row"><strong>${labels[i]}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__RED__"></span>日足</span></div>`);
    lines.push(`<div class="row"><span class="muted">open</span><strong>${fmt(o)}</strong></div>`);
    lines.push(`<div class="row"><span class="muted">close</span><strong>${fmt(c)}</strong></div>`);
    lines.push(`<div class="row"><span class="muted">lowest</span><strong>${fmt(l)}</strong></div>`);
    lines.push(`<div class="row"><span class="muted">highest</span><strong>${fmt(h)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__MA5__"></span>MA5</span><strong>${fmt(ma5)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__MA25__"></span>MA25</span><strong>${fmt(ma25)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__MA75__"></span>MA75</span><strong>${fmt(ma75)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__BB__"></span>BB 0</span><strong>${fmt(bb0)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__BB__;border:1px dashed #9ca3af"></span>BB(+1.0)</span><strong>${fmt(bbp1)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__BB__;border:1px dashed #9ca3af"></span>BB(-1.0)</span><strong>${fmt(bbm1)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__BB__"></span>BB(+2.0)</span><strong>${fmt(bbp2)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__BB__"></span>BB(-2.0)</span><strong>${fmt(bbm2)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__GREEN__"></span>雲・SpanA</span><strong>${fmt(sa)}</strong></div>`);
    lines.push(`<div class="row"><span class="legend"><span class="dot" style="background:__RED__"></span>SpanB</span><strong>${fmt(sb)}</strong></div>`);
    // 追加：マークの説明（その日のみ表示）
    if (MARK_GC[i]) {
      lines.push(`<div class="row"><span class="legend"><span class="tri"></span>GC（MA5↑MA25クロス）</span><strong>点灯</strong></div>`);
    }
    if (MARK_TRI[i]) {
      lines.push(`<div class="row"><span class="legend"><span class="dia"></span>三役好転（完成）</span><strong>点灯</strong></div>`);
    }

    tip.innerHTML = lines.join('\n');

    const wrapRect = main.parentElement.getBoundingClientRect();
    const x = Math.min(clientX - wrapRect.left + 12, wrapRect.width - tip.offsetWidth - 12);
    const y = Math.max(10, Math.min(clientY - wrapRect.top + 12, wrapRect.height - tip.offsetHeight - 10));
    tip.style.left = x + 'px';
    tip.style.top  = y + 'px';
    tip.classList.remove('hidden');
  }

  // ====== 入力（ブラシ & ホバー/ズーム） ======
  brush.addEventListener('mousedown',(e)=>{
    const r=brush.getBoundingClientRect();
    const W=brush.clientWidth*devicePixelRatio;
    const mx = (e.clientX-r.left)*devicePixelRatio;

    const dragModeLocal = brushHitTest(mx, W);
    if (!dragModeLocal) return;
    isBrushing = true;
    dragMode = dragModeLocal;
    dragStartX = mx;
    dragStartSelA = selA;
    dragStartSelB = selB;
    e.preventDefault();
  });

  window.addEventListener('mousemove',(e)=>{
    if(!isBrushing) return;
    const r=brush.getBoundingClientRect();
    const W=brush.clientWidth*devicePixelRatio;
    const mx = (e.clientX-r.left)*devicePixelRatio;

    if (dragMode === 'inside'){
      // 範囲そのものを移動
      const span = dragStartSelB - dragStartSelA;
      const dxIndex = brushXToIndex(mx, W) - brushXToIndex(dragStartX, W);
      let a = Math.max(0, Math.min((labels.length-1)-span, dragStartSelA + dxIndex));
      let b = a + span;
      selA=a; selB=b;
    } else if (dragMode === 'left'){
      let a = brushXToIndex(mx, W);
      a = Math.max(0, Math.min(dragStartSelB - 20, a));
      selA = Math.min(a, selB - 20);
    } else if (dragMode === 'right'){
      let b = brushXToIndex(mx, W);
      b = Math.max(dragStartSelA + 20, Math.min(labels.length-1, b));
      selB = Math.max(b, selA + 20);
    }

    // sel -> view に反映
    view.a = Math.min(selA, selB);
    view.b = Math.max(selA, selB);

    draw(); drawBrush();
  });

  window.addEventListener('mouseup',()=>{
    if (!isBrushing) return;
    isBrushing = false;
    dragMode = null;
  });

  // メインキャンバス：クロスヘア＆ツールチップ
  main.addEventListener('mousemove',(e)=>{
    const r=main.getBoundingClientRect();
    const W=main.clientWidth*devicePixelRatio;
    const i = Math.max(view.a, Math.min(view.b, Math.round( ( (e.clientX-r.left)*devicePixelRatio - 40) / (W-60) * (view.b-view.a) + view.a )));
    hover_i = i; draw();
    if (!isBrushing) updateTip(i, e.clientX, e.clientY);
  });
  main.addEventListener('mouseleave', ()=>{
    hover_i = null; draw(); tip.classList.add('hidden');
  });

  // ホイールズーム（中央固定）
  main.addEventListener('wheel',(e)=>{
    e.preventDefault();
    const delta=(e.deltaY>0)?5:-5;
    const mid=Math.round((view.a+view.b)/2);
    const half=Math.max(10, Math.round((view.b-view.a)/2)+delta);
    view.a=Math.max(0, Math.min(labels.length-20, mid-half));
    view.b=Math.max(20, Math.min(labels.length-1, mid+half));

    // ズーム結果をブラシ側へ反映
    selA = view.a; selB = view.b;

    draw(); drawBrush();
  }, {passive:false});

  draw(); drawBrush();
})();
</script>
"""

def to_num_list(seq):
    out=[]
    for v in seq:
        if v is None:
            out.append(None)
        else:
            try:
                fv=float(v)
                if math.isnan(fv): out.append(None)
                else: out.append(fv)
            except Exception:
                out.append(None)
    return out

def make_html(code, name, dates, opens, highs, lows, closes, vols, ind, marks_gc, marks_tri, flags, out_path: Path):
    html = (_HTML
        .replace('__CODE__', code)
        .replace('__NAME__', name or '')
        .replace('__SPAN_FROM__', dates[0] if dates else '')
        .replace('__SPAN_TO__', dates[-1] if dates else '')
        .replace('__N__', str(len(dates)))
        .replace('__GEN_TIME__', datetime.now().strftime('%Y-%m-%d %H:%M'))
        .replace('__GC__', str(flags.get('gc',0)))
        .replace('__TRI__', str(flags.get('tri',0)))
        .replace('__UP5__', str(flags.get('above5',0)))
        .replace('__UP25__', str(flags.get('above25',0)))
        .replace('__UP75__', str(flags.get('above75',0)))
        .replace('__LABELS__', json.dumps(dates, ensure_ascii=False))
        .replace('__OPENS__',  json.dumps(to_num_list(opens)))
        .replace('__HIGHS__',  json.dumps(to_num_list(highs)))
        .replace('__LOWS__',   json.dumps(to_num_list(lows)))
        .replace('__CLOSES__', json.dumps(to_num_list(closes)))
        .replace('__VOLS__',   json.dumps([int(x or 0) for x in vols]))
        .replace('__IND__',    json.dumps({
            'sma5':to_num_list(ind['sma5']),
            'sma25':to_num_list(ind['sma25']),
            'sma75':to_num_list(ind['sma75']),
            'bb_mid':to_num_list(ind['bb_mid']),
            'bb_p1':to_num_list(ind['bb_p1']),
            'bb_m1':to_num_list(ind['bb_m1']),
            'bb_p2':to_num_list(ind['bb_p2']),
            'bb_m2':to_num_list(ind['bb_m2']),
            'span_a':to_num_list(ind['span_a']),
            'span_b':to_num_list(ind['span_b']),
            'vma25':to_num_list(ind['vma25']),
        }))
        .replace('__MARK_GC__',  json.dumps([int(x) for x in marks_gc]))
        .replace('__MARK_TRI__', json.dumps([int(x) for x in marks_tri]))
        # 色・モードの埋め込み
        .replace('__GRID__', STYLE["col_grid"])
        .replace('__AXIS__', STYLE["col_axis"])
        .replace('__BB__', STYLE["bb"])
        .replace('__MA5__', STYLE["ma5"]).replace('__MA25__', STYLE["ma25"]).replace('__MA75__', STYLE["ma75"])
        .replace('__GREEN__', STYLE["col_green"]).replace('__RED__', STYLE["col_red"])
        .replace('__GREENW__', STYLE["col_green_wick"]).replace('__REDW__', STYLE["col_red_wick"])
        .replace('__CROSS__', STYLE["crosshair"])
        .replace('__VOLMODE__', STYLE["volume_mode"])
        .replace('__UP_IS__', STYLE["candle_up_is"])
        .replace('__BODYW__', str(STYLE["candle_body_w_ratio"]))
        .replace('__CLOUD_MODE__', STYLE["cloud_mode"])
        .replace('__CLOUD_PINK__', STYLE["cloud_pink"])
        .replace('__TIP_BG__', STYLE["tip_bg"])
        .replace('__TIP_BORDER__', STYLE["tip_border"])
        .replace('__TIP_SHADOW__', STYLE["tip_shadow"])
    )
    out_path.write_text(html, encoding='utf-8')

# ===== 引数ハンドリング =====
def _split_codes_arg(val:str)->List[str]:
    # カンマ/空白/改行で分割、空要素除去
    raw=[x.strip() for x in val.replace(',', ' ').split()]
    return [x for x in raw if x]

def parse_codes_arg(arg_val: Optional[str])->Optional[List[str]]:
    if not arg_val:
        return None
    s=arg_val.strip()
    if s.startswith('@'):
        path=s[1:]
        if not os.path.isfile(path):
            print(f"[WARN] codesファイルが見つかりません: {path}")
            return []
        with open(path,'r',encoding='utf-8') as f:
            lines=[ln.strip() for ln in f.readlines()]
        # コメント(#〜)と空行を除外
        out=[]
        for ln in lines:
            if not ln or ln.lstrip().startswith('#'):
                continue
            out.extend(_split_codes_arg(ln))
        return out
    else:
        return _split_codes_arg(s)

def parse_args():
    ap=argparse.ArgumentParser(description="charts60 HTML生成（ローソク足＋出来高＋MA/BB/一目）")
    ap.add_argument("--db", default=DB_PATH, help="SQLite DBパス（kani2.db）")
    ap.add_argument("--out", default=str(OUT_DIR), help="出力ディレクトリ")
    ap.add_argument("--codes", default=None,
                    help="生成対象のコード（カンマ/空白区切り or @codes.txt）")
    ap.add_argument("--min-bars", type=int, default=80, help="最低必要本数（既定:80）")
    return ap.parse_args()

def main():
    args=parse_args()
    out_dir=Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    conn=connect_db(args.db)
    try:
        ensure_flags_table(conn)
        cur=conn.cursor()
        only_codes=parse_codes_arg(args.codes)
        pairs=load_codes_and_names(cur, only_codes=only_codes)
        if only_codes and not pairs:
            print("[WARN] 指定コードはいずれも price_history に見つかりませんでした:", only_codes)
        up_cnt=0; make_cnt=0; skipped=0
        with conn:
            for code, name in pairs:
                dates, opens, highs, lows, closes, vols = fetch_series_with_today(cur, code)
                if len(dates) < max(1, getattr(args, 'min_bars', args.min_bars)):
                    skipped+=1
                    continue
                ind = compute_indicators(opens, highs, lows, closes, vols)
                marks_gc, marks_tri, flags = compute_event_marks_and_flags(dates, opens, highs, lows, closes, ind)
                upsert_flags(conn, code, name, flags); up_cnt+=1
                out_path = out_dir / f"{code}.html"
                make_html(code, name, dates, opens, highs, lows, closes, vols, ind, marks_gc, marks_tri, flags, out_path)
                make_cnt+=1
        print(f"[OK] chart_flags upsert: {up_cnt}, HTML作成: {make_cnt}, skip(本数不足など): {skipped}, 出力先: {out_dir}")
    finally:
        conn.close()

if __name__=='__main__':
    main()
