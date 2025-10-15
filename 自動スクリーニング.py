# -*- coding: utf-8 -*-
"""
自動スクリーニング_完全統合版 + 右肩上がり（Template版/両立フィルタ/Gmail/オフラインHTML/祝日対応/MIDDAY自動）

修正点（この版）
- HTML出力フェーズの JSON 生成で、DataFrame 内の bytes / NaN / pandas.Timestamp / NumPy スカラーを
  安全に変換できるように修正（TypeError: bytes is not JSON serializable 対策）

機能ダイジェスト
- EOD/MIDDAY 自動判定（JST 11:30–12:30 は MIDDAY スナップショット、それ以外は EOD）
- 祝日/土日スキップ（jpholiday + 追加休場日ファイル対応）
- yahooquery で quotes / history を一括取得（初回は 12mo、通常は 10d）
- 初動/底打ち/上昇余地スコア/右肩上がりスコア の判定とログ（signals_log）
- 前営業日の翌日検証（判定とCSV出力）
- オフライン1ファイルHTMLダッシュボード（候補一覧/検証/全カラム/price_history/signals_log）
- Gmail で index.html を送信（任意、ZIP同梱可）

前提: Python 3.11 / pip install yahooquery pandas jpholiday
"""

from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader, select_autoescape
from logging.handlers import RotatingFileHandler
from markupsafe import Markup, escape
from pathlib import Path
from typing import Literal
from urllib.parse import quote as _q
from zoneinfo import ZoneInfo
import datetime as dtm
import jpholiday
import json
import logging
import math
import numpy as np
import os
import pandas as pd
import re
import requests
import smtplib
import sqlite3
import ssl
import subprocess
import sys
import time
import warnings
import webbrowser
import yfinance as yf
import zipfile
from yahooquery import Ticker as YQ

# ========= bulk utils (v8 additions) =========
def _chunked(seq, n=500):
    it = iter(seq)
    while True:
        buf = []
        try:
            for _ in range(n):
                buf.append(next(it))
        except StopIteration:
            if buf: 
                yield buf
            break
        yield buf

def exec_many(conn, sql, rows, chunk=500):
    cur = conn.cursor()
    try:
        for part in _chunked(rows, chunk):
            cur.executemany(sql, part)
        conn.commit()
    finally:
        cur.close()
# ========= end bulk utils =========
# ========= price series features (v9 additions) =========
def add_price_features(df):
    """
    価格系列の典型指標をベクトル化で一括付与するテンプレ関数。
    - 前提: df に ['コード','日付','終値','高値','安値','出来高'] がある
    - 出力: 同じ df に各種列を追加して返す（inplaceではない）
    """
    if df.empty:
        return df
    # 防御: 必須列のみで進める
    need = ["コード","日付","終値"]
    for c in need:
        if c not in df.columns:
            return df

    df = df.sort_values(["コード","日付"]).copy()
    grp = df.groupby("コード", sort=False)

    # 移動平均
    if "終値" in df.columns:
        df["終値_ma5"]  = grp["終値"].transform(lambda s: s.rolling(5,  min_periods=1).mean())
        df["終値_ma13"] = grp["終値"].transform(lambda s: s.rolling(13, min_periods=1).mean())
        df["終値_ma20"] = grp["終値"].transform(lambda s: s.rolling(20, min_periods=1).mean())
        df["終値_ma26"] = grp["終値"].transform(lambda s: s.rolling(26, min_periods=1).mean())

    # 出来高平均とRVOL
    if "出来高" in df.columns:
        df["出来高_ma5"]  = grp["出来高"].transform(lambda s: s.rolling(5,  min_periods=1).mean())
        df["出来高_ma20"] = grp["出来高"].transform(lambda s: s.rolling(20, min_periods=1).mean())
        with pd.option_context('mode.use_inf_as_na', True):
            df["RVOL20"] = (df["出来高"] / df["出来高_ma20"]).replace([np.inf, -np.inf], np.nan)

    # ATR風（高値-安値の20MA）
    if "高値" in df.columns and "安値" in df.columns:
        df["_hl"]   = (df["高値"].astype(float) - df["安値"].astype(float))
        df["ATR20"] = grp["_hl"].transform(lambda s: s.rolling(20, min_periods=1).mean())
        df.drop(columns=["_hl"], inplace=True)

    # 変化率（参考: 当日/前日-1）
    try:
        df["終値_pct1"] = grp["終値"].transform(lambda s: s.pct_change(1) * 100.0)
    except Exception:
        pass

    # --- legacy column aliases for backward compatibility ---
    # 一部の処理が "MA13"/"MA26" など旧名を参照するため、終値系MAに別名を付与
    for _new, _legacy in [("終値_ma5","MA5"), ("終値_ma13","MA13"),
                          ("終値_ma20","MA20"), ("終値_ma26","MA26")]:
        if _new in df.columns and _legacy not in df.columns:
            try:
                df[_legacy] = df[_new]
            except Exception:
                # 何か事情で代入に失敗しても他処理を止めない
                pass
    # -------------------------------------------------------

    return df
# ========= end price series features =========



# ===== Constants (centralized) =====
# --- MISC settings ---
# eps
EPS = 0.0
# jst
JST = dtm.timezone(dtm.timedelta(hours=9))
# ===== パラメータ（好みで微調整） =====
LOOKBACK = 90
# yq

# --- KARAURI settings ---
# JS スクリプトと出力ファイルのパス
KARAURI_PY_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\空売り無しリスト出しスクリプト.py"
# 空売り関連の設定
KARAURI_OUTPUT_TXT = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\空売り無しリスト.txt"

# --- RUN settings ---
# ======== 実行モード ========
RUN_SESSION = "EOD"

# --- AUTO settings ---
# 機能フラグ（ON/OFF）
AUTO_MODE = True

# --- GMAIL settings ---
# gmail app password
GMAIL_APP_PASSWORD = "pzunutpfqophuoae"
# gmail body
GMAIL_BODY = "index.html を添付します（オフラインで開けます）。"
# gmail subj
GMAIL_SUBJ = "スクリーニング レポート"
# gmail to
GMAIL_TO = "tomoyupo9@gmail.com"
# ======== 送信設定（Gmail） ========
GMAIL_USER = "tomoyupo9@gmail.com"

# --- SEND settings ---
# send html as zip
SEND_HTML_AS_ZIP = False

# --- DB settings ---
# ======== パス ========
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\kani2.db"

# --- CSV settings ---
# ファイル/ディレクトリのパス
CSV_INPUT_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\screen_data\screener_result.csv"

# --- KARA settings ---
# ファイル/ディレクトリのパス
KARA_URI_NASHI_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\空売り無しリスト.txt"

# --- MASTER settings ---
# ファイル/ディレクトリのパス
MASTER_CODES_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\株コード番号.txt"

# --- OUTPUT settings ---
# ファイル/ディレクトリのパス
OUTPUT_DIR = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\screen_data"

# --- EXTRA settings ---
# ファイル/ディレクトリのパス
EXTRA_CLOSED_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\market_closed_extra.txt"

# --- USE settings ---
# ======== オプション ========
USE_CSV = True

# --- TEST settings ---
# 最大件数/上限
TEST_LIMIT = 50
# 機能フラグ（ON/OFF）
TEST_MODE = False

# --- YQ settings ---
# EOD処理のバッチサイズ
YQ_BATCH_EOD = 400
# 中間処理のバッチサイズ
YQ_BATCH_MID = 400
# yahooquery の設定
YQ_SLEEP_EOD = 0.15
# yahooquery の設定
YQ_SLEEP_MID = 0.10

# --- HISTORY settings ---
# 履歴取得
HISTORY_PERIOD_DAILY = "10d"
# history period full
HISTORY_PERIOD_FULL = "12mo"

# --- SIGNAL settings ---
# シグナル計算に読み込む過去日数（DBから）
SIGNAL_LOOKBACK_DAYS = 300

# --- MIDDAY settings ---
# MIDDAYの対象を絞る（Trueで速い）
MIDDAY_FILTER_BY_FLAGS = False

# --- REJUDGE settings ---
# ===== 外れ→再評価 ルール（遅れて成功を拾う） =====
REJUDGE_LOOKAHEAD_DAYS = 5
# 割合（0〜1想定）
REJUDGE_MAX_ADVERSE_PCT = 7.0
# 割合（0〜1想定）
REJUDGE_REQ_HIGH_PCT = 5.0

# --- MID settings ---
# === パラメータ（必要なら調整） ===
MID_MA = 25

# --- BREAKOUT settings ---
# 期間/窓サイズ（日数）
BREAKOUT_LOOKBACK = 90

# --- MIN settings ---
# 日数
MIN_DAYS = 60

# --- SLOPE settings ---
# slope min ann
SLOPE_MIN_ANN = 0.08

# --- R2 settings ---
# 下限
R2_MIN = 0.30

# --- MDD settings ---
# 上限
MDD_MAX = 0.30

# --- RIBBON settings ---
# 日数
RIBBON_KEEP_DAYS = 30

# --- WEEK settings ---
# 下限
WEEK_UP_MIN = 0.55

# --- HL settings ---
# hl win
HL_WIN = 5

# --- THRESH settings ---
# しきい値
THRESH_SCORE = 70



# ==== [Short-term Trading Enhancements] Derived Metrics (schema assumed) ====
def _calculate_atr_ewm(df: pd.DataFrame, period: int = 14) -> pd.Series:
    # df: 必須列 ['高値','安値','終値'] を想定。index は日付順。
    try:
        h = pd.to_numeric(df.get("高値"), errors="coerce")
        l = pd.to_numeric(df.get("安値"), errors="coerce")
        c = pd.to_numeric(df.get("終値"), errors="coerce")
        tr = pd.concat([
            (h - l).abs(),
            (h - c.shift(1)).abs(),
            (l - c.shift(1)).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(span=period, adjust=False).mean()
    except Exception as e:
        print("[derive-update][WARN] _calculate_atr_ewm:", e)
        return pd.Series(index=df.index, dtype=float)

def _apply_shortterm_metrics(conn: sqlite3.Connection):
    """
    スキーマ/テーブルは既に拡張済みで存在する前提:
      - latest_prices(コード, 日付, 終値|現在値, ..., ATR_14, Rate_Since_Signal_High, Days_Since_Signal_High)
      - price_history(コード, 日付, 始値, 高値, 安値, 終値, 出来高)
    """
    cur = conn.cursor()
    try:
        hist_tbl = "price_history"

        cur.execute("SELECT rowid, * FROM latest_prices")
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]

        def idx(name, default=-1):
            try: return col_names.index(name)
            except ValueError: return default

        i_code = idx("コード")
        i_date = idx("日付")
        i_close = idx("終値") if idx("終値") != -1 else idx("現在値")
        i_sig = idx("signal_date")
        if i_sig == -1:
            i_sig = idx("シグナル更新日")

        updates = []

        for r in rows:
            code = r[i_code]
            ldate = r[i_date]
            close_v = r[i_close]

            # ATR(14)
            q_hist = f"SELECT 日付, 高値, 安値, 終値 FROM {hist_tbl} WHERE コード = ? ORDER BY 日付"
            hdf = pd.read_sql_query(q_hist, conn, params=[code], parse_dates=["日付"])
            hdf = hdf.set_index("日付").sort_index()
            atr14 = float(_calculate_atr_ewm(hdf, 14).iloc[-1])

            # シグナル後モメンタム
            sig_val = r[i_sig]
            signal_date = pd.to_datetime(str(sig_val), errors="coerce")

            q2 = f"SELECT 日付, 高値, 終値 FROM {hist_tbl} WHERE コード = ? AND 日付 >= ? ORDER BY 日付"
            h2 = pd.read_sql_query(q2, conn, params=[code, str(signal_date.date())], parse_dates=["日付"])
            max_high = float(pd.to_numeric(h2["高値"], errors="coerce").max())
            idx_max = h2.loc[pd.to_numeric(h2["高値"], errors="coerce").idxmax(), "日付"]

            cur_close = float(close_v) if close_v is not None else float(pd.to_numeric(h2["終値"].iloc[-1], errors="coerce"))
            rate_since = (cur_close / max_high - 1.0) * 100.0
            base_date = pd.to_datetime(ldate)
            days_since = int((base_date - idx_max).days)

            updates.append((atr14, rate_since, days_since, r[0]))

        cur.executemany(
            "UPDATE latest_prices SET ATR_14 = ?, Rate_Since_Signal_High = ?, Days_Since_Signal_High = ? WHERE rowid = ?",
            updates
        )
        conn.commit()
        print(f"[derive-update] ATR_14/Rate/Days updated: {len(updates)} rows")
    finally:
        try: cur.close()
        except Exception: pass

# ==== 最新行同期（price_history -> latest_prices） ====
def phase_sync_latest_prices(conn: sqlite3.Connection):
    """
    Sync the latest row per code from price_history into latest_prices (UPSERT).
    Only updates 日付 and 終値; other columns remain as-is.
    """
    sql = """
    INSERT INTO latest_prices (コード, 日付, 終値)
    SELECT ph.コード, ph.日付, ph.終値
    FROM price_history ph
    JOIN (
      SELECT コード, MAX(日付) AS max_date
      FROM price_history
      GROUP BY コード
    ) AS t
      ON ph.コード = t.コード AND ph.日付 = t.max_date
    ON CONFLICT(コード) DO UPDATE SET
      日付 = excluded.日付,
      終値 = excluded.終値;
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA foreign_keys = ON")
        cur.execute("BEGIN")
        cur.execute(sql)
        conn.commit()
        print("[sync-latest] latest_prices upserted from price_history")
    except Exception as e:
        conn.rollback()
        print("[sync-latest][WARN]", e)
    finally:
        try: cur.close()
        except Exception: pass


def phase_shortterm_enhancements(conn: sqlite3.Connection):
    """
    既存フローの任意の場所（派生値更新の直後など）で呼び出してください。
    - ATR(14) と since-signal を計算して latest_prices に反映（スキーマは既に整備済み前提）
    """
    _apply_shortterm_metrics(conn)

# --- DASH settings ---
# dash template str
DASH_TEMPLATE_STR = r"""<!doctype html>
<html lang="ja">
<head>
<meta http-equiv="Cache-Control" content="no-store">
<meta http-equiv="Pragma" content="no-cache">
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>スクリーニング ダッシュボード</title>
<style>
  :root{
    --ink:#1f2937; --muted:#6b7280; --bg:#f9fafb; --line:#e5e7eb;
    --blue:#0d3b66; --green:#15803d; --orange:#b45309; --yellow:#a16207;
    --hit:#ffe6ef; --rowhover:#f6faff;
  }

  /* 全体 */
  body{
    font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;
    margin:16px; color:var(--ink); background:#fff;
  }

  nav{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
  nav a{padding:6px 10px;border-radius:8px;text-decoration:none;background:#e1e8f0;color:#1f2d3d;font-weight:600}
  nav a.active{background:var(--blue);color:#fff}

  .toolbar{display:flex;gap:14px;align-items:center;margin:8px 0 10px;flex-wrap:wrap}
  .toolbar input[type="text"],.toolbar input[type="number"]{padding:6px 10px;border:1px solid #ccd;border-radius:8px;background:#fff}
  .toolbar input[type="number"]{width:80px}
  .btn{background:var(--blue);color:#fff;border:none;border-radius:8px;padding:6px 12px;cursor:pointer;font-weight:600}

  /* テーブルラッパ（角丸＋横スクロール） */
  .tbl-wrap{
    border-radius:10px;
    overflow:auto; /* 縦横 */
    -webkit-overflow-scrolling:touch;
    background:#fff;
    box-shadow:0 0 0 1px var(--line) inset;
    max-height:70vh;
  }

  /* テーブル共通（コンパクト化） */
  .tbl{ border-collapse:collapse; width:100%; background:#fff; }
  .tbl th,.tbl td{
    border-bottom:1px solid var(--line);
    padding:4px 6px;
    font-size:0.85em;
    vertical-align:top;
  }
  .tbl tbody tr:nth-child(even){background:#fcfdff}
  .tbl tbody tr:hover{background:var(--rowhover)}
  .tbl th.sortable{cursor:pointer;user-select:none}
  .tbl th.sortable .arrow{margin-left:6px;font-size:11px;color:#666}
  .num{text-align:right}
  .muted{color:var(--muted)}
  .hidden{display:none}
  .count{margin-left:6px;color:var(--muted)}
  .pager{display:flex;gap:8px;align-items:center}
  tr.hit>td{background:var(--hit)}

  /* バッジ */
  .badge{display:inline-flex;gap:6px;align-items:center;padding:2px 8px;border-radius:999px;font-size:12px;line-height:1;font-weight:700}
  .b-green{background:#e7f6ed;color:var(--green);border:1px solid #cceedd}
  .b-orange{background:#fff4e6;color:#b45309;border:1px solid #ffe2c2}
  .b-yellow{background:#fff9db;color:var(--yellow);border:1px solid #ffe9a8}
  .b-gray{background:#eef2f7;color:#475569;border:1px solid #dbe4ef}

  /* 推奨バッジ */
  .rec-badge{display:inline-flex; align-items:center; gap:6px; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; line-height:1; white-space:nowrap;}
  .rec-strong{ background:#e7f6ed; color:#166534; border:1px solid #cceedd; }
  .rec-small { background:#fff4e6; color:#9a3412; border:1px solid #ffe2c2; }
  .rec-watch { background:#eef2f7; color:#475569; border:1px solid #dbe4ef; }
  .rec-dot{ display:inline-block; width:6px; height:6px; border-radius:50%; background:currentColor;}

  /* ヘッダー固定（候補一覧 / 全カラム） */
  #tbl-candidate thead th,
  #tbl-allcols  thead th{
    position:sticky;
    position:-webkit-sticky;
    top:0;
    background:#fff;
    z-index:2;
    border-bottom:2px solid #ccc;
  }

  /* ヘルプ（小窓＋暗幕） */
  .help-backdrop{
    position:fixed; inset:0; background:rgba(17,24,39,.45);
    z-index:9998; display:none;
  }
  .help-pop{
    position:absolute; z-index:9999; background:#fff; border:1px solid #e5e7eb;
    border-radius:12px; box-shadow:0 12px 32px rgba(0,0,0,.18);
    padding:12px 14px 14px; font-size:13px; line-height:1.55;
    display:none; width:clamp(720px, 90vw, 1200px);
  }
  .help-pop .help-head{ display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:6px; font-weight:700;}
  .help-pop .help-close{ display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; border-radius:6px; cursor:pointer; user-select:none; font-weight:700;}
  .help-pop .help-close:hover{ background:#f3f4f6; }

  /* ？アイコン（テーブル/ツールバー共通） */
  .qhelp{ display:inline-flex; align-items:center; justify-content:center; width:18px; height:18px; margin-left:6px;
    border-radius:50%; border:1px solid #cbd5e1; font-size:12px; cursor:pointer; background:#eef2ff; color:#334155; font-weight:700; line-height:1;}
  .qhelp:hover{ background:#e0e7ff; }

  /* 1行固定（改行は潰す） */
  #tbl-candidate td, #tbl-candidate th,
  #tbl-allcols  td, #tbl-allcols  th{ white-space:nowrap !important; word-break:keep-all !important; }
  #tbl-candidate td br, #tbl-candidate th br,
  #tbl-allcols  td br, #tbl-allcols  th br{ display:none !important; }

  /* 判定理由は極小で折り返し可 */
  th.reason-col, td.reason-col{ font-size:0.78em; line-height:1.2; white-space:normal !important; word-break:break-word !important; }

  .mini{ font-size:10px; color:var(--muted); }

  /* コードコピーリンク */
  .copylink{ color:var(--blue); text-decoration:underline; cursor:pointer; }
  .copylink.ok{ color:var(--green); text-decoration:none; font-weight:700; }

  /* 予測タブ：理由/ヒントの強調 */
  .reason-col,.hint-col {vertical-align: top;}
  .reason-box{ display:inline-block; padding:6px 8px; border-radius:10px; background:#fff9db; line-height:1.4; white-space: pre-wrap; }

  /* 財務コメント（財務リンク右） */
  .fn-note{
    color:#b91c1c; font-weight:700; font-size:0.78em; margin-left:8px;
    white-space:pre-wrap; overflow:visible; text-overflow:clip; display:inline-block;
    max-width:clamp(600px, 60vw, 1200px); vertical-align:bottom;
  }

  /* ツールチップ/ポップオーバーはクリック貫通 */
  .tooltip, .popover { pointer-events:none; }
</style>
</head>

<body>
  <nav>
    <a href="#" id="lnk-cand" class="active">候補一覧</a>
    <a href="#" id="lnk-tmr">明日用</a>
    <a href="#" id="lnk-all">全カラム</a>
    <a href="#" id="lnk-earn">決算(実績)</a>
    <a href="#" id="lnk-preearn">決算〈予測〉</a>
    {% if include_log %}<a href="#" id="lnk-log">signals_log</a>{% endif %}
    <span class="mini" style="margin-left:auto">build: {{ build_id }}</span>
  </nav>

  <div id="toolbar" class="toolbar">
    <label><input type="checkbox" id="f_shodou"> 初動のみ</label>
    <label><input type="checkbox" id="f_tei"> 底打ちのみ</label>
    <label><input type="checkbox" id="f_both"> 両立のみ</label>
    <label><input type="checkbox" id="f_rightup"> 右肩上がりのみ</label>
    <label><input type="checkbox" id="f_early"> 早期のみ</label>
    <label><input type="checkbox" id="f_etype"> 早期種別あり</label>
    <label><input type="checkbox" id="f_recstrong"> エントリー有力のみ</label>
    <label><input type="checkbox" id="f_smallpos"> 小口提案のみ</label>
    <label><input type="checkbox" id="f_noshor"> 空売り機関なしのみ</label>
    <label><input type="checkbox" id="f_opratio"> 割安（営利対時価10%以上）のみ</label>
    <label><input type="checkbox" id="f_hit"> 当たりのみ</label>
    <div class="toolbar early-filter">
      <label class="ef-chk"><input type="checkbox" value="ブレイク" ><span>ブレイク</span></label>
      <label class="ef-chk"><input type="checkbox" value="ポケット" ><span>ポケット</span></label>
      <label class="ef-chk"><input type="checkbox" value="20MAリバ" ><span>20MAリバ</span></label>
      <label class="ef-chk"><input type="checkbox" value="200MAリクレイム" ><span>200MAリクレイム</span></label>
    </div>

    <label>上昇率≥ <input type="number" id="th_rate" placeholder="3.0" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label>売買代金≥ <input type="number" id="th_turn" placeholder="10" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label>RVOL代金≥ <input type="number" id="th_rvol" placeholder="3.0" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label><input type="checkbox" id="f_defaultset"> 規定</label>

    <input type="text" id="q" placeholder="全文検索（コード/銘柄/判定理由など）" style="min-width:240px">
    <button class="btn" id="btn-stats">傾向グラフ</button>
    <button class="btn" id="btn-ts">推移グラフ</button>

    <span class="pager">
      <label>表示件数
        <select id="perpage">
          <option value="200">200</option><option value="500">500</option>
          <option value="1000">1000</option><option value="2000" selected>2000</option>
        </select>
      </label>
      <button class="btn" id="prev">前へ</button>
      <button class="btn" id="next">次へ</button>
      <span id="pageinfo" class="muted">- / -</span>
    </span>
    <span class="count">件数: <b id="count">-</b></span>
  </div>

  <script id="__DATA__" type="application/json">{{ data_json|safe }}</script>

<script>
(function(){
  "use strict";

  // ---- data ----
  const RAW = (()=>{ try{ return JSON.parse(document.getElementById("__DATA__").textContent||"{}"); }catch(_){ return {}; } })();
  const DATA_CAND = Array.isArray(RAW.cand)? RAW.cand: [];
  const DATA_ALL  = Array.isArray(RAW.all) ? RAW.all : [];
  const DATA_LOG  = Array.isArray(RAW.logs)? RAW.logs: [];
  const DATA_EARN = Array.isArray(RAW.earnings) ? RAW.earnings : [];
  const _preSrc = RAW.preearn ?? RAW.pre ?? RAW.pre_rows ?? RAW.preearn_rows ?? RAW["pre-earnings"] ?? RAW.earnings_pre ?? [];
  const DATA_PREEARN = Array.isArray(_preSrc) ? _preSrc : [];

  // ---- utils ----
  const $  = (s,r=document)=>r.querySelector(s);
  const $$ = (s,r=document)=>Array.from(r.querySelectorAll(s));
  const num = (v)=>{ const s=String(v??"").replace(/[,\s円％%]/g,""); const n=parseFloat(s); return Number.isFinite(n)?n:NaN; };
  const cmp = (a,b)=>{ if(a==null&&b==null) return 0; if(a==null) return -1; if(b==null) return 1;
    const na=+a, nb=+b, da=new Date(a), db=new Date(b);
    if(!Number.isNaN(na)&&!Number.isNaN(nb)) return na-nb;
    if(!Number.isNaN(da)&&!Number.isNaN(db)) return da-db;
    return String(a).localeCompare(String(b),"ja"); };
  const hasKouho = (v)=> String(v||"").includes("候補");

  function escapeHtml(s){ return String(s).replace(/[&<>"']/g, m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m])); }

  // copy link
  function codeLink(code){
    if(code==null) return "";
    const s = String(code).padStart(4, "0");
    return `<a href="#" class="copylink" data-copy="${s}" title="コードをコピー">${s}</a>`;
  }
  document.addEventListener("click", async (e)=>{
    const a = e.target.closest && e.target.closest("a.copylink");
    if (!a) return;
    e.preventDefault();
    const text = a.dataset.copy || "";
    try{
      if (navigator.clipboard && window.isSecureContext !== false) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement("textarea");
        ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
        document.body.appendChild(ta); ta.select(); document.execCommand("copy"); document.body.removeChild(ta);
      }
      const old = a.textContent; a.classList.add("ok"); a.textContent = "コピー済";
      setTimeout(() => { a.classList.remove("ok"); a.textContent = old; }, 1200);
    } catch (_){
      const old = a.textContent; a.textContent = "失敗"; setTimeout(() => { a.textContent = old; }, 1200);
    }
  });

  // offerings badge
  const OFFER_SET = new Set((Array.isArray(RAW.offer_codes) ? RAW.offer_codes : []).map(x => String(x).padStart(4,"0")));
  function offeringBadge(code){
    const c = String(code ?? "").padStart(4,"0");
    return OFFER_SET.has(c) ? `<span class="badge b-gray" title="直近に増資/行使/売出/CBなどの履歴あり">増資経歴</span>` : "";
  }

  // finance modal
  function openFinanceHtml(code){
    closeAllTips();
    const back = ensureChartModal();
    const body = document.getElementById("__chart_body__");
    const src = `graph/finance_${String(code).padStart(4,"0")}.html`;
    body.innerHTML = `<iframe src="${src}" style="width:100%; height:80vh; border:0;"></iframe>`;
    back.style.display = "block";
    const box = document.querySelector(".help-pop");
    box.style.display = "block";
    const sx = window.scrollX||0, sy = window.scrollY||0;
    box.style.top = `${sy+60}px`;
    requestAnimationFrame(()=>{ box.style.left = `${sx + Math.max(10, (document.documentElement.clientWidth - box.offsetWidth)/2)}px`; });
  }
  function financeLink(code){
    if(code==null) return "";
    const s = String(code).padStart(4, "0");
    return `<a href="#" class="financelink" data-code="${s}" title="財務グラフを開く">財務</a>`;
  }
  function financeNote(row){
    let v = row?.["財務コメント"];
    if (!v) return "";
    let s = String(v).trim();
    // 先頭の「⚠」「判定（…）:」「判定:」「コメント:」などを除去
    s = s
      .replace(/^\s*[⚠︎⚠️△▲※＊*]?\s*判定（[^）]*）\s*[:：]?\s*/i, "")
      .replace(/^\s*[⚠︎⚠️△▲※＊*]?\s*(判定|コメント)\s*[:：]?\s*/i, "")
      .replace(/^\s*[：:\-・]+\s*/, ""); // 余分な区切りが続く場合の掃除
    const t = escapeHtml(s);
    return ` <span class="fn-note" title="${t}">${t}</span>`;
  }
  document.addEventListener("click", (e)=>{
    const a = e.target.closest && e.target.closest("a.financelink");
    if(!a) return;
    e.preventDefault();
    const code = a.dataset.code;
    if(code) openFinanceHtml(code);
  });

  // DOM sort (generic)
  function wireDomSort(tableSelector){
    const table = document.querySelector(tableSelector); if(!table) return;
    const ths = Array.from(table.querySelectorAll('thead th.sortable'));
    const cellVal = (td)=>{
      if (!td) return '';
      const ds = td.getAttribute ? td.getAttribute('data-sort') : null;
      return (ds !== null && ds !== '') ? ds : (td.textContent || '');
    };
    const toNum = (s)=>{
      let t = String(s ?? '').trim().replace(/\s|[円％%]/g,'');
      if (t.indexOf('.') === -1 && /^-?\d+,\d+$/.test(t)) t = t.replace(',', '.'); else t = t.replace(/,/g,'');
      const n = parseFloat(t);
      return Number.isFinite(n) ? n : NaN;
    };
    ths.forEach((th)=>{
      if (th.__wiredSort) return;
      th.__wiredSort = true;
      th.style.cursor = 'pointer';
      th.addEventListener('click', ()=>{
        const colIndex = th.cellIndex;
        const dirPrev = th.dataset.dir;
        ths.forEach(h=>{ h.dataset.dir=''; const a=h.querySelector('.arrow'); if(a) a.textContent=''; });
        const dir = (dirPrev === 'asc') ? 'desc' : 'asc';
        th.dataset.dir = dir;
        const typ = th.dataset.type || 'text';
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        rows.sort((r1, r2)=>{
          const aRaw = cellVal(r1.children[colIndex]).trim();
          const bRaw = cellVal(r2.children[colIndex]).trim();
          if (typ === 'num'){
            const aKey = toNum(aRaw), bKey = toNum(bRaw);
            if (!Number.isNaN(aKey) && !Number.isNaN(bKey)){
              return dir==='asc' ? (aKey-bKey) : (bKey-aKey);
            }
          } else if (typ === 'date'){
            const aKey = Date.parse(aRaw), bKey = Date.parse(bRaw);
            if (!Number.isNaN(aKey) && !Number.isNaN(bKey)){
              return dir==='asc' ? (aKey-bKey) : (bKey-aKey);
            }
          }
          const sa = String(aRaw).toLowerCase();
          const sb = String(bRaw).toLowerCase();
          return dir==='asc' ? sa.localeCompare(sb,'ja') : sb.localeCompare(sa,'ja');
        });
        const tb = table.querySelector('tbody');
        rows.forEach(r=>tb.appendChild(r));
        const arrow = th.querySelector('.arrow'); if (arrow) arrow.textContent = (dir==='asc'?'▲':'▼');
      });
    });
  }

  // state
  const state = { tab:"cand", page:1, per:parseInt($("#perpage")?.value||"500",10), sortKey:null, sortDir:1, q:"", data: DATA_CAND.slice() };
  window.state = state;

  const DEFAULTS = { rate:3, turn:5, rvol:2 };
  function applyDefaults(on){
    const ia=$("#th_rate"), it=$("#th_turn"), ir=$("#th_rvol");
    if(!ia||!it||!ir) return;
    if(on){ ia.value=DEFAULTS.rate; it.value=DEFAULTS.turn; ir.value=DEFAULTS.rvol; }
    else  { ia.value=""; it.value=""; ir.value=""; ia.removeAttribute("value"); it.removeAttribute("value"); ir.removeAttribute("value"); }
    state.page=1; render();
  }
  function forceClearThresholds(){
    const cb=$("#f_defaultset");
    if(cb) cb.checked=false;
    applyDefaults(false);
  }
  function thRate(){ const v=num($("#th_rate")?.value); return Number.isNaN(v)?null:v; }
  function thTurn(){ const v=num($("#th_turn")?.value); return Number.isNaN(v)?null:v; }
  function thRvol(){ const v=num($("#th_rvol")?.value); return Number.isNaN(v)?null:v; }

  function getSelectedTypes(){
    const box = document.querySelector(".early-filter");
    if (!box) return [];
    return Array.from(box.querySelectorAll(".ef-chk input:checked")).map(el => el.value);
  }

  function isHitRow(r){
    const v = String((r && (r["判定"] ?? r["judge"] ?? "")) || "").trim().toLowerCase();
    if (/^当たり/.test(v)) return true;
    if (v === "win" || v === "auto" || v === "再評価ok") return true;
    return false;
  }

  function applyFilter(rows){
    const q   = ($("#q")?.value||"").trim();
    const sel = getSelectedTypes();
    const useEarly = sel.length > 0;
    return rows.filter(r=>{
      const sh  = hasKouho(r["初動フラグ"]);
      const te  = hasKouho(r["底打ちフラグ"]);
      const ru  = hasKouho(r["右肩上がりフラグ"]);
      const ea  = hasKouho(r["右肩早期フラグ"]);
      const etp = (String(r["右肩早期種別"]||"").trim().length>0);
      const ns  = String(r["空売り機関なし_flag"]||"0")==="1";
      const op  = String(r["営利対時価_flag"]||"0")==="1";
      const hit = isHitRow(r);
      if($("#f_shodou")?.checked && !sh) return false;
      if($("#f_tei")?.checked    && !te) return false;
      if($("#f_both")?.checked   && !(sh && ru)) return false;
      if($("#f_rightup")?.checked&& !ru) return false;
      if($("#f_early")?.checked  && !ea) return false;
      if($("#f_etype")?.checked  && !etp) return false;
      if($("#f_noshor")?.checked && !ns) return false;
      if($("#f_opratio")?.checked&& !op) return false;
      if($("#f_hit")?.checked    && !hit) return false;
      if (useEarly){
        const val = String(r["右肩早期種別"]||"");
        if (!sel.some(v => val.includes(v))) return false;
      }
      const rate = num(r["前日終値比率"]);
      const turn = num(r["売買代金(億)"]);
      const rvol = num(r["RVOL代金"]);
      const tr = thRate(), tt = thTurn(), tv = thRvol();
      if(tr!=null && !(rate>=tr)) return false;
      if(tt!=null && !(turn>=tt)) return false;
      if(tv!=null && !(rvol>=tv)) return false;

      if(q){
        const keys=["コード","銘柄名","判定理由","右肩早期種別","初動フラグ","底打ちフラグ","右肩上がりフラグ","右肩早期フラグ","推奨アクション"];
        if(!keys.some(k=>String(r[k]??"").includes(q))) return false;
      }
      return true;
    });
  }

  function sortRows(rows){
    return state.sortKey ? rows.slice().sort((a,b)=> state.sortDir * cmp(a[state.sortKey], b[state.sortKey])) : rows;
  }

  function formatJudgeLabel(r){
    return isHitRow(r) ? "当たり！(1)" : "外れ！(1)";
  }

  // render: candidate
// render: candidate
function renderCand(){
  const body = document.querySelector("#tbl-candidate tbody");
  if(!body) return;
  const rows = sortRows(applyFilter(state.data));
  const total = rows.length, per = state.per, maxPage = Math.max(1, Math.ceil(total/per));
  state.page = Math.min(state.page, maxPage);
  const s = (state.page-1)*per, e = Math.min(s+per, total);

  let html = "";
  for (let i = s; i < e; i++) {
    const r = rows[i] || {};
    const et = (r["右肩早期種別"] || "").trim();
    let etBadge = et;
    if (et === "ブレイク") etBadge = '<span class="badge b-green">● ブレイク</span>';
    else if (et === "20MAリバ") etBadge = '<span class="badge b-green">● 20MAリバ</span>';
    else if (et === "ポケット") etBadge = '<span class="badge b-orange">● ポケット</span>';
    else if (et === "200MAリクレイム") etBadge = '<span class="badge b-yellow">● 200MAリクレイム</span>';

    const rec = (r["推奨アクション"] || "").trim();
    let recBadge = "";
    if (rec === "エントリー有力")      recBadge = '<span class="rec-badge rec-strong" title="エントリー有力"><span class="rec-dot"></span>有力</span>';
    else if (rec === "小口提案")        recBadge = '<span class="rec-badge rec-small" title="小口提案"><span class="rec-dot"></span>小口</span>';
    else if (rec)                       recBadge = `<span class="rec-badge rec-watch" title="${rec.replace(/"/g,'&quot;')}"><span class="rec-dot"></span>${rec}</span>`;

    const isHitTr = isHitRow(r);
    html += `<tr${isHitTr ? " class='hit'" : ""}>
      <td>${codeLink(r["コード"])} ${offeringBadge(r["コード"])}</td>
      <td>${r["銘柄名"] ?? ""}</td>
      <td>${r["市場"] || "-"}</td>
      <td><a href="${r["yahoo_url"] ?? "#"}" target="_blank" rel="noopener">Yahoo</a></td>
      <td><a href="${r["x_url"] ?? "#"}" target="_blank" rel="noopener">X検索</a></td>
      <td class="num">${r["現在値"] ?? ""}</td>
      <td class="num">${r["前日終値"] ?? ""}</td>
      <td class="num">${r["前日円差"] ?? ""}</td>
      <td class="num">${r["前日終値比率"] ?? ""}</td>
      <td class="num">${r["出来高"] ?? ""}</td>
      <td class="num">${r["売買代金(億)"] ?? ""}</td>
      <td>${financeLink(r["コード"])}${financeNote(r)}</td>
      <!-- ▼▼ ここを追加：thead の「スコア」「進捗率」に対応 ▼▼ -->
      <td class="num">${r["スコア"] ?? ""}</td>
      <td class="num">${r["進捗率"] ?? ""}</td>
      <!-- ▲▲ 追加ここまで ▲▲ -->
      <td>${r["増資リスク"] ?? ""}</td>
      <td class="num">${r["増資スコア"] ?? ""}</td>
      <td class="reason-col">${r["増資理由"] || ""}</td>
      <td>${r["初動フラグ"] || ""}</td>
      <td>${r["底打ちフラグ"] || ""}</td>
      <td>${r["右肩上がりフラグ"] || ""}</td>
      <td>${r["右肩早期フラグ"] || ""}</td>
      <td class="num">${r["右肩早期スコア"] ?? ""}</td>
      <td>${etBadge}${r["右肩早期種別_mini"] || ""}</td>
      <td>${formatJudgeLabel(r)}</td>
      <td class="reason-col">${r["判定理由"] || ""}</td>
      <td>${recBadge}</td>
      <td class="num">${r["推奨比率"] ?? ""}</td>
      <td>${r["シグナル更新日"] || ""}</td>
    </tr>`;
  }
  body.innerHTML = html;
  document.querySelector("#count").textContent = String(total);
  document.querySelector("#pageinfo").textContent = `${state.page} / ${Math.max(1, Math.ceil(total/state.per))}`;
  wireDomSort("#tbl-candidate");
}


// === Tomorrow logic START =====================================

// 正規化: 任意の Date / 文字列 → YYYY-MM-DD
function toKey(x){
  if (x instanceof Date) {
    const y=x.getFullYear(), m=('0'+(x.getMonth()+1)).slice(-2), d=('0'+x.getDate()).slice(-2);
    return `${y}-${m}-${d}`;
  }
  const s = String(x ?? "").trim().slice(0,10).replace(/[./]/g,'-').replace(/\//g,'-');
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return `${m[1]}-${m[2].padStart(2,'0')}-${m[3].padStart(2,'0')}`;
  const dt = new Date(s);
  if (isNaN(+dt)) return "";
  const yy=dt.getFullYear(), mm=('0'+(dt.getMonth()+1)).slice(-2), dd=('0'+dt.getDate()).slice(-2);
  return `${yy}-${mm}-${dd}`;
}

function latestUpdateDate(rows){
  const ds = (rows||[]).map(r=>toKey(r?.["シグナル更新日"])).filter(Boolean);
  return ds.sort().pop() || null;
}

function localDateStr(d){
  const y=d.getFullYear(), m=('0'+(d.getMonth()+1)).slice(-2), da=('0'+d.getDate()).slice(-2);
  return `${y}-${m}-${da}`;
}
function prevBusinessDay(d){
  const dt=new Date(d); do{ dt.setDate(dt.getDate()-1);}while([0,6].includes(dt.getDay())); return dt;
}
function nextBusinessDay(d){
  const dt=new Date(d); do{ dt.setDate(dt.getDate()+1);}while([0,6].includes(dt.getDay())); return dt;
}

// 「候補」判定（初動/右肩/早期のいずれか）
function _hasCandidateFlag(r){
  return String(r?.["初動フラグ"]||"").includes("候補")
      || String(r?.["右肩上がりフラグ"]||"").includes("候補")
      || String(r?.["右肩早期フラグ"]||"").includes("候補");
}

// 基準日キーに対して「候補」だけを抽出
function _pickTomorrowRows(src, baseKey){
  if(!Array.isArray(src) || !baseKey) return [];
  return src.filter(r => toKey(r?.["シグナル更新日"])===baseKey && _hasCandidateFlag(r));
}

// ローカル時計から基準日/対象日を計算（14:30 凍結ルール）
function _computeBaseAndTargetFromLocalNow(){
  const now = new Date();
  const inFreeze = (now.getHours() < 14) || (now.getHours() === 14 && now.getMinutes() < 30);
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const baseDate = inFreeze ? prevBusinessDay(today) : today;
  const targetDate = nextBusinessDay(baseDate);
  return { inFreeze, baseDate, targetDate };
}

function renderTomorrow(rows){
  const body = document.querySelector("#tbl-tmr tbody");
  if (!body) return;
  let html = "";
  rows.forEach(r=>{
    const rec = (r["推奨アクション"] || "").trim();
    let recBadge = "";
    if (rec === "エントリー有力") recBadge = '<span class="rec-badge rec-strong" title="エントリー有力"><span class="rec-dot"></span>有力</span>';
    else if (rec === "小口提案")   recBadge = '<span class="rec-badge rec-small" title="小口提案"><span class="rec-dot"></span>小口</span>';
    else if (rec)                  recBadge = `<span class="rec-badge rec-watch" title="${rec.replace(/"/g,'&quot;')}"><span class="rec-dot"></span>${rec}</span>`;
    const isHit = isHitRow(r);
    html += `<tr${isHit ? " class='hit'" : ""}>
      <td>${codeLink(r["コード"])} ${offeringBadge(r["コード"])}</td>
      <td>${r["銘柄名"] ?? ""}</td>
      <td>${r["市場"] || "-"}</td>
      <td><a href="${r["yahoo_url"]??"#"}" target="_blank" rel="noopener">Yahoo</a></td>
      <td><a href="${r["x_url"]??"#"}" target="_blank" rel="noopener">X検索</a></td>
      <td class="num" data-sort="${r['現在値_raw'] ?? ''}">${r['現在値'] ?? ''}</td>
      <td class="num" data-sort="${r['前日終値比率_raw'] ?? ''}">${r['前日終値比率'] ?? ''}</td>
      <td class="num">${r["売買代金(億)"]??""}</td>
      <td>${financeLink(r["コード"])}${financeNote(r)}</td>
      <td>${r["増資リスク"] ?? ""}</td>
      <td class="num">${r["増資スコア"] ?? ""}</td>
      <td class="reason-col">${r["増資理由"] || ""}</td>
      <td class="num">${r["右肩早期スコア"]??""}</td>
      <td>${(r["右肩早期種別"]||"").trim()}</td>
      <td>${isHit ? "当たり！(1)" : "外れ！(1)"}</td>
      <td>${recBadge}</td>
    </tr>`;
  });
  body.innerHTML = html;
  wireDomSort("#tbl-tmr");
}

// 並び替え（推奨 > 右肩早期S > 売買代金）
function _sortTomorrow(rows){
  return rows.slice().sort((a,b)=>{
    const rank = (x)=> x==="エントリー有力" ? 2 : (x==="小口提案" ? 1 : 0);
    const r = rank((b["推奨アクション"]||"").trim()) - rank((a["推奨アクション"]||"").trim());
    if (r !== 0) return r;
    const s = (+b["右肩早期スコア"]||0) - (+a["右肩早期スコア"]||0);
    if (s !== 0) return s;
    return (+b["売買代金(億)"]||0) - (+a["売買代金(億)"]||0);
  });
}

function renderTomorrowWrapper(){
  // 1) 基準日/対象日（ローカル時計 & 14:30 凍結）
  const { inFreeze, baseDate, targetDate } = _computeBaseAndTargetFromLocalNow();
  const baseKey = toKey(baseDate);
  const targetKey = toKey(targetDate);

  // 2) データ側の最新更新日（フォールバック用）
  const latestKey = latestUpdateDate(DATA_CAND);

  // 3) 抽出（ゼロ回避の多段フォールバック）
  let reason = `基準日の「候補」`;
  let rows = _pickTomorrowRows(DATA_CAND, baseKey);

  if (rows.length === 0 && latestKey && latestKey !== baseKey){
    rows = _pickTomorrowRows(DATA_CAND, latestKey);
    if (rows.length) reason = `最新日(${latestKey})の「候補」にフォールバック`;
  }
  if (rows.length === 0){
    rows = (DATA_CAND||[]).filter(r => toKey(r?.["シグナル更新日"])===baseKey);
    if (rows.length) reason = `基準日(${baseKey})の全件（候補条件なし）`;
  }
  if (rows.length === 0 && latestKey){
    rows = (DATA_CAND||[]).filter(r => toKey(r?.["シグナル更新日"])===latestKey);
    if (rows.length) reason = `最新日(${latestKey})の全件（候補条件なし）`;
  }
  if (rows.length === 0){
    rows = (DATA_CAND||[]).slice();
    reason = `全体からのサンプル`;
  }

  // 並び替え & 上限（見やすさ用に最大2000はそのまま）
  rows = _sortTomorrow(rows);

  // 4) ラベル更新（基準日・対象日・凍結状態・抽出根拠）
  const lbl = document.getElementById("tmr-label");
  if (lbl){
    const baseStr = localDateStr(baseDate);
    const tgtStr  = localDateStr(targetDate);
    const freezeStr = inFreeze ? "ON" : "OFF";
    lbl.textContent = `📅 ${tgtStr} 向け（基準日: ${baseStr} / 凍結: ${freezeStr} / 抽出: ${rows.length}件・${reason}）`;
  }

  // 5) 描画
  window.DATA_TMR = rows;
  renderTomorrow(rows);
}

// === Tomorrow logic END =====================================




  // all
  function renderAll(){
    const head = document.querySelector("#all-head"), body = document.querySelector("#all-body");
    if(!head||!body) return;
    head.innerHTML=body.innerHTML="";
    const rows=DATA_ALL;
    if(!rows.length) return;
    const cols=Object.keys(rows[0]);
    head.innerHTML=cols.map(c=>{
      const typ=(c.includes("フラグ")?"flag":(c.includes("日")||c.includes("更新")||c==="日時"?"date":(["現在値","出来高","売買代金(億)","時価総額億円","右肩早期スコア","推奨比率","前日終値比率","前日終値比率（％）"].includes(c)?"num":"text")));
      return `<th class="sortable ${typ==='num'?'num':''}" data-col="${c}" data-type="${typ}">${c}<span class="arrow"></span></th>`;
    }).join("");
    body.innerHTML=rows.slice(0,2000).map(r=>`<tr>${
      cols.map(c=>{
        let v = (c === "判定") ? formatJudgeLabel(r) : (r[c] ?? "");
        if (c === "コード") v = codeLink(v);
        const isNum = ['現在値','出来高','売買代金(億)','時価総額億円','右肩早期スコア','推奨比率','前日終値比率','前日終値比率（％）'].includes(c);
        return `<td class="${isNum?'num':''}">${v}</td>`;
      }).join("")
    }</tr>`).join("");
    wireDomSort("#tbl-allcols");
  }
  
  // --- ここから追加 ---
  // タブ共通レンダラー（フィルタ/ページャ/初期化から呼ばれる）
  function render(){
    if (state.tab === "all") { renderAll(); return; }
    if (state.tab === "tmr") { renderTomorrowWrapper(); return; }
    // 既定は候補一覧
    renderCand();
  }
  // --- ここまで追加 ---

  // earn
  function fmtTime(ts){
    try{
      const d = new Date(ts);
      if (!isNaN(d)) {
        const y=d.getFullYear(), m=('0'+(d.getMonth()+1)).slice(-2), da=('0'+d.getDate()).slice(-2);
        const hh=('0'+d.getHours()).slice(-2), mm=('0'+d.getMinutes()).slice(-2);
        return `${y}/${m}/${da} ${hh}:${mm}`;
      }
      return String(ts ?? "");
    }catch(_){ return String(ts ?? ""); }
  }
  function renderEarnings(rows){
    const tbody = document.getElementById("earn-body"); if(!tbody) return;
    if(!rows.length){ tbody.innerHTML = `<tr><td colspan="4" class="muted">データなし</td></tr>`; return; }
    tbody.innerHTML = rows.map(r=>{
      const name = r.name ?? r.symbol ?? r.銘柄 ?? "";
      const sentiment = (r.sentiment ?? "").toString().toLowerCase();
      const pillClass = sentiment.includes("pos") || sentiment.includes("ポジ") ? "b-green"
                     : sentiment.includes("neg") || sentiment.includes("ネガ") ? "b-orange"
                     : "b-yellow";
      const pill = `<span class="badge ${pillClass}">● ${r.sentiment ?? "ニュートラル"}</span>`;
      return `<tr>
        <td>${name}</td>
        <td>${pill}</td>
        <td>${r.title ?? ""}</td>
        <td class="mono">${fmtTime(r.time)}</td>
      </tr>`;
    }).join("");
    wireDomSort("#tbl-earn");
  }

  // pre-earn
  function _fmt2num(x){
    if (x === null || x === undefined) return "";
    const n = parseFloat(String(x).replace(/[,％%]/g,""));
    if (!Number.isFinite(n)) return String(x ?? "");
    return Number.isInteger(n) ? String(n) : n.toFixed(2);
  }
  function _formatTwoDecimals(tableSelector){
    const tbl = document.querySelector(tableSelector);
    if (!tbl) return;
    const ths = Array.from(tbl.querySelectorAll("thead th"));
    const targets = [];
    const norm = (s)=> String(s||"").replace(/\s+/g,"").replace(/[（）]/g, v=> (v==="（"?"(" : ")")).trim();
    ths.forEach((th, idx)=>{
      const t = norm(th.textContent);
      if (t.includes("前日終値比率")) targets.push(idx);
      if (t === "売買代金(億)" || t === "売買代金億") targets.push(idx);
      if (t === "現在値" || t === "前日終値" || t === "前日比(円)") targets.push(idx);
    });
    if (!targets.length) return;
    const rows = tbl.querySelectorAll("tbody tr");
    rows.forEach(tr=>{
      targets.forEach(ci=>{
        const td = tr.children[ci]; if (!td) return;
        const raw = td.textContent.trim(); if (!raw) return;
        td.textContent = _fmt2num(raw); td.classList.add("num");
      });
    });
  }
  function renderPreEarnings(rows){
    const tbody = document.getElementById("preearn-body"); if(!tbody) return;
    if(!rows.length){
      tbody.innerHTML = `<tr><td colspan="9" class="muted">データなし</td></tr>`;
      return;
    }
    tbody.innerHTML = rows.map(r=>`
      <tr>
        <td>${r.name ?? r.symbol ?? r.銘柄 ?? ""}</td>
        <td class="num">${_fmt2num(r.pre_score)}</td>
        <td class="num">${_fmt2num(r.edge_score)}</td>
        <td class="num">${_fmt2num(r.momentum_score)}</td>
        <td class="reason-col"><div class="reason-box">${(r["スコア理由"] ?? "根拠薄め（暫定）")}</div></td>
        <td class="hint-col"><div class="reason-box">${(r["予測ヒント"] ?? "（準備中）")}</div></td>
        <td class="num">${_fmt2num(r["期待株価"]) || (_fmt2num(r.現在値) || "")}</td>
        <td>${r["修正見通し"] ?? "中立"}</td>
        <td>${r["過熱度"] ?? "中立"}</td>
      </tr>
    `).join("");
    wireDomSort("#tbl-preearn");
    _formatTwoDecimals("#tbl-preearn");
  }

  // help & modal
  function ensureHelpDom(){
    let bd=document.querySelector(".help-backdrop");
    if(!bd){ bd=document.createElement("div"); bd.className="help-backdrop"; document.body.appendChild(bd); }
    let pp=document.querySelector(".help-pop");
    if(!pp){ pp=document.createElement("div"); pp.className="help-pop"; pp.innerHTML='<div class="help-head"><div>ヘルプ</div><div class="help-close">×</div></div><div class="help-body"></div>'; document.body.appendChild(pp); }
    pp.querySelector(".help-close").onclick=()=>{ pp.style.display="none"; bd.style.display="none"; };
    bd.onclick=()=>{ pp.style.display="none"; bd.style.display="none"; };
    return {bd,pp};
  }
  function closeAllTips(){ const t=document.querySelectorAll('.tooltip, .popover'); t.forEach(el=>el.remove()); }
  function ensureChartModal(){
    let bd=document.getElementById("__chart_back__");
    if(!bd){ bd=document.createElement("div"); bd.id="__chart_back__"; bd.className="help-backdrop"; document.body.appendChild(bd); }
    let pp=document.querySelector("#__chart_box__");
    if(!pp){
      pp=document.createElement("div"); pp.id="__chart_box__"; pp.className="help-pop";
      pp.innerHTML=`<div class="help-head"><div>グラフ</div><div class="help-close">×</div></div><div id="__chart_body__"></div>`;
      document.body.appendChild(pp);
    }
    pp.querySelector(".help-close").onclick=()=>{ pp.style.display="none"; bd.style.display="none"; };
    bd.onclick=()=>{ pp.style.display="none"; bd.style.display="none"; };
    return bd;
  }

  // charts (simple canvas)
  function drawBar(canvas, labels, values, title){
    const ctx = canvas.getContext("2d"), W = canvas.width, H = canvas.height, pad = 40;
    ctx.clearRect(0,0,W,H);
    ctx.fillStyle = "#000"; ctx.font = "14px system-ui"; ctx.fillText(title, pad, 24);
    ctx.strokeStyle = "#ccc"; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(W - pad, H - pad); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(pad, pad); ctx.stroke();
    const nums = values.map(v => (v === "" || v == null ? NaN : +v));
    const finite = nums.filter(Number.isFinite);
    if (!finite.length) { ctx.fillText("データなし（欠損）", pad, H/2); return; }
    const max = Math.max(1, ...finite);
    const bw  = (W - pad*2) / labels.length * 0.7;
    labels.forEach((lb, i) => {
      const v = Number.isFinite(nums[i]) ? nums[i] : 0;
      const x = pad + (i + 0.15) * (W - pad*2) / labels.length;
      const h = (H - pad*2) * (v / max);
      ctx.fillStyle = "#4a90e2";
      if (h > 0) ctx.fillRect(x, H - pad - h, bw, h);
      ctx.fillStyle = "#333"; ctx.font = "12px system-ui";
      ctx.fillText(lb, x, H - pad + 14);
      ctx.fillText(String(v), x, H - pad - h - 4);
    });
  }
  function drawLine(canvas, labels, values, title){
    const ctx = canvas.getContext("2d"), W = canvas.width, H = canvas.height, pad = 40;
    ctx.clearRect(0,0,W,H);
    ctx.fillStyle = "#000"; ctx.font = "14px system-ui"; ctx.fillText(title, pad, 24);
    ctx.strokeStyle = "#ccc"; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(W - pad, H - pad); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(pad, pad); ctx.stroke();
    const nums = values.map(v => (v === "" || v == null ? NaN : +v));
    const finite = nums.filter(Number.isFinite);
    if (!finite.length) { ctx.fillText("データなし（欠損）", pad, H/2); return; }
    const max = Math.max(1, ...finite);
    const min = Math.min(0, ...finite);
    const step = (W - pad*2) / Math.max(1, labels.length - 1);
    ctx.strokeStyle = "#4a90e2"; ctx.lineWidth = 2; ctx.beginPath();
    nums.forEach((v, i) => {
      const val = Number.isFinite(v) ? v : 0;
      const x = pad + i * step;
      const y = H - pad - (H - pad*2) * ((val - min) / (max - min || 1));
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
    if (labels.length === 1) { const x = pad; const val = Number.isFinite(nums[0]) ? nums[0] : 0;
      const y = H - pad - (H - pad*2) * ((val - min) / (max - min || 1));
      ctx.fillStyle = "#4a90e2"; ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI*2); ctx.fill(); }
    ctx.fillStyle = "#333"; ctx.font = "12px system-ui";
    labels.forEach((lb, i) => { const x = pad + i * step; ctx.fillText(lb, x - 10, H - pad + 14); });
  }

  function openStatsChart(){
    const back = ensureChartModal(), body = $("#__chart_body__");
    const rows = applyFilter(state.data);
    const rvolBuckets=["<1","1-2","2-3","3-5","5+"], rvolCnt=[0,0,0,0,0];
    const turnBuckets=["<5","5-10","10-50","50-100","100+"], turnCnt=[0,0,0,0,0];
    rows.forEach(r=>{
      const rvol = num(r["RVOL代金"]);
      if(!Number.isNaN(rvol)){ if(rvol<1)rvolCnt[0]++; else if(rvol<2)rvolCnt[1]++; else if(rvol<3)rvolCnt[2]++; else if(rvol<5)rvolCnt[3]++; else rvolCnt[4]++; }
      const turn = num(r["売買代金(億)"]);
      if(!Number.isNaN(turn)){ if(turn<5)turnCnt[0]++; else if(turn<10)turnCnt[1]++; else if(turn<50)turnCnt[2]++; else if(turn<100)turnCnt[3]++; else turnCnt[4]++; }
    });
    body.innerHTML = `<h3>傾向グラフ（表示中データ）</h3><canvas id="cv1" style="width:100%;height:340px;"></canvas><canvas id="cv2" style="width:100%;height:340px;margin-top:16px;"></canvas>`;
    const c1 = body.querySelector("#cv1"), c2 = body.querySelector("#cv2");
    const fit = ()=>{
      const cssW = body.clientWidth || 900, cssH = 340, dpr = window.devicePixelRatio || 1;
      [c1,c2].forEach(cv=>{
        cv.width = Math.floor(cssW*dpr); cv.height = Math.floor(cssH*dpr);
        cv.style.width = cssW+"px"; cv.style.height = cssH+"px";
        const ctx = cv.getContext("2d"); if (ctx && ctx.setTransform) ctx.setTransform(dpr,0,0,dpr,0,0);
      });
      drawBar(c1, rvolBuckets, rvolCnt, "RVOL代金の分布");
      drawBar(c2, turnBuckets, turnCnt, "売買代金(億)の分布");
    };
    if (window.__stats_fit__) window.removeEventListener("resize", window.__stats_fit__);
    window.__stats_fit__ = fit;
    window.addEventListener("resize", fit, { passive:true });
    fit();
    back.style.display="block";
    const box = document.querySelector("#__chart_box__"); box.style.display="block";
    const sy = window.scrollY||0; box.style.top = `${sy+80}px`;
    requestAnimationFrame(()=>{ box.style.left = `${Math.max(10,(document.documentElement.clientWidth - box.offsetWidth)/2)}px`; });
  }

  function openTrendChart(){
    const back = ensureChartModal(), body = $("#__chart_body__");
    let src = [];
    try { const RAW = JSON.parse(document.getElementById("__DATA__").textContent || "{}"); if (Array.isArray(RAW.hist) && RAW.hist.length) src = RAW.hist; } catch(_) {}
    if (!src.length) src = applyFilter(state.data);
    const byDay = new Map();
    src.forEach(r=>{
      const d = String(r["シグナル更新日"]||r["日付"]||r["日時"]||"").slice(0,10);
      if(!d) return;
      const hit = isHitRow(r) ? 1 : 0;
      const o = byDay.get(d) || {tot:0,hit:0};
      o.tot++; o.hit += hit;
      byDay.set(d,o);
    });
    const days = Array.from(byDay.keys()).sort();
    const rate = days.map(d=>{ const o = byDay.get(d)||{tot:0,hit:0}; return o.tot ? Math.round(1000*o.hit/o.tot)/10 : 0; });
    body.innerHTML = `<h3>推移グラフ（日別 当たり率 %）</h3><canvas id="cv3" style="width:100%;height:340px;"></canvas>`;
    const cv = body.querySelector("#cv3");
    const fit = ()=>{
      const cssW = body.clientWidth || 900, cssH = 340, dpr = window.devicePixelRatio || 1;
      cv.width = Math.floor(cssW*dpr); cv.height = Math.floor(cssH*dpr);
      cv.style.width = cssW+"px"; cv.style.height = cssH+"px";
      const ctx = cv.getContext("2d"); if (ctx && ctx.setTransform) ctx.setTransform(dpr,0,0,dpr,0,0);
      drawLine(cv, days, rate, "当たり率（%）");
    };
    if (window.__trend_fit__) window.removeEventListener("resize", window.__trend_fit__);
    window.__trend_fit__ = fit;
    window.addEventListener("resize", fit, {passive:true});
    fit();
    back.style.display="block";
    const box = document.querySelector("#__chart_box__"); box.style.display="block";
    const sy = window.scrollY||0; box.style.top = `${sy+80}px`;
    requestAnimationFrame(()=>{ box.style.left = `${Math.max(10,(document.documentElement.clientWidth - box.offsetWidth)/2)}px`; });
  }

  // events
  $("#perpage")?.addEventListener("change",(e)=>{ const v=parseInt(e.target.value,10); state.per=Number.isFinite(v)?v:500; state.page=1; render(); });
  $("#prev")?.addEventListener("click",()=>{ if(state.page>1){state.page--; render();} });
  $("#next")?.addEventListener("click",()=>{ state.page++; render(); });
  $("#q")?.addEventListener("input",(e)=>{ state.q=e.target.value||""; state.page=1; render(); });
  ["th_rate","th_turn","th_rvol","f_shodou","f_tei","f_both","f_rightup","f_early","f_etype","f_recstrong","f_smallpos","f_noshor","f_opratio","f_hit"]
    .forEach(id=>{ $("#"+id)?.addEventListener("input", ()=>{ state.page=1; render(); });
                   $("#"+id)?.addEventListener("change",()=>{ state.page=1; render(); }); });
  $("#btn-stats")?.addEventListener("click",openStatsChart);
  $("#btn-ts")?.addEventListener("click",openTrendChart);
  $("#f_defaultset")?.addEventListener("change",(e)=>applyDefaults(e.target.checked));

  // tabs
  function switchTab(to){
    state.tab = to;
    $$(".tab").forEach(el=>el.classList.add("hidden"));
    $$("nav a").forEach(a=>a.classList.remove("active"));
    if (to === "cand"){
      $("#tab-candidate")?.classList.remove("hidden");
      $("#lnk-cand")?.classList.add("active");
      state.data = DATA_CAND.slice();
      state.page = 1;
      render();
      return;
    }
    if (to === "tmr"){
      $("#tab-tmr")?.classList.remove("hidden");
      $("#lnk-tmr")?.classList.add("active");
      renderTomorrowWrapper();
      return;
    }
    if (to === "all"){
      $("#tab-all")?.classList.remove("hidden");
      $("#lnk-all")?.classList.add("active");
      state.page = 1;
      render();
      return;
    }
    if (to === "log"){
      $("#tab-log")?.classList.remove("hidden");
      $("#lnk-log")?.classList.add("active");
      const lb = $("#log-body");
      if (lb && !lb.getAttribute("data-inited")){
        lb.innerHTML = (DATA_LOG||[]).map(r=> `<tr><td>${r["日時"]||""}</td><td>${r["コード"]||""}</td><td>${r["種別"]||""}</td><td>${r["詳細"]||""}</td></tr>`).join("");
        lb.setAttribute("data-inited","1");
      }
      return;
    }
    if (to === "earn"){
      $("#tab-earn")?.classList.remove("hidden");
      $("#lnk-earn")?.classList.add("active");
      renderEarnings(DATA_EARN);
      return;
    }
    if (to === "preearn"){
      $("#tab-preearn")?.classList.remove("hidden");
      $("#lnk-preearn")?.classList.add("active");
      renderPreEarnings(DATA_PREEARN);
      return;
    }
  }
  (function(){
    const map = { "lnk-cand":"cand","lnk-tmr":"tmr","lnk-all":"all","lnk-log":"log","lnk-earn":"earn","lnk-preearn":"preearn" };
    Object.keys(map).forEach(id=>{
      const a = document.getElementById(id);
      if (!a) return;
      a.addEventListener("click", (e)=>{ e.preventDefault(); switchTab(map[id]); });
    });
  })();

  // initial（ここを差し替え）
  window.addEventListener("DOMContentLoaded", () => {
    // 1) タブ初期化（この時点でDOMが出来ているので描画される）
    switchTab("cand");
    forceClearThresholds();

    // 2) 1行表での <br> をスペースに置換（元の処理を統合）
    document.querySelectorAll('#tbl-candidate td br, #tbl-allcols td br')
      .forEach(br => br.replaceWith(' '));
  });


})();</script>

  <section id="tab-candidate" class="tab">
    <div class="tbl-wrap">
      <table id="tbl-candidate" class="tbl">
        <thead>
          <tr>
            <th class="sortable" data-col="コード" data-type="text">コード<span class="arrow"></span></th>
            <th class="sortable" data-col="銘柄名" data-type="text">銘柄<span class="arrow"></span></th>
            <th data-col="市場">市場</th>
            <th>Yahoo</th>
            <th>X</th>
            <th class="num sortable" data-col="現在値" data-type="num">現在値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値" data-type="num">前日終値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日円差" data-type="num">前日比(円)<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値比率" data-type="num">前日終値比率（％）<span class="arrow"></span></th>
            <th class="num sortable" data-col="出来高" data-type="num">出来高<span class="arrow"></span></th>
            <th class="num sortable" data-col="売買代金(億)" data-type="num">売買代金(億)<span class="arrow"></span></th>
            <th>財務</th>
<th class="num sortable" data-col="スコア" data-type="num">スコア<span class="arrow"></span></th>
<th class="num sortable" data-col="進捗率" data-type="num">進捗率<span class="arrow"></span></th>
            <th data-col="増資リスク">増資リスク</th>
            <th class="num sortable" data-col="増資スコア" data-type="num">増資スコア<span class="arrow"></span></th>
            <th class="reason-col" data-col="増資理由">理由</th>
            <th class="sortable" data-col="初動フラグ" data-type="text">初動<span class="arrow"></span></th>
            <th class="sortable" data-col="底打ちフラグ" data-type="text">底打ち<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩上がりフラグ" data-type="text">右肩<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期フラグ" data-type="text">早期<span class="arrow"></span></th>
            <th class="num sortable" data-col="右肩早期スコア" data-type="num">早期S<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期種別" data-type="text">右肩早期種別<span class="arrow"></span></th>
            <th class="sortable" data-col="判定" data-type="text">判定<span class="arrow"></span></th>
            <th class="sortable reason-col" data-col="判定理由" data-type="text">判定理由<span class="arrow"></span></th>
            <th class="sortable" data-col="推奨アクション" data-type="text">推奨<span class="arrow"></span></th>
            <th class="num sortable" data-col="推奨比率" data-type="num">推奨比率%<span class="arrow"></span></th>
            <th class="sortable" data-col="シグナル更新日" data-type="date">更新<span class="arrow"></span></th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section id="tab-tmr" class="tab hidden">
    <div class="tbl-wrap">
      <div id="tmr-label" style="margin:8px 0 4px;font-weight:800;font-size:16px;color:#0d3b66;"></div>
      <table id="tbl-tmr" class="tbl">
        <thead>
          <tr>
            <th class="sortable" data-col="コード" data-type="text">コード<span class="arrow"></span></th>
            <th class="sortable" data-col="銘柄名" data-type="text">銘柄<span class="arrow"></span></th>
            <th data-col="市場">市場</th>
            <th>Yahoo</th>
            <th>X</th>
            <th class="num sortable" data-col="現在値" data-type="num">現在値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値比率" data-type="num">前日終値比率（％）<span class="arrow"></span></th>
            <th class="num sortable" data-col="売買代金(億)" data-type="num">売買代金(億)<span class="arrow"></span></th>
            <th>財務</th>
            <th data-col="増資リスク">増資リスク</th>
            <th class="num sortable" data-col="増資スコア" data-type="num">増資スコア<span class="arrow"></span></th>
            <th class="reason-col" data-col="増資理由">理由</th>
            <th class="num sortable" data-col="右肩早期スコア" data-type="num">早期S<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期種別" data-type="text">右肩早期種別<span class="arrow"></span></th>
            <th class="sortable" data-col="判定" data-type="text">判定<span class="arrow"></span></th>
            <th class="sortable" data-col="推奨アクション" data-type="text">推奨<span class="arrow"></span></th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section id="tab-all" class="tab hidden">
    <div class="tbl-wrap">
      <table id="tbl-allcols" class="tbl">
        <thead><tr id="all-head"></tr></thead>
        <tbody id="all-body"></tbody>
      </table>
    </div>
  </section>

  <section id="tab-earn" class="tab hidden">
    <div class="tbl-wrap">
      <table id="tbl-earn" class="tbl">
        <thead>
          <tr>
            <th>銘柄</th>
            <th>センチメント</th>
            <th>タイトル</th>
            <th>時刻</th>
          </tr>
        </thead>
        <tbody id="earn-body"></tbody>
      </table>
     </div>
   </section>

  <section id="tab-preearn" class="tab hidden">
     <div class="tbl-wrap">
       <table id="tbl-preearn" class="tbl">
         <thead>
           <tr>
             <th>銘柄</th>
             <th class="num sortable" data-col="pre_score" data-type="num">pre_score<span class="arrow"></span></th>
             <th class="num sortable" data-col="edge_score" data-type="num">edge_score<span class="arrow"></span></th>
             <th class="num sortable" data-col="momentum_score" data-type="num">momentum_score<span class="arrow"></span></th>
             <th class="sortable reason-col" data-col="スコア理由" data-type="text">スコア理由<span class="arrow"></span></th>
             <th class="sortable" data-col="予測ヒント" data-type="text">予測ヒント<span class="arrow"></span></th>
             <th class="num sortable" data-col="期待株価" data-type="num">期待株価<span class="arrow"></span></th>
             <th class="sortable" data-col="修正見通し" data-type="text">修正見通し<span class="arrow"></span></th>
             <th class="sortable" data-col="過熱度" data-type="text">過熱度<span class="arrow"></span></th>
           </tr>
         </thead>
         <tbody id="preearn-body"></tbody>
       </table>
     </div>
  </section>

  {% if include_log %}
  <section id="tab-log" class="tab hidden">
    <div class="tbl-wrap">
      <table id="tbl-log" class="tbl">
        <thead>
          <tr>
            <th class="sortable" data-col="日時" data-type="date">日時<span class="arrow"></span></th>
            <th class="sortable" data-col="コード" data-type="text">コード<span class="arrow"></span></th>
            <th class="sortable" data-col="種別" data-type="text">種別<span class="arrow"></span></th>
            <th class="sortable" data-col="詳細" data-type="text">詳細<span class="arrow"></span></th>
          </tr>
        </thead>
        <tbody id="log-body"></tbody>
      </table>
    </div>
  </section>
  {% endif %}

</body>
</html>"""
# --- FUND settings ---
# fund script
FUND_SCRIPT = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\株探ファンダ.py"

# --- MARKER settings ---
# marker file
MARKER_FILE = Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\screen_data\last_funda.txt")

# ---  settings ---
# “決算系”だけを抽出するゆるいフィルタ
_DECISION_PAT = re.compile(r"(決算短信|四半期決算短信|通期決算|四半期報告書|有価証券報告書|業績予想|配当予想)")
# JST（日付判定を日本時間で行う）
_JST = dtm.timezone(dtm.timedelta(hours=9))
#  neg keys
_NEG_KEYS = [
    "下方修正", "下方", "減配", "特別損失", "業績予想の修正（減額）",
    "通期予想修正（減額）", "配当予想の修正（減額）",
]
# 簡易センチメント用キーワード
_POS_KEYS = [
    "上方修正", "上方", "増配", "自社株買い", "復配", "上期予想修正（増額）",
    "業績予想の修正（増額）", "通期予想修正（増額）", "配当予想の修正（増額）",
]

try:
    from plyer import notification
except Exception:
    class _DummyNoti:
        @staticmethod
        def notify(title="", message="", timeout=3):
            print(f"[NOTIFY] {title} - {message}")
    notification = _DummyNoti()

try:
    import workdays
except Exception:
    class _WorkdaysShim:
        @staticmethod
        def networkdays(start: dtm.date, end: dtm.date, holidays=None):
            if holidays is None: holidays = []
            if start > end: start, end = end, start
            d, cnt = start, 0
            while d <= end:
                if d.weekday() < 5 and d not in holidays:
                    cnt += 1
                d += dtm.timedelta(days=1)
            return cnt
        @staticmethod
        def workday(start: dtm.date, days: int, holidays=None):
            if holidays is None: holidays = []
            step = 1 if days >= 0 else -1
            d, moved = start, 0
            while moved < abs(days):
                d += dtm.timedelta(days=step)
                if d.weekday() < 5 and d not in holidays:
                    moved += 1
            return d
    workdays = _WorkdaysShim()

# -*- coding: utf-8 -*-
"""
自動スクリーニング_完全統合版 + 右肩上がり（Template版/両立フィルタ/Gmail/オフラインHTML/祝日対応/MIDDAY自動）

修正点（この版）
- HTML出力フェーズの JSON 生成で、DataFrame 内の bytes / NaN / pandas.Timestamp / NumPy スカラーを
  安全に変換できるように修正（TypeError: bytes is not JSON serializable 対策）

機能ダイジェスト
- EOD/MIDDAY 自動判定（JST 11:30–12:30 は MIDDAY スナップショット、それ以外は EOD）
- 祝日/土日スキップ（jpholiday + 追加休場日ファイル対応）
- yahooquery で quotes / history を一括取得（初回は 12mo、通常は 10d）
- 初動/底打ち/上昇余地スコア/右肩上がりスコア の判定とログ（signals_log）
- 前営業日の翌日検証（判定とCSV出力）
- オフライン1ファイルHTMLダッシュボード（候補一覧/検証/全カラム/price_history/signals_log）
- Gmail で index.html を送信（任意、ZIP同梱可）

前提: Python 3.11 / pip install yahooquery pandas jpholiday
"""

def run_karauri_script():
    """Node.js の puppeteer スクリプトを起動して、空売り無しリストを更新する"""
    if not os.path.exists(KARAURI_PY_PATH):
        print(f"[karauri]  スクリプトが見つかりません: {KARAURI_PY_PATH}")
        return

    try:
        print("[karauri] 空売り無しリスト抽出を開始...")
        # python 実行。スキップ条件（日付チェック）は JS 側に組み込み済み
        subprocess.run(["python", KARAURI_PY_PATH], check=True)
        print("[karauri] 抽出処理 完了")
    except subprocess.CalledProcessError as e:
        print(f"[karauri][WARN] スクリプト実行に失敗しました: {e}")
    except Exception as e:
        print(f"[karauri][WARN] 予期せぬエラー: {e}")

# --- EDINET 取得で使う ---

# =====================================

# 速度チューニング
YQ_MAX_WORKERS = 16

# ===== フェイルセーフ =====

warnings.simplefilter(action="ignore", category=FutureWarning)

# ===== ユーティリティ =====
def ffloat(x, default=None):
    try:
        return default if pd.isna(x) else float(x)
    except Exception:
        try:
            return float(str(x))
        except Exception:
            return default

def fint(x, default=None):
    try:
        if pd.isna(x):
            return default
        if isinstance(x, (int,)) and not isinstance(x, bool):
            return int(x)
        if isinstance(x, float):
            return int(x)
        return int(float(str(x)))
    except Exception:
        return default

def today_str():
    return dtm.datetime.now().strftime("%Y-%m-%d")
    
# ================== 表示整形ヘルパ ==================

def _safe_jsonable(val):
    """
    JSONに安全に落とし込むための変換（bytes, NaN, Timestamp 等を処理）
    """

    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8", errors="ignore")
        except Exception:
            return str(val)
    # pandas/NumPyの欠損
    if (isinstance(val, float) and (math.isnan(val))) or (hasattr(pd, "isna") and pd.isna(val)):
        return None
    if isinstance(val, (np.floating, np.integer)):
        return val.item()
    # 日付・日時
    if isinstance(val, (pd.Timestamp, dtm.datetime, dtm.date, dtm.time)):
        return str(val)[:19]
    return val

# ===== 祝日判定 =====
def _load_extra_closed(path: str):
    s = set()
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    s.add(line[:10])
    except Exception:
        pass
    return s

def is_jp_market_holiday(d: dtm.date, extra_closed: set = None) -> bool:
    if d.weekday() >= 5:
        return True
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day in (2, 3)):
        return True
    if extra_closed and d.strftime("%Y-%m-%d") in extra_closed:
        return True
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return True
    except Exception:
        pass
    return False

def next_business_day_jp(d: dtm.date, extra_closed: set = None) -> dtm.date:
    cur = d
    while True:
        cur += dtm.timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

def prev_business_day_jp(d: dtm.date, extra_closed: set = None) -> dtm.date:
    cur = d
    while True:
        cur -= dtm.timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

# ===== DB =====
def open_conn(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA cache_size=-200000;")
    return conn

def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, decl: str):
    """Schema fixed: no-op."""
    return

def phase_csv_import(conn, csv_path=None, overwrite_registered_date=False):

    """
    CSV から「コード・銘柄名・市場・登録日」のみを取り込む固定スキーマ実装。
    - スキーマ操作（PRAGMA/ALTER/CREATE）は一切行わない
    - overwrite_registered_date=False の場合、既存登録日が空/NULLのときのみ上書き
    """
    path = csv_path or CSV_INPUT_PATH
    if not os.path.isfile(path):
        print("CSVがないのでスキップ:", path)
        return

    df = pd.read_csv(path, encoding="utf8", sep=",", engine="python").astype(str)
    needed = ["コード", "銘柄名", "市場", "登録日"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"[csv-import] CSVに必須列がありません: {missing}")

    df = df[needed].copy()
    df["コード"] = df["コード"].astype(str).str.strip().str.zfill(4)
    df["銘柄名"] = df["銘柄名"].astype(str).str.strip()
    df["市場"]   = df["市場"].astype(str).str.strip()
    df["登録日"] = df["登録日"].where(df["登録日"].notna() & (df["登録日"].str.strip() != ""), None)

    cur = conn.cursor()
    if overwrite_registered_date:
        sql = """
        INSERT INTO screener(コード, 銘柄名, 市場, 登録日)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(コード) DO UPDATE SET
          銘柄名 = excluded.銘柄名,
          市場   = excluded.市場,
          登録日 = excluded.登録日
        """
    else:
        sql = """
        INSERT INTO screener(コード, 銘柄名, 市場, 登録日)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(コード) DO UPDATE SET
          銘柄名 = excluded.銘柄名,
          市場   = excluded.市場,
          登録日 = CASE
                     WHEN screener.登録日 IS NULL OR screener.登録日 = '' THEN excluded.登録日
                     ELSE screener.登録日
                   END
        """
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()

    print(f"[csv-import] 取り込み完了: {len(rows)}件（コード/銘柄名/市場/登録日のみ反映, overwrite_registered_date={overwrite_registered_date})")
def phase_delist_cleanup(conn: sqlite3.Connection,
                         master_csv_path: str = MASTER_CODES_PATH,
                         also_clean_notes: bool = False) -> None:
    """
    マスタCSV(列名: コード)に存在しない銘柄コードを screener から削除する。
    also_clean_notes=True の場合は finance_notes も同様に削除する。
    """

    if not os.path.isfile(master_csv_path):
        print("上場廃止の基準CSVが見つからないためスキップ:", master_csv_path)
        return

    def _norm(code) -> str | None:
        try:
            return f"{int(str(code).strip()):04d}"
        except Exception:
            return None

    # マスタ側の有効コード集合（4桁ゼロ埋めで正規化）
    master = pd.read_csv(master_csv_path, encoding="utf8", sep=",", engine="python")
    valid = {c for c in ( _norm(x) for x in master["コード"] ) if c is not None}
    if not valid:
        print("マスタ側の有効コードが0件のためスキップ:", master_csv_path)
        return

    cur = conn.cursor()

    # DB内コードを取得して正規化
    cur.execute("SELECT コード FROM screener")
    rows = cur.fetchall()
    targets = []
    for (db_code,) in rows:
        n = _norm(db_code)
        if n is None or n not in valid:
            targets.append((db_code,))

    if not targets:
        print("上場廃止による削除対象はありません。")
        cur.close()
        return

    # 削除（まずは screener）
    print(f"上場廃止による削除: {len(targets)} 件")
    cur.executemany("DELETE FROM screener WHERE コード = ?", targets)

    # オプション: finance_notes も掃除
    if also_clean_notes:
        cur.executemany("DELETE FROM finance_notes WHERE コード = ?", targets)

    conn.commit()
    cur.close()

# ===== 任意：空売り無し反映 =====
def phase_mark_karauri_nashi(conn: sqlite3.Connection):
    if not os.path.isfile(KARA_URI_NASHI_PATH):
        print("空売り無しリストが見つからないためスキップ:", KARA_URI_NASHI_PATH)
        return
    df = pd.read_csv(KARA_URI_NASHI_PATH, encoding="utf8", sep=",", engine="python")
    rows = [(getattr(row, 'コード'),) for row in df.itertuples()]
    try:
        exec_many(conn, 'UPDATE screener SET 空売り機関="なし" WHERE コード=?', rows, chunk=500)
    except NameError:
        import sqlite3
        _tmp_conn = sqlite3.connect(DB_PATH)
        try:
            exec_many(_tmp_conn, 'UPDATE screener SET 空売り機関="なし" WHERE コード=?', rows, chunk=500)
        finally:
            _tmp_conn.close()

    # 派生指標更新を実行
    phase_shortterm_enhancements(conn)
# 必要: pip install yfinance pandas

def _latest2_ok(conn: sqlite3.Connection, code: str) -> bool:
    """
    price_historyから直近2行を取り、前日終値が有効ならTrue。
    - 行数が2未満 → False
    - 前日終値がNaN/0 → False
    """
    df = pd.read_sql_query(
        "SELECT 日付, 終値 FROM price_history WHERE コード=? ORDER BY 日付 DESC LIMIT 2",
        conn, params=(str(code),)
    )
    if df.shape[0] < 2:
        return False
    prev_close = df.iloc[1]["終値"]
    try:
        return (prev_close is not None) and (not pd.isna(prev_close)) and float(prev_close) != 0.0
    except Exception:
        return False

def list_insufficient_codes(conn: sqlite3.Connection, universe_codes=None) -> list[str]:
    """
    直近2営業日の終値が揃っていない（=前日終値比率が計算できない）銘柄を列挙。
    universe_codes を省略すると screener 全件を対象にする。
    """
    if universe_codes is None:
        cur = conn.cursor()
        cur.execute("SELECT コード FROM screener")
        universe_codes = [str(r[0]) for r in cur.fetchall()]
        cur.close()

    bad = []
    for c in universe_codes:
        if not _latest2_ok(conn, c):
            bad.append(str(c))
    return bad

def refresh_full_history_for_insufficient(conn: sqlite3.Connection, universe_codes=None, batch_size: int = 200) -> list[str]:
    """
    「データ不足（直近2日そろわず）」な銘柄だけを抽出し、初回だけ 12ヶ月 を取り直して upsert。
    処理後に _update_screener_from_history で前日終値比率などを再計算する。
    戻り値: 再取得を行った銘柄コードのリスト
    """
    # 1) 対象抽出
    targets = list_insufficient_codes(conn, universe_codes)
    if not targets:
        print("[full-refresh] 不足銘柄なし")
        return []

    print(f"[full-refresh] 12mo 取り直し対象: {len(targets)} 件")
    total_added = 0

    # 2) yfinance で 12mo を取得して price_history に upsert
    for i in range(0, len(targets), batch_size):
        chunk = targets[i:i+batch_size]
        tickers_map = {c: f"{c}.T" for c in chunk}   # 日本株想定
        try:
            df_wide = yf.download(
                list(tickers_map.values()),
                period="12mo", interval="1d",
                group_by="ticker", threads=True, auto_adjust=False
            )
        except Exception as e:
            print(f"[full-refresh][WARN] download失敗: {e}  chunk先頭={chunk[0] if chunk else ''}")
            continue

        df_add = _to_long_history(df_wide, tickers_map)  # 既存の整形関数を流用:contentReference[oaicite:1]{index=1}
        added = _upsert_price_history(conn, df_add)      # 既存のupsertを流用:contentReference[oaicite:2]{index=2}
        total_added += added
        print(f"[full-refresh] {i+len(chunk)}/{len(targets)} (+{added} rows)")

    # 3) screener の 現在値/前日終値比率/出来高 を再計算して反映
    _update_screener_from_history(conn, targets)         # 既存の更新関数を流用:contentReference[oaicite:3]{index=3}

    print(f"[full-refresh] 追記 {total_added} 行 / 再取得 {len(targets)} 銘柄")
    return targets

def _codes_with_data(conn):
    q = "SELECT DISTINCT コード FROM price_history"
    return {row[0] for row in conn.execute(q).fetchall()}

def _to_long_history(df_wide: pd.DataFrame, codes_map) -> pd.DataFrame:
    """
    yf.download の戻り(MultiIndex列)をロング形式にする。
    codes_map: { '7203': '7203.T', ... } 逆引きに使う。
    """
    if df_wide is None or df_wide.empty:
        return pd.DataFrame(columns=["日付","コード","始値","高値","安値","終値","出来高"])
    # 単一銘柄のときは列がMultiIndexではない場合がある
    if isinstance(df_wide.columns, pd.MultiIndex):
        df = df_wide.stack(level=0).reset_index()  # Date, Ticker, [Open,High,Low,Close,Adj Close,Volume]
        df.rename(columns={"level_1":"Ticker","Date":"日付"}, inplace=True)
        df["コード"] = df["Ticker"].map({v:k for k,v in codes_map.items()})
    else:
        # 1銘柄のみ
        df = df_wide.reset_index().copy()
        df["Ticker"] = list(codes_map.values())[0]
        code = list(codes_map.keys())[0]
        df["コード"] = code
        df.rename(columns={"Date":"日付"}, inplace=True)

    df = df.rename(columns={
        "Open":"始値","High":"高値","Low":"安値","Close":"終値","Volume":"出来高"
    })
    # 欲しい列だけ、欠損行は落とす
    cols = ["日付","コード","始値","高値","安値","終値","出来高"]
    df = df[cols].dropna(subset=["日付","終値"])

    # 日付→date
    df["日付"] = pd.to_datetime(df["日付"]).dt.date
    # 重複除去（同一日・同一コード）
    df = df.drop_duplicates(subset=["コード","日付"], keep="last")
    return df.sort_values(["コード","日付"])

def _upsert_price_history(conn, df_add: pd.DataFrame) -> int:
    if df_add is None or df_add.empty:
        return 0
    cur = conn.cursor()
    cur.executemany(
        "DELETE FROM price_history WHERE コード=? AND 日付=?",
        [(r["コード"], r["日付"]) for _, r in df_add.iterrows()]
    )
    conn.commit()
    df_add.to_sql("price_history", conn, if_exists="append", index=False)
    return len(df_add)

def _update_screener_from_history(conn, codes):
    """
    price_history の直近2日から
    現在値 / 前日終値 / 前日円差 / 前日終値比率(％) / 出来高 を更新する。
    すべて小数2桁（％含む）でDB保存する。
    """
    def _r2(x):
        try:
            return None if x is None else round(float(x), 2)
        except Exception:
            return None

    updated = []
    for code in codes:
        df = pd.read_sql_query(
            "SELECT 日付, 終値, 出来高 FROM price_history WHERE コード=? ORDER BY 日付 DESC LIMIT 2",
            conn, params=(str(code),)
        )
        if df.empty:
            continue
        today = df.iloc[0]
        close_t = today["終値"]
        vol_t   = today["出来高"]

        prev = df.iloc[1]["終値"] if len(df) >= 2 else None
        yen = pct = None
        if prev is not None and pd.notna(prev) and float(prev) != 0.0:
            yen = float(close_t) - float(prev)
            pct = yen / float(prev) * 100.0

        updated.append((
            _r2(close_t),                        # 現在値 → 2桁
            _r2(prev),                           # 前日終値 → 2桁
            _r2(yen),                            # 前日円差 → 2桁
            _r2(pct),                            # 前日終値比率(％) → 2桁
            int(vol_t) if pd.notna(vol_t) else None,  # 出来高
            dtm.datetime.today().strftime("%Y-%m-%d"),
            str(code),
        ))

    if updated:
        cur = conn.cursor()
        cur.executemany("""
            UPDATE screener
               SET 現在値=?,
                   前日終値=?,
                   前日円差=?,
                   前日終値比率=?,
                   出来高=?,
                   シグナル更新日=?
             WHERE コード=?
        """, updated)
        conn.commit()

def phase_yahoo_bulk_refresh(conn, codes, batch_size=200):
    """
    高速版:
      - 既存銘柄: period="2d", interval="1d" をバルクで取得し、差分だけ upsert
      - 未収録銘柄: period="12mo" をバルクで取得して初期投入
      - screener は price_history の直近2日から 前日終値比率/出来高/現在値 を更新
      - 時価総額は速度優先で更新しない（必要なら別フェーズで）
    """
    codes = [str(c) for c in codes]
    have = _codes_with_data(conn)
    exist_codes = [c for c in codes if c in have]
    new_codes   = [c for c in codes if c not in have]

    total_added = 0

    # 1) 既存銘柄: 2日分だけ一括取得（バッチ分割）
    for i in range(0, len(exist_codes), batch_size):
        chunk = exist_codes[i:i+batch_size]
        tickers_map = {c: f"{c}.T" for c in chunk}  # 日本株前提
        df_wide = yf.download(list(tickers_map.values()), period="2d", interval="1d", group_by="ticker", threads=True, auto_adjust=False)
        df_add = _to_long_history(df_wide, tickers_map)
        total_added += _upsert_price_history(conn, df_add)
        print(f"[refresh/exist] {i+len(chunk)}/{len(exist_codes)} (+{len(df_add)} rows)")

    # 2) 新規銘柄: 12ヶ月ぶんを一括取得（バッチ分割）
    for i in range(0, len(new_codes), batch_size):
        chunk = new_codes[i:i+batch_size]
        tickers_map = {c: f"{c}.T" for c in chunk}
        df_wide = yf.download(list(tickers_map.values()), period="12mo", interval="1d", group_by="ticker", threads=True, auto_adjust=False)
        df_add = _to_long_history(df_wide, tickers_map)
        total_added += _upsert_price_history(conn, df_add)
        print(f"[refresh/new ] {i+len(chunk)}/{len(new_codes)} (+{len(df_add)} rows)")

    # 3) screener 更新（price_history 由来）
    _update_screener_from_history(conn, codes)
    apply_auto_metrics_eod(conn)
    apply_composite_score(conn)
    print(f"[refresh] 追記 {total_added} 行 / 銘柄 {len(codes)} 件（既存{len(exist_codes)}・新規{len(new_codes)}）")

# ==== 時価総額取得

# ===== 全銘柄の時価総額を一括更新 =====

def _to_symbol(c: str) -> str:
    s = str(c).strip()
    return s if "." in s else s + ".T"   # 4桁数字コード想定：.T 付与

def _normalize_map(obj):
    """
    yahooquery.YQ(...).summary_detail / price の戻りを
    {symbol: { ... }} 形式の dict に正規化。文字列はスキップ。
    """
    if isinstance(obj, dict):
        # まれに '7203.T': 'Not Found' みたいな文字列が入るので弾く
        return {k: v for k, v in obj.items() if not isinstance(v, str)}
    if isinstance(obj, pd.DataFrame):
        if 'symbol' in obj.columns:
            d = obj.set_index('symbol').to_dict(orient='index')
            # 値が文字列の行は弾く
            return {k: v for k, v in d.items() if not isinstance(v, str)}
        try:
            d = obj.to_dict(orient='index')
            return {k: v for k, v in d.items() if not isinstance(v, str)}
        except Exception:
            return {}
    return {}

def _extract_mcap(entry):
    """
    entry から marketCap を float に取り出す。
    entry が dict/Series/list/str など何が来ても安全に None 返し。
    """
    # dict 以外を可能な限り dict 化（Series, list[dict] 等）
    if isinstance(entry, pd.Series):
        entry = entry.to_dict()
    elif isinstance(entry, (list, tuple)):
        entry = entry[0] if entry and isinstance(entry[0], dict) else {}
    elif isinstance(entry, str) or entry is None:
        return None
    elif not isinstance(entry, dict):
        entry = {}

    v = entry.get('marketCap')
    if v is None:
        return None
    if isinstance(v, dict):           # {'raw': 123..., 'fmt': '...'} 形式
        v = v.get('raw') or v.get('fmt') or v.get('longFmt')
    try:
        return float(str(v).replace(',', ''))
    except Exception:
        return None

def update_market_cap_all(conn, batch_size=300, max_workers=8):
    """
    全銘柄の時価総額（億円）を高速に更新。
    - yahooqueryでまとめ取り（非同期）
    - .T の日本株は JPY想定 → 億円に変換 (mcap/1e8)
    """
    codes = [str(r[0]) for r in conn.execute("SELECT コード FROM screener").fetchall()]
    if not codes:
        print("[mcap] 対象なし")
        return

    updated_total = 0
    try:
        for i in range(0, len(codes), batch_size):
            chunk = codes[i:i+batch_size]
            symbols = [_to_symbol(c) for c in chunk]

            tq = YQ(symbols, asynchronous=True, max_workers=max_workers)
            sd = _normalize_map(tq.summary_detail)
            pr = _normalize_map(tq.price)

            rows = []
            for sym in symbols:
                entry_sd = sd.get(sym)
                mcap = _extract_mcap(entry_sd)
                if mcap is None:
                    entry_pr = pr.get(sym)
                    mcap = _extract_mcap(entry_pr)
                if mcap is None:
                    continue
                mcap_oku = mcap / 1e8  # 円建て想定（.T）
                code = sym.split('.', 1)[0]
                rows.append((None if mcap_oku is None else round(mcap_oku, 2), code))
                
            if rows:
                cur = conn.cursor()
                cur.executemany("UPDATE screener SET 時価総額億円=? WHERE コード=?", rows)
                conn.commit(); cur.close()
                updated_total += len(rows)
            print(f"[mcap/all] {i+len(chunk)}/{len(codes)} 更新 {len(rows)} 件")

        print(f"[mcap] 合計更新 {updated_total} 件（全銘柄）")

    except ImportError:
        # フォールバック（遅い）：必要時のみ
        print("[mcap] yahooquery未導入 → yfinance fast_info にフォールバック（遅い）")
        rows = []
        for c in codes:
            try:
                fi = yf.YQ(_to_symbol(c)).fast_info
                mc = getattr(fi, "market_cap", None)
                if mc:
                    rows.append((float(mc)/1e8, c))
            except Exception:
                pass
        if rows:
            cur = conn.cursor()
            cur.executemany("UPDATE screener SET 時価総額億円=? WHERE コード=?", rows)
            conn.commit(); cur.close()
        print(f"[mcap/yf] 更新 {len(rows)} 件（フォールバック）")

# ===== Yahoo（MIDDAY スナップショット） =====

def r2(x):
    """小数点2桁に丸め（None安全）"""
    try:
        return None if x is None else round(float(x), 2)
    except Exception:
        return None

def phase_yahoo_intraday_snapshot(conn: sqlite3.Connection):
    cur = conn.cursor()
    if MIDDAY_FILTER_BY_FLAGS:
        cur.execute("""
            SELECT コード FROM screener
               OR (時価総額億円 BETWEEN 50 AND 5000)
        """)
    else:
        cur.execute("SELECT コード FROM screener")
    codes = [str(r[0]) for r in cur.fetchall()]
    cur.close()

    if not codes:
        print("対象コードなし：intradayスナップショットスキップ")
        return
    if TEST_MODE:
        codes = codes[:TEST_LIMIT]
        print(f"[TEST] {len(codes)}銘柄(MIDDAY)に絞って実行")

    symbols_all = [f"{c}.T" for c in codes]
    print(f"[MIDDAY] quotes取得: {len(symbols_all)}銘柄")
    today = today_str()

    up_screener, up_hist = [], []
    batch = YQ_BATCH_MID
    for i in range(0, len(symbols_all), batch):
        symbols = symbols_all[i:i+batch]
        t = YQ(symbols, max_workers=YQ_MAX_WORKERS)
        quotes = t.quotes if isinstance(t.quotes, dict) else {}

        for sym in symbols:
            q = quotes.get(sym) or {}
            code = sym.replace(".T", "")

            last = ffloat(q.get("regularMarketPrice"), None)
            prev_api = ffloat(q.get("regularMarketPreviousClose"), None)

            # DB優先、無ければAPIフォールバック
            prev = get_prev_close_db_first(conn, code, quotes_prev=prev_api)

            yen = pct = None
            if last is not None and prev is not None and prev != 0:
                yen = last - prev
                pct = yen / prev * 100.0

            vol  = fint(q.get("regularMarketVolume"), 0)
            mcap = ffloat(q.get("marketCap"), 0.0)
            zika_oku = None if not mcap else round(mcap / 100_000_000.0, 2)

            # ← tupleの順序を変更：前日終値・前日円差・前日終値比率を全部入れる
            # これに置換
            up_screener.append((
                None if last is None else round(float(last), 2),   # 現在値 2桁
                None if prev is None else round(float(prev), 2),   # 前日終値 2桁
                None if yen  is None else round(float(yen),  2),   # 前日円差 2桁
                None if pct  is None else round(float(pct),  2),   # 前日終値比率(％) 2桁
                int(vol or 0),                                     # 出来高
                zika_oku,                                          # 時価総額億円（元実装のまま）
                today,
                code
            ))

            o1 = ffloat(q.get("regularMarketOpen"), None)
            h1 = ffloat(q.get("regularMarketDayHigh"), None)
            l1 = ffloat(q.get("regularMarketDayLow"), None)
            c1 = last
            # これに置換
            up_hist.append((
                code, today,
                None if o1 is None else round(float(o1), 2),
                None if h1 is None else round(float(h1), 2),
                None if l1 is None else round(float(l1), 2),
                None if c1 is None else round(float(c1), 2),
                int(vol or 0)
            ))

        time.sleep(YQ_SLEEP_MID)

    if up_screener:
        cur = conn.cursor()
        cur.executemany(
            "UPDATE screener SET "
            "現在値=ROUND(?,2), 前日終値=ROUND(?,2), 前日円差=ROUND(?,2), 前日終値比率=ROUND(?,2), "
            "出来高=?, 時価総額億円=?, 更新日=? WHERE コード=?",
            up_screener
        )

        conn.commit()
        cur.close()

    if up_hist:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO price_history(コード,日付,始値,高値,安値,終値,出来高)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(コード,日付) DO UPDATE SET
              始値=COALESCE(excluded.始値, 始値),
              高値=COALESCE(excluded.高値, 高値),
              安値=COALESCE(excluded.安値, 安値),
              終値=COALESCE(excluded.終値, 終値),
              出来高=COALESCE(excluded.出来高, 出来高)
        """, up_hist)
        conn.commit()
        apply_auto_metrics_midday(conn, use_time_progress=True)
        apply_composite_score(conn)
        cur.close()

# ===== 派生指標の更新 =====

def phase_snapshot_shodou_baseline(conn):
    """
    初動フラグ='候補' で、まだ基準が未設定(初動株価/初動出来高 が NULL)の銘柄に対して、
    その時点の 現在値/出来高 をスナップショットして基準化する。
    ・CSVは一切参照しない
    ・倍率は 1.0 で初期化
    """
    cur = conn.cursor()
    # 候補 かつ 基準が未設定のものを抽出
    cur.execute("""
        SELECT コード, 現在値, 出来高
        FROM screener
        WHERE 初動フラグ='候補'
          AND (初動株価 IS NULL OR 初動出来高 IS NULL)
          AND 現在値 IS NOT NULL
    """)
    rows = cur.fetchall()

    updates = []
    for code, now_price, now_vol in rows:
        try:
            ip = float(now_price)
            iv = int(now_vol) if now_vol is not None else None
        except Exception:
            continue
        if ip is None:
            continue
        # 出来高が取れない時は 0 扱いでOK（倍率計算時は0除算を避ける）
        iv = iv or 0
        updates.append((ip, 1.0, iv, 1.0, code))

    if updates:
        cur.executemany("""
            UPDATE screener
               SET 初動株価=?,
                   初動株価倍率=?,
                   初動出来高=?,
                   初動出来高倍率=?
             WHERE コード=?
        """, updates)
        conn.commit()
        print(f"[shodou-baseline] snapshotted {len(updates)} symbols")
    else:
        print("[shodou-baseline] no new baseline")
    cur.close()

def phase_update_shodou_multipliers(conn):
    """
    既に基準(初動株価/初動出来高)がある銘柄の倍率を、最新の 現在値/出来高 から再計算して反映。
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 現在値, 出来高, 初動株価, 初動出来高
        FROM screener
        WHERE 初動株価 IS NOT NULL OR 初動出来高 IS NOT NULL
    """)
    rows = cur.fetchall()

    updates = []
    for code, now_price, now_vol, base_price, base_vol in rows:
        try:
            cp = float(now_price) if now_price is not None else None
            bp = float(base_price) if base_price is not None else None
            cv = int(now_vol) if now_vol is not None else None
            bv = int(base_vol) if base_vol is not None else None
        except Exception:
            continue

        # 価格倍率
        mul_price = None
        if cp is not None and bp not in (None, 0):
            mul_price = cp / bp

        # 出来高倍率
        mul_vol = None
        if cv is not None and bv not in (None, 0):
            mul_vol = cv / bv

        if mul_price is not None or mul_vol is not None:
            updates.append((
                (mul_price if mul_price is not None else None),
                (mul_vol if mul_vol is not None else None),
                code
            ))

    if updates:
        cur.executemany("""
            UPDATE screener
               SET 初動株価倍率 = COALESCE(?, 初動株価倍率),
                   初動出来高倍率 = COALESCE(?, 初動出来高倍率)
             WHERE コード=?
        """, updates)
        conn.commit()
        print(f"[shodou-mults] updated {len(updates)} symbols")
    else:
        print("[shodou-mults] no updates")
    cur.close()

def phase_derive_update(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT コード, 初動株価, 現在値, UP継続回数, DOWN継続回数, 登録日, UPDOWN, 出来高, 時価総額億円 FROM screener")
    rows = cur.fetchall()
    d_today = today_str()
    cal_today = dtm.date.today()
    holidays = []

    for (code, initial_price, current_price, up_con, down_con, regist_date, db_updown, db_volume, zika_oku) in rows:
        up_con = int(up_con or 0)
        down_con = int(down_con or 0)

        cur2 = conn.cursor()
        cur2.execute("SELECT 前日終値比率 FROM screener WHERE コード=?", (code,))
        r = cur2.fetchone()
        cur2.close()
        db_zenhi = float(r[0]) if r and r[0] is not None else 0.0

        try:
            db_reg = date.fromisoformat(str(regist_date))
        except Exception:
            db_reg = cal_today
        last_date = workdays.workday(db_reg, 10, holidays)
        diff_days = max(0, (last_date - db_reg).days)
        eigyo_sabun = workdays.networkdays(db_reg, cal_today, holidays)

        updown = "同値"
        if db_zenhi > 0:
            up_con += 1; down_con = 0
            if db_updown and db_updown.startswith("↑") and db_updown[1:].isdigit():
                updown = f"↑{int(db_updown[1:]) + 1}"
            elif db_updown == "↑":
                updown = "↑2"
            else:
                updown = "↑"
        elif db_zenhi < 0:
            down_con += 1; up_con = 0
            if db_updown and db_updown.startswith("↓") and db_updown[1:].isdigit():
                updown = f"↓{int(db_updown[1:]) + 1}"
            elif db_updown == "↓":
                updown = "↓2"
            else:
                updown = "↓"

        try:
            z = float(zika_oku or 0)
        except Exception:
            z = 0.0
        if z >= 100:
            maru = f"組入済?:{int(z)}"
        elif 90 < z < 100:
            maru = f"組入期待:{int(z)}"
        else:
            maru = f"組入前?:{int(z)}"

        cur2 = conn.cursor()
        cur2.execute(
            "UPDATE screener SET 機関組入時価総額=?, 残日=?, 経過日数=?, UPDOWN=?, UP継続回数=?, DOWN継続回数=?, 更新日=? WHERE コード=?",
            (maru, diff_days, eigyo_sabun, updown, up_con, down_con, d_today, code),
        )
        conn.commit()
        cur2.close()

        try:
            ip = float(initial_price) if initial_price is not None else None
            cp = float(current_price) if current_price is not None else None
            if ip is not None and cp is not None and eigyo_sabun is not None:
                if cp < ip and eigyo_sabun > 14:
                    cur2 = conn.cursor()
                    cur2.execute(
                        'UPDATE screener SET 初動検知成功=?, 初動株価=NULL, 初動株価倍率=NULL, 初動出来高=NULL, 初動出来高倍率=NULL, UP継続回数=0 WHERE コード=?',
                        ("失敗または未検知", code),
                    )
                    conn.commit()
                    cur2.close()
        except Exception:
            pass
    cur.close()

# ===== シグナル判定（初動/底打ち/上昇余地/Migikata） =====

def _pivot_ratio_higher_lows(high: pd.Series, low: pd.Series, win=5):
    """5日窓などで谷(安値)のピボットを取り、連続して切り上げている比率を返す"""
    n = len(low)
    if n < win*2+1: return 0.0
    is_trough = []
    for i in range(win, n-win):
        if low.iloc[i] == low.iloc[i-win:i+win+1].min():
            is_trough.append(i)
    if len(is_trough) < 2: return 0.0
    cnt = 0
    for a,b in zip(is_trough, is_trough[1:]):
        if low.iloc[b] > low.iloc[a]: cnt += 1
    return cnt / max(1, (len(is_trough)-1))

def _trend_metrics_df(g: pd.DataFrame):
    """必要メトリクスをまとめて計算"""
    px = g["終値"].astype(float).copy()
    hi = (g["高値"] if "高値" in g else g["終値"]).astype(float)
    lo = (g["安値"] if "安値" in g else g["終値"]).astype(float)

    # 1) 回帰（log終値 ~ 日数）
    y = np.log(px.values)
    x = np.arange(len(px), dtype=float)
    b1, b0 = np.polyfit(x, y, 1)
    y_hat = b0 + b1*x
    ss_res = np.sum((y - y_hat)**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r2 = 1 - (ss_res/ss_tot) if ss_tot > 0 else 0.0
    slope_ann = np.exp(b1*252) - 1.0  # 年率換算

    # 2) MAs
    s20  = px.rolling(20,  min_periods=20).mean()
    s50  = px.rolling(50,  min_periods=50).mean()
    s100 = px.rolling(100, min_periods=100).mean()

    # 直近RIBBON_KEEP_DAYSで 20>50>100 を維持した日の比率
    ribbon_days = min(RIBBON_KEEP_DAYS, len(px))
    rib_ok = 0
    for i in range(len(px)-ribbon_days, len(px)):
        if i >= 100 and s20.iloc[i] > s50.iloc[i] > s100.iloc[i]:
            rib_ok += 1
    ribbon_ratio = rib_ok / max(1, ribbon_days)

    # SMA50の上にいた日比率（窓内全体）
    above50_ratio = float((px > s50).sum()) / max(1, (~s50.isna()).sum())

    # 3) 週次の上昇継続（“上昇週”の割合）
    w = g.set_index("日付")["終値"].resample("W-FRI").last().dropna()
    wk_ratio = float((w.diff() > 0).sum()) / max(1, (w.diff().dropna().shape[0]))

    # 4) 最大ドローダウン
    cummax = px.cummax()
    mdd = float((px/cummax - 1.0).min()) * -1.0  # 正の値

    # 5) 安値の切り上げ比率（ピボット）
    hl_ratio = _pivot_ratio_higher_lows(hi, lo, win=HL_WIN)

    return dict(
        slope_ann=float(slope_ann), r2=float(max(0,min(1,r2))),
        ribbon_ratio=float(ribbon_ratio), above50_ratio=float(above50_ratio),
        week_up_ratio=float(wk_ratio), mdd=float(mdd), hl_ratio=float(hl_ratio)
    )

def compute_right_up_persistent(conn, as_of=None):
    """『ずーーっと右肩上がり』をスコア化して screener を更新"""
    # 対象日
    dmax = pd.read_sql_query("SELECT MAX(日付) d FROM price_history", conn, parse_dates=["d"])
    if dmax.empty or pd.isna(dmax.loc[0,"d"]): 
        print("[右肩上がり] price_history空"); return
    today = pd.to_datetime(as_of) if as_of is not None else dmax.loc[0,"d"]

    start = (today - pd.Timedelta(days=int(LOOKBACK*1.6))).strftime("%Y-%m-%d")
    ph = pd.read_sql_query(
        f"SELECT 日付, コード, 終値, 高値, 安値 FROM price_history WHERE 日付>=date('{start}') ORDER BY コード, 日付",
        conn, parse_dates=["日付"]
    )
    ph = add_price_features(ph)  # v10: unify price feature calc
    if ph.empty: 
        print("[右肩上がり] データ無し"); return

    outs = []
    for code, g0 in ph.groupby("コード", sort=False):
        g = g0[g0["日付"]<=today].tail(LOOKBACK).copy()
        if len(g) < MIN_DAYS: 
            continue
        met = _trend_metrics_df(g)

        # ---- スコア（0-100） ----
        # 回帰傾き（0→40点）：12%/年で0点、50%/年で満点
        slope = met["slope_ann"]
        slope_score = 0.0 if slope <= SLOPE_MIN_ANN else min(1.0, (slope - SLOPE_MIN_ANN)/0.38)*40.0

        # R^2（0→15点）
        r2_score = met["r2"]*15.0

        # リボン維持（0→20点）: 直近でどれだけ20>50>100を維持
        ribbon_score = min(1.0, met["ribbon_ratio"]/0.8)*20.0  # 80%維持で満点

        # 週足の上昇比率（0→10点）
        week_score = 0.0 if met["week_up_ratio"] <= WEEK_UP_MIN else min(1.0,(met["week_up_ratio"]-WEEK_UP_MIN)/(0.9-WEEK_UP_MIN))*10.0

        # SMA50上回り比率（0→10点）
        above50_score = min(1.0, max(0.0, (met["above50_ratio"]-0.6)/(0.9-0.6)))*10.0

        # 安値の切り上げ（0→10点）
        hl_score = min(1.0, met["hl_ratio"]/0.7)*10.0  # 70%がHLなら満点

        # DDペナルティ（～-15点）
        dd_pen = 0.0
        if met["mdd"] > MDD_MAX:
            dd_pen = min(1.0, (met["mdd"]-MDD_MAX)/0.2)*15.0  # 30%超→減点、50%で最大

        score = max(0.0, slope_score + r2_score + ribbon_score + week_score + above50_score + hl_score - dd_pen)

        # 最低限の基礎条件
        base_ok = (slope > 0) and (met["r2"] >= R2_MIN)
        flag = "候補" if (base_ok and score >= THRESH_SCORE) else ""

        outs.append((round(score,1), flag, str(code)))

    if not outs:
        print("[右肩上がり] 該当なし"); return

    cur = conn.cursor()
    cur.execute("UPDATE screener SET 右肩上がりフラグ='', 右肩上がりスコア=NULL")
    cur.executemany("""
        UPDATE screener SET 右肩上がりスコア=?, 右肩上がりフラグ=? WHERE コード=?
    """, outs)
    conn.commit(); cur.close()
    print(f"[右肩上がり] 持続トレンド版 {len(outs)} 銘柄を更新 / 閾値={THRESH_SCORE}")

# ========================= 右肩上がり・早期トリガー（完全版：置換用） =========================

# ---- しきい値（好みに応じて調整してください）----
HH_N = 60                   # ブレイク判定の過去高値期間
POCKET_WIN = 10             # ポケットピボットの参照日数
REB_WIN = 10                # 20MA割れ→奪回を探すウィンドウ
RECLAIM_WIN = 10            # 200MA上抜けの探索ウィンドウ
SCORE_TH = 70               # 早期フラグのスコア閾値（これ以上で候補）
PIVOT_EPS = 0.002           # ブレイク余白(+0.2%)
VOL_BOOST = 1.5             # ブレイク時の出来高ブースト(×20日平均)
EXT_20_MAX = 0.05           # 20MAからの乖離上限(=+5%)
EXT_50_MAX = 0.10           # 50MAからの乖離上限(=+10%)

# ---- スキーマ確保（screener の列/ signals_log の最小列）----
def _ma(s, n):  return s.rolling(n, min_periods=n).mean()
def _avg_vol(v, n=20): return v.rolling(n, min_periods=n).mean()

def _atr(df, n=20):
    c = df["終値"].astype(float)
    h = (df["高値"] if "高値" in df else df["終値"]).astype(float)
    l = (df["安値"] if "安値" in df else df["終値"]).astype(float)
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

# ---- 1銘柄の“今日”のベストシグナルを返す ----
def _best_signal_today(g: pd.DataFrame):
    """
    g: 1銘柄分の DataFrame（昇順）。終値/高値/安値/出来高 必須。
    戻り: (score:float, tag:str, details:str) or (0,'','')
    """
    g = g.sort_values("日付").copy()
    px = g["終値"].astype(float)
    hi = (g["高値"] if "高値" in g else g["終値"]).astype(float)
    lo = (g["安値"] if "安値" in g else g["終値"]).astype(float)
    vol = g["出来高"].astype(float)

    # 指標
    s10  = _ma(px, 10)
    s20  = _ma(px, 20)
    s50  = _ma(px, 50)
    s100 = _ma(px, 100)
    s200 = _ma(px, 200)
    v20  = _avg_vol(vol, 20)
    atr20 = _atr(g, 20)  # 予備（未使用）
    hh60 = hi.shift(1).rolling(HH_N, min_periods=HH_N).max()  # 当日を除く60日高値

    # 当日（末行）
    if len(g) < max(60, 50):
        return 0.0, "", ""
    close_t = float(px.iloc[-1])
    vol_t   = float(vol.iloc[-1])

    s10_t  = s10.iloc[-1]  if len(s10)  else np.nan
    s20_t  = s20.iloc[-1]  if len(s20)  else np.nan
    s50_t  = s50.iloc[-1]  if len(s50)  else np.nan
    s100_t = s100.iloc[-1] if len(s100) else np.nan
    s200_t = s200.iloc[-1] if len(s200) else np.nan

    if pd.isna(s20_t) or pd.isna(s50_t):
        return 0.0, "", ""

    ext20 = (close_t - s20_t) / s20_t if s20_t > 0 else 0.0
    ext50 = (close_t - s50_t) / s50_t if s50_t > 0 else 0.0

    sigs = []

    # A) 60日高値ブレイク
    if not pd.isna(hh60.iloc[-1]):
        cond_break = (close_t >= hh60.iloc[-1] * (1.0 + PIVOT_EPS))
        cond_vol   = (not pd.isna(v20.iloc[-1]) and vol_t >= v20.iloc[-1] * VOL_BOOST)
        cond_ma    = (s20_t > s50_t) and (len(s50) >= 2 and not pd.isna(s50.iloc[-2]) and s50.iloc[-1] > s50.iloc[-2])
        cond_ext   = (ext20 <= EXT_20_MAX) and (ext50 <= EXT_50_MAX)
        if cond_break and cond_ma and cond_ext:
            near = max(0.0, 1.0 - (close_t / hh60.iloc[-1] - 1.0) / 0.05)  # 5%上抜きで0点
            vol_score = 0.0 if pd.isna(v20.iloc[-1]) else min(1.0, (vol_t / max(1.0, v20.iloc[-1])) / 2.5)
            ma_gap = min(1.0, (s20_t/s50_t - 1.0) / 0.05) if s50_t > 0 else 0.0
            score = 55*near + 25*vol_score + 20*ma_gap
            sigs.append((score, "ブレイク", f"HH{HH_N}+{PIVOT_EPS*100:.1f}%, vol≥{VOL_BOOST}x, 20>50"))

    # B) ポケットピボット
    if not pd.isna(s10_t) and not pd.isna(hh60.iloc[-1]):
        down_mask = px.diff() < 0
        down_vol_max = vol.where(down_mask).tail(POCKET_WIN).max()
        near_pivot = (close_t / hh60.iloc[-1] - 1.0)
        cond_pp = (close_t > s10_t) and (not pd.isna(down_vol_max)) and (vol_t > down_vol_max)
        cond_near = (-0.03 <= near_pivot <= 0.02)
        if cond_pp and cond_near:
            tight = px.tail(10).pct_change().dropna().std()
            tight_score = max(0.0, 1.0 - (tight / 0.025)) if pd.notna(tight) else 0.0
            vol_score = min(1.0, vol_t / max(1.0, down_vol_max) / 2.0)
            score = 35 + 35*vol_score + 30*tight_score
            sigs.append((score, "ポケット", f">10MA, vol>{POCKET_WIN}dDownMax, near HH{HH_N}"))

    # C) 20MAリバウンド（NaN安全化）
    # 20MAが存在する日のみで below20 を作る → ブールdtype維持
    below20 = (px < s20) & s20.notna()
    # 直前は20MAの下・現在は上（=リバウンド）を REB_WIN 内に含むか
    cross_up = (below20.shift(1, fill_value=False) & (~below20)).tail(REB_WIN).any()
    cond_c = (cross_up
              and close_t >= s20_t
              and (not pd.isna(s50_t)) and close_t >= s50_t
              and (not pd.isna(v20.iloc[-1])) and vol_t >= v20.iloc[-1])
    if cond_c:
        near20 = max(0.0, 1.0 - abs(ext20)/0.04)  # ±4%で0点
        score = 30 + 40*near20 + 30*min(1.0, vol_t/max(1.0, v20.iloc[-1]))
        sigs.append((score, "20MAリバ", "20MA reclaim & vol≥Avg20 & ≥50MA"))

    # D) 200MAリクレイム（NaN安全化／ここがエラーだった箇所）
    if not pd.isna(s200_t):
        # 200MAが存在する日のみ評価（True/FalseのみのSeriesになる）
        above200 = (px >= s200) & s200.notna()
        crossed  = ((~above200.shift(1, fill_value=False)) & above200).tail(RECLAIM_WIN).any()
        stay3    = above200.tail(3).all()
        slope50_up   = (len(s50.dropna())  >= 6 and s50.iloc[-1]  > s50.iloc[-5])
        slope100_ok  = (len(s100.dropna()) >= 6 and s100.iloc[-1] >= s100.iloc[-5])
        cond_d = crossed and stay3 and slope50_up and slope100_ok
        if cond_d:
            ext200 = (close_t - s200_t)/s200_t if s200_t > 0 else 0.0
            near200   = max(0.0, 1.0 - abs(ext200)/0.06)  # ±6%で0点
            vol_score = 0.0 if pd.isna(v20.iloc[-1]) else min(1.0, vol_t/max(1.0, v20.iloc[-1]))
            score = 25 + 45*near200 + 30*vol_score
            sigs.append((score, "200MAリクレイム", "cross&stay3d, 50MA↑,100MA↔↑"))

    if not sigs:
        return 0.0, "", ""
    sigs.sort(key=lambda x: x[0], reverse=True)
    return sigs[0]  # (score, tag, detail)

# ---- メイン：早期トリガー計算・DB更新・ログ記録（日時で記録）----
def compute_right_up_early_triggers(conn, as_of=None, log_datetime=None):
    """
    右肩上がりの“早めに仕掛ける”4シグナル（ブレイク/ポケット/20MAリバ/200MAリクレイム）を判定。
    - price_history の日足のみで判定
    - screener: 右肩早期フラグ/種別/スコア を更新
    - signals_log: 日時で記録（前場/後場など1日複数回の実行に対応）
    引数:
      as_of        … 判定する“日付”（例 '2025-08-22'）。未指定なら price_history の MAX(日付)
      log_datetime … ログに書く“日時”（例 '2025-08-22 09:01:00'）。未指定なら now()
    """
    # 判定対象日（price_history の最終営業日に合わせる）
    dmax = pd.read_sql_query("SELECT MAX(日付) d FROM price_history", conn, parse_dates=["d"])
    if dmax.empty or pd.isna(dmax.loc[0,"d"]):
        print("[右肩早期] price_history が空です"); return
    as_of_date = pd.to_datetime(as_of).date() if as_of is not None else pd.to_datetime(dmax.loc[0,"d"]).date()

    # ログ用の日時（前場/後場で区別したい時はここを指定）
    dt_log = pd.to_datetime(log_datetime) if log_datetime is not None else pd.Timestamp.now()
    dt_str = dt_log.strftime("%Y-%m-%d %H:%M:%S")

    # 必要期間だけ抽出（200MAまで使うので余裕を持って）
    start = (pd.Timestamp(as_of_date) - pd.Timedelta(days=320)).strftime("%Y-%m-%d")
    ph = pd.read_sql_query(
        "SELECT 日付, コード, 終値, 高値, 安値, 出来高 "
        "FROM price_history WHERE 日付 >= date(?) AND 日付 <= date(?) "
        "ORDER BY コード, 日付",
        conn, params=(start, as_of_date)
    )
    if ph.empty:
        print("[右肩早期] データなし"); return
    ph["日付"] = pd.to_datetime(ph["日付"])

    results, logs = [], []
    cnt_flag = 0

    for code, g in ph.groupby("コード", sort=False):
        if len(g) < 60:
            continue
        score, tag, detail = _best_signal_today(g)
        flag = "候補" if score >= SCORE_TH and tag else ""
        results.append((
            None if score==0 else round(float(score),1),
            (tag if tag else None),
            (flag if flag else ""),
            str(code)
        ))
        if flag:
            cnt_flag += 1
            logs.append((dt_str, str(code), "右肩上がり-早期", f"{tag} | score={round(float(score),1)} | {detail}"))

    # screener を初期化 → 更新
    cur = conn.cursor()
    cur.execute("UPDATE screener SET 右肩早期フラグ='', 右肩早期種別=NULL, 右肩早期スコア=NULL")
    if results:
        cur.executemany("""
            UPDATE screener
               SET 右肩早期スコア=?,
                   右肩早期種別=?,
                   右肩早期フラグ=?
             WHERE コード=?
        """, results)
    conn.commit(); cur.close()

    # signals_log へ書き込み（日時のみ、同一(コード,日時,種別)は上書き）
    if logs:
        cur = conn.cursor()
        try:
            cur.executemany("""
                INSERT INTO signals_log(日時, コード, 種別, 詳細)
                VALUES(?,?,?,?)
                ON CONFLICT(コード, 日時, 種別) DO UPDATE SET
                  詳細 = excluded.詳細
            """, logs)
        except Exception:
            # 互換用フォールバック（主キーなし等の古いスキーマ）
            cur.executemany("INSERT INTO signals_log(日時, コード, 種別, 詳細) VALUES(?,?,?,?)", logs)
        conn.commit(); cur.close()

    print(f"[右肩早期] 候補 {cnt_flag} 件 / 総{len(results)}件  as_of={as_of_date}  閾値{SCORE_TH}  dt={dt_str}")
# ========================= /右肩上がり・早期トリガー（完全版：置換用） =========================

# ===== シグナル判定（初動/底打ち/上昇余地） =====
def phase_signal_detection(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT コード FROM price_history")
    codes = [r[0] for r in cur.fetchall()]
    cur.close()
    if not codes:
        return

    today = today_str()
    upd_rows, log_rows = [], []
    start_cut = (dtm.date.today() - dtm.timedelta(days=SIGNAL_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    for i in range(0, len(codes), 500):
        part = codes[i:i+500]
        qmarks = ",".join("?" * len(part))
        df = pd.read_sql_query(f"""
            SELECT コード, 日付, 終値, 高値, 安値, 出来高
            FROM price_history
            WHERE 日付 >= ? AND コード IN ({qmarks})
            ORDER BY コード, 日付
        """, conn, params=[start_cut, *part])

        if df.empty:
            continue

        df = add_price_features(df)
        for code, g in df.groupby("コード", sort=False):

            diff = g["終値"].diff()
            up = diff.clip(lower=0).rolling(14).mean()
            down = (-diff.clip(upper=0)).rolling(14).mean()
            rs = up / (down.replace(0, 1e-9))
            g["RSI14"] = 100 - (100 / (1 + rs))

            last = g.iloc[-1]
            if len(g) < 21 or pd.isna(last["終値_ma5"]) or pd.isna(last["終値_ma20"]) or pd.isna(last["出来高_ma5"]):
                continue

            prev_close = g["終値"].iloc[-2] if len(g) >= 2 else last["終値"]
            zenhi = (last["終値"] - prev_close)

            # --- 初動 ---
            vol_bai = (last["出来高"] / last["出来高_ma5"]) if last["出来高_ma5"] else 0
            price_ma5_ratio = last["終値"] / last["終値_ma5"] if last["終値_ma5"] else 0
            shodou = "候補" if (vol_bai >= 2 and price_ma5_ratio >= 1.03 and zenhi > 0) else None

            # --- 底打ち ---
            range_ok = False
            if ffloat(last["高値"], None) is not None and ffloat(last["安値"], None) is not None and last["高値"] > last["安値"]:
                pos = (last["終値"] - last["安値"]) / (last["高値"] - last["安値"])
                range_ok = pos >= 0.6
            bottom = "候補" if (ffloat(last["RSI14"], 100) <= 30 and zenhi > 0 and range_ok) else None

            # --- 上昇余地スコア ---
            c2 = conn.cursor()
            c2.execute("SELECT 時価総額億円 FROM screener WHERE コード=?", (code,))
            r = c2.fetchone()
            c2.close()
            try:
                zika_oku = float(r[0]) if r and r[0] is not None else 0.0
            except Exception:
                zika_oku = 0.0

            score_cap   = max(0, min(40, (100 - zika_oku) / 100 * 40))
            score_trend = max(0, min(40, (last["終値"] / last["終値_ma20"] - 1) * 200))
            score_vol   = max(0, min(20, (vol_bai - 1) * 20))
            potential_score = round(score_cap + score_trend + score_vol, 1)

            # --- 右肩上がりモメンタムスコア ---
            # 52週高値（データ不足時は期間内高値）
            try:
                high_52 = g["終値"].rolling(252, min_periods=20).max().iloc[-1]
            except Exception:
                high_52 = g["終値"].max()
            near_high = (high_52 and last["終値"] and (last["終値"] / high_52 >= 0.95))

            slope13 = (g["MA13"].iloc[-1] / g["MA13"].iloc[-13] - 1) if len(g) >= 26 and not pd.isna(g["MA13"].iloc[-13]) else 0
            slope26 = (g["MA26"].iloc[-1] / g["MA26"].iloc[-26] - 1) if len(g) >= 52 and not pd.isna(g["MA26"].iloc[-26]) else 0

            above_ma20_ratio = (g["終値"].tail(60) > g["終値_ma20"].tail(60)).mean() if len(g) >= 60 else 0
            vol_contraction = (last["ATR20"] / last["終値"] <= 0.03) if (last["終値"] and not pd.isna(last["ATR20"])) else False

            vol20 = g["出来高"].rolling(20).mean().iloc[-1] if len(g) >= 20 else None
            vol60 = g["出来高"].rolling(60).mean().iloc[-1] if len(g) >= 60 else None
            dryup = (vol20 is not None and vol60 is not None and vol20 < vol60 * 0.8)

            rumor_spike = (vol20 is not None and last["出来高"] is not None and last["出来高"] >= vol20 * 1.5 and zenhi > 0)

            tob_score = 0
            tob_score += 20 if near_high else 0
            tob_score += 15 if slope13 > 0 else 0
            tob_score += 15 if slope26 > 0 else 0
            tob_score += 20 if above_ma20_ratio >= 0.80 else 0
            tob_score += 15 if vol_contraction else 0
            tob_score += 10 if dryup else 0
            tob_score += 5  if rumor_spike else 0
            tob_flag = "候補" if tob_score >= 60 else None

            upd_rows.append((shodou, bottom, potential_score, today, code))

            # signals_log へ
            def _append_log(kind, score_value):
                log_rows.append((
                    code, today, kind,
                    last['終値'], last['高値'], last['安値'],
                    fint(last['出来高'], 0),
                    score_value,
                    0,
                    None, None, None, None,
                    None, None, None,
                    None, None
                ))

            if shodou: _append_log('初動', potential_score or None)
            if bottom: _append_log('底打ち', potential_score or None)
            if potential_score is not None and potential_score >= 60: _append_log('上昇余地', potential_score)
            if tob_flag: _append_log('右肩上がり', float(tob_score))

    if upd_rows:
        cur = conn.cursor()
        cur.executemany("""
            UPDATE screener
               SET 初動フラグ=?,
                   底打ちフラグ=?,
                   上昇余地スコア=?,
                   シグナル更新日=?
             WHERE コード=?
        """, upd_rows)
        conn.commit()
        cur.close()

    if log_rows:
        cur = conn.cursor()
        cur.executemany("""
            INSERT OR IGNORE INTO signals_log
              (コード,日時,種別,終値,高値,安値,出来高,スコア,検証済み,
               次日始値,次日終値,次日高値,次日安値,
               リターン終値pct,フォロー高値pct,最大逆行pct,判定,理由)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, log_rows)
        conn.commit()
        cur.close()

# ===== 翌営業日検証 =====
def phase_validate_prev_business_day(conn: sqlite3.Connection):
    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    today = dtm.date.today()
    d0 = prev_business_day_jp(today, extra_closed)
    d1 = next_business_day_jp(d0,    extra_closed)
    d0s, d1s = d0.strftime("%Y-%m-%d"), d1.strftime("%Y-%m-%d")

    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 種別, 終値, 高値, 安値, スコア
        FROM signals_log
        WHERE 日時=? AND (検証済み IS NULL OR 検証済み=0)
    """, (d0s,))
    sigs = cur.fetchall()
    cur.close()
    if not sigs:
        print(f"前営業日({d0s})の未検証シグナルなし")
        return

    codes = sorted(set([s[0] for s in sigs]))
    qmarks = ",".join("?"*len(codes))
    df1 = pd.read_sql_query(f"""
        SELECT コード, 日付, 始値, 高値, 安値, 終値
        FROM price_history
        WHERE 日付 IN (?, ?) AND コード IN ({qmarks})
        ORDER BY コード, 日付
    """, conn, params=[d0s, d1s, *codes])

    if df1.empty:
        
        return

    rows = {(r["コード"], r["日付"]): r for _, r in df1.iterrows()}
    updates = []
    for code, kind, base_close, base_high, base_low, score in sigs:
        r_next = rows.get((code, d1s))
        if not r_next:
            continue
        o1 = ffloat(r_next["始値"], None)
        h1 = ffloat(r_next["高値"], None)
        l1 = ffloat(r_next["安値"], None)
        c1 = ffloat(r_next["終値"], None)

        if not base_close or base_close == 0 or c1 is None or h1 is None or l1 is None:
            ret_close = None; follow_high = None; mae = None
        else:
            ret_close = (c1 / base_close - 1) * 100
            follow_high = (h1 / base_close - 1) * 100
            mae = (l1 / base_close - 1) * 100

        verdict, reason = "見送り", ""
        if kind == "初動":
            if (follow_high is not None and follow_high >= 2.0) or (ret_close is not None and ret_close > 0):
                verdict = "的中"
            reason = f"follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%" if (follow_high is not None and ret_close is not None) else "insufficient data"
        elif kind == "底打ち":
            pos = (c1 - l1) / (h1 - l1) if (c1 is not None and h1 is not None and l1 is not None and h1 > l1) else 0
            if (ret_close is not None and ret_close > 0) and pos >= 0.5:
                verdict = "的中"
            reason = f"close_ret={ret_close:.2f}% pos={pos:.2f}"
        elif kind == "上昇余地":
            need = 1.0
            if score is not None:
                s = float(score)
                if s >= 90: need = 3.0
                elif s >= 80: need = 2.0
                elif s >= 60: need = 1.0
            if (follow_high is not None and follow_high >= need) or (ret_close is not None and ret_close > 0):
                verdict = "的中"
            reason = f"need={need:.1f}% follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%"
        elif kind == "右肩上がり":
            if (follow_high is not None and follow_high >= 1.0) or (ret_close is not None and ret_close >= 0):
                verdict = "傾向維持"
            reason = f"follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%"
        else:
            reason = "unknown kind"

        updates.append((1, o1, c1, h1, l1,
                        None if ret_close is None else round(ret_close, 2),
                        None if follow_high is None else round(follow_high, 2),
                        None if mae is None else round(mae, 2),
                        verdict, reason, code, d0s, kind))

    if updates:
        cur = conn.cursor()
        cur.executemany("""
            UPDATE signals_log
            SET 検証済み=?,
                次日始値=?, 次日終値=?, 次日高値=?, 次日安値=?,
                リターン終値pct=?, フォロー高値pct=?, 最大逆行pct=?,
                判定=?, 理由=?
            WHERE コード=? AND 日時=? AND 種別=?
        """, updates)
        conn.commit()
        cur.close()

    out = os.path.join(OUTPUT_DIR, f"validate_{d0s}_vs_{d1s}.csv")
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 種別, 日時, 終値, 次日始値, 次日終値, 次日高値, 次日安値,
               リターン終値pct, フォロー高値pct, 最大逆行pct, 判定, 理由, スコア
        FROM signals_log
        WHERE 日時=? AND 検証済み=1
        ORDER BY 判定 DESC, フォロー高値pct DESC
    """, (d0s,))
    rows = cur.fetchall()
    cur.close()

    if rows:
        with open(out, "w", encoding="utf-8", newline="") as f:
            f.write("コード,種別,日時,終値,次日始値,次日終値,次日高値,次日安値,リターン終値pct,フォロー高値pct,最大逆行pct,判定,理由,スコア\n")
            for r in rows:
                f.write(",".join([str(x if x is not None else "") for x in r]) + "\n")
        try:
            notification.notify(title="翌日検証レポート", message=f"{d0s}→{d1s} 検証完了: {len(rows)}件", timeout=5)
        except Exception:
            pass

# ===== HTML（オフライン/5タブ/各表にリンク列/Migikataフィルタ追加） =====

# ========== Template exporter（CDNなし・JSON直埋め・フォールバック強化・完全置換） ==========

# しきい値（ヘルプ表示用：未使用でも残す）
HH_N        = globals().get('HH_N', 60)
PIVOT_EPS   = globals().get('PIVOT_EPS', 0.002)
VOL_BOOST   = globals().get('VOL_BOOST', 1.5)
EXT_20_MAX  = globals().get('EXT_20_MAX', 0.05)
EXT_50_MAX  = globals().get('EXT_50_MAX', 0.10)
POCKET_WIN  = globals().get('POCKET_WIN', 10)
REB_WIN     = globals().get('REB_WIN', 10)
RECLAIM_WIN = globals().get('RECLAIM_WIN', 10)
SCORE_TH    = globals().get('SCORE_TH', 70)

# ---------- 安全整形 ----------
def _to_float(v):
    if v is None: return None
    try:
        s = str(v).replace(',', '').replace('％','').replace('%','').strip()
        if s == '': return None
        x = float(s)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def _fmt_cell(v):
    """HTML混在・数値を安全に整形して返す。<span>/<br>/<div> を含む場合は safe 出力。"""
    try:
        if v is None or (isinstance(v,float) and math.isnan(v)): 
            return ""
        s = str(v)
        if ("<span" in s) or ("<br" in s) or ("<div" in s):
            return Markup(s)
        if isinstance(v,int): 
            return f"{v:,}"
        if isinstance(v,float):
            if abs(v-round(v))<1e-9: 
                return f"{int(round(v)):,}"
            return f"{v:.2f}"
        fv = _to_float(v)
        if fv is not None: 
            return _fmt_cell(fv)
        return escape(s)
    except Exception:
        return escape(str(v)) if v is not None else ""

def _noshor_from_agency(val) -> str:
    if val is None: return "1"
    s = str(val).strip()
    return "1" if s=="" or s in ("なし","-","0","NaN","nan","None") else "0"

def _op_ratio_flag(d):
    """営利対時価(=営業利益 / 時価総額) >= 10% を疑似判定（DB列に依存しないA案）"""
    e = _to_float(d.get("営業利益"))
    z = _to_float(d.get("時価総額億円"))
    if e is None or z is None or z == 0: 
        return "0"
    # 営業利益は億円相当を想定。保守的に 10% を閾値とする
    try:
        ratio = (e / z) * 100.0
        return "1" if ratio >= 10.0 else "0"
    except Exception:
        return "0"

def _bucket_turn(v):
    if v is None: return "売買:<不明>"
    v=float(v);  return "売買:<5" if v<5 else ("売買:5-10" if v<10 else ("売買:10-50" if v<50 else ("売買:50-100" if v<100 else "売買:100+")))
def _bucket_rvol(v):
    if v is None: return "RVOL:<不明>"
    v=float(v);  return "RVOL:<1" if v<1 else ("RVOL:1-2" if v<2 else ("RVOL:2-3" if v<3 else ("RVOL:3-5" if v<5 else "RVOL:5+")))
def _bucket_vol(v):
    if v is None: return "出来:<不明>"
    v=float(v);  return "出来:<10万" if v<1e5 else ("出来:10-100万" if v<1e6 else ("出来:100-500万" if v<5e6 else ("出来:500-1000万" if v<1e7 else "出来:1000万+")))
def _bucket_atr(v):
    if v is None: return "ATR:<不明>"
    v=float(v);  return "ATR:<4" if v<4 else ("ATR:4-6" if v<6 else ("ATR:6-10" if v<10 else "ATR:10+"))
def _bucket_mcap(v):
    if v is None: return "時価:<不明>"
    v=float(v);  return "時価:<300" if v<300 else ("時価:300-1000" if v<1000 else ("時価:1000-5000" if v<5000 else ("時価:5000-20000" if v<20000 else "時価:20000+")))
def _bucket_comp(v):
    if v is None: return "合成S:<不明>"
    v=float(v);  return "合成S:<70" if v<70 else ("合成S:70-80" if v<80 else ("合成S:80-90" if v<90 else "合成S:90+"))
def _bucket_rate(v):
    if v is None: return "上昇率:不明"
    v=float(v);  return "上昇率:0-1" if v<1 else ("上昇率:1-3" if v<3 else ("上昇率:3-5" if v<5 else "上昇率:5+"))

def _build_reason(d):
    tags = []
    tags.append(_bucket_rate(_to_float(d.get("前日終値比率"))))
    tags.append(_bucket_turn(_to_float(d.get("売買代金(億)"))))
    tags.append(_bucket_rvol(_to_float(d.get("RVOL代金"))))
    tags.append(_bucket_vol(_to_float(d.get("出来高"))))
    tags.append(_bucket_atr(_to_float(d.get("ATR14%"))))
    tags.append(_bucket_mcap(_to_float(d.get("時価総額億円"))))
    tags.append(_bucket_comp(_to_float(d.get("合成スコア"))))
    etype = (d.get("右肩早期種別") or "").strip()
    if etype: tags.append(f"早期:{etype}")
    if (d.get("初動フラグ") or "") == "候補": tags.append("初動")
    if (d.get("底打ちフラグ") or "") == "候補": tags.append("底打ち")
    if (d.get("右肩上がりフラグ") or "") == "候補": tags.append("右肩")
    if (d.get("右肩早期フラグ") or "") == "候補": tags.append("早期")
    if _op_ratio_flag(d) == "1": tags.append("割安")
    if d.get("空売り機関なし_flag","0") == "1": tags.append("機関:なし")
    return " / ".join(tags)

# === 推奨／比率・営利対時価の自動算出（DB非依存）========================

# === helper（無ければ追記） ===
def _clamp(x, lo, hi):
    try:
        x = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, x))

def _to_float_safe(d, key, default=None):
    try:
        v = d.get(key, None)
        if v is None or (isinstance(v, str) and not v.strip()):
            return default
        return float(str(v).replace('%',''))
    except Exception:
        return default

# === 推奨ロジック（置換） ===
def _derive_recommendation(d: dict):
    """
    連続比率（0〜1）を作り、UI/運用は離散バンド（1.0/0.75/0.5/0.25/—）で安定化。
    ・比率は comp（合成スコア）を中核に、流動性と上昇率で補正
    ・境界付近はヒステリシス（±0.02）でフリップ抑制
    返り値: (label:str or "", ratio_band:float or None, ratio_raw:float or None)
    """
    # 参照値の取得
    comp = _to_float_safe(d, "合成スコア", None)           # 0〜100想定
    rvol = _to_float_safe(d, "RVOL_売買代金", None)        # ≥2が目安
    turn = _to_float_safe(d, "売買代金(億)", None)         # ≥5が目安
    rate = _to_float_safe(d, "前日終値比率（％）", None)    # 上昇率（％）

    # 候補フラグ（どれか一つでも「候補」）
    flags = [
        str(d.get("右肩早期フラグ", "") or "").strip(),
        str(d.get("右肩上がりフラグ", "") or "").strip(),
        str(d.get("初動フラグ", "") or "").strip(),
    ]
    is_setup = any(f == "候補" for f in flags)

    # 連続化：合成S=65→0.0, 100→1.0（下駄・上限付き）
    if comp is None:
        return ("", None, None)
    ratio = _clamp((comp - 65.0) / 35.0, 0.0, 1.0)

    # 流動性補正（満たない場合は減点方式）
    if rvol is not None and rvol < 2.0:
        ratio *= 0.7
    if turn is not None and turn < 5.0:
        ratio *= 0.8

    # 上昇率レンジ補正（小口レンジ外は減点、有力レンジは微加点）
    if rate is not None:
        if 0.0 <= rate <= 2.0:
            ratio = min(1.0, ratio * 1.10)   # 有力レンジ微ブースト
        elif -2.0 <= rate <= 3.0:
            pass                              # 小口レンジ＝補正なし
        else:
            ratio *= 0.6                      # レンジ外は減点

    # 候補でない場合は弱め扱い（完全にゼロにせず、わずかに残す選択も可能）
    if not is_setup:
        ratio *= 0.5

    ratio = _clamp(ratio, 0.0, 1.0)
    ratio_raw = ratio  # 生値を保持

    # ===== バンド分け（UI/運用向け）＋ヒステリシス =====
    # 直近の raw が dict にあれば取得（ダッシュボード側で前回値を入れてくれていれば粘りが効く）
    prev_raw = _to_float_safe(d, "推奨比率_raw", None)

    # バンド（下限しきい値, ラベル, 表示倍率）
    BANDS = [
        (0.88, "エントリー有力", 1.00),
        (0.63, "中強度",        0.75),
        (0.38, "小口提案",      0.50),
        (0.13, "微小口",        0.25),
    ]
    EPS = 0.02  # ヒステリシス幅

    def _band_index(val):
        for i, (thr, _, _) in enumerate(BANDS):
            if val >= thr:
                return i
        return None

    cand_idx = _band_index(ratio)
    prev_idx = _band_index(prev_raw) if prev_raw is not None else None

    # ヒステリシス：境界±EPSで前回バンドを維持
    if prev_idx is not None and cand_idx is not None:
        # 上方向遷移のときは “thr + EPS” まで到達しない限り据え置き
        if cand_idx < prev_idx:
            thr_up = BANDS[cand_idx][0]
            if ratio < (thr_up + EPS):
                cand_idx = prev_idx
        # 下方向遷移のときは “prev_thr - EPS” を割るまで据え置き
        elif cand_idx > prev_idx:
            prev_thr = BANDS[prev_idx][0]
            if ratio >= (prev_thr - EPS):
                cand_idx = prev_idx

    # バンド決定
    if cand_idx is None:
        # しきい値未満は「空欄」
        return ("", None, ratio_raw)
    label, ratio_band = BANDS[cand_idx][1], BANDS[cand_idx][2]

    return (label, ratio_band, ratio_raw)

def _derive_opratio_flag(d, threshold_pct: float = 10.0) -> str:
    """
    営業利益 / 時価総額(億円) * 100 >= threshold_pct なら "1"、それ以外は "0"
    ※どちらか欠損/0 のときも "0"
    """
    op   = _to_float(d.get("営業利益"))
    mcap = _to_float(d.get("時価総額億円"))
    if op is None or mcap in (None, 0):
        return "0"
    ratio_pct = (op / mcap) * 100.0
    return "1" if ratio_pct >= threshold_pct else "0"
# =======================================================================

# ---------- 行整形（欠損安全 & 外部列に依存しない） ----------
def _prepare_rows(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        d = {k: (None if (isinstance(r.get(k), float) and math.isnan(r.get(k))) else r.get(k)) for k in df.columns}

        # コード/銘柄
        if "コード" in d: d["コード"] = str(d.get("コード") or "").zfill(4)
        if "銘柄名" in d: d["銘柄名"] = str(d.get("銘柄名") or "")

        # Yahoo / X
        code4 = (d.get("コード") or "").zfill(4)
        d["yahoo_url"] = f"https://finance.yahoo.co.jp/quote/{code4}.T" if code4 else ""
        d["x_url"]  = f"https://x.com/search?q={_q(d.get('銘柄名') or '')}" if d.get("銘柄名") else ""

        # 売買代金(億) 補完
        if d.get("売買代金(億)") is None:
            fv=_to_float(d.get("現在値")); fvol=_to_float(d.get("出来高"))
            if fv is not None and fvol is not None:
                d["売買代金(億)"] = fv * fvol / 1e8

        # RVOL代金 補完
        fturn=_to_float(d.get("売買代金(億)"))
        favg20=_to_float(d.get("売買代金20日平均億"))
        if d.get("RVOL代金") is None and (fturn is not None) and (favg20 and favg20!=0):
            d["RVOL代金"] = fturn / favg20

        # 前日比 補完
        now_=_to_float(d.get("現在値")); prev_=_to_float(d.get("前日終値"))
        if d.get("前日円差") is None and (now_ is not None and prev_ is not None):
            d["前日円差"] = now_ - prev_
        if d.get("前日終値比率") is None and (now_ is not None and prev_ not in (None,0)):
            d["前日終値比率"] = (now_/prev_ - 1.0) * 100.0

        # 現在値 raw
        cv = _to_float(d.get("現在値"))
        d["現在値_raw"] = cv if cv is not None else ""

        # 前日終値比率 raw（「前日終値比率（％）」という列名の揺れにも対応）
        pct_val = d.get("前日終値比率")
        if pct_val is None:
            pct_val = d.get("前日終値比率（％）")
        pctf = _to_float(pct_val)
        d["前日終値比率_raw"] = pctf if pctf is not None else ""

        # 表示は％文字列に統一（rawは数値のまま残す）
        if pctf is not None:
            d["前日終値比率"] = f"{round(float(pctf), 2)}%"

        # 付加フラグ
        d["空売り機関なし_flag"] = _noshor_from_agency(d.get("空売り機関"))
        d["営利対時価_flag"]     = _op_ratio_flag(d)  # ← チェックボックス用（割安）

        # 判定/理由
        pct = _to_float(d.get("前日終値比率"))
        d["判定"] = "当たり！" if (pct is not None and pct > 0) else ""
        d["判定理由"] = _build_reason(d)
        
        # --- 推奨アクション／推奨比率（連続＆バンド） ---
        rec, ratio_band, ratio_raw = _derive_recommendation(d)

        # 表示列：％は見た目、raw は後続のヒステリシス維持に使える
        if rec:
            d["推奨アクション"] = rec
        else:
            d["推奨アクション"] = d.get("推奨アクション", "") or ""

        d["推奨比率_raw"] = ratio_raw if ratio_raw is not None else d.get("推奨比率_raw", "")

        # UI表示はバンド値を％に（None は空白）
        if ratio_band is None:
            d["推奨比率"] = ""
        else:
            d["推奨比率"] = f"{int(round(float(ratio_band)*100))}%"

        # --- 営利対時価_flag（DB列が無い/空なら導出） ---
        if not (d.get("営利対時価_flag") or "").strip():
            d["営利対時価_flag"] = _derive_opratio_flag(d)

        # 念のため：空売り機関なし_flag を維持
        d["空売り機関なし_flag"] = _noshor_from_agency(d.get("空売り機関"))

        rows.append(d)
    return rows

# =================== HTMLテンプレ ===================

# =================== HTMLテンプレートの保存 ===================
def _ensure_template_file(template_dir: str, overwrite=True):
    os.makedirs(template_dir, exist_ok=True)
    path = os.path.join(template_dir, "dashboard.html")
    if overwrite or not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(DASH_TEMPLATE_STR)
    return path

# =================== ダッシュボード書き出し（完全版） ===================
def _load_offering_codes_from_db(conn, days=400):
    """
    offerings_events から直近 days 日の増資/行使/売出/CB/EB などの履歴があるコードを集合で返す。
    無ければ空集合。
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT printf('%04d', CAST(コード AS INTEGER)) "
            "FROM offerings_events WHERE date(提出時刻) >= date('now', ?)",
            (f"-{days} day",)
        )
        return {r[0] for r in cur.fetchall() if r and r[0]}
    except Exception:
        return set()

def phase_export_html_dashboard_offline(conn, html_path, template_dir="templates",
                                        include_log: bool=False, log_limit: int=2000):
    """
    ・候補/全カラム/（任意）signals_log を 1ファイルHTMLで出力
    ・表描画はフロント側JS。ここでは “JSONを __DATA__ に直埋め” する
    ・DBに存在しない列（推奨アクション等）は参照しないので、DB差異でも落ちない
    """

    # ---------- 1) DBから候補用の必要最小列だけ取得（存在しない列は触らない） ----------
    df_cand = pd.read_sql_query("""
      SELECT
        コード, 銘柄名,市場,
        現在値,
        "前日終値" AS 前日終値,
        "前日円差" AS "前日円差",
        前日終値比率,
        出来高, 時価総額億円,
        COALESCE(
          売買代金億,
          CASE WHEN 現在値 IS NOT NULL AND 出来高 IS NOT NULL
               THEN (現在値 * 出来高) / 100000000.0 END
        ) AS "売買代金(億)",
        売買代金20日平均億,
        RVOL代金,
        合成スコア,
        ATR14_PCT AS "ATR14%",
        初動フラグ, 初動開始日,
        底打ちフラグ, 底打ち開始日,
        右肩上がりフラグ, 右肩開始日,
        右肩早期フラグ, 右肩早期開始日,
        右肩早期種別, 右肩早期種別開始日, 右肩早期前回種別,
        右肩早期スコア,
        空売り機関,
        シグナル更新日,
        営業利益,
        増資リスク, 増資スコア, 増資理由,財務コメント
      FROM screener
      ORDER BY COALESCE(時価総額億円,0) DESC, COALESCE(出来高,0) DESC, コード
    """, conn)

    # 全カラムタブは素直に全件（DB差異を吸収したいので * でOK）
    df_all = pd.read_sql_query("""
      SELECT * FROM screener
      ORDER BY COALESCE(時価総額億円,0) DESC, COALESCE(出来高,0) DESC, コード
    """, conn)
    
    
    # === 小数点第2位で数値型のまま統一（候補一覧 / 明日用 / 全カラム）===
    def _round2_inplace(df):
        if df is None or df.empty:
            return

        # 2桁丸め対象のカラム集合（存在するものだけ処理）
        percent_cols = [
            "前日終値比率", "前日終値比率（％）",
            "フォロー高値pct", "最大逆行pct", "リターン終値pct",
            "推奨比率", "ATR14%"
        ]
        money_cols = [
            "売買代金(億)", "売買代金億",          # テンプレ用の別名/DB実体の両対応
            "売買代金20日平均億", "RVOL代金",
            "時価総額億円"
        ]
        price_cols = [
            "現在値", "前日終値", "前日円差",      # Yahoo由来でブレやすい
            # 全カラムタブに混ざる可能性を考慮（存在すれば丸める）
            "始値", "高値", "安値", "終値"
        ]
        score_cols = [
            "右肩早期スコア", "合成スコア"
        ]

        # ラウンド対象を一括で2桁に統一
        targets = percent_cols + money_cols + price_cols + score_cols
        for col in targets:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    _round2_inplace(df_cand)
    _round2_inplace(df_all)

    # ログ（任意）
    if include_log:
        try:
            df_log = pd.read_sql_query(
                f"SELECT 日時, コード, 種別, 詳細 FROM signals_log ORDER BY 日時 DESC, コード LIMIT {int(log_limit)}",
                conn
            )
        except Exception:
            df_log = pd.DataFrame(columns=["日時","コード","種別","詳細"])
    else:
        df_log = pd.DataFrame(columns=["日時","コード","種別","詳細"])

    # ---------- 2) 候補一覧のセル内補足（“列を増やさず” フラグに日付など小さく追記） ----------
    def _mini(text):
        if not text:
            return ""
        # 改行(<br>)はやめて同一行に小さく追記
        return f"&nbsp;<span class='mini'>{text}</span>"

    def _flag_with_since(flag, since):
        flag = (flag or "").strip()
        if flag == "候補" and since:
            return f"{flag}{_mini(f'{since}〜')}"
        return flag

    def _early_kind_mini(since_kind, prev_kind):
        extras = []
        if since_kind:
            extras.append(f"{since_kind}〜")
        if prev_kind:
            extras.append(f"prev: {prev_kind}")
        return _mini(" / ".join(extras)) if extras else ""

    if not df_cand.empty:
        df_cand["初動フラグ"]       = df_cand.apply(lambda r: _flag_with_since(r.get("初動フラグ"), r.get("初動開始日")), axis=1)
        df_cand["底打ちフラグ"]     = df_cand.apply(lambda r: _flag_with_since(r.get("底打ちフラグ"), r.get("底打ち開始日")), axis=1)
        df_cand["右肩上がりフラグ"] = df_cand.apply(lambda r: _flag_with_since(r.get("右肩上がりフラグ"), r.get("右肩開始日")), axis=1)
        df_cand["右肩早期フラグ"]   = df_cand.apply(lambda r: _flag_with_since(r.get("右肩早期フラグ"), r.get("右肩早期開始日")), axis=1)
        # 早期種別セルの下段補足（本体の種別文字列はそのまま）
        df_cand["右肩早期種別_mini"] = df_cand.apply(
            lambda r: _early_kind_mini(r.get("右肩早期種別開始日"), r.get("右肩早期前回種別"))
                      if (r.get("右肩早期種別") or "").strip() else "",
            axis=1
        )
        # 補助列は候補テーブルには出さない
        for c in ["初動開始日","底打ち開始日","右肩開始日","右肩早期開始日","右肩早期種別開始日","右肩早期前回種別"]:
            if c in df_cand.columns:
                df_cand.drop(columns=[c], inplace=True)

    # ---------- 3) Python辞書化（NaN/np型/日付などは _safe_jsonable で丸める） ----------
    def _records_safe(df: pd.DataFrame):
        out = []
        for rec in df.to_dict("records"):
            out.append({k: _safe_jsonable(v) for k, v in rec.items()})
        return out

    # URL列などの補完は _prepare_rows に任せる（判定・理由・Yahoo/X等の生成も含む）
    cand_rows = _prepare_rows(df_cand) if not df_cand.empty else []
    n_strong = sum(1 for r in cand_rows if r.get("推奨アクション") == "エントリー有力")
    n_small  = sum(1 for r in cand_rows if r.get("推奨アクション") == "小口提案")
    print(f"[recommend] 有力:{n_strong} / 小口:{n_small}")

    all_rows  = _prepare_rows(df_all)  if not df_all.empty  else []
    log_rows  = df_log.to_dict("records") if include_log else []

    # そのままだと numpy スカラーが混じるので再度 safe 化
    cand_rows = _records_safe(pd.DataFrame(cand_rows)) if cand_rows else []
    all_rows  = _records_safe(pd.DataFrame(all_rows))  if all_rows  else []
    log_rows  = _records_safe(pd.DataFrame(log_rows))  if log_rows  else []

    # --- meta: DBの最新シグナル更新日から翌営業日を作る（祝日/土日補正） ---

    def _to_date10(s):
        s = str(s or '')[:10]
        try:
            return dtm.datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    # cand_rows は直前で _records_safe 済みの list[dict]
    _base = None
    if cand_rows:
        try:
            dates = [_to_date10(r.get("シグナル更新日")) for r in cand_rows]
            dates = [d for d in dates if d]
            _base = max(dates) if dates else None
        except Exception:
            _base = None

    meta = {"base_day": None, "next_business_day": None}
    if _base:
        meta["base_day"] = _base.strftime("%Y-%m-%d")
        # 既存の祝日/休場ロジックを利用
        _next = next_business_day_jp(_base)
        meta["next_business_day"] = _next.strftime("%Y-%m-%d")

    # ---------- 4) JSON を __DATA__ に直埋め ----------
    # --- 祝日/土日補正：翌営業日を JSON(meta) に埋め込む ---

    try:
        import jpholiday
        def _is_holiday(d: dtm.date) -> bool:
            return (d.weekday() >= 5) or jpholiday.is_holiday(d)
    except Exception:
        # jpholiday が無い環境では土日のみ補正
        def _is_holiday(d: dtm.date) -> bool:
            return d.weekday() >= 5

    def _to_date(s) -> dtm.date | None:
        if not s:
            return None
        s = str(s)[:10]  # "YYYY-MM-DD ..." 形式にも対応
        try:
            return dtm.datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _latest_update_day(rows) -> dtm.date | None:
        try:
            if hasattr(rows, "iterrows"):  # pandas.DataFrame
                dates = [_to_date(r.get("シグナル更新日")) for _, r in rows.iterrows()]
            else:                           # list[dict]
                dates = [_to_date(r.get("シグナル更新日")) for r in rows]
            dates = [d for d in dates if d]
            return max(dates) if dates else None
        except Exception:
            return None

    def _next_business_day(d: dtm.date) -> dtm.date:
        d = d + dtm.timedelta(days=1)
        while _is_holiday(d):
            d += dtm.timedelta(days=1)
        return d

    _base = _latest_update_day(cand_rows)  # ← cand_rows は既存の候補リスト/DF
    meta = {"base_day": None, "next_business_day": None}
    if _base:
        _next = _next_business_day(_base)
        meta["base_day"] = _base.strftime("%Y-%m-%d")
        meta["next_business_day"] = _next.strftime("%Y-%m-%d")

    # ---------- 4) JSON を __DATA__ に直埋め ----------
    
    # 直近30営業日の「コード・日付・判定」を hist として付与（無ければ空でOK）
    # signals_bg から直近の銘柄別・日次判定を取る版
    def _build_hist_rows():

        # cand の銘柄と最終日を基準に期間を決める
        codes = sorted({ r.get("コード") for r in cand_rows if r.get("コード") })
        if not codes:
            return []

        try:
            last_day = max(pd.to_datetime([r.get("シグナル更新日") for r in cand_rows
                                           if r.get("シグナル更新日")])).normalize()
        except Exception:
            last_day = pd.Timestamp.today().normalize()
        start_day = last_day - pd.tseries.offsets.BDay(60)  # 直近60営業日ぶん

        # --- ここから修正：IN (:codes) を qmarks に展開して渡す ---
        # ※ codes が多すぎると SQLite の変数上限（既定 999）に触れるので安全側で分割
        def _fetch_chunk(chunk):
            qmarks = ",".join("?" * len(chunk))
            sql = f"""
                SELECT
                  コード,
                  DATE(日時) AS シグナル更新日,
                  判定,
                  フォロー高値pct,
                  最大逆行pct,
                  リターン終値pct
                FROM signals_log
                WHERE DATE(日時) BETWEEN ? AND ?
                  AND コード IN ({qmarks})
            """
            params = [str(start_day.date()), str(last_day.date()), *chunk]
            return pd.read_sql_query(sql, conn, params=params)

        dfs = []
        MAX_VARS = 900  # 余裕を見て 900
        for i in range(0, len(codes), MAX_VARS):
            dfs.append(_fetch_chunk(codes[i:i+MAX_VARS]))
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["コード","シグナル更新日","判定"])

        if df.empty:
            return []

        # 判定 正規化
        def _norm_hit(row):
            s = str(row.get("判定") or "").strip()
            if "当たり" in s: return "当たり！"
            if "外れ" in s:   return "外れ！"
            try:
                return "当たり！" if float(row.get("フォロー高値pct") or 0) > 0 else "外れ！"
            except Exception:
                return "外れ！"

        df["判定"] = df.apply(_norm_hit, axis=1)
        df["シグナル更新日"] = pd.to_datetime(df["シグナル更新日"]).dt.strftime("%Y-%m-%d")

        hist = df[["コード", "シグナル更新日", "判定"]].dropna().drop_duplicates()
        return hist.sort_values(["コード", "シグナル更新日"]).to_dict("records")

    hist_rows = _build_hist_rows()

    # ---- data_obj を作る直前（この塊でOK）----
    # 実績（決算）
    try:
        earnings_rows = load_recent_earnings_from_db(DB_PATH, days=7, limit=300)
    except Exception as e:
        print(f"[earnings][WARN] failed to load: {e}")
        earnings_rows = []

    # ---- 予測タブの行を組み立て（5列を追加してJSON化） ----
    try:
        with open_conn(DB_PATH) as _c:
            tbl = build_earnings_tables(_c)  # (ev_df, pre_df) or {"ev":..., "pre":...} どちらでも可

            # pre_df を取り出し
            if isinstance(tbl, tuple):
                _, pre_df = tbl
            elif isinstance(tbl, dict):
                pre_df = tbl.get("pre")
            else:
                pre_df = None

            if pre_df is not None and not pre_df.empty:
                # 最新終値を取得（screenerから）
                px = pd.read_sql_query("SELECT コード, 現在値 FROM screener", _c)

                # 期待株価/修正見通し/過熱度/スコア理由/予測ヒントを作る
                df = pre_df.merge(px, on="コード", how="left")
                def _row_apply(r):
                    code = str(r.get("コード") or "")
                    last = float(r.get("現在値") or np.nan)
                    mom  = float(r.get("momentum_score") or 0.0)
                    edge = float(r.get("edge_score") or 0.0)
                    return pd.Series({
                        "期待株価":   calc_expected_price(_c, code, last, mom, edge),
                        "修正見通し": classify_revision_bias(edge, mom),
                        "過熱度":     judge_overheat(_c, code, mom),
                        "スコア理由": _mk_score_reason(r.to_dict()),
                        "予測ヒント": _mk_hint(r.to_dict())
                    })
                extra = df.apply(_row_apply, axis=1)
                df = pd.concat([df, extra], axis=1)
                # --- 欠損の埋め＆型整形（空欄になりがちな列を強制的に埋める） ---
                df["スコア理由"] = df["スコア理由"].fillna("根拠薄め（暫定）")
                df["予測ヒント"] = df["予測ヒント"].fillna("（準備中）")
                df["修正見通し"] = df["修正見通し"].fillna("中立")
                df["過熱度"]     = df["過熱度"].fillna("中立")
                df["期待株価"]   = pd.to_numeric(df["期待株価"], errors="coerce")
                # 期待株価が NaN のときは現在値で代用（空欄回避）
                if "現在値" in df.columns:
                    df.loc[df["期待株価"].isna(), "期待株価"] = pd.to_numeric(df["現在値"], errors="coerce")

                # フロント互換の銘柄名
                if "銘柄" not in df.columns:
                    names = pd.read_sql_query("SELECT コード, 銘柄名 FROM screener", _c)
                    df = df.merge(names, on="コード", how="left")
                    df["銘柄"] = df["銘柄名"].fillna(df["コード"])

                # JSON化
                preearn_rows = [{k: _safe_jsonable(v) for k, v in rec.items()}
                                for rec in df.to_dict("records")]
            else:
                preearn_rows = []
    except Exception as e:
        print(f"[preearn][WARN] failed to build pre-earnings: {e}")
        preearn_rows = []

    
    # フォールバック：空なら screener から暫定ランキングを生成
    if not preearn_rows:
        try:
            preearn_rows = _build_preearn_fallback(_c)
            print(f"[preearn][fallback] generated rows: {len(preearn_rows)}")
        except Exception as _e:
            print(f"[preearn][fallback][WARN] {_e}")
    
    # ---- data_obj 構築 ----
    offer_codes = sorted(list(_load_offering_codes_from_db(conn, days=400)))
    data_obj = {
        "cand": cand_rows,
        "all":  all_rows,
        "logs": log_rows,
        "hist": hist_rows,
        "meta": meta,
        "earnings": earnings_rows,
        "preearn": preearn_rows,
        "offer_codes": offer_codes
}
    data_json = json.dumps(data_obj, ensure_ascii=False, default=str, separators=(",", ":"))

    # ---------- 5) テンプレート描画 ----------
    template_dir = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\templates"
    _ensure_template_file(template_dir, overwrite=True)
    env = Environment(loader=FileSystemLoader(template_dir, encoding="utf-8"),
                      autoescape=select_autoescape(["html"]))
    env.filters["fmt_cell"] = _fmt_cell

    try:
        _tz = ZoneInfo("Asia/Tokyo")
        build_id = dtm.datetime.now(_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # tzデータが無い環境でも動くようフォールバック
        build_id = dtm.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    tpl = env.get_template("dashboard.html")
    html = tpl.render(
        include_log=include_log,
        data_json=data_json,                         # ← JSが読むJSON
        generated_at=build_id,                       # 既存のgenerated_atも更新時刻でOK
        build_id=build_id                            # ★ これをテンプレに渡す
    )

    # ---------- 6) 書き出し ----------
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8", newline="") as f:
        f.write(html)
    print(f"[export] HTML書き出し: {html_path} (logs={'ON' if include_log else 'OFF'}) | build: {build_id}")

# ========== /Template exporter ==========

# ========= 営業利益 

def update_operating_income_and_ratio(conn, batch_size=300, max_workers=12, use_quarterly=False):
    """
    既存カラムに合わせて保存：
      - 営業利益（整数・単位=億円）
      - 営利対時価（小数2桁・単位=%）
    yahooqueryのincome_statementから最新（年次/四半期）を取得。
    """
    try:
        pass
    except ImportError:
        print("[oper] yahooquery が未導入のためスキップします")
        return

    codes = [str(r[0]) for r in conn.execute("SELECT コード FROM screener").fetchall()]
    if not codes:
        print("[oper] 対象コードなし")
        return

    freq = "quarterly" if use_quarterly else "annual"
    total_updated = 0

    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i+batch_size]
        symbols = [f"{c}.T" for c in chunk]

        tq = YQ(symbols, asynchronous=True, max_workers=max_workers)

        # income_statement を "最新 asOfDate" で1行に正規化
        latest = {}
        try:
            is_resp = tq.income_statement(freq)
            if hasattr(is_resp, "reset_index"):  # DataFrameパターン
                df = is_resp.reset_index()
                # ['symbol','asOfDate','OperatingIncome'(または OperatingIncomeLoss), ...]
                for sym, g in df.groupby("symbol"):
                    g = g.dropna(subset=["asOfDate"]).sort_values("asOfDate")
                    if len(g):
                        latest[sym] = g.iloc[-1].to_dict()
            else:  # dictパターン
                for sym, rows in (is_resp or {}).items():
                    if isinstance(rows, list):
                        rows = [r for r in rows if isinstance(r, dict) and r.get("asOfDate")]
                        rows.sort(key=lambda r: r.get("asOfDate"))
                        if rows:
                            latest[sym] = rows[-1]
        except Exception:
            pass

        updates = []  # (営業利益_億円_int, 営利対時価_pct_2f, コード)
        for sym in symbols:
            ent = latest.get(sym) or {}
            op = ent.get("OperatingIncome", ent.get("OperatingIncomeLoss"))
            if op is None:
                continue
            try:
                # yahooqueryは数値/文字/辞書混在のことがある
                if isinstance(op, dict):
                    op = op.get("raw", op.get("fmt"))
                op = float(str(op).replace(",", ""))
            except Exception:
                continue

# ===== Gmail送信 =====
def send_index_html_via_gmail(attach_path: str) -> bool:
    try:
        pass
    except Exception as e:
        print("[SEND][Gmail] import失敗:", e); return False

    path_to_send = attach_path
    maintype, subtype = "text", "html"
    filename = os.path.basename(attach_path)

    if SEND_HTML_AS_ZIP:
        zip_path = os.path.splitext(attach_path)[0] + ".zip"
        try:
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
                z.write(attach_path, arcname=os.path.basename(attach_path))
            path_to_send = zip_path; maintype, subtype = "application", "zip"; filename = os.path.basename(zip_path)
            print(f"[SEND][Gmail] HTMLをZIP化: {zip_path}")
        except Exception as e:
            print("[SEND][Gmail] ZIP作成失敗:", e); return False

    try:
        msg = EmailMessage()
        msg["Subject"] = GMAIL_SUBJ; msg["From"] = GMAIL_USER; msg["To"] = GMAIL_TO
        msg.set_content(GMAIL_BODY)
        with open(path_to_send, "rb") as f: data = f.read()
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD); smtp.send_message(msg)
        print(f"[SEND][Gmail] 送信 -> {GMAIL_TO} ({filename})"); return True
    except Exception as e:
        print("[SEND][Gmail] 送信失敗:", e); return False

# --- ここから: 前日終値アップデータ（履歴ベース／唯一の定義） ---
# ---- 前日終値比率(%)＋現在値から前日終値を逆算してDB更新 ----

def ensure_prevclose_columns(conn):
    """Schema fixed: no-op."""
    return

def _to_raw(v):
    if isinstance(v, dict):
        return v.get("raw", v.get("fmt"))
    return v

# ==== _to_raw が無ければ保険で定義（既にあれば不要）====
try:
    _to_raw
except NameError:
    def _to_raw(val):
        """yahooqueryが返す {'raw':x,'fmt':y} や文字列などを数値に寄せる."""
        if isinstance(val, dict):
            val = val.get("raw", val.get("fmt"))
        if val is None:
            return None
        try:
            return float(str(val).replace(",", ""))
        except Exception:
            return None

def get_prev_close_db_first(conn: sqlite3.Connection, code: str, quotes_prev: float | None = None) -> float | None:
    """
    優先: price_history 直近2営業日の『前日終値』
    代替: quotes.regularMarketPreviousClose（与えられていれば）
    """
    df = pd.read_sql_query(
        "SELECT 日付, 終値 FROM price_history WHERE コード=? ORDER BY 日付 DESC LIMIT 2",
        conn, params=(str(code),)
    )
    if df.shape[0] >= 2:
        prev = df.iloc[1]["終値"]
        if pd.notna(prev) and float(prev) != 0.0:
            return float(prev)
    if quotes_prev is not None and quotes_prev not in (0,):
        try:
            return float(quotes_prev)
        except Exception:
            pass
    return None

# ===== RVOL/売買代金 自動更新ユーティリティ =====

def _ensure_turnover_cols(conn: sqlite3.Connection):
    """Schema fixed: no-op."""
    return

def _jp_session_progress(dt: dtm.datetime | None = None) -> float:
    """場中の進捗率(0.0〜1.0)。東証 9:00–11:30 / 12:30–15:00 を300分=1.0で換算。"""
    if dt is None:
        dt = dtm.datetime.utcnow().replace(tzinfo=dtm.timezone.utc) + dtm.timedelta(hours=9)  # JST
    m = dt.hour * 60 + dt.minute
    s1, e1, s2, e2 = 9*60, 11*60+30, 12*60+30, 15*60
    if m < s1: return 0.0
    if s1 <= m <= e1: return (m - s1) / 300.0
    if e1 < m < s2:  return 150 / 300.0
    if s2 <= m <= e2: return (150 + (m - s2)) / 300.0
    return 1.0

def apply_auto_metrics_midday(conn: sqlite3.Connection,
                              use_time_progress: bool = True,
                              denom_floor: float = 1.0,       # 分母の床（20日平均が小さすぎるときの下限）
                              progress_floor: float = 0.33):   # 進捗の下限（寄り直後の誤差抑制）
    """
    現在値×出来高→『売買代金億』、そこから『RVOL代金』を更新する。
    すべて DB 保存時点で小数 2 桁に丸める。
      - 売買代金億            : ROUND((現在値 * 出来高) / 1e8, 2)
      - RVOL代金              : 当日代金 / max(20日平均代金, denom_floor) / progress(任意) を ROUND(..., 2)
    """
    cur = conn.cursor()

    # 売買代金（億）= 現在値×出来高/1e8 → 2桁で保存（出来高>0 のときだけ計算）
    cur.execute("""
        UPDATE screener
        SET 売買代金億 =
          CASE
            WHEN 現在値 IS NOT NULL AND 出来高 IS NOT NULL AND 出来高 > 0
            THEN ROUND((現在値 * 出来高) / 100000000.0, 2)
          END
    """)

    # RVOL = 当日代金 / max(20日平均代金, denom_floor) / progress
    if use_time_progress:
        f = max(_jp_session_progress(), progress_floor)  # 進捗（下限でクランプ）
        cur.execute("""
            WITH p AS (SELECT ? AS f, ? AS dmin)
            UPDATE screener
            SET RVOL代金 =
              CASE
                WHEN 売買代金億 IS NOT NULL
                 AND 売買代金20日平均億 IS NOT NULL
                THEN ROUND(
                  売買代金億 /
                  (
                    (CASE
                       WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                       THEN (SELECT dmin FROM p)
                       ELSE 売買代金20日平均億
                     END) * (SELECT f FROM p)
                  ),
                  2
                )
              END
        """, (f, denom_floor))
    else:
        # 進捗補正を使わない場合も 2桁で保存
        cur.execute("""
            WITH p AS (SELECT ? AS dmin)
            UPDATE screener
            SET RVOL代金 =
              CASE
                WHEN 売買代金億 IS NOT NULL
                 AND 売買代金20日平均億 IS NOT NULL
                THEN ROUND(
                  売買代金億 /
                  (CASE
                     WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                     THEN (SELECT dmin FROM p)
                     ELSE 売買代金20日平均億
                   END),
                  2
                )
              END
        """, (denom_floor,))

    conn.commit()
    cur.close()

def apply_auto_metrics_eod(conn: sqlite3.Connection,
                           denom_floor: float = 1.0):
    """
    終値×出来高で当日代金（億）を確定 → 20日平均（億）を更新 → RVOL代金を更新。
    すべて DB 保存時点で小数 2 桁に丸める。
      - 売買代金億            : ROUND( (終値*出来高)/1e8 , 2 )
      - 売買代金20日平均億    : 直近20本の平均を ROUND(..., 2)
      - RVOL代金              : 当日代金 / max(20日平均代金, denom_floor) を ROUND(..., 2)
    """
    cur = conn.cursor()

    # -- 当日確定 代金（億）: 2桁で保存
    cur.execute("""
        UPDATE screener AS s
        SET 売買代金億 = (
          SELECT ROUND( (ph.終値 * COALESCE(ph.出来高, 0)) / 100000000.0, 2 )
          FROM price_history ph
          WHERE ph.コード = s.コード
          ORDER BY ph.日付 DESC
          LIMIT 1
        )
    """)

    # -- 20日平均（直近20本の単純平均）: 2桁で保存
    cur.execute("""
        WITH lastdate AS (
          SELECT コード, MAX(日付) AS mx
          FROM price_history
          GROUP BY コード
        ),
        hist AS (
          SELECT
            ph.コード,
            (ph.終値 * COALESCE(ph.出来高, 0)) / 100000000.0 AS 代金億_raw,
            ROW_NUMBER() OVER (PARTITION BY ph.コード ORDER BY ph.日付 DESC) AS rn
          FROM price_history ph
          JOIN lastdate ld
            ON ph.コード = ld.コード
           AND ph.日付 <= ld.mx
        ),
        avg20 AS (
          SELECT コード, ROUND(AVG(代金億_raw), 2) AS avg20
          FROM hist
          WHERE rn <= 20
          GROUP BY コード
        )
        UPDATE screener
        SET 売買代金20日平均億 = (SELECT avg20 FROM avg20 WHERE avg20.コード = screener.コード)
    """)

    # -- RVOL代金（分母に床）: 2桁で保存
    cur.execute("""
        WITH p AS (SELECT ? AS dmin)
        UPDATE screener
        SET RVOL代金 =
          CASE
            WHEN 売買代金億 IS NOT NULL
             AND 売買代金20日平均億 IS NOT NULL
            THEN ROUND(
              売買代金億 /
              (CASE
                 WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                 THEN (SELECT dmin FROM p)
                 ELSE 売買代金20日平均億
               END),
              2
            )
          END
    """, (denom_floor,))

    conn.commit()
    cur.close()

# ===== /RVOL/売買代金 自動更新ユーティリティ =====

def open_html_locally(html_path: str, wait_sec: float = 0.0, cool_min: int = 0, force: bool = False) -> bool:
    """
    ローカルHTMLを既定ブラウザで開く（Windowsは os.startfile を優先）。
    - cool_min > 0 なら、直近オープンからその分(分)は再オープンしない
    - force=True でクールダウン無視
    戻り値: 開けたら True
    """
    p = Path(html_path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"not found: {p}")

    stamp = p.with_suffix(p.suffix + ".opened")
    if not force and cool_min > 0 and stamp.exists():
        if time.time() - stamp.stat().st_mtime < cool_min * 60:
            return False  # クールダウン中

    if wait_sec > 0:
        dtm.time.sleep(wait_sec)

    opened = False

    # 1) Windowsは最も確実な ShellExecute 相当で開く
    if os.name == "nt":
        try:
            os.startfile(str(p))  # 既定ブラウザで開く
            opened = True
        except Exception:
            opened = False

    # 2) うまくいかなければ webbrowser にフォールバック
    if not opened:
        url = p.as_uri()  # file:///H:/... に変換
        try:
            opened = webbrowser.open(url) or webbrowser.open_new_tab(url)
        except Exception:
            opened = False

    # 3) 成功したらスタンプ作成
    if opened:
        try:
            stamp.touch()
        except Exception:
            pass

    return opened

def apply_composite_score(conn: sqlite3.Connection,
                          w_rate=0.4, w_rvol=0.4, w_turn=0.2):
    """
    合成スコア = 0.4*rank(前日終値比率) + 0.4*rank(RVOL代金) + 0.2*rank(売買代金億)
    （rankはパーセンタイル0-100、小数1桁）
    """
    df = pd.read_sql_query("""
        SELECT コード, 前日終値比率, RVOL代金, 売買代金億
        FROM screener
    """, conn)

    for c in ["前日終値比率", "RVOL代金", "売買代金億"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # パーセンタイル（NaNは0扱い）
    pct = lambda s: (s.rank(pct=True) * 100.0)
    r_rate = pct(df["前日終値比率"].fillna(-1e18))
    r_rvol = pct(df["RVOL代金"].fillna(0))
    r_turn = pct(df["売買代金億"].fillna(0))

    score = (w_rate*r_rate + w_rvol*r_rvol + w_turn*r_turn).round(1)
    up = list(zip(score.fillna(0).tolist(), df["コード"].astype(str).tolist()))

    cur = conn.cursor()
    # （列が無ければ既にあなたの _ensure_* 系で追加済み。二重ALTERにならないよう注意）
    cur.executemany("UPDATE screener SET 合成スコア=? WHERE コード=?", up)
    conn.commit()
    cur.close()

def apply_atr14_pct(conn: sqlite3.Connection) -> int:
    """
    price_history から TR→ATR14→ATR14% を算出し、screener.ATR14_PCT を更新する。
      TR  = max(高値-安値, |高値-前日終値|, |安値-前日終値|)
      ATR14% = ATR14 / 直近終値 * 100
    戻り値: 更新対象になった銘柄数（目安値）
    """
    # 1) カラムが無ければ追加
    cur = conn.cursor()
    if "ATR14_PCT" not in cols:
        cur.execute("ALTER TABLE screener ADD COLUMN ATR14_PCT REAL")
        conn.commit()

    # 2) まずはSQL（ウィンドウ関数）で一括更新を試す
    try:
        cur.execute("""
            WITH ph AS (
              SELECT
                コード, 日付, 始値, 高値, 安値, 終値,
                LAG(終値) OVER (PARTITION BY コード ORDER BY 日付) AS prevC
              FROM price_history
            ),
            tr AS (
              SELECT
                コード, 日付,
                MAX(
                  高値 - 安値,
                  ABS(高値 - prevC),
                  ABS(安値 - prevC)
                ) AS TR
              FROM ph
            ),
            atr AS (
              SELECT
                コード, 日付,
                AVG(TR) OVER (
                  PARTITION BY コード
                  ORDER BY 日付
                  ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS ATR14
              FROM tr
            ),
            last_row AS (
              SELECT a.コード, a.ATR14, p.終値
              FROM atr a
              JOIN (
                SELECT コード, MAX(日付) AS mx
                FROM price_history
                GROUP BY コード
              ) ld
                ON a.コード = ld.コード AND a.日付 = ld.mx
              JOIN price_history p
                ON p.コード = ld.コード AND p.日付 = ld.mx
            )
            UPDATE screener
            SET ATR14_PCT = ROUND(
                  (SELECT (ATR14 / NULLIF(終値,0)) * 100.0
                   FROM last_row
                   WHERE last_row.コード = screener.コード),
                2)
            WHERE EXISTS (SELECT 1 FROM last_row WHERE last_row.コード = screener.コード);
        """)
        conn.commit()
        # 何件更新されたかの推定（厳密な件数取得は難しいためサンプルで代用）
        cur.execute("SELECT COUNT(*) FROM screener WHERE ATR14_PCT IS NOT NULL")
        n = cur.fetchone()[0]
        cur.close()
        return n

    except sqlite3.OperationalError:
        # 3) フォールバック（pandasで計算）
        cur.close()
        try:
            import pandas as pd
            import numpy as np
        except Exception as e:
            # pandasが無ければ何もせず終了
            return 0

        ph = pd.read_sql_query(
            "SELECT コード, 日付, 高値, 安値, 終値 "
            "FROM price_history ORDER BY コード, 日付",
            conn
        )
        if ph.empty:
            return 0

        ph["prevC"] = ph.groupby("コード")["終値"].shift(1)

        tr = np.maximum.reduce([
            (ph["高値"] - ph["安値"]).abs().values,
            (ph["高値"] - ph["prevC"]).abs().values,
            (ph["安値"] - ph["prevC"]).abs().values
        ])
        ph["TR"] = tr

        atr14 = (
            ph.groupby("コード")["TR"]
              .rolling(14, min_periods=14)
              .mean()
              .reset_index(level=0, drop=True)
        )
        ph["ATR14"] = atr14

        last = (
            ph.groupby("コード")
              .tail(1)[["コード", "ATR14", "終値"]]
              .dropna(subset=["ATR14", "終値"])
              .copy()
        )
        last["ATR14_PCT"] = (last["ATR14"] / last["終値"] * 100.0).round(2)

        cur = conn.cursor()
        cur.executemany(
            "UPDATE screener SET ATR14_PCT=? WHERE コード=?",
            list(zip(last["ATR14_PCT"].tolist(),
                     last["コード"].astype(str).tolist()))
        )
        conn.commit()
        n = len(last)
        cur.close()
        return n

def ensure_since_schema(conn):
    """Schema fixed: no-op."""
    return
    def need(col, decl):
        if col not in cols:
            add.append((col, decl))
    need("初動開始日", "TEXT")
    need("底打ち開始日", "TEXT")
    need("早期開始日", "TEXT")
    need("右肩開始日", "TEXT")
    need("早期種別開始日", "TEXT")
    need("早期前回種別", "TEXT")
    if add:
        cur = conn.cursor()
        for c, d in add:
            try:
                cur.execute(f"ALTER TABLE screener ADD COLUMN {c} {d}")
            except Exception:
                pass
        conn.commit(); cur.close()

def _parse_early_tag(detail: str) -> str | None:
    if detail is None:
        return None
    try:
        # 例: "ブレイク | score=..." → "ブレイク"
        head = str(detail).split("|", 1)[0].strip()
        return head if head else None
    except Exception:
        return None

def _prev_business_day(d, extra_closed):
    # 既存の prev_business_day_jp / is_jp_market_holiday を想定
    return prev_business_day_jp(d, extra_closed)

def _streak_start_for_kind(conn, code: str, kind: str, extra_closed) -> str | None:
    """
    signals_log(コード, 種別)の「直近の連続区間の開始日」を返す。
    休場日はスキップして、営業日連続が切れた所で打ち切り。
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 日時 FROM signals_log 
        WHERE コード=? AND 種別=? 
        ORDER BY 日時 DESC
    """, (str(code), kind))
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    if not rows:
        return None

    def dstr(d): return d.strftime("%Y-%m-%d")
    try:
        latest = dtm.datetime.strptime(rows[0][:10], "%Y-%m-%d").date()
    except Exception:
        return None

    run_start = latest
    cur_check = latest
    have = {r[:10] for r in rows}

    while True:
        prev_d = _prev_business_day(cur_check, extra_closed)
        if dstr(prev_d) in have:
            run_start = prev_d
            cur_check = prev_d
            continue
        break
    return run_start.strftime("%Y-%m-%d") if run_start else None

def _early_type_range_and_prev(conn, code: str, extra_closed):
    """
    '右肩上がり-早期' のタグ推移を解析。
    戻り値: (start_date_str, current_tag, previous_tag) いずれも無ければ None
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 日時, 詳細 FROM signals_log
        WHERE コード=? AND 種別='右肩上がり-早期'
        ORDER BY 日時 DESC
    """, (str(code),))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None, None, None

    cur_tag = None
    cur_dates = []
    prev_tag = None
    for dt_s, detail in rows:
        tag = _parse_early_tag(detail)
        d = dtm.datetime.strptime(dt_s[:10], "%Y-%m-%d").date()
        if cur_tag is None:
            cur_tag = tag
            cur_dates.append(d)
            continue
        if tag == cur_tag:
            cur_dates.append(d)
            continue
        prev_tag = tag  # タグが切り替わった直前のタグ
        break

    if not cur_dates:
        return None, cur_tag, prev_tag
    start_date = min(cur_dates).strftime("%Y-%m-%d")
    return start_date, cur_tag, prev_tag

def phase_update_since_dates(conn):
    """
    screener に以下を埋める:
      - 初動開始日 / 底打ち開始日 / 早期開始日 / 右肩開始日
      - 早期種別開始日 / 早期前回種別
    """
    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 初動フラグ, 底打ちフラグ, 右肩上がりフラグ, 右肩早期フラグ, 右肩早期種別
        FROM screener
    """)
    rows = cur.fetchall()
    cur.close()

    upd = []
    for code, sh, bt, ru, er, etype in rows:
        code = str(code)

        s_sh = _streak_start_for_kind(conn, code, "初動", extra_closed) if (sh == "候補") else None
        s_bt = _streak_start_for_kind(conn, code, "底打ち", extra_closed) if (bt == "候補") else None
        s_ru = _streak_start_for_kind(conn, code, "右肩上がり", extra_closed) if (ru == "候補") else None
        s_er = _streak_start_for_kind(conn, code, "右肩上がり-早期", extra_closed) if (er == "候補") else None

        etype_start = None
        prev_type = None
        if etype and str(etype).strip() != "":
            etype_start, cur_type, prev_type = _early_type_range_and_prev(conn, code, extra_closed)
            # 表示とログが食い違ったらログ側に合わせる（任意）
            if cur_type and cur_type != etype:
                etype = cur_type

        upd.append((s_sh, s_bt, s_er, s_ru, etype_start, prev_type, code))

    if upd:
        cur = conn.cursor()
        cur.executemany("""
            UPDATE screener
               SET 初動開始日=?,
                   底打ち開始日=?,
                   早期開始日=?,
                   右肩開始日=?,
                   早期種別開始日=?,
                   早期前回種別=?
             WHERE コード=?
        """, upd)
        conn.commit(); cur.close()

def relax_rejudge_signals(
    conn,
    lookahead_days: int = None,
    req_high_pct: float = None,
    max_adverse_pct: float = None,
):
    """
    signals_log で '外れ' になっているシグナルを、発生日から一定日数内の値動きで再評価。
    条件:
      ・lookahead_days 日以内に +req_high_pct% 到達
      ・かつ 最大逆行（安値基準）が -max_adverse_pct% 以内
    満たせば 判定='再評価OK' / 理由 を上書き。
    """

    L = lookahead_days or REJUDGE_LOOKAHEAD_DAYS
    UP = req_high_pct   or REJUDGE_REQ_HIGH_PCT
    DN = max_adverse_pct or REJUDGE_MAX_ADVERSE_PCT

    cur = conn.cursor()
    # 直近30日分の “外れ” を対象（必要に応じて期間は調整可）
    cur.execute("""
      SELECT コード, 日時
        FROM signals_log
       WHERE 判定='外れ'
         AND 日時 >= date('now','-30 day')
    """)
    rows = cur.fetchall()

    upd = 0
    for code, ts in rows:
        base_date = str(ts)[:10]
        g = pd.read_sql_query("""
          SELECT 日付, 終値, 高値, 安値
            FROM price_history
           WHERE コード=?
             AND 日付 BETWEEN date(?) AND date(?, '+' || ? || ' day')
           ORDER BY 日付 ASC
        """, conn, params=(code, base_date, base_date, L))

        if g.empty:
            continue

        entry = float(g.iloc[0]["終値"])
        if entry is None or entry == 0:
            continue

        max_up = (g["高値"].max() / entry - 1.0) * 100.0
        max_dn = (g["安値"].min() / entry - 1.0) * 100.0  # 負の値（例: -6.3）

        if (max_up >= UP) and (max_dn >= -DN):
            cur2 = conn.cursor()
            cur2.execute("""
              UPDATE signals_log
                 SET 判定='再評価OK',
                     理由=?
               WHERE コード=? AND 日時=?
            """, (f"delayed hit: {L}D +{max_up:.1f}% / MAE {max_dn:.1f}%", code, ts))
            conn.commit()
            cur2.close()
            upd += 1

    cur.close()
    if upd:
        print(f"[rejudge] 再評価OK に更新: {upd} 件")
    else:
        print("[rejudge] 該当なし")

# === 追加：ロガー共通セットアップ（新規） ===

def setup_fin_logger(verbose: bool = False):
    """
    増資リスク系の処理で使う共通ロガー。
    - コンソール & ファイルに出力（ローテーション）
    - verbose=True で DEBUG、False で INFO
    """
    logger = logging.getLogger("dilution")
    # すでにハンドラ付いてたら再利用
    if logger.handlers:
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        return logger

    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # 出力先ディレクトリ（既存の OUTPUT_DIR を利用）
    try:
        base_dir = OUTPUT_DIR  # 既存変数を利用:contentReference[oaicite:1]{index=1}
    except NameError:
        base_dir = os.getcwd()

    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"dilution_{dtm.datetime.now().strftime('%Y%m%d')}.log")

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

    # ファイル（1MBローテーション×3）
    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    # コンソール
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    logger.info(f"Logger initialized. log_path={log_path}")
    return logger

# ===== 増資判定用

def _yf_num(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)): return None
        return float(x)
    except Exception:
        return None

def _sum_quarters(df_like, keys, n=4):
    """直近n四半期の合計"""
    if df_like is None or df_like.empty: return 0.0
    for k in keys:
        if k in df_like.index:
            vals = [ _yf_num(v) or 0.0 for v in list(df_like.loc[k].values)[:n] ]
            return float(sum(vals))
    return 0.0

# --- BEGIN: batch_update_all_financials (貼り付け用) ---
# 依存: pip install yahooquery
try:
    pass
except Exception:
    YQ = None

YQ_MAX_WORKERS = 8

def _safe_num(v):
    try:
        if v is None: return None
        if isinstance(v, str):
            s = v.strip().replace(",", "").replace(" ", "")
            if s in ("", "-", "None", "nan", "NaN"): return None
            return float(s)
        if isinstance(v, (int, float)):
            if isinstance(v, float) and (v != v): return None
            return float(v)
    except Exception:
        return None

def _get_from_periods(obj, keys):
    if obj is None: return None
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                try: return float(obj[k])
                except Exception: pass
        # iterate periods
        for per, fields in obj.items():
            if isinstance(fields, dict):
                for k in keys:
                    if k in fields and fields[k] is not None:
                        try: return float(fields[k])
                        except Exception: pass
    return None

def _sum_recent(obj, keys, n=4):
    if obj is None: return None
    total = 0.0; cnt = 0
    if isinstance(obj, dict):
        for per, fields in obj.items():
            if isinstance(fields, dict):
                v = None
                for k in keys:
                    if k in fields and fields[k] is not None:
                        try: v = float(fields[k]); break
                        except Exception: v = None
                if v is not None:
                    total += v
                cnt += 1
                if cnt >= n: break
    return total if cnt>0 else None

def _sum_dividends_1y(divs, one_year_ago):
    if divs is None: return 0.0
    s = 0.0
    try:
        if hasattr(divs, "items"):
            for k, v in dict(divs).items():
                try:
                    s += float(v)
                except Exception:
                    continue
            return float(s)
    except Exception:
        pass
    try:
        for item in divs:
            if isinstance(item, dict):
                amt = item.get("amount") or item.get("dividend") or item.get("value")
                if amt is not None:
                    try: s += float(amt)
                    except Exception: pass
        return float(s)
    except Exception:
        return 0.0

def add_column_if_missing(conn, table, colname, decl):
    """Schema fixed: no-op."""
    return

def _fmt(x, nd=2):
    """数値を安全にフォーマット（None→'NA'、例外時も'NA'）"""
    try:
        if x is None:
            return "NA"
        return f"{float(x):.{nd}f}"
    except Exception:
        return "NA"

# === 置換：本体（yahooquery 取得→解析→DB反映） ===
def batch_update_all_financials(conn,
                                chunk_size: int = 200,
                                force_refresh: bool = False,
                                sleep_between_chunks: float = 0.1,
                                verbose: bool = False,
                                set_wal: bool = True):
    """
    yahooquery 一括取得 -> raw_fin_json キャッシュ -> 指標抽出 -> DB 一括更新
    ログを詳細に出す（INFO=要約 / DEBUG=銘柄ごとの詳細）。
    - DataFrame 返却時のパースに対応
    - 数値フォーマット安全化
    """
    # --------------------------
    # ロガー
    # --------------------------
    log = setup_fin_logger(verbose)  # 既存の共通ロガーを利用

    # --------------------------
    # 依存のフォールバック
    # --------------------------
    try:
        _safe_num  # noqa
    except NameError:
        def _safe_num(v):
            try:
                if v is None: return None
                if isinstance(v, str):
                    s = v.strip().replace(",", "").replace(" ", "")
                    if s in ("", "-", "None", "nan", "NaN"): return None
                    return float(s)
                if isinstance(v, (int, float)):
                    if isinstance(v, float) and (v != v): return None
                    return float(v)
            except Exception:
                return None

    # --------------------------
    # ユーティリティ（安全フォーマット／pandas対応）
    # --------------------------
    def _fmt(x, nd=2):
        """数値を安全にフォーマット（None→'NA'、例外時も'NA'）"""
        try:
            if x is None:
                return "NA"
            return f"{float(x):.{nd}f}"
        except Exception:
            return "NA"

    def _is_nonempty_df(x):
        try:
            import pandas as pd  # optional
            return isinstance(x, pd.DataFrame) and (not x.empty)
        except Exception:
            return False

    def _yf_pick_recent_from_df(df, keys):
        """DataFrame（index=項目、columns=期）から最も直近列の数値を取る"""
        try:
            if not _is_nonempty_df(df):
                return None
            for k in keys:
                if k in df.index:
                    vals = list(df.loc[k].values)  # 直近が先頭の想定（yahooquery）
                    for v in vals:
                        try:
                            if v is None:
                                continue
                            return float(v)
                        except Exception:
                            continue
            return None
        except Exception:
            return None

    def _yf_sum_quarters_df(df, keys, n=4):
        """DataFrame 版 直近n期合計"""
        try:
            if not _is_nonempty_df(df):
                return 0.0
            for k in keys:
                if k in df.index:
                    vals = list(df.loc[k].values)[:n]
                    acc = 0.0
                    for v in vals:
                        try:
                            acc += float(v or 0.0)
                        except Exception:
                            pass
                    return float(acc)
            return 0.0
        except Exception:
            return 0.0

    def _get_from_periods(obj, keys):
        """dict系（yahooquery 通常返却）の period→field から最初に見つかった値を返す"""
        if obj is None: return None
        if isinstance(obj, dict):
            # 直アクセス
            for k in keys:
                if k in obj and obj[k] is not None:
                    try: return float(obj[k])
                    except Exception: pass
            # periods を走査
            for per, fields in obj.items():
                if isinstance(fields, dict):
                    for k in keys:
                        if k in fields and fields[k] is not None:
                            try: return float(fields[k])
                            except Exception: pass
        return None

    def _sum_recent(obj, keys, n=4):
        """dict系の直近n期合計"""
        if obj is None: return None
        total = 0.0; cnt = 0
        if isinstance(obj, dict):
            for per, fields in obj.items():
                if isinstance(fields, dict):
                    v = None
                    for k in keys:
                        if k in fields and fields[k] is not None:
                            try:
                                v = float(fields[k]); break
                            except Exception:
                                v = None
                    if v is not None:
                        total += v
                    cnt += 1
                    if cnt >= n: break
        return total if cnt > 0 else None

    def _sum_dividends_1y(divs, one_year_ago):
        """配当は構造が様々なので、dict/iterable/DF の順にトライ。1年制限は最小限（DFは全合計）。"""
        # dict/iterable
        if divs is None:
            return 0.0
        try:
            # dict 形式
            if hasattr(divs, "items"):
                s = 0.0
                for _, v in dict(divs).items():
                    try: s += float(v)
                    except Exception: pass
                return float(s)
        except Exception:
            pass
        try:
            # iterable of dict
            s = 0.0
            for item in divs:
                if isinstance(item, dict):
                    amt = item.get("amount") or item.get("dividend") or item.get("value")
                    if amt is not None:
                        try: s += float(amt)
                        except Exception: pass
            return float(s)
        except Exception:
            pass
        # DataFrame
        if _is_nonempty_df(divs):
            try:
                return float(divs.sum(numeric_only=True).sum())
            except Exception:
                return 0.0
        return 0.0

    # --------------------------
    # 前処理・カラム確保
    # --------------------------

    one_year_ago = dtm.date.today() - dtm.timedelta(days=365)

    if set_wal:
        try:
            log.debug("[DB] PRAGMA set WAL / synchronous=OFF")
        except Exception as e:
            log.warning(f"[DB] PRAGMA set failed: {e}")

    for name, decl in [
        ("raw_fin_json", "TEXT"),
        ("財務更新日", "TEXT"),
        ("自己資本比率", "REAL"),
        ("営業CF_直近", "REAL"),
        ("営業CF_4Q合計", "REAL"),
        ("配当1年合計", "REAL"),
        ("自社株買い4Q合計", "REAL"), ("増資リスク", "INTEGER"), ("増資スコア", "REAL"), ("増資理由", "TEXT"),
    ]:
        try:
            pass
        except Exception as e:
            log.warning(f"[batch] add column {name} failed: {e}")

    cur = conn.cursor()
    cur.execute('SELECT コード, raw_fin_json, 財務更新日 FROM screener')
    rows = cur.fetchall()
    cur.close()
    codes = [str(r[0]) for r in rows]
    raw_map = {str(r[0]): r[1] for r in rows}
    fin_date_map = {str(r[0]): r[2] for r in rows}

    total = len(codes)
    processed = 0; updated_rows = 0; flags_set = 0; errors = 0

    log.info(f"[batch.start] total={total} chunk={chunk_size} force_refresh={force_refresh} yq=ON")

    # --------------------------
    # batched commit
    # --------------------------
    def commit_batch(metrics_rows, flags_rows):
        nonlocal updated_rows, flags_set
        if metrics_rows:
            conn.executemany("""
                UPDATE screener SET
                  "自己資本比率"      = ?,
                  "営業CF_直近"       = ?,
                  "営業CF_4Q合計"     = ?,
                  "配当1年合計"       = ?,
                  "自社株買い4Q合計" = ?,
                  "財務更新日"        = ?,
                  "raw_fin_json"      = ?
                WHERE "コード" = ?
            """, metrics_rows)
            conn.commit()
            updated_rows += len(metrics_rows)
            log.info(f"[commit.metrics] rows={len(metrics_rows)} total_updated={updated_rows}")
        if flags_rows:
            conn.executemany("""
                UPDATE screener SET "増資リスク"=?, "増資スコア"=?, "増資理由"=? WHERE "コード"=?
            """, flags_rows)
            conn.commit()
            flags_set += len(flags_rows)
            log.info(f"[commit.flags] rows={len(flags_rows)} total_flags={flags_set}")

    # --------------------------
    # main loop
    # --------------------------
    for i in range(0, total, chunk_size):
        chunk = codes[i:i+chunk_size]
        syms = [c if c.endswith(".T") else f"{c}.T" for c in chunk]
        log.info(f"[batch.chunk] {i}-{i+len(chunk)-1} ({len(chunk)})")

        # 取得要否判定
        to_fetch = []
        for c, s in zip(chunk, syms):
            if force_refresh:
                to_fetch.append(s); continue
            raw = raw_map.get(c)
            if not raw:
                to_fetch.append(s); continue
            fin_d = fin_date_map.get(c)
            if not fin_d:
                to_fetch.append(s); continue
            try:
                fd = date.fromisoformat(str(fin_d))
                if (dtm.date.today() - fd).days >= 30:
                    to_fetch.append(s)
            except Exception:
                to_fetch.append(s)
        log.info(f"[fetch.plan] need_fetch={len(to_fetch)}/{len(chunk)}")

        # 取得
        fetched_raw = {}
        if to_fetch:
            if False:
                log.error("[fetch] yahooquery not installed; skip this chunk fetch")
                errors += len(to_fetch)
            else:
                try:
                    tk = YQ(to_fetch, max_workers=YQ_MAX_WORKERS)
                    quotes = getattr(tk, "quotes", {}) or {}
                    try: bs = tk.balance_sheet()
                    except Exception: bs = None
                    try: cf = tk.cash_flow()
                    except Exception: cf = None
                    try: divs = tk.dividends()
                    except Exception: divs = None

                    for s in to_fetch:
                        q = quotes.get(s) if isinstance(quotes, dict) else quotes
                        b = bs.get(s) if isinstance(bs, dict) else bs
                        cflow = cf.get(s) if isinstance(cf, dict) else cf
                        d = divs.get(s) if isinstance(divs, dict) else divs
                        fetched_raw[s] = {"quotes": q, "balance_sheet": b, "cashflow": cflow, "dividends": d}
                    log.info(f"[fetch.done] symbols={len(to_fetch)}")
                except Exception as e:
                    log.exception(f"[fetch.error] {e}")
                    errors += len(to_fetch)

        # 解析→DB行
        metrics_rows = []; flags_rows = []
        for c, s in zip(chunk, syms):
            processed += 1
            raw_text = None; sym_raw = None
            if s in fetched_raw:
                sym_raw = fetched_raw[s]
                try:
                    raw_text = json.dumps(sym_raw, default=str, ensure_ascii=False)
                except Exception:
                    raw_text = None
            else:
                raw_text = raw_map.get(c)

            marketCap = None; equity_ratio = None; ocf_recent_val = None; ocf_4q_val = None
            div_1y = 0.0; buyback_4q = 0.0

            try:
                if sym_raw is not None:
                    # --- quotes ---
                    q = sym_raw.get("quotes") or {}
                    mc = None
                    if isinstance(q, dict):
                        mc = q.get("marketCap") or q.get("market_cap") or q.get("regularMarketMarketCap")
                    marketCap = _safe_num(mc)

                    # --- balance_sheet ---
                    bsobj = sym_raw.get("balance_sheet")
                    if bsobj is not None:
                        assets = _get_from_periods(bsobj, ["totalAssets","Total Assets","total_assets"])
                        equity = _get_from_periods(bsobj, ["totalStockholderEquity","Total Stockholder Equity","total_equity"])
                        if (assets is None or equity is None) and _is_nonempty_df(bsobj):
                            assets = assets or _yf_pick_recent_from_df(bsobj, ["totalAssets","Total Assets","total_assets"])
                            equity = equity or _yf_pick_recent_from_df(bsobj, ["totalStockholderEquity","Total Stockholder Equity","total_equity"])
                        if assets and equity:
                            try: equity_ratio = float(equity) / float(assets) * 100.0
                            except Exception: equity_ratio = None

                    # --- cash_flow ---
                    cfobj = sym_raw.get("cashflow")
                    if cfobj is not None:
                        ocf_recent = _get_from_periods(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"])
                        ocf_4q_val = _sum_recent(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"], 4) or 0.0
                        ocf_recent_val = _safe_num(ocf_recent)
                        buy = _sum_recent(cfobj, ["repurchaseOfStock","Repurchase Of Stock","repurchaseOfCapitalStock","RepurchaseOfCapitalStock"], 4)
                        buyback_4q = float(buy) if buy is not None else 0.0

                        if (ocf_recent_val is None or ocf_4q_val == 0.0) and _is_nonempty_df(cfobj):
                            ocf_recent_val = ocf_recent_val if ocf_recent_val is not None else _yf_pick_recent_from_df(
                                cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"]
                            )
                            if not ocf_4q_val:
                                ocf_4q_val = _yf_sum_quarters_df(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"], 4)
                            if buyback_4q == 0.0:
                                buyback_4q = _yf_sum_quarters_df(cfobj, ["repurchaseOfStock","Repurchase Of Stock",
                                                                         "repurchaseOfCapitalStock","RepurchaseOfCapitalStock"], 4)

                    # --- dividends ---
                    divobj = sym_raw.get("dividends")
                    div_1y = _sum_dividends_1y(divobj, one_year_ago)

                else:
                    # 既存 raw から解析
                    parsed = None
                    if raw_text:
                        try:
                            if isinstance(raw_text, str) and raw_text.strip().startswith("{"):
                                parsed = json.loads(raw_text)
                            elif isinstance(raw_text, dict):
                                parsed = raw_text
                        except Exception as e:
                            log.debug(f"[parse.fallback.warn] {c} json.loads failed: {e}")

                    if parsed is not None and isinstance(parsed, dict):
                        # --- quotes ---
                        q = parsed.get("quotes") or {}
                        marketCap = _safe_num(q.get("marketCap") or q.get("market_cap") or q.get("regularMarketMarketCap"))

                        # --- balance_sheet ---
                        bsobj = parsed.get("balance_sheet")
                        if bsobj is not None:
                            assets = _get_from_periods(bsobj, ["totalAssets","Total Assets","total_assets"])
                            equity = _get_from_periods(bsobj, ["totalStockholderEquity","Total Stockholder Equity","total_equity"])
                            if (assets is None or equity is None) and _is_nonempty_df(bsobj):
                                assets = assets or _yf_pick_recent_from_df(bsobj, ["totalAssets","Total Assets","total_assets"])
                                equity = equity or _yf_pick_recent_from_df(bsobj, ["totalStockholderEquity","Total Stockholder Equity","total_equity"])
                            if assets and equity:
                                try: equity_ratio = float(equity) / float(assets) * 100.0
                                except Exception: equity_ratio = None

                        # --- cash_flow ---
                        cfobj = parsed.get("cashflow")
                        if cfobj is not None:
                            ocf_recent = _get_from_periods(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"])
                            ocf_4q_val = _sum_recent(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"], 4) or 0.0
                            ocf_recent_val = _safe_num(ocf_recent)
                            buyback_4q = _sum_recent(cfobj, ["repurchaseOfStock","Repurchase Of Stock","repurchaseOfCapitalStock","RepurchaseOfCapitalStock"], 4) or 0.0

                            if (ocf_recent_val is None or ocf_4q_val == 0.0) and _is_nonempty_df(cfobj):
                                ocf_recent_val = ocf_recent_val if ocf_recent_val is not None else _yf_pick_recent_from_df(
                                    cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"]
                                )
                                if not ocf_4q_val:
                                    ocf_4q_val = _yf_sum_quarters_df(cfobj, ["operatingCashflow","Operating Cash Flow","operatingCashFlow","OperatingCashFlow"], 4)
                                if buyback_4q == 0.0:
                                    buyback_4q = _yf_sum_quarters_df(cfobj, ["repurchaseOfStock","Repurchase Of Stock",
                                                                             "repurchaseOfCapitalStock","RepurchaseOfCapitalStock"], 4)

                        # --- dividends ---
                        divobj = parsed.get("dividends")
                        div_1y = _sum_dividends_1y(divobj, one_year_ago)

            except Exception as e:
                log.debug(f"[parse.warn] {c} parse error: {e}")

            # 判定
            mcap_ok = False
            if marketCap is not None:
                try:
                    if marketCap >= 300e8: mcap_ok = True
                except Exception:
                    mcap_ok = False

            ok_equity = (equity_ratio is not None) and (equity_ratio >= 60.0)
            ok_ocf    = (ocf_recent_val is not None and ocf_recent_val > 0) or (ocf_4q_val is not None and ocf_4q_val > 0)
            ok_return = (div_1y > 0) or (buyback_4q < 0)

            # 詳細ログ（銘柄ごと）
            log.debug(
                f"[judge] {c} "
                f"EQ={_fmt(equity_ratio)} "
                f"OCF1={_fmt(ocf_recent_val)} "
                f"OCF4Q={_fmt(ocf_4q_val)} "
                f"DIV1Y={_fmt(div_1y,1)} "
                f"BUY4Q={_fmt(buyback_4q,1)} "
                f"MCAP={'NA' if marketCap is None else int(marketCap)} "
                f"flags:EQ={ok_equity} OCF={ok_ocf} RET={ok_return} MCAP_OK={mcap_ok}"
            )

            today_iso = dtm.date.today().isoformat()
            metrics_rows.append((
                float(equity_ratio) if (equity_ratio is not None) else None,
                float(ocf_recent_val) if (ocf_recent_val is not None) else None,
                float(ocf_4q_val) if (ocf_4q_val is not None) else None,
                float(div_1y) if (div_1y is not None) else 0.0,
                float(buyback_4q) if (buyback_4q is not None) else 0.0,
                today_iso,
                raw_text,
                c
            ))

            if all([ok_equity, ok_ocf, ok_return, mcap_ok]):
                reasons = []
                if ok_equity: reasons.append("自己資本比率≥60")
                if ok_ocf:    reasons.append("営業CF黒字")
                if ok_return: reasons.append("配当/自社株買いあり")
                if mcap_ok:   reasons.append("時価総額≥300億")
                flags_rows.append(("○", " / ".join(reasons), c))
                log.info(f"[flag.ok] {c} 旧ロジック(参考)=○ reasons={'; '.join(reasons)}")
            else:
                miss = []
                if not ok_equity: miss.append("EQ<60 or NA")
                if not ok_ocf:    miss.append("OCF<=0 or NA")
                if not ok_return: miss.append("無配/買戻しなし")
                if not mcap_ok:   miss.append("MCAP<300億 or NA")
                log.debug(f"[flag.ng] {c} reasons_miss={'; '.join(miss)}")

        # commit
        try:
            commit_batch(metrics_rows, flags_rows)
        except Exception as e:
            log.exception(f"[DB.commit.error] chunk {i}-{i+len(chunk)-1}: {e}")
            errors += 1

        log.info(f"[batch.progress] processed={min(i+chunk_size, total)}/{total} updated_rows={updated_rows} flags={flags_set} errors={errors}")
        time.sleep(sleep_between_chunks)

    summary = {"total": total, "processed": processed, "updated_rows": updated_rows, "flags_set": flags_set, "errors": errors}
    log.info(f"[batch.done] {summary}")
    return summary

# --- END: batch_update_all_financials ---

# ===== fetch_all 連携 =====
def _run_fetch_all(fetch_path: str | None = None,
                   extra_args: list[str] | None = None,
                   timeout_sec: int | None = None,
                   use_lock: bool = True) -> None:
    """
    自分と同じ Python で fetch_all.py をサブプロセス実行。
    ・stdout を逐次そのままコンソールへ流す
    ・異常終了/タイムアウト時は例外
    ・多重起動を避けるため lock ファイル(任意)を利用
    """
    # 1) スクリプトの場所を解決（指定がなければ自分と同じフォルダを探す）
    if fetch_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cand = os.path.join(base_dir, "fetch_all.py")
        if not os.path.exists(cand):
            # ユーザー環境の絶対パス例（必要ならここをあなたの環境に合わせて固定も可）
            cand = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\fetch_all.py"
        fetch_path = cand

    if not os.path.exists(fetch_path):
        raise FileNotFoundError(f"fetch_all.py が見つかりません: {fetch_path}")

    py = sys.executable  # いま実行中の Python を使う（仮想環境の取り違え防止）
    cmd = [py, "-u", fetch_path]
    if extra_args:
        cmd.extend(extra_args)

    # 2) ロック（簡易）
    lock_path = os.path.splitext(fetch_path)[0] + ".lock"  # 例: fetch_all.lock
    if use_lock:
        if os.path.exists(lock_path):
            # 古いロックは5時間で無視（適当な保険）
            try:
                if time.time() - os.path.getmtime(lock_path) < 5*60*60:
                    print(f"[fetch_all] lock検知のためスキップ: {lock_path}")
                    return
            except Exception:
                pass
        # 作成
        try:
            with open(lock_path, "w", encoding="utf-8") as lf:
                lf.write(f"pid={os.getpid()}\nstart={time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        except Exception:
            pass

    print(f"[fetch_all] 実行開始: {cmd}")
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
        )
        start = time.time()
        # 逐次出力
        for line in proc.stdout:
            print(line.rstrip())
            if timeout_sec and (time.time() - start) > timeout_sec:
                proc.kill()
                raise TimeoutError(f"fetch_all タイムアウト（{timeout_sec}s）")

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"fetch_all 異常終了: returncode={rc}")

        print("[fetch_all] 正常終了")
    finally:
        # ロック解除
        if use_lock:
            try:
                if os.path.exists(lock_path):
                    os.remove(lock_path)
            except Exception:
                pass

# ===== カブタン呼び出し

def run_fundamental_daily(force: bool = False):
    today = dtm.date.today()

    # 1) mtimeで判定（中身は見ない）
    if not force and MARKER_FILE.exists():
        mtime = dtm.datetime.fromtimestamp(MARKER_FILE.stat().st_mtime).date()
        print(f"[fundamental] marker_mtime={mtime} today={today}")
        if mtime == today:
            print("[fundamental] 今日すでに実行済み → スキップ")
            return

    # 2) 実行
    try:
        print(f"[fundamental] 実行開始: {FUND_SCRIPT}")
        subprocess.run(["python", FUND_SCRIPT], check=True)
        print("[fundamental] 株探ファンダ 全銘柄処理 OK")
    except subprocess.CalledProcessError as e:
        print(f"[fundamental][ERROR] 子プロセス失敗: returncode={e.returncode}")
        return

    # 3) マーカー更新（touch相当）
    MARKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    MARKER_FILE.touch()  # 中身は不要、更新日時だけ使う
    print(f"[fundamental] マーカー更新(mtime): {MARKER_FILE}")

# ======= 決算関連 =======

#  signals_log依存
def build_earnings_edge_scores(conn, lookback_n=6, follow_days=5, since="2022-01-01"):
    """
    signals_log('決算')に依存せず、earnings_events×price_history から
    各コードの直近N回の“勝率/ギャップ平均/フォロー平均/直近連勝”をオンザフライで算出。
    DBには書き込まない（返り値DFのみ）。
    """

    ev = pd.read_sql_query(f"""
        SELECT コード, DATE(提出時刻) AS ev_date
        FROM earnings_events
        WHERE DATE(提出時刻) >= DATE(?)
        ORDER BY コード, 提出時刻 DESC
    """, conn, params=[since])

    if ev.empty:
        return pd.DataFrame(columns=["コード","edge_score"])

    codes = ev["コード"].astype(str).unique().tolist()
    ph = pd.read_sql_query(f"""
        SELECT コード, substr(日付,1,10) AS 日付, 始値, 高値, 安値, 終値
        FROM price_history
        WHERE コード IN ({",".join(["?"]*len(codes))})
        ORDER BY コード, 日付
    """, conn, params=codes)

    if ph.empty:
        # price_history が無いと計算不可 → エッジ0扱い（空DF）
        return pd.DataFrame(columns=["コード","edge_score"])

    ph["日付"] = pd.to_datetime(ph["日付"], format="%Y-%m-%d", errors="coerce")
    ev["ev_date"] = pd.to_datetime(ev["ev_date"], errors="coerce")

    outs = []
    for code, g_ev in ev.groupby("コード", sort=False):
        g_ph = ph[ph["コード"]==code].dropna(subset=["日付"]).sort_values("日付").reset_index(drop=True)
        if g_ph.empty:
            continue
        g_ph["idx"] = range(len(g_ph))

        # 直近N回のイベントだけ見る
        g_ev = g_ev.head(max(lookback_n, 6))  # 少し多めに拾っておく
        rets, follows, wins = [], [], []

        for _, row in g_ev.iterrows():
            d0 = row["ev_date"]
            if pd.isna(d0):
                continue
            after = g_ph[g_ph["日付"] > d0]
            if after.empty:
                continue
            d1_idx = int(after["idx"].iloc[0])
            if d1_idx == 0:
                continue
            d0_idx = d1_idx - 1

            try:
                prev_close = float(g_ph.loc[d0_idx, "終値"])
                next_close = float(g_ph.loc[d1_idx, "終値"])
            except Exception:
                continue

            ret_close_pct = ((next_close/prev_close) - 1.0) * 100.0
            hi_window = g_ph.loc[d1_idx : d1_idx + follow_days - 1, "高値"]
            follow_hi_pct = None if hi_window.empty else ((float(hi_window.max())/next_close) - 1.0) * 100.0

            rets.append(ret_close_pct)
            follows.append(follow_hi_pct)
            win = (ret_close_pct is not None and ret_close_pct >= 3.0) or\
                  (follow_hi_pct is not None and follow_hi_pct >= 7.0)
            wins.append(1 if win else 0)

        if not rets and not follows:
            continue

        # 直近N件に絞ってスコア化
        wins2 = wins[:lookback_n]
        rets2 = [x for x in rets[:lookback_n] if x is not None]
        fol2  = [x for x in follows[:lookback_n] if x is not None]

        win_rate   = (sum(wins2)/len(wins2)) if wins2 else 0.0
        gap_mean   = (sum(rets2)/len(rets2)) if rets2 else 0.0
        follow_mean= (sum(fol2)/len(fol2)) if fol2 else 0.0
        cons       = (sum(wins2[:3])/3.0) if len(wins2)>=3 else (sum(wins2)/max(1,len(wins2)))

        s = (win_rate*60.0) + (max(0.0,gap_mean)/6.0*20.0) + (max(0.0,follow_mean)/10.0*10.0) + (cons*10.0)
        outs.append((code, round(min(100.0, max(0.0, s)), 1)))

    return pd.DataFrame(outs, columns=["コード","edge_score"])

def build_pre_earnings_rank(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    “決算前の良さ”を全銘柄にスコアリング：
    事前スコア = 0.6*edge_score + 0.4*momentum_score
    momentum_score は「右肩上がり/HH近接/出来高増加」から簡易合成（0-100）
    """
    edge = build_earnings_edge_scores(conn)  # コード, edge_score

    # モメンタム側（既存カラムを利用）
    q = """
       SELECT コード, 銘柄名, 現在値, 前日終値比率, 出来高, 右肩上がりスコア
       FROM screener
    """
    s = pd.read_sql_query(q, conn)
    if s.empty:
        s = pd.DataFrame(columns=["コード"])
    # 高値接近度：直近60日高値比（price_historyから）
    hi = pd.read_sql_query("""
      WITH z AS(
        SELECT コード, MAX(日付) AS d FROM price_history GROUP BY コード
      )
      SELECT p.コード, p.終値 AS close, (
               SELECT MAX(高値) FROM price_history q
               WHERE q.コード=p.コード AND q.日付 >= date(p.日付, '-60 day')
             ) AS hh60
      FROM price_history p
      JOIN z ON p.コード=z.コード AND p.日付=z.d
    """, conn)
    if not hi.empty:
        hi["near_hh"] = (hi["close"]/hi["hh60"]-1.0)*100.0
        hi["near_hh_score"] = hi["near_hh"].apply(lambda x: 100.0 if x>=-1.0 else (50.0 if x>=-5.0 else 0.0))
    else:
        hi = pd.DataFrame(columns=["コード","near_hh_score"])

    # 出来高ブースト（過去20日移動平均比）
    vol = pd.read_sql_query("""
      WITH cur AS(
        SELECT コード, MAX(日付) d FROM price_history GROUP BY コード
      ),
      v AS(
        SELECT p.コード, p.出来高 AS v0,
               (SELECT AVG(出来高) FROM price_history q WHERE q.コード=p.コード AND q.日付>=date(p.日付,'-20 day') AND q.日付<p.日付) AS v20
        FROM price_history p
        JOIN cur ON p.コード=cur.コード AND p.日付=cur.d
      )
      SELECT 代码 as コード, v0, v20 FROM (
        SELECT コード as 代码, v0, v20 FROM v
      )
    """, conn)
    # SQLite互換のため別名経由

    if not vol.empty:
        vol["boost"] = vol.apply(lambda r: (r["v0"]/max(1.0, r["v20"])) if r["v20"] else 1.0, axis=1)
        vol["vol_score"] = vol["boost"].apply(lambda x: 100.0 if x>=3.0 else (70.0 if x>=2.0 else (40.0 if x>=1.3 else 0.0)))
    else:
        vol = pd.DataFrame(columns=["コード","vol_score"])

    # 結合
    df = s.merge(edge, on="コード", how="left").merge(hi[["コード","near_hh_score"]], on="コード", how="left").merge(vol[["コード","vol_score"]], on="コード", how="left")
    df["edge_score"] = df["edge_score"].fillna(0.0)
    # 右肩スコアがあれば優遇
    df["mom_raw"] = df[["右肩上がりスコア"]].fillna(0.0).clip(lower=0, upper=100).iloc[:,0]*0.6 + df["near_hh_score"].fillna(0.0)*0.25 + df["vol_score"].fillna(0.0)*0.15
    df["momentum_score"] = df["mom_raw"].clip(0,100)
    df["pre_score"] = (0.6*df["edge_score"] + 0.4*df["momentum_score"]).round(1)
    df = df.sort_values(["pre_score"], ascending=False)
    print("pre_earn sample:", df.head(5).to_dict(orient="records"))
    cols = ["コード","銘柄名","pre_score","edge_score","momentum_score"]
    df = df
    return df[cols]

def yj_board(code: str, name: str):
    c = str(code).zfill(4)
    return f'<a href="https://finance.yahoo.co.jp/quote/{c}.T/bbs" target="_blank" rel="noopener">{name} <span class="code">({c})</span></a>'

def _build_preearn_fallback(conn):
    """
    フォールバック：preearn_rows が空になった場合、screener から暫定ランキングを作る。
    ・pre_score = 右肩上がりスコア(無ければ0)
    ・momentum_score = 右肩上がりスコア
    ・edge_score = 0
    ・銘柄 は Yahooリンク付き
    返り値: list[dict]
    """
    try:
        s = pd.read_sql_query(
            """
            SELECT コード, 銘柄名, 右肩上がりスコア
              FROM screener
            """, conn
        )
    except Exception:
        return []

    if s.empty:
        return []

    def _mk(code, name):
        c = str(code).zfill(4)
        nm = name if (isinstance(name, str) and name) else str(code)
        return f'<a href="https://finance.yahoo.co.jp/quote/{c}.T" target="_blank" rel="noopener">{nm} <span class="code">({c})</span></a>'

    s["銘柄"] = [_mk(c, n) for c, n in zip(s["コード"], s.get("銘柄名", ""))]
    s["edge_score"] = 0.0
    s["momentum_score"] = s["右肩上がりスコア"].fillna(0.0)
    s["pre_score"] = s["momentum_score"].fillna(0.0).round(1)
    s = s.sort_values(["pre_score"], ascending=False)
    out = s[["銘柄", "pre_score", "edge_score", "momentum_score"]].head(200)
    # 予測タブ互換の5列をダミーで付与（空欄化を避ける）
    out["スコア理由"] = "根拠薄め（暫定）"
    out["予測ヒント"] = ""
    out["期待株価"]   = float("nan")
    out["修正見通し"] = "中立"
    out["過熱度"]     = "中立"

    # JSON化で NaN を弾く
    return [{k: (None if (isinstance(v, float) and (v != v)) else v) for k, v in r.items()}
            for r in out.to_dict("records")]
def build_earnings_tables(conn):
    """
    実績(直近1日) と 予測ランキング を返す (ev_df, pre_df)
    """

    # ========== 実績（列の存在を動的に確認してSELECT） ==========
    # 1) 実テーブルの列名を取得
    have = set(cols_df["name"].astype(str))

    base_cols = ["コード", "提出時刻", "タイトル", "センチメント"]  # 必須候補
    opt_cols  = ["ヒットKW"]                                        # あれば使う
    sel_cols  = [c for c in base_cols + opt_cols if c in have]      # 実在列だけ

    # 2) SQLを実在列だけで作成
    ev_sql = f"""
      SELECT {", ".join("e."+c for c in sel_cols)}
      FROM earnings_events e
      WHERE e.提出時刻 >= datetime(date('now','-1 day') || ' 00:00:00')
      ORDER BY e.提出時刻 DESC
    """
    ev_df = pd.read_sql_query(ev_sql, conn) if sel_cols else pd.DataFrame()
    ev_df = ev_df

    if not ev_df.empty:
        names = pd.read_sql_query("SELECT コード, 銘柄名 FROM screener", conn)
        names = names
        ev_df = ev_df
        ev_df = ev_df.merge(names, on="コード", how="left")

        # 時刻整形（失敗時は元の値）
        ts = pd.to_datetime(ev_df.get("提出時刻"), errors="coerce")
        ev_df["時刻"] = ts.dt.strftime("%Y/%m/%d %H:%M").fillna(ev_df.get("提出時刻", "").astype(str))

        # 銘柄リンク（yj_board 未定義でも落ちない）
        def _mk_name(code, name):
            label = name if (isinstance(name, str) and name) else str(code)
            try:
                return yj_board(code, label)
            except Exception:
                return label

        ev_df["銘柄"] = [_mk_name(c, n) for c, n in zip(ev_df["コード"], ev_df.get("銘柄名", ""))]
        ev_df = ev_df[["銘柄", "センチメント", "タイトル", "時刻"]]
    else:
        ev_df = pd.DataFrame(columns=["銘柄", "センチメント", "タイトル", "時刻"])

    # ========== 予測 ==========
    pre_df = build_pre_earnings_rank(conn)
    pre_df = pre_df

    if not pre_df.empty:
        # 銘柄名が無ければ付与
        if "銘柄名" not in pre_df.columns:
            names = pd.read_sql_query("SELECT コード, 銘柄名 FROM screener", conn)
            names = names
            pre_df = pre_df
            pre_df = pre_df.merge(names, on="コード", how="left")

        def _mk_name2(code, name):
            label = name if (isinstance(name, str) and name) else str(code)
            try:
                return yj_board(code, label)
            except Exception:
                return label

        pre_df["銘柄"] = [_mk_name2(c, n) for c, n in zip(pre_df["コード"], pre_df.get("銘柄名", ""))]
        pre_df = pre_df[["銘柄", "pre_score", "edge_score", "momentum_score"]].head(200)
    else:
        pre_df = pd.DataFrame(columns=["銘柄", "pre_score", "edge_score", "momentum_score"])
    
    print("[dbg] ev/pre rows:", len(ev_df), len(pre_df))
    return ev_df, pre_df

# ===================== （決算リスト） =====================

def _guess_sentiment_by_title(title: str) -> str:
    t = title or ""
    if any(k in t for k in _POS_KEYS): return "Bullish"
    if any(k in t for k in _NEG_KEYS): return "Bearish"
    return "Neutral"

def _edinet_list(date_str: str) -> list:
    """
    EDINET のメタ一覧を1日分取得（type=2=一覧）
    """
    url = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
    params = {"date": date_str, "type": 2}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    j = r.json() if r.headers.get("Content-Type","").startswith("application/json") else {}
    return j.get("results", [])

def _is_kessan_like(doc: dict) -> bool:
    desc = (doc.get("docDescription") or "")
    if _DECISION_PAT.search(desc):
        return True
    # formCode/ordinanceCode で厳密化したい場合はここに追加
    return False

def _normalize_sec_code(sec: str) -> str | None:
    """
    EDINETの secCode を 4桁に正規化（株式以外は None）
    """
    if not sec: return None
    s = str(sec).strip()
    # 先頭0埋め4桁（5桁以上はETF/投信などの可能性が高いので除外）
    if s.isdigit() and 1 <= len(s) <= 4:
        return s.zfill(4)
    return None

def _yahoo_quote_url(code4: str) -> str:
    # 掲示板まで飛ばすなら "/bbs" を末尾に付ける（Yahoo側の仕様変更に注意）
    return f"https://finance.yahoo.co.jp/quote/{code4}.T/bbs"

def _x_search_url(code4: str) -> str:
    # ハッシュタグ #コード で検索
    return f"https://x.com/search?q=%23{code4}"

# ================= 安全版：直近決算読み込み（完全置き換え） =================

# ===== TDnet決算(earnings)の直近N日をDBから読む =====
def load_recent_earnings_from_db(db_path: str, days: int = 7, limit: int = 300):
    """
    earnings_events を“日本語カラムのみ”で読む（DBパス指定版）
    """

    if not os.path.exists(db_path):
        print(f"[earnings][WARN] DB not found: {db_path} → []")
        return []

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # テーブル存在チェック
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='earnings_events'")
        if not cur.fetchone():
            print("[earnings][WARN] table earnings_events not found → []")
            return []

        since = (dtm.datetime.now() - dtm.timedelta(days=int(days))).strftime("%Y-%m-%d 00:00:00")

        cur.execute("""
            SELECT
              コード,
              銘柄名,
              タイトル,
              リンク,
              COALESCE(発表日時, 提出時刻) AS ts,
              要約,
              判定,
              判定スコア,
              理由JSON,
              指標JSON,
              進捗率,
              センチメント,
              素点
            FROM earnings_events
            WHERE COALESCE(発表日時, 提出時刻) >= ?
            ORDER BY COALESCE(発表日時, 提出時刻) DESC
            LIMIT ?
        """, (since, int(limit)))

        rows = []
        for row in cur.fetchall():
            d = dict(row)
            for k in ("理由JSON", "指標JSON"):
                if k in d and isinstance(d[k], str):
                    try:
                        d[k] = json.loads(d[k])
                    except Exception:
                        d[k] = [] if k == "理由JSON" else {}
            rows.append({
                "ticker":   str(d.get("コード") or "").zfill(4),
                "name":     d.get("銘柄名") or "",
                "title":    d.get("タイトル") or "",
                "link":     d.get("リンク") or "",
                "time":     d.get("ts") or "",
                "summary":  d.get("要約") or "",
                "verdict":  d.get("判定") or "",
                "score_judge": int(d.get("判定スコア") or 0),
                "reasons":  d.get("理由JSON") or [],
                "metrics":  d.get("指標JSON") or {},
                "progress": d.get("進捗率"),
                "sentiment": d.get("センチメント") or "",
                "score":     int(d.get("素点") or 0),
            })
        return rows
    finally:
        conn.close()

# ===== 予測タブの付加情報（列）を作るユーティリティ =====

def _mk_score_reason(rec: dict) -> str:
    # 決算〈予測〉タブ向けの“決算っぽい”根拠文を生成。
    # 使う要素: momentum_score / near_hh_score / vol_score / edge_score / 修正見通し
    t = []
    def f(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    mom = f(rec.get("momentum_score"))
    hh  = f(rec.get("near_hh_score"))
    vol = f(rec.get("vol_score"))
    edg = f(rec.get("edge_score"))
    bias = str(rec.get("修正見通し") or "").strip()

    # ---- エッジ（過去決算の“翌日リターン”由来スコア） ----
    if edg >= 85:
        t.append("過去決算の翌日リターン勝率が高め（アノマリー良）")
    elif edg >= 70:
        t.append("過去決算の翌日パフォーマンスがやや良好")

    # ---- 需給（直前の買い集め/期待先行） ----
    if mom >= 90:
        t.append("決算前のモメンタム強")
    elif mom >= 80:
        t.append("需給改善（買い優勢）")

    if hh >= 90:
        t.append("60日高値圏（サプライズ期待の買い上がり）")
    elif hh >= 50:
        t.append("60日高値に接近")

    if vol >= 90:
        t.append("出来高ブースト（決算プレイの資金流入）")
    elif vol >= 70:
        t.append("出来高増加")

    # ---- 修正バイアスの補足 ----
    if bias and bias != "中立":
        t.append(f"修正見通し:{bias}")

    return " / ".join(t) or "根拠不足（決算前の気配は弱め）"

def _mk_hint(rec: dict) -> str:
    # 決算前の“運用ヒント”。スコアしきい値で簡易に分岐。
    # - エッジ高×需給強: ブレイク狙い or 直前分割IN
    # - エッジ中×需給中: 押し目待ち（発表跨ぎは小口）
    # - 弱: 見送り/材料待ち
    def f(x):
        try:
            return float(x)
        except Exception:
            return 0.0
    mom = f(rec.get("momentum_score"))
    hh  = f(rec.get("near_hh_score"))
    vol = f(rec.get("vol_score"))
    edg = f(rec.get("edge_score"))
    rvol= f(rec.get("RVOL代金") or rec.get("RVOL_代金") or rec.get("rvol"))

    strong_flow = (mom >= 90) or (hh >= 80 and vol >= 70) or (rvol >= 2.0)

    if edg >= 85 and strong_flow:
        return "上方サプライズ狙い。直近高値ブレイクで分割IN、失速なら即撤退。"
    if edg >= 70 and (mom >= 80 or rvol >= 1.5):
        return "好トラックレコード。-3〜-5%押しで拾い、発表は小口跨ぎ。"
    if edg >= 55:
        return "中立。発表跨ぎは最小ロット、決算後の初動で追随。"
    return "見送り。材料/出来高の増加待ち。"

def _mk_overheat_bucket(r20_pct: float, mom: float) -> str:
    """過熱度のバケット化（過熱/やや過熱/中立/やや調整/調整）"""
    if r20_pct >= 90 or mom >= 90: return "過熱"
    if r20_pct >= 75 or mom >= 85: return "やや過熱"
    if r20_pct <= 10 or mom <= 40: return "調整"
    if r20_pct <= 25 or mom <= 60: return "やや調整"
    return "中立"

def _attach_overheat_and_hints(conn, pre_df):
    """pre_df に『過熱度』『予測ヒント』を付与（順位は変更しない）"""
    if pre_df is None or pre_df.empty:
        pre_df = pd.DataFrame(columns=list(pre_df.columns)+["過熱度","予測ヒント"])
        return pre_df

    # 直近20日リターンとパーセンタイル
    r20 = pd.read_sql_query("""
        WITH cur AS (SELECT コード, MAX(日付) d FROM price_history GROUP BY コード),
             base AS (
               SELECT p.コード, p.終値 AS c0,
                      (SELECT 終値 FROM price_history q
                       WHERE q.コード=p.コード AND q.日付=date(p.日付,'-20 day')) AS c20
               FROM price_history p JOIN cur ON p.コード=cur.コード AND p.日付=cur.d
             )
        SELECT コード, 100.0*(c0 - c20)/NULLIF(c20,0) AS r20
        FROM base
    """, conn)
    r20["r20"] = pd.to_numeric(r20["r20"], errors="coerce").fillna(0.0)
    r20["r20_pct"] = (r20["r20"].rank(pct=True) * 100.0).clip(0, 100)

    df = pre_df.merge(r20[["コード","r20","r20_pct"]], on="コード", how="left")
    df["過熱度"] = df.apply(
        lambda r: _mk_overheat_bucket(float(r.get("r20_pct") or 0), float(r.get("momentum_score") or 0)),
        axis=1
    )
    # 予測ヒント（行に足りないカラムはあってもなくてもOKな設計）
    df["予測ヒント"] = df.apply(_mk_hint, axis=1)
    return df

def _compute_expected_price_and_revision(conn, pre_df):
    """
    DBの過去決算反応から“期待価格”を作る＋簡易ルールで“業績修正予想”を付与。
    - 期待価格 = 現在値 × (1 + 期待リターン[%]/100)
      期待リターンは、過去の earnings_events 当日〜翌営業日の市場平均反応をベースに、
      今のモメンタム（r20_pct, momentum_score）で弱/強補正。
    - 業績修正予想 = {上方観測 / 据置見通し / 下方観測}
      （四半期PLがあれば進捗とYoY、なければ最近のイベント・センチメントで近似）
    """

    if pre_df is None or pre_df.empty:
        return pd.DataFrame(columns=["コード","期待価格","業績修正予想"])

    codes = tuple(pre_df["コード"].astype(str).tolist())

    # いまの現在値（最新終値）
    px = pd.read_sql_query("""
        WITH cur AS (SELECT コード, MAX(日付) d FROM price_history GROUP BY コード)
        SELECT p.コード, p.終値 AS 現在値
        FROM price_history p JOIN cur ON p.コード=cur.コード AND p.日付=cur.d
    """, conn)
    px["現在値"] = pd.to_numeric(px["現在値"], errors="coerce")

    # 過去の決算イベント日と翌営業日のリターン（全銘柄平均）
    # ※ テーブル・列が無い場合は except で 0% にフォールバック
    try:
        ev = pd.read_sql_query("""
            SELECT コード, DATE(提出時刻) AS d
            FROM earnings_events
            WHERE 提出時刻 IS NOT NULL
              AND DATE(提出時刻) >= date('now','-720 day')
        """, conn)

        if ev.empty:
            base_ret = 0.0
        else:
            # イベント日の前日終値と翌営業日終値で +1日反応を測る
            # 前日＝d-1営業日扱いは困難なので近似：d-1カレンダー日/翌営業日をSQLで近似取得
            # ずれを許容しつつ平均をとる
            ret = pd.read_sql_query("""
                WITH e AS (
                  SELECT コード, DATE(提出時刻) AS d FROM earnings_events
                  WHERE 提出時刻 IS NOT NULL AND DATE(提出時刻) >= date('now','-720 day')
                ),
                p0 AS (
                  SELECT e.コード, e.d,
                         (SELECT 終値 FROM price_history p WHERE p.コード=e.コード AND p.日付<=date(e.d,'-1 day') ORDER BY p.日付 DESC LIMIT 1) AS c0
                  FROM e
                ),
                p1 AS (
                  SELECT e.コード, e.d,
                         (SELECT 終値 FROM price_history p WHERE p.コード=e.コード AND p.日付>=date(e.d,'+1 day') ORDER BY p.日付 ASC LIMIT 1) AS c1
                  FROM e
                )
                SELECT e.コード, e.d,
                       100.0 * (p1.c1 - p0.c0) / NULLIF(p0.c0,0) AS r1d
                FROM e
                JOIN p0 ON p0.コード=e.コード AND p0.d=e.d
                JOIN p1 ON p1.コード=e.コード AND p1.d=e.d
            """, conn)
            ret["r1d"] = pd.to_numeric(ret["r1d"], errors="coerce")
            base_ret = float(ret["r1d"].median()) if not ret.empty else 0.0  # 中央値でロバスト
    except Exception:
        base_ret = 0.0

    # いまの r20_pct と momentum で弱/強補正（±5%幅）
    # 例：r20_pctが上位の時は +2.5%、momentumが強い時は +2.5% 上乗せ 等
    # → 過去平均に “状況係数” を軽く掛ける
    r20 = pd.read_sql_query("""
        WITH cur AS (SELECT コード, MAX(日付) d FROM price_history GROUP BY コード),
             base AS (
               SELECT p.コード, p.終値 AS c0,
                      (SELECT 終値 FROM price_history q
                       WHERE q.コード=p.コード AND q.日付=date(p.日付,'-20 day')) AS c20
               FROM price_history p JOIN cur ON p.コード=cur.コード AND p.日付=cur.d
             )
        SELECT コード, 100.0*(c0 - c20)/NULLIF(c20,0) AS r20
        FROM base
    """, conn)
    r20["r20"] = pd.to_numeric(r20["r20"], errors="coerce").fillna(0.0)
    r20["r20_pct"] = (r20["r20"].rank(pct=True) * 100.0).clip(0,100)

    df = pre_df.merge(px, on="コード", how="left")\
               .merge(r20[["コード","r20_pct"]], on="コード", how="left")

    def _situ_boost(r):
        boost = 1.0
        r20p = float(r.get("r20_pct") or 0)
        mom  = float(r.get("momentum_score") or 0)
        if r20p >= 75: boost += 0.025
        if r20p >= 90: boost += 0.025
        if mom  >= 85: boost += 0.025
        if mom  >= 95: boost += 0.025
        if r20p <= 25: boost -= 0.025
        if r20p <= 10: boost -= 0.025
        if mom  <= 60: boost -= 0.025
        if mom  <= 40: boost -= 0.025
        return boost

    df["期待リターン％"] = base_ret * df.apply(_situ_boost, axis=1)
    df["期待価格"] = (pd.to_numeric(df["現在値"], errors="coerce") *
                   (1.0 + df["期待リターン％"].astype(float)/100.0)).round(2)

    # 業績修正予想（四半期PLがあれば使う／無ければイベント近似）
    try:
        pl = pd.read_sql_query("""
            SELECT コード, 決算期, 四半期,
                   売上高, 営業利益,
                   CASE WHEN 売上高 IS NOT NULL AND 売上高 != 0
                        THEN 100.0 * 営業利益/売上高 END AS 利益率,
                   通期進捗率
            FROM pl_quarter
        """, conn)
        pl["決算期_ord"] = pd.to_datetime(pl["決算期"], errors="coerce")
        last = pl.dropna(subset=["決算期_ord"]).sort_values(["コード","決算期_ord"]).groupby("コード").tail(1)
        # 進捗の基準：Q1:25/Q2:50/Q3:75/Q4:100
        import numpy as np
        q = last["四半期"].fillna(0)
        target = np.select([q==1,q==2,q==3,q>=4], [25.0,50.0,75.0,100.0], default=0.0)
        last = last.assign(_prog_gap=(last["通期進捗率"].fillna(0.0) - target))

        # YoY（営業利益）も見る（4Q前比）
        pl = pl.sort_values(["コード","決算期_ord"])
        pl["営利YoY"] = pl.groupby("コード")["営業利益"].pct_change(4) * 100.0
        yoy = pl.groupby("コード").tail(1)[["コード","営利YoY"]]

        base_rev = last[["コード","_prog_gap"]].merge(yoy, on="コード", how="left")
        def _rev_label(r):
            prog = float(r.get("_prog_gap") or 0.0)
            yoyp = float(r.get("営利YoY") or 0.0)
            if prog >= 5 and yoyp >= 0:  return "上方観測"
            if prog <= -5 and yoyp <= 0: return "下方観測"
            return "据置見通し"
        base_rev["業績修正予想"] = base_rev.apply(_rev_label, axis=1)
        rev = base_rev[["コード","業績修正予想"]]
    except Exception:
        # 近60日イベントセンチメントで近似（プラス多→上方、マイナス多→下方）
        try:
            evs = pd.read_sql_query("""
                SELECT コード,
                       SUM(CASE WHEN センチメント LIKE '%pos%' OR センチメント LIKE '%ポジ%' THEN 1
                                WHEN センチメント LIKE '%neg%' OR センチメント LIKE '%ネガ%' THEN -1
                                ELSE 0 END) AS s
                FROM earnings_events
                WHERE 提出時刻 >= datetime('now','-60 day')
                GROUP BY コード
            """, conn)
            def _lbl(s):
                s = float(s or 0)
                if s >= 2:  return "上方観測"
                if s <= -2: return "下方観測"
                return "据置見通し"
            evs["業績修正予想"] = evs["s"].apply(_lbl)
            rev = evs[["コード","業績修正予想"]]
        except Exception:
            rev = pd.DataFrame(columns=["コード","業績修正予想"])

    out = df.merge(rev, on="コード", how="left")
    return out[["コード","期待価格","業績修正予想"]]

def _pct(a, b):
    try:
        if b in (0, None) or pd.isna(b):
            return np.nan
        return 100.0 * (a - b) / b
    except Exception:
        return np.nan

def calc_expected_price(conn, code: str, latest_close: float,
                        momentum_score: float = 0.0, edge_score: float = 0.0) -> float:
    """
    DBの過去決算翌日の実リターン平均から「期待株価」を見積もる簡易版。
    標本が乏しい場合はモメンタム/エッジで微調整（±0.5%/10pt）。
    """
    # 直近の終値が無ければそのまま返す
    if latest_close in (None, 0) or pd.isna(latest_close):
        return float("nan")

    # 1) コードの過去決算日を取得
    ev = pd.read_sql_query("""
      SELECT 日付 AS ev_day
      FROM earnings_events
      WHERE コード = ?
      ORDER BY 日付 DESC
      LIMIT 10
    """, conn, params=[code])

    # 2) 各決算日の「翌営業日」終値を引いて、前営業日比リターンを作る
    rets = []
    if not ev.empty:
      for d in ev["ev_day"].tolist():
        pr = pd.read_sql_query("""
          WITH prev AS (
            SELECT 日付, 終値 FROM price_history
            WHERE コード=? AND 日付 <= ?
            ORDER BY 日付 DESC LIMIT 1
          ),
          next AS (
            SELECT 日付, 終値 FROM price_history
            WHERE コード=? AND 日付 > ?
            ORDER BY 日付 ASC LIMIT 1
          )
          SELECT
            (SELECT 終値 FROM next) AS c_next,
            (SELECT 終値 FROM prev) AS c_prev
        """, conn, params=[code, d, code, d])
        if not pr.empty:
            c_prev = pr.at[0, "c_prev"]
            c_next = pr.at[0, "c_next"]
            if pd.notna(c_prev) and pd.notna(c_next) and c_prev:
                rets.append(_pct(c_next, c_prev))

    # 3) 平均反応（%）
    if rets:
        base_ret = float(np.nanmean(rets))
    else:
        base_ret = 0.0  # 標本が無ければニュートラル

    # 4) サンプル不足のときはスコアで微調整（±0.5%/10pt）
    if len(rets) < 3:
        base_ret += 0.05 * ((momentum_score - 50.0) / 10.0)   # 50基準
        base_ret += 0.05 * ((edge_score     - 50.0) / 10.0)

    # 5) 期待株価 = 最新終値 × (1 + 期待リターン%)
    expected = latest_close * (1.0 + base_ret / 100.0)
    return round(float(expected), 2)

def classify_revision_bias(edge_score: float,
                           momentum_score: float) -> Literal["上方寄り", "中立", "下方寄り"]:
    """
    決算の上振れ/下振れ“見通し”を超単純化。
    edge（内容寄り）を主、mom（需給寄り）を従として判定。
    """
    e = float(edge_score or 0.0)
    m = float(momentum_score or 0.0)

    if e >= 65 or (e >= 55 and m >= 70):
        return "上方寄り"
    if e <= 35 or (e <= 45 and m <= 40):
        return "下方寄り"
    return "中立"

def judge_overheat(conn, code: str,
                   momentum_score: float) -> Literal["過熱", "やや過熱", "中立", "やや押し目", "押し目"]:
    """
    銘柄相対の20日リターン（r20 = 直近終値/20日前終値 - 1）と
    市場全体に対するr20のパーセンタイル（<= r20 の比率）＋ momentum_score で“過熱度”を5分類。
    """
    # 対象銘柄の r20
    r = pd.read_sql_query(
        """
        WITH cur AS (
          SELECT MAX(日付) d FROM price_history WHERE コード=?
        )
        SELECT
          100.0 * (
            (SELECT 終値 FROM price_history p WHERE p.コード=? AND p.日付=cur.d)
            -
            (SELECT 終値 FROM price_history q WHERE q.コード=? AND q.日付=date(cur.d,'-20 day'))
          ) / NULLIF(
            (SELECT 終値 FROM price_history q WHERE q.コード=? AND q.日付=date(cur.d,'-20 day')), 0
          ) AS r20
        FROM cur
        """,
        conn, params=[code, code, code, code]
    )
    r20 = float(r.at[0, "r20"]) if (not r.empty and pd.notna(r.at[0, "r20"])) else 0.0

    # 市場全体の r20 分布
    allr = pd.read_sql_query(
        """
        WITH cur AS (
          SELECT コード, MAX(日付) d FROM price_history GROUP BY コード
        ),
        base AS (
          SELECT p.コード,
                 p.終値 AS c0,
                 (SELECT 終値 FROM price_history q
                  WHERE q.コード=p.コード AND q.日付=date(p.日付,'-20 day')) AS c20
          FROM price_history p
          JOIN cur ON p.コード=cur.コード AND p.日付=cur.d
        )
        SELECT 100.0 * (c0 - c20) / NULLIF(c20,0) AS r20 FROM base
        """,
        conn
    )

    if allr.empty:
        r20pct = 50.0
    else:
        s = pd.to_numeric(allr["r20"], errors="coerce").fillna(0.0)
        # パーセンタイルは「全体の中で自分以下の割合」を採用
        r20pct = float(((s <= r20).mean()) * 100.0)

    mom = float(momentum_score or 0.0)

    # しきい値（必要に応じて微調整可）
    if r20pct >= 90 or mom >= 90:
        return "過熱"
    if r20pct >= 75 or mom >= 80:
        return "やや過熱"
    if r20pct <= 10 or mom <= 35:
        return "押し目"
    if r20pct <= 25 or mom <= 55:
        return "やや押し目"
    return "中立"
    
def phase_sync_finance_comments(conn):
    """
    finance_notes(コード, 財務コメント, score, progress_percent)
      -> screener.財務コメント, screener.スコア, screener.進捗率 に同期
    """
    import sqlite3
    cur = conn.cursor()

    # 1) 後方互換: 列が無ければ追加
    cur.execute("PRAGMA table_info(screener)")
    cols = {r[1] for r in cur.fetchall()}
    add_sql = []
    if "財務コメント" not in cols:
        add_sql.append("ALTER TABLE screener ADD COLUMN 財務コメント TEXT")
    if "スコア" not in cols:
        add_sql.append("ALTER TABLE screener ADD COLUMN スコア REAL")
    if "進捗率" not in cols:
        add_sql.append("ALTER TABLE screener ADD COLUMN 進捗率 REAL")
    for s in add_sql:
        try:
            cur.execute(s)
        except Exception:
            pass
    conn.commit()

    # 2) finance_notes -> screener 同期（コードは0埋め4桁で比較）
    cur.execute(
        """
        UPDATE screener AS s
        SET 財務コメント = (
              SELECT n.財務コメント
              FROM finance_notes n
              WHERE printf('%04d', CAST(n.コード AS INTEGER)) = printf('%04d', CAST(s.コード AS INTEGER))
            ),
            スコア = (
              SELECT n.score
              FROM finance_notes n
              WHERE printf('%04d', CAST(n.コード AS INTEGER)) = printf('%04d', CAST(s.コード AS INTEGER))
            ),
            進捗率 = (
              SELECT n.progress_percent
              FROM finance_notes n
              WHERE printf('%04d', CAST(n.コード AS INTEGER)) = printf('%04d', CAST(s.コード AS INTEGER))
            )
        WHERE EXISTS (
          SELECT 1 FROM finance_notes n
          WHERE printf('%04d', CAST(n.コード AS INTEGER)) = printf('%04d', CAST(s.コード AS INTEGER))
        )
        """
    )
    conn.commit()
    cur.close()

def _timed(label, func, *args, **kwargs):
    """関数の処理時間を計測してログ出力するラッパー"""
    t0 = time.time()
    try:
        return func(*args, **kwargs)
    finally:
        dt = time.time() - t0
        print(f"[TIMER] {label}: {dt:.2f} 秒")
        

# ===== 実行モード判定ユーティリティ =====
def _auto_run_mode():
    """JSTで 11:30-12:30 は MIDDAYそれ以外はEOD"""
    if not AUTO_MODE:
        return RUN_SESSION.upper()

    try:
        now = dtm.datetime.now(ZoneInfo("Asia/Tokyo")).time()
    except Exception:
        now = dtm.datetime.now().time()

    return "MIDDAY" if dtm.time(11,30) <= now < dtm.time(12,30) else "EOD"

# ===== メイン処理 =====
def main():
    t0 = time.time()
    print("=== 開始 ===")

    # (0) 付帯処理：空売り機関リストの更新
    try:
        _timed("run_karauri_script", run_karauri_script)
    except Exception as e:
        print("[karauri][WARN]", e)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # ダッシュボード生成前などに
    run_fundamental_daily()
    
    # ▼ ここを追加：起動時にまず fetch_all を実行（DBに収集・保存させる）
    try:
        _timed("fetch_all", _run_fetch_all,
               # fetch_path=None → 自動解決。固定したければ絶対パスを渡す
               fetch_path=r"H:\desctop\株攻略\2-トレンドツール\fetch_all.py",
               # extra_args は fetch_all 側の引数仕様に合わせて適宜
               extra_args=[],     # 例: ["--earnings-only", "--force"]
               timeout_sec=None,  # 必要なら秒指定
               use_lock=True)
    except Exception as e:
        # 収集に失敗してもダッシュボード生成自体は続行したいなら warn で握りつぶす
        print(f"[fetch_all][WARN] {e}")

    # (1) DB open & スキーマ保証
    conn = open_conn(DB_PATH)
    # [v12] removed: lib.parse import quote as _q


    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    # if is_jp_market_holiday(dtm.date.today(), extra_closed):
    #     print(f"日本の休場日（{dtm.date.today()}）のためスキップします。")
    #     print("=== 終了 ==="); return

    # (3) CSV取り込み（コード・銘柄名・市場・登録日のみ）
    if USE_CSV:
        try:
            _timed("phase_csv_import", phase_csv_import, conn, overwrite_registered_date=False)
        except Exception as e:
            print("[csv-import][WARN]", e)

    # (4) 上場廃止/空売り無しの反映
    try:
       _timed("phase_delist_cleanup", phase_delist_cleanup, conn, also_clean_notes=True)
    except Exception as e:
        print("[delist][WARN]", e)

    try:
        _timed("phase_mark_karauri_nashi", phase_mark_karauri_nashi, conn)
    except Exception as e:
        print("[karauri-flag][WARN]", e)

    # (5) 実行モード決定
    RUN = _auto_run_mode()
    print(f"[AUTO_MODE={AUTO_MODE}] mode={RUN}")

    # (6) 処理対象銘柄
    codes = [str(r[0]) for r in conn.execute("SELECT コード FROM screener").fetchall()]
    if TEST_MODE:
        codes = codes[:TEST_LIMIT]
        print(f"[TEST] 対象 {len(codes)} 銘柄に制限")

    try:
        if RUN == "MIDDAY":
            # ===== MIDDAYモード =====
            _timed("yahoo_intraday_snapshot", phase_yahoo_intraday_snapshot, conn)
            _timed("snapshot_shodou_baseline", phase_snapshot_shodou_baseline, conn)
            _timed("update_shodou_multipliers", phase_update_shodou_multipliers, conn)
            _timed("compute_right_up_persistent", compute_right_up_persistent, conn)
            _timed("compute_right_up_early_triggers", compute_right_up_early_triggers, conn)
            _timed("derive_update", phase_derive_update, conn)
            _timed("signal_detection", phase_signal_detection, conn)
            _timed("update_since_dates", phase_update_since_dates, conn)

        else:
            # ===== EODモード =====
            _timed("yahoo_bulk_refresh", phase_yahoo_bulk_refresh, conn, codes, batch_size=200)
            _timed("refresh_full_history_for_insufficient", refresh_full_history_for_insufficient,conn, codes, batch_size=200)
            _timed("compute_right_up_persistent", compute_right_up_persistent, conn)
            _timed("compute_right_up_early_triggers", compute_right_up_early_triggers, conn)

            # 現在時刻が12:30以前なら「重い処理」も実行する
            now = dtm.datetime.now().time()
            if now < dtm.time(12,30):
                _timed("update_market_cap_all", update_market_cap_all, conn, batch_size=100, max_workers=4)
                try:
                    _timed("update_operating_income_and_ratio", update_operating_income_and_ratio, conn)
                except Exception as e:
                    print("[operating-income][WARN]", e)
            else:
                print("[SKIP] 営業利益・時価総額の更新（12:30以降のためスキップ）")

            _timed("snapshot_shodou_baseline", phase_snapshot_shodou_baseline, conn)
            _timed("update_shodou_multipliers", phase_update_shodou_multipliers, conn)
            _timed("derive_update", phase_derive_update, conn)
            _timed("signal_detection", phase_signal_detection, conn)
            _timed("update_since_dates", phase_update_since_dates, conn)

            try:
                _timed("validate_prev_business_day", phase_validate_prev_business_day, conn)
            except Exception as e:
                print("[validate-prev][WARN]", e)
                
        
        # 最後に build_earnings_tables(conn) を呼んで HTML にタブを追加

        # (6.5)
        _timed("relax_rejudge_signals", relax_rejudge_signals, conn)

        # (7) 数値の正規化
        # [v12] 数値の正規化ステップはスキップ（固定スキーマ前提）






        # (7.1) 財務コメント追加
        phase_sync_finance_comments(conn)

        # (8) ダッシュボード出力
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        _timed("export_html_dashboard", phase_export_html_dashboard_offline, conn, html_path)

        # (9) メール送信（任意）
        #try:
        #    _timed("send_index_html_via_gmail", send_index_html_via_gmail, html_path)
        # クールダウンなしで強制オープン
        ok = open_html_locally(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\screen_data\index.html", cool_min=0, force=True)
        print("opened:", ok)
        #
        #except Exception as e:
        #    print("[gmail][WARN]", e)
        #    
        #

    finally:
        conn.close()
    print(f"実行時間： {time.time() - t0:.2f}s")
    print("=== 終了 ===")

# ===== エントリーポイント =====
if __name__ == "__main__":
    main()