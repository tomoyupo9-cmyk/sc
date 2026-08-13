# === 2026-08-13 PTS対応 ===
# - 株探 financeページ上部のPTS株価/時刻を同じHTTP取得から抽出
# - screener.PTS株価 / screener.PTS時刻 に保存
# - 同日再取得したい場合は --force-refresh を使用
# === 2026-08-12 v14: 四半期実績履歴DB化（ソリトン型対応の土台） ===
# - 株探 3ヵ月決算【実績】を quarterly_actual_history へ保存
# - fiscal_key / quarter_no / 発表日 / 売上 / 営業益 / 経常益 / 最終益 / EPS を保持
# - 既存 finance_notes の意味は変更しない（actual_operating_profit は従来通り直近通期実績）
# === 2026-08-12 v13: 過去決算反応データ復元 ===
# - 四半期実績の発表日から past_earnings_dates を再生成（直近8回）
# - return へ past_earnings_dates を復帰
# - 取得失敗/空配列で既存履歴を消さない保護を追加
# - 過去反応列の元データを自己修復可能にする
# -*- coding: utf-8 -*-

#💻 コマンドラインオプションの使い方一覧
#この統合エンジンは、コマンドラインから様々な動作を指定して実行できます。
#
#1. 基本的な実行（全銘柄を一括処理）
#マスターファイル（株コード番号.txt）に記載されている全銘柄の株探データを取得し、直後にローカルDBを使って適正株価を計算・更新します。
#
#DOS
#python 統合スクリプト名.py
#2. 特定の1銘柄だけテスト実行する
#動作確認やデバッグのために、1つのコードだけをすばやく処理して適正株価まで計算させたい場合：
#
#DOS
#python 統合スクリプト名.py 7211
#(例：三菱自動車のコード 7211 を指定)
#
#3. 同時並列数（ワーカー数）を変更する
#株探サーバーへの負荷や速度を調整するため、同時にリクエストを送る数を変更します（デフォルトは 7）。
#
#DOS
#python 統合スクリプト名.py --workers 10
#4. キャッシュを無視して強制的に最新データを再取得する（強制的フルリフレッシュ）
#前回のスキップ判定を無視して、全銘柄をもう一度株探から強制的に取り直したい場合：
#
#DOS
#python 統合スクリプト名.py --force-refresh
#5. DBスキップ機能を一時的に無効化して実行する
#コード内の設定（ENABLE_DB_SKIP = True）による直近の発表日スキップを無視させたい場合：
#
#DOS
#python 統合スクリプト名.py --no-db-skip
#6. データベースへの書き込みをせずに動作確認する
#DBを汚したくないテスト用のオプション：
#
#DOS
#python 統合スクリプト名.py --no-db




# -*- coding: utf-8 -*-
# 株探ファンダ_v12_retry_dbskip_default__FULL_FIXED_v6.py
# - リトライ（指数バックオフ＋ジッター）
# - 並列数可変 (--workers)
# - 進捗率ログ (--log-progress, --log-file)
# - 最新発表日でのスキップ：DBキャッシュ基準（HTTP前）【デフォルト有効】
#   * ENABLE_DB_SKIP = True
#   * SKIP_RECENT_DAYS_DEFAULT = 1 にデフォルト設定
# - 直近が異常終了ならスキップ無効（必ず再取得）
# - DB書き込みのON/OFF (--no-db)
#
# 追加：
# - スコア→アルファ（S++/A+/B/C/D-）を overall_alpha として finance_notes に保存
# - 四半期テーブルが取れなくても「発表日だけ取得できたら」earnings_cache を ANNOUNCE_ONLY で更新
# - 例外時も可能なら発表日だけ拾って ANNOUNCE_ONLY でキャッシュ更新（次回スキップが効く）
# - 【厳格対策】未定義エラー防止：日本語名の残骸参照を無害化（globals にダミー定義）
# - 通期データ取得のついでに財務テーブルと実績データ（売上・利益・EPS等）を取得して保存
#
# ★統合追加：
# - ローカルDB完全依存・YahooQuery排除型の「動的適正株価算出エンジン」を実装
# - 「基準PER12倍＋成長率キャップ＋正常化EPS＋PBR考慮」の保守・堅牢モデルを採用
# - Pandas重複カラムバグを修正し、確実なDBアップデートを実現

import re
import argparse
import sys
import os
import sqlite3
import asyncio
import json
from datetime import datetime, timezone, timedelta
from io import StringIO
import random
from typing import Any, Optional, Dict, List

import aiohttp
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from bs4 import BeautifulSoup

# --- NameError防止（他ファイル由来の日本語識別子の残骸に備える） ---
globals().update({'最新_label': None, '最新_cumulative_op': None})

# ===== 日本語フォント =====
def _set_japanese_font():
    preferred = ["Meiryo", "Yu Gothic", "YuGothic", "IPAexGothic",
                 "Noto Sans CJK JP", "Noto Sans JP", "TakaoGothic", "MS Gothic"]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in preferred:
        if name in available:
            rcParams["font.family"] = name
            break
    rcParams["axes.unicode_minus"] = False
_set_japanese_font()

# ===== 定数 =====
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36")
}
ASYNC_CONCURRENCY_LIMIT = 7
MASTER_CODES_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt"
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
OUTPUT_DIR = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data"

# リトライ設定
RETRY_HTTP_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 4
BACKOFF_BASE_SEC = 2.0   # 1.0,1.5,2.25,3.38... + ジッター
JITTER_SEC = 0.3

# DBスキップ：デフォルト有効（無効化は --no-db-skip）
ENABLE_DB_SKIP = True
SKIP_RECENT_DAYS_DEFAULT = 1

BAD_LAST_STATUSES = {
    "ERROR_429", "ERROR_500", "ERROR_502", "ERROR_503", "ERROR_504",
    "TIMEOUT", "CONN_ERROR", "HTTP_ERROR", "PARSE_ERROR", "UNKNOWN_ERROR"
}
GOOD_LAST_STATUSES = {"OK", "FETCHED", "EMPTY_QUARTERLY", "ANNOUNCE_ONLY"}

_ARGS = None
SKIP_MARKET_KEYWORDS = ("ETF", "指数")

# ===== 共通ユーティリティ =====
def safe_float(value: Any) -> Optional[float]:
    if value is None: return None
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value): return None
        return float(value)
    if isinstance(value, (int, np.integer)): return float(value)
    s = str(value).strip()
    if not s: return None
    s = s.replace(",", "").replace("円", "").replace("%", "").replace("倍", "")
    s = s.replace("▲", "-").replace("△", "-").replace("－", "-").replace("―", "-")
    if s in ("-", "--", "N/A", "NA"): return None
    try: return float(s)
    except Exception: return None

def _parse_kabutan_pts(soup: BeautifulSoup) -> dict:
    """株探ページ上部の PTS 株価・表示時刻を取得する。

    例:
      <div class="si_i1_3">
        <div class="kabuka1">PTS</div>
        <div class="kabuka2">5,003円</div>
        <div class="kabuka3">17:32　08/13</div>
      </div>

    PTS表示が無い場合は None を返す。
    """
    out = {"pts_price": None, "pts_time": None}
    try:
        # まず既知の株価ブロック単位で探し、別の kabuka2 を誤取得しないようにする。
        for block in soup.select("div.si_i1_3"):
            label = block.select_one("div.kabuka1")
            if label and label.get_text(" ", strip=True).upper() == "PTS":
                price_el = block.select_one("div.kabuka2")
                time_el = block.select_one("div.kabuka3")
                if price_el:
                    out["pts_price"] = safe_float(price_el.get_text(" ", strip=True))
                if time_el:
                    out["pts_time"] = time_el.get_text(" ", strip=True) or None
                return out

        # HTML構造変更への保険。PTSラベルの親要素から価格/時刻を探す。
        for label in soup.find_all("div", class_="kabuka1"):
            if label.get_text(" ", strip=True).upper() != "PTS":
                continue
            block = label.parent
            price_el = block.find("div", class_="kabuka2") if block else None
            time_el = block.find("div", class_="kabuka3") if block else None
            if price_el:
                out["pts_price"] = safe_float(price_el.get_text(" ", strip=True))
            if time_el:
                out["pts_time"] = time_el.get_text(" ", strip=True) or None
            return out
    except Exception:
        pass
    return out


def safe_div(a: Any, b: Any) -> Optional[float]:
    a, b = safe_float(a), safe_float(b)
    if a is None or b is None or abs(b) < 1e-12: return None
    return a / b

def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

def growth_rate(current: Any, previous: Any) -> Optional[float]:
    current, previous = safe_float(current), safe_float(previous)
    if current is None or previous is None or abs(previous) < 1e-12: return None
    return current / previous - 1.0

# ===== ヘルパー =====
def _get_market_from_db(code: str) -> str | None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 市場 FROM screener WHERE コード = ? LIMIT 1;", (str(code),))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception: return None
    finally:
        try: conn.close()
        except Exception: pass

def _is_skip_market(market: str | None) -> bool:
    if not market: return False
    s = str(market).replace(" ", "").replace(" ", "")
    return any(k in s for k in SKIP_MARKET_KEYWORDS)

async def _fetch_text_with_retry(session: aiohttp.ClientSession, url: str, *, timeout_sec: int = 15) -> str:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=timeout_sec) as res:
                if res.status in RETRY_HTTP_STATUSES:
                    _ = await res.text()
                    raise aiohttp.ClientResponseError(
                        request_info=res.request_info, history=res.history,
                        status=res.status, message=f"retryable status {res.status}", headers=res.headers
                    )
                res.raise_for_status()
                return await res.text(encoding=res.get_encoding() or "utf-8")
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            last_err = e
            if isinstance(e, aiohttp.ClientResponseError) and e.status not in RETRY_HTTP_STATUSES: break
            if attempt < MAX_RETRIES:
                sleep_sec = (BACKOFF_BASE_SEC ** (attempt - 1)) + random.uniform(0, JITTER_SEC)
                await asyncio.sleep(sleep_sec)
    raise last_err if last_err else RuntimeError("Unknown retry failure")

def _fiscal_key(label: str) -> str | None:
    if not isinstance(label, str): return None
    m = re.search(r'(\d{4}\.\d{1,2})', label)
    return m.group(1) if m else None

def _fiscal_key_from_quarter_label(q_label: str, fiscal_end_month: int) -> str | None:
    if not isinstance(q_label, str) or fiscal_end_month is None: return None
    trans = str.maketrans({'／':'/', '－':'-', '―':'-', 'ー':'-', '．':'.'})
    s = q_label.translate(trans).replace('/', '.')
    m = re.search(r'(?P<yy>\d{2})[\.\/](?P<m1>\d{2})[-–-](?P<m2>\d{2})', s)
    if not m: return None
    yy = int(m.group('yy')); end_m = int(m.group('m2'))
    base_year = 2000 + yy
    fy_year = base_year if end_m <= fiscal_end_month else base_year + 1
    return f"{fy_year}.{fiscal_end_month:02d}"

def _print_html_snippet_on_error(code: str, html_content: str, func_name: str):
    start_index = html_content.find('<table')
    if start_index != -1:
        snippet = html_content[start_index:start_index + 500]
        print(f"[DEBUG_HTML] {code} in {func_name}: Table snippet: {snippet}...")

# ===== DB：earnings_cache（発表日キャッシュ） =====
def _ensure_cache_table():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS earnings_cache (
          コード TEXT PRIMARY KEY, latest_announce_date TEXT, last_status TEXT, updated_at TEXT
        );
        """)
        conn.commit()
    except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass

def db_get_cached_announce(code: str) -> tuple[datetime | None, str | None]:
    _ensure_cache_table()
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT updated_at, last_status FROM earnings_cache WHERE コード = ?", (code,))
        row = cur.fetchone()
        if not row or not row[0]: return (None, None)
        try: dt = datetime.fromisoformat(str(row[0]))
        except Exception: dt = datetime.strptime(str(row[0])[:10], "%Y-%m-%d")
        return (dt, row[1])
    except Exception: return (None, None)
    finally:
        try: conn.close()
        except Exception: pass

def db_upsert_cached_announce(code: str, latest_announce_date: datetime | None, last_status: str):
    _ensure_cache_table()
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        iso_date = latest_announce_date.date().isoformat() if latest_announce_date else None
        ts = datetime.now().isoformat(timespec="seconds")
        cur.execute("""
        INSERT INTO earnings_cache (コード, latest_announce_date, last_status, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(コード) DO UPDATE SET
          latest_announce_date = excluded.latest_announce_date,
          last_status = excluded.last_status,
          updated_at = excluded.updated_at;
        """, (code, iso_date, last_status, ts))
        conn.commit()
    except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass

# ===== アルファ評価 =====
def _get_overall_alpha(score: int) -> str:
    if score >= 8: return "S++"
    elif score >= 5: return "A+"
    elif score >= 2: return "B"
    elif score >= 0: return "C"
    else: return "D-"

def _get_overall_verdict_label(score: int) -> str:
    if score >= 8: return "超優良 (S++)"
    elif score >= 5: return "優良 (A+)"
    elif score >= 2: return "成長期待 (B)"
    elif score >= 0: return "現状維持 (C)"
    else: return "要注意 (D-)"

# ===== DB：finance_notes 書き込み =====
def batch_record_to_sqlite(results: list[dict]):
    if not results:
        print("[DB] 記録すべき成功結果がありません。")
        return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS finance_notes (
          コード TEXT PRIMARY KEY, 財務コメント TEXT, score INTEGER, progress_percent REAL, html_path TEXT, updated_at TEXT
        );
        """)
        for col, coltype in [
            ("overall_alpha", "TEXT"), ("forecast_op", "REAL"), ("forecast_eps", "REAL"), ("actual_eps", "REAL"),
            ("prev_eps", "REAL"), ("forecast_revenue", "REAL"), ("actual_revenue", "REAL"), ("prev_revenue", "REAL"),
            ("forecast_net_profit", "REAL"), ("actual_net_profit", "REAL"), ("prev_net_profit", "REAL"),
            ("actual_operating_profit", "REAL"), ("prev_operating_profit", "REAL"), ("bps", "REAL"), ("equity", "REAL"),
            ("past_earnings_dates", "TEXT")
        ]:
            try: cur.execute(f"SELECT {col} FROM finance_notes LIMIT 1;")
            except sqlite3.OperationalError: cur.execute(f"ALTER TABLE finance_notes ADD COLUMN {col} {coltype};")
        conn.commit()

        # PTSはダッシュボードで直接使うため screener に保持する。
        for col, coltype in [("PTS株価", "REAL"), ("PTS時刻", "TEXT")]:
            try:
                cur.execute(f"ALTER TABLE screener ADD COLUMN {col} {coltype};")
            except sqlite3.OperationalError:
                pass
        conn.commit()

        ts = datetime.now().isoformat(timespec="seconds")
        data_to_insert = []
        for res in results:
            if res.get('status') == 'OK':
                data_to_insert.append((
                    res['code'], res.get('formatted_verdict') or "", res.get('score'), res.get('progress_percent'), res.get('out_html') or "", ts,
                    res.get('overall_alpha'), res.get('forecast_op'), res.get('forecast_eps'), res.get('actual_eps'), res.get('prev_eps'),
                    res.get('forecast_revenue'), res.get('actual_revenue'), res.get('prev_revenue'),
                    res.get('forecast_net_profit'), res.get('actual_net_profit'), res.get('prev_net_profit'),
                    res.get('actual_operating_profit'), res.get('prev_operating_profit'), res.get('bps'), res.get('equity'), res.get('past_earnings_dates')
                ))

        if data_to_insert:
            cur.executemany("""
            INSERT INTO finance_notes (
                コード, 財務コメント, score, progress_percent, html_path, updated_at,
                overall_alpha, forecast_op, forecast_eps, actual_eps, prev_eps,
                forecast_revenue, actual_revenue, prev_revenue,
                forecast_net_profit, actual_net_profit, prev_net_profit,
                actual_operating_profit, prev_operating_profit, bps, equity, past_earnings_dates
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(コード) DO UPDATE SET
                財務コメント = excluded.財務コメント, score = excluded.score, progress_percent = excluded.progress_percent,
                html_path = excluded.html_path, updated_at = excluded.updated_at, overall_alpha = excluded.overall_alpha,
                forecast_op = excluded.forecast_op, forecast_eps = excluded.forecast_eps, actual_eps = excluded.actual_eps,
                prev_eps = excluded.prev_eps, forecast_revenue = excluded.forecast_revenue, actual_revenue = excluded.actual_revenue,
                prev_revenue = excluded.prev_revenue, forecast_net_profit = excluded.forecast_net_profit, actual_net_profit = excluded.actual_net_profit,
                prev_net_profit = excluded.prev_net_profit, actual_operating_profit = excluded.actual_operating_profit,
                prev_operating_profit = excluded.prev_operating_profit, bps = excluded.bps, equity = excluded.equity,
                past_earnings_dates = CASE
                    WHEN excluded.past_earnings_dates IS NOT NULL
                     AND TRIM(excluded.past_earnings_dates) NOT IN ('', '[]')
                    THEN excluded.past_earnings_dates
                    ELSE finance_notes.past_earnings_dates
                END;
            """, data_to_insert)
            conn.commit()

        pts_updates = [
            (res.get("pts_price"), res.get("pts_time"), res.get("code"))
            for res in results
            if res.get("status") == "OK" and res.get("code")
        ]
        if pts_updates:
            cur.executemany(
                "UPDATE screener SET PTS株価=?, PTS時刻=? WHERE コード=?",
                pts_updates,
            )
            conn.commit()
            pts_count = sum(1 for p, _, _ in pts_updates if p is not None)
            print(f"[PTS] screener 更新={len(pts_updates)}銘柄 / PTS表示あり={pts_count}銘柄")

        q_saved = upsert_quarterly_actual_history(conn, results)
        if q_saved:
            print(f"[QUARTERLY_HISTORY] {q_saved}行 UPSERT")

    except Exception as e:
        print(f"[DB][CRITICAL ERROR] 予期せぬエラー: {e}")
    finally:
        try: conn.close()
        except Exception: pass

# ===== 取得関数 =====
async def fetch_quarterly_financials(code: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    print(f"[INFO] fetching quarterly data: {url}")
    try:
        html_content = await _fetch_text_with_retry(session, url, timeout_sec=15)
        soup = BeautifulSoup(html_content, "lxml")
        target = None
        target_heading = soup.find(string=lambda t: t and '3ヵ月決算【実績】' in t)
        if target_heading:
            target = target_heading.find_next('table')
        if target is None: return pd.DataFrame()
        
        df = pd.read_html(StringIO(str(target)), flavor="lxml", header=0)[0]
        df = df.drop(columns=[c for c in df.columns if "損益率" in c], errors='ignore')

        df.columns = df.columns.astype(str).str.replace(" ", "", regex=False).str.replace(" ", "", regex=False)
        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年同期比|予|通期", na=False)
            df = df[mask].copy()
        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)

        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")

        if "発表日" in df.columns:
            df["発表日"] = pd.to_datetime(
                df["発表日"].astype(str).str.split(r'\(| ').str[0].str.strip(),
                format="%y/%m/%d", errors='coerce'
            )
            df.dropna(subset=["発表日"], inplace=True)

        return df.sort_values(by="発表日", ascending=True).reset_index(drop=True)

    except Exception: return pd.DataFrame()

async def fetch_full_year_financials(code: str, session: aiohttp.ClientSession) -> tuple[pd.DataFrame, dict]:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    print(f"[INFO] fetching full year actual data: {url}")
    extra_data = {"bps": None, "equity": None, "pts_price": None, "pts_time": None}

    try:
        html_content = await _fetch_text_with_retry(session, url, timeout_sec=15)
        soup = BeautifulSoup(html_content, "lxml")

        # 株探ページ上部のPTS表示も、このHTTP取得のついでに回収する。
        pts_data = _parse_kabutan_pts(soup)
        extra_data.update(pts_data)
        if pts_data.get("pts_price") is not None:
            print(f"[PTS] {code}: {pts_data.get('pts_price')}円 / {pts_data.get('pts_time') or '-'}")

        tables = soup.find_all('table')
        
        target_annual, target_financial = None, None
        
        for table in tables:
            header_row = table.find('tr')
            if header_row:
                header_text = header_row.get_text()
                if '決算期' in header_text or '通期' in header_text:
                    if target_annual is None: target_annual = table
                if '自己資本' in header_text or '純資産' in header_text or '1株純資産' in header_text:
                    if target_financial is None: target_financial = table

        if target_financial is not None:
            try:
                fin_df = pd.read_html(StringIO(str(target_financial)), flavor="lxml", header=0)[0]
                cols = fin_df.columns.astype(str)
                for idx in reversed(fin_df.index):
                    row = fin_df.loc[idx]
                    for c in cols:
                        if ("1株" in c or "１株" in c) and "純資産" in c:
                            val = safe_float(row[c])
                            if val is not None and extra_data["bps"] is None: extra_data["bps"] = val
                        elif "BPS" in c.upper():
                            val = safe_float(row[c])
                            if val is not None and extra_data["bps"] is None: extra_data["bps"] = val
                        elif "自己資本" in c or "純資産" in c:
                            if "1株" not in c and "１株" not in c and "比率" not in c:
                                val = safe_float(row[c])
                                if val is not None and extra_data["equity"] is None: extra_data["equity"] = val
            except Exception: pass

        if target_annual is None:
            return pd.DataFrame(), extra_data

        df = pd.read_html(StringIO(str(target_annual)), flavor="lxml", header=0)[0]
        df.columns = df.columns.astype(str).str.replace(r'\s+', '', regex=True)
        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年比", na=False)
            df = df[mask].copy()
        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)
        
        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.drop(columns=["発表日"], errors='ignore')
        return df.sort_values(by="決算期", ascending=True).reset_index(drop=True), extra_data

    except Exception: return pd.DataFrame(), extra_data

def _parse_latest_announce_from_any_table(html_content: str) -> datetime | None:
    try:
        soup = BeautifulSoup(html_content, "lxml")
        for tbl in soup.find_all("table"):
            try: df = pd.read_html(StringIO(str(tbl)), flavor="lxml", header=0)[0]
            except Exception: continue
            if any("発表日" in c for c in [str(c) for c in df.columns]):
                try:
                    ser = df[[c for c in df.columns if "発表日" in str(c)][0]].astype(str)
                    ser = pd.to_datetime(ser.str.split(r'\(| ').str[0].str.strip(), format="%y/%m/%d", errors="coerce").dropna()
                    if not ser.empty: return pd.to_datetime(ser.iloc[-1])
                except Exception: continue
        return None
    except Exception: return None

async def fetch_latest_announce_date_only(session: aiohttp.ClientSession, code: str) -> datetime | None:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    try:
        html_content = await _fetch_text_with_retry(session, url, timeout_sec=10)
        return _parse_latest_announce_from_any_table(html_content)
    except Exception: return None

def compute_progress_details(quarterly_df: pd.DataFrame, forecast_series: pd.Series):
    details = {"latest_label": None, "key_q": None, "key_f": None, "fiscal_end_month": None, "cumulative_op": None, "full_op_val": None, "status": None, "progress": None}
    if (quarterly_df is None or quarterly_df.empty or "営業益" not in quarterly_df.columns or "決算期" not in quarterly_df.columns):
        details["status"] = "四半期DF欠損";  return details
    latest_row = quarterly_df.tail(1)
    if latest_row.empty: details["status"] = "四半期DF空";  return details
    latest_label = str(latest_row["決算期"].iloc[0]);  details["latest_label"] = latest_label
    if forecast_series is None or forecast_series.empty: details["status"] = "予想未発表";  return details
    full_op_val = pd.to_numeric(forecast_series.get("営業益"), errors="coerce")
    details["full_op_val"] = None if pd.isna(full_op_val) else float(full_op_val)
    if pd.isna(full_op_val): details["status"] = "予想データ欠損";  return details
    if full_op_val == 0: details["status"] = "予想ゼロ";  return details
    key_f = _fiscal_key(str(forecast_series.get("決算期", "")));  details["key_f"] = key_f
    if not key_f: details["status"] = "予想未発表";  return details
    m = re.search(r"(\d{4})\.(\d{1,2})", key_f);  fiscal_end_month = int(m.group(2)) if m else None
    details["fiscal_end_month"] = fiscal_end_month
    key_q = _fiscal_key_from_quarter_label(latest_label, fiscal_end_month);  details["key_q"] = key_q
    if not key_q: details["status"] = "予想データ欠損";  return details
    mask_same_fy = quarterly_df["決算期"].astype(str).apply(lambda s: _fiscal_key_from_quarter_label(s, fiscal_end_month) == key_q)
    cumulative_op = pd.to_numeric(quarterly_df.loc[mask_same_fy, "営業益"], errors="coerce").sum(min_count=1)
    details["cumulative_op"] = None if pd.isna(cumulative_op) else float(cumulative_op)
    if pd.isna(cumulative_op): details["status"] = "予想データ欠損";  return details
    if key_q != key_f: details["status"] = "会計期不一致";  return details
    if full_op_val < 0 or (cumulative_op < 0 and full_op_val > 0): details["status"] = "予想データ欠損";  return details
    progress = float(cumulative_op) / float(full_op_val) * 100.0
    if progress > 3000: details["status"] = "予想データ欠損";  return details
    details["progress"] = round(progress, 1);  details["status"] = "OK";  return details

def log_progress_details(code: str, details: dict, log_path: str | None = None):
    print(f"[PROG] {code} label={details.get('latest_label')} key_q={details.get('key_q')} key_f={details.get('key_f')} cum_op={details.get('cumulative_op')} full_op={details.get('full_op_val')} status={details.get('status')} progress={details.get('progress')}%")
    if log_path:
        import csv
        header = ["code","latest_label","key_q","key_f","fiscal_end_month","cumulative_op","full_op_val","status","progress"]
        write_header = not os.path.isfile(log_path)
        with open(log_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            if write_header: w.writerow(header)
            w.writerow([code, details.get("latest_label"), details.get("key_q"), details.get("key_f"), details.get("fiscal_end_month"), details.get("cumulative_op"), details.get("full_op_val"), details.get("status"), details.get("progress")])

def _safe_growth(latest, prev):
    if pd.isna(latest) or pd.isna(prev): return None
    if prev == 0: return 100.0 if latest > 0 else (0.0 if latest == 0 else -100.0)
    return ((latest - prev) / abs(prev)) * 100.0

def _format_verdict_with_progress(qp, raw_verdict: str, score: int) -> str:
    overall_label = _get_overall_verdict_label(score)
    prog_header = "[進捗率 (情報不足)]"
    if qp is not None:
        latest_label, progress, status, _ = qp
        if status == "OK": prog_header = f"[{latest_label}：進捗{progress}%]"
        elif status == "1Q未発表(期ズレ)": prog_header = f"[{latest_label}：今期予想発表済 (1Q待機)]"
        elif status in ("予想未発表", "予想ゼロ", "予想データ欠損", "会計期不一致"): prog_header = f"[{latest_label}：進捗率 ({status})]"

    score_header = f"スコア {score} 点"
    body_lines = [s.strip() for s in (raw_verdict or "").splitlines() if s.strip()]
    body_fixed = body_lines[:4]
    while len(body_fixed) < 4: body_fixed.append("（詳細なし）")
    lines = [f"【総合評価】{overall_label}", prog_header, score_header, "--- 詳細 ---", *body_fixed[:4]]
    return "\n".join(lines[:8])

def judge_and_score_performance(df: pd.DataFrame) -> tuple[str, int]:
    if df.empty or len(df) < 2: return "データ不足のため判定できません。", 0
    is_forecast = "予" in str(df.iloc[-1]["決算期"])
    df_latest, df_prev = df.iloc[-1], df.iloc[-2]
    comparison_period = "通期予想 vs 前期実績" if is_forecast else "直近実績 vs 前期実績 (通期予想データ欠損)"
    latest_sales, latest_op, latest_eps = df_latest.get("売上高"), df_latest.get("営業益"), df_latest.get("修正1株益")
    prev_sales, prev_op, prev_eps = df_prev.get("売上高"), df_prev.get("営業益"), df_prev.get("修正1株益")
    
    op_growth, eps_growth, sales_growth = _safe_growth(latest_op, prev_op), _safe_growth(latest_eps, prev_eps), _safe_growth(latest_sales, prev_sales)
    msgs, score = [f"（比較期間：{comparison_period}）"], 0

    def get_status_str(latest, prev):
        if pd.isna(latest) or pd.isna(prev): return ""
        if prev < 0 and latest > 0: return " (黒字転換🎉)"
        if prev < 0 and latest < 0 and latest > prev: return " (赤字縮小✨)"
        if prev > 0 and latest < 0: return " (赤字転落⚠️)"
        if prev < 0 and latest < 0 and latest < prev: return " (赤字拡大🚨)"
        return ""

    if eps_growth is not None:
        status_str = get_status_str(latest_eps, prev_eps)
        if eps_growth > 20: score += 3; msgs.append(f"🟢 EPS成長率: {eps_growth:.1f}%{status_str or ' (高成長)'}")
        elif eps_growth > 10: score += 2; msgs.append(f"🟡 EPS成長率: {eps_growth:.1f}%{status_str or ' (安定成長)'}")
        elif eps_growth > 0: score += 1; msgs.append(f"⚪️ EPS成長率: {eps_growth:.1f}%{status_str or ' (微増)'}")
        elif eps_growth == 0: msgs.append(f"⚫️ EPS成長率: 0.0%{status_str or ' (横ばい)'}")
        else: score -= 2; msgs.append(f"🔴 EPS成長率: {eps_growth:.1f}%{status_str or ' (減益注意)'}")
    else: msgs.append("EPS成長率: データ欠損")
        
    if op_growth is not None:
        status_str = get_status_str(latest_op, prev_op)
        if op_growth > 20: score += 2; msgs.append(f"🟢 営業益成長率: {op_growth:.1f}%{status_str or ' (高成長)'}")
        elif op_growth > 10: score += 1; msgs.append(f"🟡 営業益成長率: {op_growth:.1f}%{status_str or ' (安定成長)'}")
        elif op_growth < 0: score -= 1; msgs.append(f"🔴 営業益成長率: {op_growth:.1f}%{status_str or ' (減益注意)'}")
        else: msgs.append(f"⚪️ 営業益成長率: {op_growth:.1f}%{status_str}")
    else: msgs.append("営業益成長率: データ欠損")
        
    if sales_growth is not None:
        if sales_growth > 10: score += 1; msgs.append(f"🟢 売上高成長率: {sales_growth:.1f}% (高成長)")
        elif sales_growth < 0: score -= 1; msgs.append(f"🔴 売上高成長率: {sales_growth:.1f}% (減収注意)")
        else: msgs.append(f"⚪️ 売上高成長率: {sales_growth:.1f}%")
    else: msgs.append("売上高成長率: データ欠損")
        
    if (op_growth is not None and op_growth > 0) and (eps_growth is not None and eps_growth > 0):
        msgs.insert(1, "💡 " + ("通期予想は増収・増益（または改善）の見込みです。" if is_forecast else "直近実績は増収・増益（または改善）でした。"))
        
    return "\n".join(msgs), score

def calc_progress_from_df_op(quarterly_df: pd.DataFrame, forecast_series: pd.Series):
    required_cols = ["営業益", "決算期"]
    if quarterly_df is None or quarterly_df.empty or any(c not in quarterly_df.columns for c in required_cols): return None
    latest_row = quarterly_df.tail(1)
    if latest_row.empty: return None
    latest_label = str(latest_row["決算期"].iloc[0])
    if forecast_series is None or forecast_series.empty or "営業益" not in forecast_series: return (latest_label, 0.0, "予想未発表", -3.0)
    raw_op_val = str(forecast_series.get("営業益", "")).replace(",", "").strip()
    full_op_val = pd.to_numeric(raw_op_val, errors="coerce")
    if pd.isna(full_op_val): return (latest_label, 0.0, "予想データ欠損", -1.0)
    if float(full_op_val) == 0.0: return (latest_label, 0.0, "予想ゼロ", -2.0)
    key_f = _fiscal_key(str(forecast_series.get("決算期", "")))
    if not key_f: return (latest_label, 0.0, "予想未発表", -3.0)
    m = re.search(r"(\d{4})\.(\d{1,2})", key_f); fiscal_end_month = int(m.group(2)) if m else None
    key_q = _fiscal_key_from_quarter_label(latest_label, fiscal_end_month)
    if not key_q: return (latest_label, 0.0, "予想データ欠損", -1.0)
    mask_same_fy = quarterly_df["決算期"].astype(str).apply(lambda s: _fiscal_key_from_quarter_label(s, fiscal_end_month) == key_q)
    latest_cumulative_op = pd.to_numeric(quarterly_df.loc[mask_same_fy, "営業益"], errors="coerce").sum(min_count=1)
    if pd.isna(latest_cumulative_op): return (latest_label, 0.0, "予想データ欠損", -1.0)
    if key_q != key_f: return (latest_label, 0.0, "1Q未発表(期ズレ)", -4.0)
    if float(full_op_val) < 0 or (float(latest_cumulative_op) < 0 and float(full_op_val) > 0): return (latest_label, 0.0, "予想データ欠損", -1.0)
    progress = float(latest_cumulative_op) / float(full_op_val) * 100.0
    if progress > 3000: return (latest_label, 0.0, "予想データ欠損", -1.0)
    return (latest_label, round(progress, 1), "OK", progress)

def get_latest_announce_date(quarterly_df: pd.DataFrame) -> datetime | None:
    if quarterly_df is None or quarterly_df.empty or "発表日" not in quarterly_df.columns: return None
    if quarterly_df["発表日"].isna().all(): return None
    try: return pd.to_datetime(quarterly_df["発表日"].iloc[-1])
    except Exception: return None



# ===== v14: 四半期実績履歴（株探3ヵ月決算） =====
def ensure_quarterly_actual_history_schema(conn: sqlite3.Connection):
    """株探の3ヵ月決算【実績】を、後から同Q比較できる形で保存する。"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS quarterly_actual_history(
        コード TEXT NOT NULL,
        fiscal_key TEXT NOT NULL,
        quarter_no INTEGER NOT NULL,
        quarter_label TEXT,
        announcement_date TEXT,
        sales REAL,
        operating_profit REAL,
        ordinary_profit REAL,
        net_profit REAL,
        eps REAL,
        source TEXT DEFAULT 'kabutan_3m_actual',
        updated_at TEXT,
        PRIMARY KEY(コード, fiscal_key, quarter_no)
    );
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_quarterly_actual_code_date
    ON quarterly_actual_history(コード, announcement_date DESC);
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_quarterly_actual_code_fiscal_q
    ON quarterly_actual_history(コード, fiscal_key, quarter_no);
    """)
    conn.commit()


def _quarter_end_month_from_label(q_label: str) -> int | None:
    if not isinstance(q_label, str):
        return None
    trans = str.maketrans({'／':'/', '－':'-', '―':'-', 'ー':'-', '．':'.'})
    s = q_label.translate(trans).replace('/', '.')
    m = re.search(r'(?P<yy>\d{2})[\.](?P<m1>\d{2})[-–-](?P<m2>\d{2})', s)
    return int(m.group('m2')) if m else None


def _quarter_no_from_label(q_label: str, fiscal_end_month: int | None) -> int | None:
    """3ヵ月区間の終了月から会計年度内Q番号(1..4)を復元。"""
    end_m = _quarter_end_month_from_label(q_label)
    if end_m is None or fiscal_end_month is None:
        return None
    diff = (int(end_m) - int(fiscal_end_month)) % 12
    if diff == 0:
        return 4
    if diff in (3, 6, 9):
        return diff // 3
    return None


def build_quarterly_actual_history_rows(
    code: str,
    quarterly_df: pd.DataFrame,
    forecast_series: pd.Series,
) -> list[dict]:
    """
    fetch_quarterly_financials() で既に取得済みの3ヵ月実績をDB保存用に整形。
    fiscal_key は forecast 行の決算月を基準に各3ヵ月ラベルから復元する。
    """
    if quarterly_df is None or quarterly_df.empty or "決算期" not in quarterly_df.columns:
        return []

    fiscal_end_month = None
    try:
        key_f = _fiscal_key(str(forecast_series.get("決算期", ""))) if forecast_series is not None and not forecast_series.empty else None
        if key_f:
            m = re.search(r"(\d{4})\.(\d{1,2})", key_f)
            fiscal_end_month = int(m.group(2)) if m else None
    except Exception:
        fiscal_end_month = None

    # 予想行が取れない会社でも、直近の年次決算月が通常固定なので、
    # 3ヵ月ラベル4本の終了月パターンからは安全に推定できない。無理に推測せず保存を見送る。
    if fiscal_end_month is None:
        return []

    rows = []
    ts = datetime.now().isoformat(timespec="seconds")
    for _, row in quarterly_df.iterrows():
        label = str(row.get("決算期") or "").strip()
        fk_dot = _fiscal_key_from_quarter_label(label, fiscal_end_month)
        qno = _quarter_no_from_label(label, fiscal_end_month)
        if not fk_dot or qno not in (1, 2, 3, 4):
            continue
        fk = fk_dot.replace('.', '-')

        ad = row.get("発表日")
        try:
            ad_iso = pd.Timestamp(ad).date().isoformat() if pd.notna(ad) else None
        except Exception:
            ad_iso = None

        def fv(col):
            if col not in quarterly_df.columns:
                return None
            v = pd.to_numeric(row.get(col), errors="coerce")
            return None if pd.isna(v) else float(v)

        rows.append({
            "コード": str(code).strip(),
            "fiscal_key": fk,
            "quarter_no": int(qno),
            "quarter_label": label,
            "announcement_date": ad_iso,
            "sales": fv("売上高"),
            "operating_profit": fv("営業益"),
            "ordinary_profit": fv("経常益"),
            "net_profit": fv("最終益"),
            "eps": fv("修正1株益"),
            "source": "kabutan_3m_actual",
            "updated_at": ts,
        })
    return rows


def upsert_quarterly_actual_history(conn: sqlite3.Connection, results: list[dict]) -> int:
    """process_single_code の戻り値に含めた四半期履歴を一括UPSERT。"""
    ensure_quarterly_actual_history_schema(conn)
    vals = []
    for r in results or []:
        for q in (r.get("quarterly_history_rows") or []):
            vals.append((
                q.get("コード"), q.get("fiscal_key"), q.get("quarter_no"),
                q.get("quarter_label"), q.get("announcement_date"),
                q.get("sales"), q.get("operating_profit"), q.get("ordinary_profit"),
                q.get("net_profit"), q.get("eps"), q.get("source"), q.get("updated_at"),
            ))
    if not vals:
        return 0
    conn.executemany("""
        INSERT INTO quarterly_actual_history(
          コード,fiscal_key,quarter_no,quarter_label,announcement_date,
          sales,operating_profit,ordinary_profit,net_profit,eps,source,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(コード,fiscal_key,quarter_no) DO UPDATE SET
          quarter_label=excluded.quarter_label,
          announcement_date=COALESCE(excluded.announcement_date, quarterly_actual_history.announcement_date),
          sales=COALESCE(excluded.sales, quarterly_actual_history.sales),
          operating_profit=COALESCE(excluded.operating_profit, quarterly_actual_history.operating_profit),
          ordinary_profit=COALESCE(excluded.ordinary_profit, quarterly_actual_history.ordinary_profit),
          net_profit=COALESCE(excluded.net_profit, quarterly_actual_history.net_profit),
          eps=COALESCE(excluded.eps, quarterly_actual_history.eps),
          source=excluded.source,
          updated_at=excluded.updated_at;
    """, vals)
    conn.commit()
    return len(vals)

def build_past_earnings_dates(quarterly_df: pd.DataFrame, limit: int = 8) -> str | None:
    """
    3ヵ月決算【実績】の発表日から、過去決算日のJSON配列を作る。

    - HTML側の「過去8回」に合わせ、直近 limit 回を保存。
    - 取得失敗・発表日欠損時は None を返す。
      DB更新側は None/空配列を既存値の上書きに使わない。
    """
    if quarterly_df is None or quarterly_df.empty or "発表日" not in quarterly_df.columns:
        return None
    try:
        ser = pd.to_datetime(quarterly_df["発表日"], errors="coerce").dropna()
        if ser.empty:
            return None
        dates = sorted({pd.Timestamp(v).date().isoformat() for v in ser})
        if limit and limit > 0:
            dates = dates[-int(limit):]
        return json.dumps(dates, ensure_ascii=False) if dates else None
    except Exception as e:
        print(f"[REACTION][WARN] past_earnings_dates 生成失敗: {e}")
        return None

# ===== HTML出力 =====
def export_html(df: pd.DataFrame, code: str, out_html: str, qp_result=None, verdict_str: str = ""):
    x_actual_period = df["決算期"].tolist() if not df.empty and "決算期" in df.columns else []
    x_custom_labels = [f"{period}" + ("\n(通期予想)" if "予" in period else "\n(実績)") for period in x_actual_period]
    x_full_plot = x_custom_labels

    if df.empty:
        fig = go.Figure().update_layout(title=f"通期実績データなし（code={code}）")
        body = fig.to_html(include_plotlyjs="cdn", full_html=False)
    else:
        df_growth = df.copy()
        use_cols = [c for c in ["売上高", "営業益", "修正1株益"] if c in df_growth.columns]
        if use_cols: df_growth = df_growth.set_index("決算期")[use_cols].pct_change(fill_method=None)*100
        else: df_growth = pd.DataFrame(index=df_growth["決算期"])
        qp = qp_result

        title_suffix = ""
        if qp:
            latest_label, progress, status, _ = qp
            if status == "OK": title_suffix = f" / {latest_label}：進捗{progress}%"
            elif status == "予想未発表": title_suffix = " / 進捗率 (予想未発表)"
            elif status == "予想ゼロ": title_suffix = " / 進捗率 (予想ゼロ)"
            elif status == "予想データ欠損": title_suffix = " / 進捗率 (予想データ欠損)"
            elif status == "会計期不一致": title_suffix = " / 進捗率 (会計期不一致)"

        fig = make_subplots(rows=2, cols=1, shared_xaxes=False, specs=[[{"secondary_y": True}], [{"secondary_y": False}]], vertical_spacing=0.12, subplot_titles=(f"通期実績推移（株探データ, code={code}）{title_suffix}", "成長率推移"))

        if "売上高" in df.columns and df["売上高"].notna().any():
            sales_full = (df["売上高"]/1e2).round(1).tolist()
            colors = ['rgba(173, 216, 230, 0.8)'] * (len(sales_full)-1)
            if len(sales_full) > 0: colors.append('rgba(173, 216, 230, 0.4)' if pd.notna(sales_full[-1]) else 'rgba(173, 216, 230, 0.2)')
            fig.add_trace(go.Bar(x=x_full_plot, y=sales_full, name="売上高（億円）", marker_color=colors, width=0.4, customdata=df["決算期"].tolist(), hovertemplate='<b>%{customdata}</b><br>売上高: %{y:.1f}億円<extra></extra>',), row=1, col=1, secondary_y=False)

        if "営業益" in df.columns and df["営業益"].notna().any():
            op_full = (df["営業益"]/1e2).round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=op_full[:-1], mode="lines+markers", name="営業益（実績）", marker_symbol="circle", customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>営業益: %{y:.1f}億円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=False)
            if len(op_full) >= 2 and pd.notna(op_full[-1]):
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[op_full[-2], op_full[-1]], mode="lines", name="営業益（予）接続", line=dict(dash='dot'), showlegend=False), row=1, col=1, secondary_y=False)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[op_full[-1]], mode="markers", name="営業益（予）", marker=dict(symbol="circle-open"), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>営業益(予): %{y:.1f}億円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=False)

        if "修正1株益" in df.columns and df["修正1株益"].notna().any():
            eps_full = df["修正1株益"].round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=eps_full[:-1], mode="lines+markers", name="EPS（円）", marker_symbol="diamond", customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>EPS: %{y:.1f}円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=True)
            if len(eps_full) >= 2 and pd.notna(eps_full[-1]):
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[eps_full[-2], eps_full[-1]], mode="lines", name="EPS（予）接続", line=dict(dash='dot'), showlegend=False), row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[eps_full[-1]], mode="markers", name="EPS（予）", marker=dict(symbol="diamond-open"), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>EPS(予): %{y:.1f}円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=True)

        if "修正1株配" in df.columns and df["修正1株配"].notna().any():
            div_full = df["修正1株配"].round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=div_full[:-1], mode="lines+markers", name="配当（円）", marker_symbol="x", customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>配当: %{y:.1f}円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=True)
            if len(div_full) >= 2 and pd.notna(div_full[-1]):
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[div_full[-2], div_full[-1]], mode="lines", name="配当（予）接続", line=dict(dash='dot'), showlegend=False), row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[div_full[-1]], mode="markers", name="配当（予）", marker=dict(symbol="x-open"), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>配当(予): %{y:.1f}円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=True)

        try: max_sales_op = df[["売上高", "営業益"]].apply(lambda s: s/1e2).stack().max()
        except Exception: max_sales_op = None
        max_sales_op_range = (max_sales_op * 1.1) if (max_sales_op and max_sales_op > 0.5) else None
        try: max_eps_div = df[["修正1株益", "修正1株配"]].stack().max()
        except Exception: max_eps_div = None
        max_eps_div_range = (max_eps_div * 1.1) if (max_eps_div and max_eps_div > 0.5) else None

        fig.update_xaxes(title_text="決算期", tickangle=0, type='category', ticktext=x_full_plot, tickvals=x_full_plot, row=1, col=1)
        fig.update_yaxes(title_text="金額（億円）", range=[None, max_sales_op_range], row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="EPS / 配当（円）", range=[None, max_eps_div_range], row=1, col=1, secondary_y=True)

        x_full_growth_plot = x_full_plot[1:]
        if not df_growth.empty:
            if "売上高" in df_growth.columns and df_growth["売上高"].notna().any():
                sales_growth_full = df_growth["売上高"].tolist()
                fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=sales_growth_full[:-1], mode="lines+markers", name="売上高成長率（%）", hovertemplate='売上高成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
                if len(sales_growth_full) >= 2 and pd.notna(sales_growth_full[-1]):
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[sales_growth_full[-2], sales_growth_full[-1]], mode="lines", name="売上高（予）接続（成長率）", line=dict(dash='dot'), hovertemplate='売上高(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[sales_growth_full[-1]], mode="markers", name="売上高成長率（予）", marker_symbol="square-open", customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>売上高成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True), row=2, col=1)

            if "営業益" in df_growth.columns and df_growth["営業益"].notna().any():
                op_growth_full = df_growth["営業益"].tolist()
                fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=op_growth_full[:-1], mode="lines+markers", name="営業益成長率（%）", hovertemplate='営業益成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
                if len(op_growth_full) >= 2 and pd.notna(op_growth_full[-1]):
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[op_growth_full[-2], op_growth_full[-1]], mode="lines", name="営業益（予）接続（成長率）", line=dict(dash='dot'), hovertemplate='営業益(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[op_growth_full[-1]], mode="markers", name="営業益成長率（予）", marker_symbol="circle-open", customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>営業益成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True), row=2, col=1)

            if "修正1株益" in df_growth.columns and df_growth["修正1株益"].notna().any():
                eps_growth_full = df_growth["修正1株益"].tolist()
                fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=eps_growth_full[:-1], mode="lines+markers", name="EPS成長率（%）", hovertemplate='EPS成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
                if len(eps_growth_full) >= 2 and pd.notna(eps_growth_full[-1]):
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[eps_growth_full[-2], eps_growth_full[-1]], mode="lines", name="EPS（予）接続（成長率）", line=dict(dash='dot'), hovertemplate='EPS(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                    fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[eps_growth_full[-1]], mode="markers", name="EPS成長率（予）", marker_symbol="diamond-open", customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>EPS成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True), row=2, col=1)

        fig.update_xaxes(title_text="決算期", tickangle=0, type='category', ticktext=x_full_growth_plot, tickvals=x_full_growth_plot, row=2, col=1)
        fig.update_yaxes(title_text="成長率（%）", row=2, col=1)
        fig.update_layout(height=900, legend=dict(orientation="v"), margin=dict(l=60, r=40, t=60, b=100))
        body = fig.to_html(include_plotlyjs="cdn", full_html=False)

    verdict_html_content = (verdict_str or "").replace('\n', '<br>')
    verdict_html = f'<div id="verdict_placeholder" style="margin-top:20px; color:#c9302c; font-weight:600; text-align:left; white-space:pre-wrap; border:1px solid #ccc; padding:10px; margin-left:10%; margin-right:10%; background-color:#f9f9f9;">{verdict_html_content}</div>'
    html = f"""<!doctype html><html lang="ja"><meta charset="utf-8"><title>Fundamentals {code}</title><style>body{{font-family:system-ui,-apple-system,'Noto Sans JP',sans-serif;margin:16px;}}</style>{body}{verdict_html}</html>"""
    with open(out_html, "w", encoding="utf-8") as f: f.write(html)

# =============================================================================
# ★ 適正株価計算エンジン（ローカルDB参照・保守的モデル）
# =============================================================================

class FairValueEngine:
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        price = data.get("current_price")
        # 予想EPSを優先、なければ実績EPS
        raw_eps = data.get("forecast_eps")
        if raw_eps is None:
            raw_eps = data.get("actual_eps")
        bps = data.get("bps")

        # 成長率の取得（通期の営業益成長率）
        f_op = data.get("forecast_op")
        a_op = data.get("actual_operating_profit")
        p_op = data.get("prev_operating_profit")
        
        op_growth = 0.0
        if f_op is not None and a_op is not None and a_op > 0:
            op_growth = (f_op / a_op) - 1.0
        elif a_op is not None and p_op is not None and p_op > 0:
            op_growth = (a_op / p_op) - 1.0

        # 直近四半期の営業益YoY
        recent_q_op_yoy = data.get("op_yoy")

        # ① 基準PER
        base_per = 12.0
        
        # ② 継続成長によるPER補正
        growth_adj = 0.0
        if op_growth > 0:
            growth_adj = min(op_growth * 10.0, 4.0)  # 最大でもPER+4倍（16倍）まで
        elif op_growth < 0:
            growth_adj = max(op_growth * 10.0, -3.0) # 減益ならPERを下げる
            
        fair_per = base_per + growth_adj

        # ③ 正常化EPSの作成
        sustainability_coef = 1.0
        if recent_q_op_yoy is not None and recent_q_op_yoy < 0:
            sustainability_coef = 0.88 # 減益傾向が見られる場合は割り引く
        elif op_growth > 0.5:
            sustainability_coef = 0.90 # 異常な急回復時も少し割り引いて暴走を防ぐ

        normalized_eps = raw_eps * sustainability_coef if raw_eps is not None else None

        # ④ PERモデルとPBRモデルの算出
        per_fair_value = None
        if normalized_eps is not None and normalized_eps > 0:
            per_fair_value = normalized_eps * fair_per

        pbr_fair_value = None
        if bps is not None and bps > 0:
            fair_pbr = 1.2 # PBRの保守的な基準値
            pbr_fair_value = bps * fair_pbr

        # ⑤ ブレンド (PER 75% : PBR 25%)
        final_fair_value = None
        if per_fair_value is not None and pbr_fair_value is not None:
            final_fair_value = (per_fair_value * 0.75) + (pbr_fair_value * 0.25)
        elif per_fair_value is not None:
            final_fair_value = per_fair_value
        elif pbr_fair_value is not None:
            final_fair_value = pbr_fair_value

        # ⑥ 3つの価格帯の出力
        conservative = final_fair_value * 0.85 if final_fair_value else None
        standard = final_fair_value
        aggressive = final_fair_value * 1.15 if final_fair_value else None

        upside = (final_fair_value / price - 1.0) if (price and final_fair_value and price > 0) else None

        if upside is None: valuation = "判定不能"
        elif upside >= 0.30: valuation = "大幅割安"
        elif upside >= 0.15: valuation = "割安"
        elif upside >= -0.10: valuation = "適正"
        elif upside >= -0.25: valuation = "やや割高"
        else: valuation = "割高"

        return {
            "fair_per": fair_per,
            "fair_value": standard,
            "conservative_fair_value": conservative,
            "aggressive_fair_value": aggressive,
            "upside": upside,
            "valuation": valuation
        }

class LocalDatabaseEvaluator:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def evaluate_all(self):
        print("[Engine] 適正株価の一括計算を開始します...")
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fair_value (
            code TEXT PRIMARY KEY,
            updated_at TEXT,
            current_price REAL,
            forecast_eps REAL,
            bps REAL,
            fair_value REAL,
            conservative_fair_value REAL,
            aggressive_fair_value REAL,
            upside REAL,
            valuation TEXT
        );
        """)
        
        # マイグレーション
        for col, coltype in [("適正株価", "REAL"), ("割安度", "REAL"), ("期待株価", "REAL")]:
            try: cur.execute(f"ALTER TABLE screener ADD COLUMN {col} {coltype};")
            except sqlite3.OperationalError: pass
            
        for col, coltype in [("conservative_fair_value", "REAL"), ("aggressive_fair_value", "REAL")]:
            try: cur.execute(f"ALTER TABLE fair_value ADD COLUMN {col} {coltype};")
            except sqlite3.OperationalError: pass
            
        conn.commit()

        # ★修正: f.* をやめて、必要なカラムだけを個別にSELECTし、同名カラムの重複を排除
        query = """
            SELECT 
                s.コード, 
                s.現在値 AS screener_price, 
                s.直近営業益YoY AS op_yoy, 
                f.forecast_eps,
                f.actual_eps,
                f.prev_eps,
                f.forecast_op,
                f.actual_operating_profit,
                f.prev_operating_profit,
                f.bps
            FROM screener s
            LEFT JOIN finance_notes f ON s.コード = f.コード
        """
        try:
            df = pd.read_sql_query(query, conn)
        except Exception as e:
            print(f"[Engine] DBからのデータ読み込みに失敗しました: {e}")
            conn.close()
            return

        engine = FairValueEngine()
        updates_screener = []
        updates_fair_value = []
        ts = datetime.now().isoformat(timespec="seconds")

        for _, row in df.iterrows():
            code = str(row["コード"]).strip().zfill(4)
            data = {}
            data["current_price"] = safe_float(row["screener_price"])
            data["forecast_eps"] = safe_float(row["forecast_eps"])
            data["actual_eps"] = safe_float(row["actual_eps"])
            data["prev_eps"] = safe_float(row["prev_eps"])
            data["bps"] = safe_float(row["bps"])
            data["forecast_op"] = safe_float(row["forecast_op"])
            data["actual_operating_profit"] = safe_float(row["actual_operating_profit"])
            data["prev_operating_profit"] = safe_float(row["prev_operating_profit"])
            data["op_yoy"] = safe_float(row["op_yoy"])

            res = engine.calculate(data)

            std_val = res.get("fair_value")
            cons_val = res.get("conservative_fair_value")
            agg_val = res.get("aggressive_fair_value")
            upside = res.get("upside")
            val_text = res.get("valuation")
            
            discount_rate = (upside * 100.0) if upside is not None else None

            # 期待株価列に強気価格(aggressive)を保存、適正株価列に標準価格を保存
            updates_screener.append((std_val, discount_rate, agg_val, code))
            updates_fair_value.append((
                code, ts, data["current_price"], data.get("forecast_eps") or data.get("actual_eps"),
                data["bps"], std_val, cons_val, agg_val, upside, val_text
            ))

        if updates_screener:
            cur.executemany("UPDATE screener SET 適正株価 = ?, 割安度 = ?, 期待株価 = ? WHERE コード = ?", updates_screener)
            cur.executemany("""
            INSERT OR REPLACE INTO fair_value (
                code, updated_at, current_price, forecast_eps, bps, 
                fair_value, conservative_fair_value, aggressive_fair_value, upside, valuation
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, updates_fair_value)
            conn.commit()

        conn.close()
        print("[Engine] 適正株価の計算・反映が完了しました。")

# =============================================================================

# ===== ワーカー =====
async def process_single_code(code: str, out_dir: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> dict:
    code_full = str(code).strip()
    if not code_full or not re.match(r'^[0-9A-Za-z]{4,}$', code_full):
        print(f"[ERROR] {code_full}: 銘柄コード形式不正。スキップ。")
        return {'status': 'ERROR', 'code': code_full}
    
    market = _get_market_from_db(code_full)
    if _is_skip_market(market):
        return {'status': 'SKIP', 'code': code_full}

    opt = _ARGS
    use_db_skip = ENABLE_DB_SKIP and (not bool(getattr(opt, 'no_db_skip', False)))
    skip_days = getattr(opt, 'skip_recent_days', None) or SKIP_RECENT_DAYS_DEFAULT
    force_refresh = bool(getattr(opt, 'force_refresh', False))

    if use_db_skip and not force_refresh:
        cached_updated_at, cached_status = db_get_cached_announce(code_full)
        if cached_updated_at is not None and cached_status not in BAD_LAST_STATUSES:
            delta_days = (datetime.now().date() - cached_updated_at.date()).days
            if delta_days < skip_days:
                return {'status': 'SKIP', 'code': code_full}

    async with semaphore:
        try:
            (df_full_year, extra_data), df_quarterly = await asyncio.gather(
                fetch_full_year_financials(code_full, session),
                fetch_quarterly_financials(code_full, session)
            )

            latest_ad = get_latest_announce_date(df_quarterly) or await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "OK" if not df_quarterly.empty else "ANNOUNCE_ONLY")
            else:
                db_upsert_cached_announce(code_full, None, "EMPTY_QUARTERLY")

            df_forecast_row = pd.Series()
            if not df_full_year.empty and "決算期" in df_full_year.columns:
                _m = df_full_year["決算期"].astype(str).str.contains("予|予想", na=False)
                if _m.any():
                    df_forecast_row = df_full_year[_m].iloc[-1]
                else:
                    latest_fy_str = str(df_full_year["決算期"].iloc[-1]).replace(".", "")
                    if latest_fy_str[:6].isdigit() and latest_fy_str[:6] > datetime.now().strftime("%Y%m"):
                        df_forecast_row = df_full_year.iloc[-1]

            verdict, score = judge_and_score_performance(df_full_year)
            overall_alpha = _get_overall_alpha(score)
            qp = calc_progress_from_df_op(df_quarterly, df_forecast_row)
            progress_percent_db = qp[3] if qp is not None else None

            # 進捗ログ出力
            _log_on = bool(getattr(opt, 'log_progress', False))
            _log_file = getattr(opt, 'log_file', 'progress_debug.csv')
            details = compute_progress_details(df_quarterly, df_forecast_row)
            if _log_on or (progress_percent_db == -1.0):
                log_progress_details(code_full, details, (_log_file if _log_on else None))

            out_html = os.path.join(out_dir, f"finance_{code_full}.html")
            formatted_v = _format_verdict_with_progress(qp, verdict, score)
            export_html(df_full_year, code_full, out_html, qp, formatted_v)
            print(f"[OK] {code_full} done. Score:{score}, Alpha:{overall_alpha}", flush=True)

            def _safe_float_from_df(val):
                if pd.isna(val): return None
                try: return float(str(val).replace(",", ""))
                except: return None

            forecast_op = _safe_float_from_df(df_forecast_row.get("営業益")) if not df_forecast_row.empty else None
            forecast_eps = _safe_float_from_df(df_forecast_row.get("修正1株益")) if not df_forecast_row.empty else None
            forecast_revenue = _safe_float_from_df(df_forecast_row.get("売上高")) if not df_forecast_row.empty else None
            forecast_net_profit = _safe_float_from_df(df_forecast_row.get("最終益")) if not df_forecast_row.empty else None

            actual_eps = prev_eps = actual_revenue = prev_revenue = actual_op = prev_op = actual_net = prev_net = None
            if not df_full_year.empty:
                _m = df_full_year["決算期"].astype(str).str.contains("予|予想", na=False)
                df_actual = df_full_year[~_m].copy()
                if not df_forecast_row.empty and not _m.any():
                    df_actual = df_actual.iloc[:-1] 
                
                if len(df_actual) >= 1:
                    latest = df_actual.iloc[-1]
                    actual_eps, actual_revenue, actual_op, actual_net = _safe_float_from_df(latest.get("修正1株益")), _safe_float_from_df(latest.get("売上高")), _safe_float_from_df(latest.get("営業益")), _safe_float_from_df(latest.get("最終益"))
                
                if len(df_actual) >= 2:
                    prev = df_actual.iloc[-2]
                    prev_eps, prev_revenue, prev_op, prev_net = _safe_float_from_df(prev.get("修正1株益")), _safe_float_from_df(prev.get("売上高")), _safe_float_from_df(prev.get("営業益")), _safe_float_from_df(prev.get("最終益"))

            # 過去決算反応の元データ。四半期実績の発表日から直近8回を再構成する。
            past_earnings_dates = build_past_earnings_dates(df_quarterly, limit=8)

            # v14: 取得済みの3ヵ月実績を捨てず、同Q比較用の履歴として保存する。
            quarterly_history_rows = build_quarterly_actual_history_rows(
                code_full, df_quarterly, df_forecast_row
            )

            sales_yoy, op_yoy, accel_flag = None, None, 0
            if df_quarterly is not None and len(df_quarterly) >= 5:
                try:
                    latest, prev_1, prev_4 = df_quarterly.iloc[-1], df_quarterly.iloc[-2], df_quarterly.iloc[-5]
                    prev_5 = df_quarterly.iloc[-6] if len(df_quarterly) >= 6 else None 
                    
                    if pd.notna(latest.get('売上高')) and pd.notna(prev_4.get('売上高')) and prev_4['売上高'] != 0:
                        sales_yoy = (latest['売上高'] - prev_4['売上高']) / abs(prev_4['売上高']) * 100.0
                        
                    if pd.notna(latest.get('営業益')) and pd.notna(prev_4.get('営業益')) and prev_4['営業益'] != 0:
                        op_yoy = (latest['営業益'] - prev_4['営業益']) / abs(prev_4['営業益']) * 100.0
                        
                    op_yoy_prev = None
                    if prev_5 is not None and pd.notna(prev_1.get('営業益')) and pd.notna(prev_5.get('営業益')) and prev_5['営業益'] != 0:
                        op_yoy_prev = (prev_1['営業益'] - prev_5['営業益']) / abs(prev_5['営業益']) * 100.0
                        
                    if op_yoy is not None and op_yoy_prev is not None and op_yoy > 0 and op_yoy > op_yoy_prev:
                        accel_flag = 1
                except Exception: pass

            return {
                'status': 'OK', 'code': code_full, 'score': score, 'overall_alpha': overall_alpha,
                'progress_percent': progress_percent_db, 'formatted_verdict': formatted_v, 'out_html': out_html,
                'bps': extra_data.get('bps'), 'equity': extra_data.get('equity'),
                'pts_price': extra_data.get('pts_price'), 'pts_time': extra_data.get('pts_time'),
                'forecast_op': forecast_op, 'forecast_eps': forecast_eps,
                'forecast_revenue': forecast_revenue, 'forecast_net_profit': forecast_net_profit,
                'past_earnings_dates': past_earnings_dates,
                'actual_eps': actual_eps, 'prev_eps': prev_eps,
                'actual_revenue': actual_revenue, 'prev_revenue': prev_revenue,
                'actual_operating_profit': actual_op, 'prev_operating_profit': prev_op,
                'actual_net_profit': actual_net, 'prev_net_profit': prev_net,
                'sales_yoy': sales_yoy, 'op_yoy': op_yoy, 'accel_flag': accel_flag,
                'quarterly_history_rows': quarterly_history_rows
            }
        except Exception as e:
            print(f"[ERROR] {code_full}: {e}")
            return {'status': 'ERROR', 'code': code_full}

# ===== メイン =====
async def main_async(target_code: str | None = None):
    out_dir = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data\graph"
    os.makedirs(out_dir, exist_ok=True)

    if target_code:
        codes = {target_code}
    else:
        try:
            master = pd.read_csv(MASTER_CODES_PATH, encoding="utf8", sep=",", engine="python", dtype={'コード': str})
            codes = set(master["コード"].astype(str))
        except FileNotFoundError:
            return

    concurrency = getattr(_ARGS, 'workers', ASYNC_CONCURRENCY_LIMIT) or ASYNC_CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(headers=HEADERS, trust_env=True) as session:
        tasks = [process_single_code(code, out_dir, session, semaphore) for code in codes]
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_results = [r for r in all_results if isinstance(r, dict) and r.get('status') == 'OK']

    if successful_results:
        reaction_ready = sum(1 for r in successful_results if r.get("past_earnings_dates") not in (None, "", "[]"))
        print(f"[REACTION] past_earnings_dates 再構成: {reaction_ready}/{len(successful_results)}銘柄")

    if successful_results and not getattr(_ARGS, 'no_db', False):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            cur = conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS finance_notes (
              コード TEXT PRIMARY KEY, 財務コメント TEXT, score INTEGER, progress_percent REAL, html_path TEXT, updated_at TEXT
            );
            """)
            for col, coltype in [
                ("overall_alpha", "TEXT"), ("forecast_op", "REAL"), ("forecast_eps", "REAL"), ("past_earnings_dates", "TEXT"),
                ("bps", "REAL"), ("equity_ratio", "REAL"), ("assets", "REAL"), ("equity", "REAL"), ("interest_debt_ratio", "REAL"),
                ("actual_eps", "REAL"), ("prev_eps", "REAL"), ("forecast_revenue", "REAL"), ("actual_revenue", "REAL"),
                ("prev_revenue", "REAL"), ("actual_operating_profit", "REAL"), ("prev_operating_profit", "REAL"),
                ("forecast_net_profit", "REAL"), ("actual_net_profit", "REAL"), ("prev_net_profit", "REAL")
            ]:
                try: cur.execute(f"SELECT {col} FROM finance_notes LIMIT 1;")
                except sqlite3.OperationalError: cur.execute(f"ALTER TABLE finance_notes ADD COLUMN {col} {coltype};")
            conn.commit()

            # PTS株価/時刻は screener に永続保存。既存DBは自動マイグレーションする。
            for col, coltype in [("PTS株価", "REAL"), ("PTS時刻", "TEXT")]:
                try:
                    cur.execute(f"ALTER TABLE screener ADD COLUMN {col} {coltype};")
                except sqlite3.OperationalError:
                    pass
            conn.commit()

            ts = datetime.now().isoformat(timespec="seconds")
            data_to_insert = [(
                r['code'], r.get('formatted_verdict') or "", r.get('score'), r.get('progress_percent'), r.get('out_html') or "", ts,
                r.get('overall_alpha'), r.get('forecast_op'), r.get('forecast_eps'), r.get('past_earnings_dates'),
                r.get('bps'), r.get('equity_ratio'), r.get('assets'), r.get('equity'), r.get('interest_debt_ratio'),
                r.get('actual_eps'), r.get('prev_eps'), r.get('forecast_revenue'), r.get('actual_revenue'), r.get('prev_revenue'),
                r.get('actual_operating_profit'), r.get('prev_operating_profit'), r.get('forecast_net_profit'), r.get('actual_net_profit'), r.get('prev_net_profit')
            ) for r in successful_results]

            if data_to_insert:
                cur.executemany("""
                INSERT INTO finance_notes (
                  コード, 財務コメント, score, progress_percent, html_path, updated_at, 
                  overall_alpha, forecast_op, forecast_eps, past_earnings_dates,
                  bps, equity_ratio, assets, equity, interest_debt_ratio,
                  actual_eps, prev_eps, forecast_revenue, actual_revenue, prev_revenue,
                  actual_operating_profit, prev_operating_profit, forecast_net_profit, actual_net_profit, prev_net_profit
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(コード) DO UPDATE SET
                  財務コメント = excluded.財務コメント, score = excluded.score, progress_percent = excluded.progress_percent,
                  html_path = excluded.html_path, updated_at = excluded.updated_at, overall_alpha = excluded.overall_alpha,
                  forecast_op = excluded.forecast_op, forecast_eps = excluded.forecast_eps,
                  past_earnings_dates = CASE
                    WHEN excluded.past_earnings_dates IS NOT NULL
                     AND TRIM(excluded.past_earnings_dates) NOT IN ('', '[]')
                    THEN excluded.past_earnings_dates
                    ELSE finance_notes.past_earnings_dates
                  END,
                  bps = excluded.bps, equity_ratio = excluded.equity_ratio, assets = excluded.assets, equity = excluded.equity,
                  interest_debt_ratio = excluded.interest_debt_ratio, actual_eps = excluded.actual_eps, prev_eps = excluded.prev_eps,
                  forecast_revenue = excluded.forecast_revenue, actual_revenue = excluded.actual_revenue, prev_revenue = excluded.prev_revenue,
                  actual_operating_profit = excluded.actual_operating_profit, prev_operating_profit = excluded.prev_operating_profit,
                  forecast_net_profit = excluded.forecast_net_profit, actual_net_profit = excluded.actual_net_profit, prev_net_profit = excluded.prev_net_profit;
                """, data_to_insert)
                conn.commit()

                # v14: 株探の四半期実績を履歴DBへ。シンデンB型（期中実績先行）の元データ。
                q_saved = upsert_quarterly_actual_history(conn, successful_results)
                if q_saved:
                    print(f"[QUARTERLY_HISTORY] {q_saved}行 UPSERT")

                for col, coltype in [("直近売上YoY", "REAL"), ("直近営業益YoY", "REAL"), ("利益加速フラグ", "INTEGER")]:
                    try: cur.execute(f"ALTER TABLE screener ADD COLUMN {col} {coltype};")
                    except sqlite3.OperationalError: pass
                
                screener_updates = [(r.get('sales_yoy'), r.get('op_yoy'), r.get('accel_flag'), r['code']) 
                                    for r in successful_results if 'accel_flag' in r]
                if screener_updates:
                    cur.executemany("UPDATE screener SET 直近売上YoY=?, 直近営業益YoY=?, 利益加速フラグ=? WHERE コード=?", screener_updates)
                    conn.commit()

                # 成功取得した銘柄はPTSも更新。株探にPTS表示が無い場合はNULLにして
                # 前回値を残さない（古いPTSを今日の値と誤認するのを防ぐ）。
                pts_updates = [
                    (r.get('pts_price'), r.get('pts_time'), r['code'])
                    for r in successful_results
                ]
                if pts_updates:
                    cur.executemany(
                        "UPDATE screener SET PTS株価=?, PTS時刻=? WHERE コード=?",
                        pts_updates
                    )
                    conn.commit()
                    pts_count = sum(1 for p, _, _ in pts_updates if p is not None)
                    print(f"[PTS] screener 更新={len(pts_updates)}銘柄 / PTS表示あり={pts_count}銘柄")
        finally:
            try: conn.close()
            except Exception: pass

    # 適正株価の計算・反映
    if not getattr(_ARGS, 'no_db', False):
        evaluator = LocalDatabaseEvaluator(DB_PATH)
        evaluator.evaluate_all()

def main(target_code: str | None = None):
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_async(target_code=target_code))
    except Exception as e:
        print(f"[CRITICAL ERROR] メイン処理中に予期せぬエラー: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='株探から財務データを取得し、グラフを生成します。（非同期・高速化対応）')
    parser.add_argument('code', nargs='?', default=None, help='処理する銘柄コード (省略時はマスターファイル内の全コード)')
    parser.add_argument('--workers', type=int, default=ASYNC_CONCURRENCY_LIMIT, help='同時並列数（デフォルト: 7）')
    parser.add_argument('--log-progress', action='store_true', help='進捗の式と使った値を標準出力に出す')
    parser.add_argument('--log-file', type=str, default='progress_debug.csv', help='進捗ログのCSV出力先')
    parser.add_argument('--skip-recent-days', type=int, default=None, help='最新の決算発表日からこの日数以内なら処理をスキップ')
    parser.add_argument('--force-refresh', action='store_true', help='DBキャッシュを無視して必ず取得する')
    parser.add_argument('--no-db-skip', action='store_true', help='DBスキップ機能を無効化')
    parser.add_argument('--no-db', action='store_true', help='DB書き込みを無効化')

    args, _unknown = parser.parse_known_args()
    _GLOBALS = globals()
    _GLOBALS['_ARGS'] = args

    try:
        main(target_code=args.code)
    except Exception:
        sys.exit(1)