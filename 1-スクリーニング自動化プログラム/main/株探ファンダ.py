# -*- coding: utf-8 -*-
# 株探ファンダ_v12_retry_dbskip_default__FULL_FIXED_v6.py
# - リトライ（指数バックオフ＋ジッター）
# - 並列数可変 (--workers)
# - 進捗率ログ (--log-progress, --log-file)
# - 最新発表日でのスキップ：DBキャッシュ基準（HTTP前）【デフォルト有効】
#   * ENABLE_DB_SKIP = True
#   * SKIP_RECENT_DAYS_DEFAULT = 84 (=28日×3) にデフォルト設定
# - 直近が異常終了ならスキップ無効（必ず再取得）
# - DB書き込みのON/OFF (--no-db)
#
# 追加：
# - スコア→アルファ（S++/A+/B/C/D-）を overall_alpha として finance_notes に保存
# - 四半期テーブルが取れなくても「発表日だけ取得できたら」earnings_cache を ANNOUNCE_ONLY で更新
# - 例外時も可能なら発表日だけ拾って ANNOUNCE_ONLY でキャッシュ更新（次回スキップが効く）
# - 【厳格対策】未定義エラー防止：日本語名の残骸参照を無害化（globals にダミー定義）

import re
import argparse
import sys
import os
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from io import StringIO
import random

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
ASYNC_CONCURRENCY_LIMIT = 15
MASTER_CODES_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt"
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

# リトライ設定
RETRY_HTTP_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 4
BACKOFF_BASE_SEC = 1.5   # 1.0,1.5,2.25,3.38... + ジッター
JITTER_SEC = 0.3

# DBスキップ：デフォルト有効（無効化は --no-db-skip）
ENABLE_DB_SKIP = True
SKIP_RECENT_DAYS_DEFAULT = 84

# DBスキップ判定用ステータス分類
BAD_LAST_STATUSES = {
    "ERROR_429", "ERROR_500", "ERROR_502", "ERROR_503", "ERROR_504",
    "TIMEOUT", "CONN_ERROR", "HTTP_ERROR", "PARSE_ERROR", "UNKNOWN_ERROR"
}
GOOD_LAST_STATUSES = {"OK", "FETCHED", "EMPTY_QUARTERLY", "ANNOUNCE_ONLY"}

# グローバル引数
_ARGS = None

# 市場によるスキップ判定に使うキーワード
SKIP_MARKET_KEYWORDS = ("ETF", "指数")

# ===== ヘルパー =====

def _get_market_from_db(code: str) -> str | None:
    """screener から市場を取得（なければ None）。"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 市場 FROM screener WHERE コード = ? LIMIT 1;", (str(code),))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _is_skip_market(market: str | None) -> bool:
    """市場がETF/指数ならTrue。表記ゆれ(空白等)もざっくり吸収。"""
    if not market:
        return False
    s = str(market).replace("　", "").replace(" ", "")
    return any(k in s for k in SKIP_MARKET_KEYWORDS)

async def _fetch_text_with_retry(session: aiohttp.ClientSession, url: str, *, timeout_sec: int = 15) -> str:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=timeout_sec) as res:
                if res.status in RETRY_HTTP_STATUSES:
                    _ = await res.text()  # 読み捨て
                    raise aiohttp.ClientResponseError(
                        request_info=res.request_info, history=res.history,
                        status=res.status, message=f"retryable status {res.status}",
                        headers=res.headers
                    )
                res.raise_for_status()
                return await res.text(encoding=res.get_encoding() or "utf-8")
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            last_err = e
            if isinstance(e, aiohttp.ClientResponseError) and e.status not in RETRY_HTTP_STATUSES:
                break
            if attempt < MAX_RETRIES:
                sleep_sec = (BACKOFF_BASE_SEC ** (attempt - 1)) + random.uniform(0, JITTER_SEC)
                print(f"[RETRY] {url} attempt {attempt}/{MAX_RETRIES} -> sleep {sleep_sec:.2f}s ({type(e).__name__}: {getattr(e,'status', '')})")
                await asyncio.sleep(sleep_sec)
    raise last_err if last_err else RuntimeError("Unknown retry failure")

def _fiscal_key(label: str) -> str | None:
    if not isinstance(label, str):
        return None
    m = re.search(r'(\d{4}\.\d{1,2})', label)
    return m.group(1) if m else None

def _fiscal_key_from_quarter_label(q_label: str, fiscal_end_month: int) -> str | None:
    """
    '25.04-06' / '25/04-06' などの四半期ラベル → 'YYYY.MM'（決算月）へ。
    終了月 <= 決算月 なら当年、> 決算月なら翌年をFY年とする。
    """
    if not isinstance(q_label, str) or fiscal_end_month is None:
        return None
    trans = str.maketrans({'／':'/', '－':'-', '―':'-', 'ー':'-', '．':'.'})
    s = q_label.translate(trans).replace('/', '.')
    m = re.search(r'(?P<yy>\d{2})[\.\/](?P<m1>\d{2})[-–-](?P<m2>\d{2})', s)
    if not m:
        return None
    yy = int(m.group('yy')); end_m = int(m.group('m2'))
    base_year = 2000 + yy
    fy_year = base_year if end_m <= fiscal_end_month else base_year + 1
    return f"{fy_year}.{fiscal_end_month:02d}"

def _print_html_snippet_on_error(code: str, html_content: str, func_name: str):
    start_index = html_content.find('<table')
    if start_index != -1:
        snippet = html_content[start_index:start_index + 500]
        print(f"[DEBUG_HTML] {code} in {func_name}: Table snippet: {snippet}...")
    else:
        print(f"[DEBUG_HTML] {code} in {func_name}: Table not found near start. Snippet: {html_content[:500]}...")

# ===== DB：earnings_cache（発表日キャッシュ） =====

def _ensure_cache_table():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS earnings_cache (
          コード TEXT PRIMARY KEY,
          latest_announce_date TEXT,   -- 'YYYY-MM-DD'
          last_status TEXT,            -- OK / ERROR_503 / etc
          updated_at TEXT              -- ISO
        );
        """)
        conn.commit()
    except Exception:
        pass
    finally:
        try: conn.close()
        except Exception: pass

def db_get_cached_announce(code: str) -> tuple[datetime | None, str | None]:
    _ensure_cache_table()
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT latest_announce_date, last_status FROM earnings_cache WHERE コード = ?", (code,))
        row = cur.fetchone()
        if not row or not row[0]:
            return (None, None)
        try:
            dt = datetime.fromisoformat(row[0])
        except Exception:
            dt = datetime.strptime(row[0], "%Y-%m-%d")
        return (dt, row[1])
    except Exception:
        return (None, None)
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
    except Exception as e:
        print(f"[DB][WARN] earnings_cache upsert failed: {type(e).__name__}: {e}")
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
        # 既定テーブル（古い環境でも通る最小構成）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS finance_notes (
          コード TEXT PRIMARY KEY,
          財務コメント TEXT,
          score INTEGER,
          progress_percent REAL,
          html_path TEXT,
          updated_at TEXT
        );
        """)
        # マイグレーション（不足列を追加）
        for col, coltype in [
            ("score", "INTEGER"),
            ("progress_percent", "REAL"),
            ("html_path", "TEXT"),
            ("updated_at","TEXT"),
            ("overall_alpha","TEXT"),          # ★ 追加列：アルファ評価
        ]:
            try:
                cur.execute(f"SELECT {col} FROM finance_notes LIMIT 1;")
            except sqlite3.OperationalError:
                print(f"[DB] スキーマ修正: '{col}' カラムを追加します。")
                cur.execute(f"ALTER TABLE finance_notes ADD COLUMN {col} {coltype};")
        conn.commit()

        ts = datetime.now().isoformat(timespec="seconds")
        data_to_insert = []
        for res in results:
            if res.get('status') == 'OK':
                data_to_insert.append((
                    res['code'],
                    res.get('formatted_verdict') or "",
                    res.get('score'),
                    res.get('progress_percent'),
                    res.get('out_html') or "",
                    ts,
                    res.get('overall_alpha') or None,   # ★ 追加
                ))
        if data_to_insert:
            cur.executemany("""
            INSERT INTO finance_notes (コード, 財務コメント, score, progress_percent, html_path, updated_at, overall_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(コード) DO UPDATE SET
              財務コメント    = excluded.財務コメント,
              score           = excluded.score,
              progress_percent= excluded.progress_percent,
              html_path       = excluded.html_path,
              updated_at      = excluded.updated_at,
              overall_alpha   = excluded.overall_alpha;
            """, data_to_insert)
            conn.commit()
            print(f"[DB][INFO] {len(data_to_insert)}件の結果をSQLiteに一括記録しました。")
    except sqlite3.OperationalError as e:
        print(f"[DB][CRITICAL ERROR] SQLite DB操作エラー: {e}")
    except Exception as e:
        print(f"[DB][CRITICAL ERROR] 予期せぬエラー: {type(e).__name__}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
        if target is None:
            return pd.DataFrame()
        df = pd.read_html(StringIO(str(target)), flavor="lxml", header=0)[0]
        df = df.drop(columns=[c for c in df.columns if "損益率" in c], errors='ignore')

        # 整形
        df.columns = df.columns.astype(str).str.replace(" ", "", regex=False).str.replace("　", "", regex=False)
        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年同期比|予|通期", na=False)
            df = df[mask].copy()
        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)

        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "発表日" in df.columns:
            df["発表日"] = pd.to_datetime(
                df["発表日"].astype(str).str.split(r'\(| ').str[0].str.strip(),
                format="%y/%m/%d", errors='coerce'
            )
            df.dropna(subset=["発表日"], inplace=True)

        df = df.sort_values(by="発表日", ascending=True).reset_index(drop=True)
        return df

    except Exception as e:
        _print_html_snippet_on_error(code, locals().get('html_content',""), "fetch_quarterly_financials")
        print(f"[ERROR] {code}: 3ヵ月実績データ解析中にエラー - {type(e).__name__}: {e}")
        return pd.DataFrame()

async def fetch_full_year_financials(code: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    print(f"[INFO] fetching full year actual data: {url}")
    try:
        html_content = await _fetch_text_with_retry(session, url, timeout_sec=15)
        soup = BeautifulSoup(html_content, "lxml")
        tables = soup.find_all('table')
        target = None
        for table in tables:
            header_row = table.find('tr')
            if header_row and header_row.find(string=lambda t: t and ('決算期' in t or '通期' in t)):
                target = table
                break
        if target is None:
            print(f"[WARN] 通期業績推移テーブルが見つかりません (code={code})")
            return pd.DataFrame()

        df = pd.read_html(StringIO(str(target)), flavor="lxml", header=0)[0]
        df.columns = df.columns.astype(str).str.replace(r'\s+', '', regex=True)
        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年比", na=False)
            df = df[mask].copy()
        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)
        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.drop(columns=["発表日"], errors='ignore')
        df = df.sort_values(by="決算期", ascending=True).reset_index(drop=True)
        return df

    except Exception as e:
        _print_html_snippet_on_error(code, locals().get('html_content',""), "fetch_full_year_financials")
        print(f"[ERROR] {code}: 通期実績データ解析中にエラー - {type(e).__name__}: {e}")
        return pd.DataFrame()

# ===== フォールバック：発表日だけ抽出 =====

def _parse_latest_announce_from_any_table(html_content: str) -> datetime | None:
    try:
        soup = BeautifulSoup(html_content, "lxml")
        for tbl in soup.find_all("table"):
            try:
                df = pd.read_html(StringIO(str(tbl)), flavor="lxml", header=0)[0]
            except Exception:
                continue
            cols = [str(c) for c in df.columns]
            if any("発表日" in c for c in cols):
                try:
                    ser = df[[c for c in df.columns if "発表日" in str(c)][0]].astype(str)
                    ser = pd.to_datetime(ser.str.split(r'\(| ').str[0].str.strip(), format="%y/%m/%d", errors="coerce")
                    ser = ser.dropna()
                    if not ser.empty:
                        return pd.to_datetime(ser.iloc[-1])
                except Exception:
                    continue
        return None
    except Exception:
        return None

async def fetch_latest_announce_date_only(session: aiohttp.ClientSession, code: str) -> datetime | None:
    """ページ全体を見て“発表日”だけ拾うフォールバック。"""
    url = f"https://kabutan.jp/stock/finance?code={code}"
    try:
        html_content = await _fetch_text_with_retry(session, url, timeout_sec=10)
        return _parse_latest_announce_from_any_table(html_content)
    except Exception as e:
        print(f"[FALLBACK] announce-only failed for {code}: {type(e).__name__}: {e}")
        return None

# ===== 進捗＆評価 =====
def compute_progress_details(quarterly_df: pd.DataFrame, forecast_series: pd.Series):
    details = {"latest_label": None, "key_q": None, "key_f": None,
               "fiscal_end_month": None, "cumulative_op": None, "full_op_val": None,
               "status": None, "progress": None}
    if (quarterly_df is None or quarterly_df.empty or
        "営業益" not in quarterly_df.columns or "決算期" not in quarterly_df.columns):
        details["status"] = "四半期DF欠損";  return details
    latest_row = quarterly_df.tail(1)
    if latest_row.empty:
        details["status"] = "四半期DF空";  return details
    latest_label = str(latest_row["決算期"].iloc[0]);  details["latest_label"] = latest_label
    if forecast_series is None or forecast_series.empty:
        details["status"] = "予想未発表";  return details
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
    mask_same_fy = quarterly_df["決算期"].astype(str).apply(
        lambda s: _fiscal_key_from_quarter_label(s, fiscal_end_month) == key_q
    )
    cumulative_op = pd.to_numeric(quarterly_df.loc[mask_same_fy, "営業益"], errors="coerce").sum(min_count=1)
    details["cumulative_op"] = None if pd.isna(cumulative_op) else float(cumulative_op)
    if pd.isna(cumulative_op): details["status"] = "予想データ欠損";  return details
    if key_q != key_f: details["status"] = "会計期不一致";  return details
    if full_op_val < 0 or (cumulative_op < 0 and full_op_val > 0): details["status"] = "予想データ欠損";  return details
    progress = float(cumulative_op) / float(full_op_val) * 100.0
    if progress > 3000: details["status"] = "予想データ欠損";  return details
    details["progress"] = round(progress, 1);  details["status"] = "OK";  return details

def log_progress_details(code: str, details: dict, log_path: str | None = None):
    print(
        f"[PROG] {code} label={details.get('latest_label')}  "
        f"key_q={details.get('key_q')}  key_f={details.get('key_f')}  "
        f"cum_op={details.get('cumulative_op')}  full_op={details.get('full_op_val')}  "
        f"status={details.get('status')}  progress={details.get('progress')}%"
    )
    if log_path:
        import csv
        header = ["code","latest_label","key_q","key_f","fiscal_end_month","cumulative_op","full_op_val","status","progress"]
        write_header = not os.path.isfile(log_path)
        with open(log_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(header)
            w.writerow([
                code,
                details.get("latest_label"),
                details.get("key_q"),
                details.get("key_f"),
                details.get("fiscal_end_month"),
                details.get("cumulative_op"),
                details.get("full_op_val"),
                details.get("status"),
                details.get("progress"),
            ])

def _safe_growth(latest, prev):
    if pd.isna(latest) or pd.isna(prev): return None
    if prev == 0: return 100 if latest > 0 else (0 if latest == 0 else -100)
    return ((latest - prev) / prev) * 100

def _format_verdict_with_progress(qp, raw_verdict: str, score: int) -> str:
    """
    固定テンプレ版（常に8行、同じ順序）。
      1: 【総合評価】…
      2: [決算期/進捗…] or [進捗率 (情報不足)]
      3: スコア … 点
      4: --- 詳細 ---
      5-8: 詳細本文（最大4行にトリム、足りなければ '（詳細なし）' でパディング）
    """
    overall_label = _get_overall_verdict_label(score)

    # 進捗ヘッダ
    prog_header = "[進捗率 (情報不足)]"
    if qp is not None:
        latest_label, progress, status, _ = qp
        if status == "OK":
            prog_header = f"[{latest_label}：進捗{progress}%]"
        elif status in ("予想未発表", "予想ゼロ", "予想データ欠損", "会計期不一致"):
            prog_header = f"[{latest_label}：進捗率 ({status})]"

    score_header = f"スコア {score} 点"

    # 本文を最大4行に揃える
    body_lines = [s.strip() for s in (raw_verdict or "").splitlines() if s.strip()]
    body_fixed = body_lines[:4]
    while len(body_fixed) < 4:
        body_fixed.append("（詳細なし）")

    lines = [
        f"【総合評価】{overall_label}",
        prog_header,
        score_header,
        "--- 詳細 ---",
        *body_fixed[:4]
    ]
    return "\n".join(lines[:8])

def judge_and_score_performance(df: pd.DataFrame) -> tuple[str, int]:
    if df.empty or len(df) < 2:
        return "データ不足のため判定できません。", 0
    is_forecast = "予" in str(df.iloc[-1]["決算期"])
    df_latest = df.iloc[-1]
    df_prev = df.iloc[-2]
    comparison_period = "通期予想 vs 前期実績" if is_forecast else "直近実績 vs 前期実績 (通期予想データ欠損)"
    latest_sales, latest_op, latest_eps = df_latest.get("売上高"), df_latest.get("営業益"), df_latest.get("修正1株益")
    prev_sales, prev_op, prev_eps = df_prev.get("売上高"), df_prev.get("営業益"), df_prev.get("修正1株益")
    op_growth = _safe_growth(latest_op, prev_op)
    eps_growth = _safe_growth(latest_eps, prev_eps)
    sales_growth = _safe_growth(latest_sales, prev_sales)
    msgs, score = [f"（比較期間：{comparison_period}）"], 0
    # EPS
    if eps_growth is not None:
        if eps_growth > 20: score += 3; msgs.append(f"🟢 EPS成長率: {eps_growth:.1f}% (高成長)")
        elif eps_growth > 10: score += 2; msgs.append(f"🟡 EPS成長率: {eps_growth:.1f}% (安定成長)")
        elif eps_growth > 0: score += 1; msgs.append(f"⚪️ EPS成長率: {eps_growth:.1f}% (微増)")
        elif eps_growth == 0: msgs.append("⚫️ EPS成長率: 0.0% (横ばい)")
        else: score -= 2; msgs.append(f"🔴 EPS成長率: {eps_growth:.1f}% (減益注意)")
    else:
        msgs.append("EPS成長率: データ欠損")
    # 営業益
    if op_growth is not None:
        if op_growth > 20: score += 2; msgs.append(f"🟢 営業益成長率: {op_growth:.1f}% (高成長)")
        elif op_growth > 10: score += 1; msgs.append(f"🟡 営業益成長率: {op_growth:.1f}% (安定成長)")
        elif op_growth < 0: score -= 1; msgs.append(f"🔴 営業益成長率: {op_growth:.1f}% (減益注意)")
        else: msgs.append(f"⚪️ 営業益成長率: {op_growth:.1f}%")
    else:
        msgs.append("営業益成長率: データ欠損")
    # 売上高
    if sales_growth is not None:
        if sales_growth > 10: score += 1; msgs.append(f"🟢 売上高成長率: {sales_growth:.1f}% (高成長)")
        elif sales_growth < 0: score -= 1; msgs.append(f"🔴 売上高成長率: {sales_growth:.1f}% (減収注意)")
        else: msgs.append(f"⚪️ 売上高成長率: {sales_growth:.1f}%")
    else:
        msgs.append("売上高成長率: データ欠損")
    if (op_growth is not None and op_growth > 0) and (eps_growth is not None and eps_growth > 0):
        msgs.insert(1, "💡 " + ("通期予想は増収増益の見込みです。" if is_forecast else "直近実績は増収増益でした。"))
    return "\n".join(msgs), score

def calc_progress_from_df_op(quarterly_df: pd.DataFrame, forecast_series: pd.Series):
    """
    返り値: (最新決算期ラベル, 表示用%, ステータス, DB保存値)
      - OK: 実数
      - 予想未発表: -3.0
      - 予想ゼロ:   -2.0
      - 予想データ欠損: -1.0
      - 会計期不一致: -4.0
    """
    required_cols = ["営業益", "決算期"]
    if quarterly_df is None or quarterly_df.empty or any(c not in quarterly_df.columns for c in required_cols):
        return None

    latest_row = quarterly_df.tail(1)
    if latest_row.empty:
        return None

    latest_label = str(latest_row["決算期"].iloc[0])

    # 分母（通期予想）
    if forecast_series is None or forecast_series.empty or "営業益" not in forecast_series:
        return (latest_label, 0.0, "予想未発表", -3.0)

    full_op_val = pd.to_numeric(forecast_series.get("営業益"), errors="coerce")
    if pd.isna(full_op_val):
        return (latest_label, 0.0, "予想データ欠損", -1.0)
    if float(full_op_val) == 0.0:
        return (latest_label, 0.0, "予想ゼロ", -2.0)

    key_f = _fiscal_key(str(forecast_series.get("決算期", "")))
    if not key_f:
        return (latest_label, 0.0, "予想未発表", -3.0)

    m = re.search(r"(\d{4})\.(\d{1,2})", key_f)
    fiscal_end_month = int(m.group(2)) if m else None

    # 分子（同一FYの四半期営業益を合算）
    key_q = _fiscal_key_from_quarter_label(latest_label, fiscal_end_month)
    if not key_q:
        return (latest_label, 0.0, "予想データ欠損", -1.0)

    mask_same_fy = quarterly_df["決算期"].astype(str).apply(
        lambda s: _fiscal_key_from_quarter_label(s, fiscal_end_month) == key_q
    )
    latest_cumulative_op = pd.to_numeric(quarterly_df.loc[mask_same_fy, "営業益"], errors="coerce").sum(min_count=1)
    if pd.isna(latest_cumulative_op):
        return (latest_label, 0.0, "予想データ欠損", -1.0)

    if key_q != key_f:
        return (latest_label, 0.0, "会計期不一致", -4.0)

    if float(full_op_val) < 0 or (float(latest_cumulative_op) < 0 and float(full_op_val) > 0):
        return (latest_label, 0.0, "予想データ欠損", -1.0)

    progress = float(latest_cumulative_op) / float(full_op_val) * 100.0
    if progress > 3000:
        return (最新_label, 0.0, "予想データ欠損", -1.0)

    return (latest_label, round(progress, 1), "OK", progress)

def get_latest_announce_date(quarterly_df: pd.DataFrame) -> datetime | None:
    if quarterly_df is None or quarterly_df.empty or "発表日" not in quarterly_df.columns:
        return None
    if quarterly_df["発表日"].isna().all():
        return None
    try:
        return pd.to_datetime(quarterly_df["発表日"].iloc[-1])
    except Exception:
        return None

# ===== HTML出力 =====
def export_html(df: pd.DataFrame, code: str, out_html: str, qp_result=None, verdict_str: str = ""):
    x_actual_period = df["決算期"].tolist() if not df.empty and "決算期" in df.columns else []
    x_custom_labels = []
    for period in x_actual_period:
        label = f"{period}" + ("\n(通期予想)" if "予" in period else "\n(実績)")
        x_custom_labels.append(label)
    x_full_plot = x_custom_labels

    if df.empty:
        print(f"[WARN] code={code}: データがないため、HTML出力はスキップされます。")
        fig = go.Figure().update_layout(title=f"通期実績データなし（code={code}）")
        body = fig.to_html(include_plotlyjs="cdn", full_html=False)
    else:
        df_growth = df.copy()
        use_cols = [c for c in ["売上高", "営業益", "修正1株益"] if c in df_growth.columns]
        if use_cols:
            df_growth = df_growth.set_index("決算期")[use_cols].pct_change(fill_method=None)*100
        else:
            df_growth = pd.DataFrame(index=df_growth["決算期"])
        qp = qp_result

        title_suffix = ""
        if qp:
            latest_label, progress, status, _ = qp
            if status == "OK": title_suffix = f" / {latest_label}：進捗{progress}%"
            elif status == "予想未発表": title_suffix = " / 進捗率 (予想未発表)"
            elif status == "予想ゼロ": title_suffix = " / 進捗率 (予想ゼロ)"
            elif status == "予想データ欠損": title_suffix = " / 進捗率 (予想データ欠損)"
            elif status == "会計期不一致": title_suffix = " / 進捗率 (会計期不一致)"

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=False,
            specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
            vertical_spacing=0.12,
            subplot_titles=(f"通期実績推移（株探データ, code={code}）{title_suffix}", "成長率推移")
        )

        # 売上高
        if "売上高" in df.columns and df["売上高"].notna().any():
            sales_full = (df["売上高"]/1e2).round(1).tolist()
            colors = ['rgba(173, 216, 230, 0.8)'] * (len(sales_full)-1)
            if len(sales_full) > 0:
                colors.append('rgba(173, 216, 230, 0.4)' if pd.notna(sales_full[-1]) else 'rgba(173, 216, 230, 0.2)')
            fig.add_trace(
                go.Bar(
                    x=x_full_plot, y=sales_full, name="売上高（億円）",
                    marker_color=colors, width=0.4,
                    customdata=df["決算期"].tolist(),
                    hovertemplate='<b>%{customdata}</b><br>売上高: %{y:.1f}億円<extra></extra>',
                ),
                row=1, col=1, secondary_y=False
            )

        # 営業益
        if "営業益" in df.columns and df["営業益"].notna().any():
            op_full = (df["営業益"]/1e2).round(1).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_full_plot[:-1], y=op_full[:-1], mode="lines+markers",
                    name="営業益（実績）", marker_symbol="circle",
                    customdata=df["決算期"].tolist()[:-1],
                    hovertemplate='<b>%{customdata}</b><br>営業益: %{y:.1f}億円<extra></extra>',
                    showlegend=True
                ),
                row=1, col=1, secondary_y=False
            )
            if len(op_full) >= 2 and pd.notna(op_full[-1]):
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-2], x_full_plot[-1]], y=[op_full[-2], op_full[-1]],
                        mode="lines", name="営業益（予）接続",
                        line=dict(dash='dot'), showlegend=False
                    ),
                    row=1, col=1, secondary_y=False
                )
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-1]], y=[op_full[-1]], mode="markers",
                        name="営業益（予）",
                        marker=dict(symbol="circle-open"),
                        customdata=[df["決算期"].iloc[-1]],
                        hovertemplate='<b>%{customdata}</b><br>営業益(予): %{y:.1f}億円<extra></extra>',
                        showlegend=False
                    ),
                    row=1, col=1, secondary_y=False
                )

        # EPS
        if "修正1株益" in df.columns and df["修正1株益"].notna().any():
            eps_full = df["修正1株益"].round(1).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_full_plot[:-1], y=eps_full[:-1], mode="lines+markers",
                    name="EPS（円）", marker_symbol="diamond",
                    customdata=df["決算期"].tolist()[:-1],
                    hovertemplate='<b>%{customdata}</b><br>EPS: %{y:.1f}円<extra></extra>',
                    showlegend=True
                ),
                row=1, col=1, secondary_y=True
            )
            if len(eps_full) >= 2 and pd.notna(eps_full[-1]):
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-2], x_full_plot[-1]], y=[eps_full[-2], eps_full[-1]],
                        mode="lines", name="EPS（予）接続",
                        line=dict(dash='dot'), showlegend=False
                    ),
                    row=1, col=1, secondary_y=True
                )
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-1]], y=[eps_full[-1]], mode="markers",
                        name="EPS（予）",
                        marker=dict(symbol="diamond-open"),
                        customdata=[df["決算期"].iloc[-1]],
                        hovertemplate='<b>%{customdata}</b><br>EPS(予): %{y:.1f}円<extra></extra>',
                        showlegend=False
                    ),
                    row=1, col=1, secondary_y=True
                )

        # 配当
        if "修正1株配" in df.columns and df["修正1株配"].notna().any():
            div_full = df["修正1株配"].round(1).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_full_plot[:-1], y=div_full[:-1], mode="lines+markers",
                    name="配当（円）", marker_symbol="x",
                    customdata=df["決算期"].tolist()[:-1],
                    hovertemplate='<b>%{customdata}</b><br>配当: %{y:.1f}円<extra></extra>',
                    showlegend=True
                ),
                row=1, col=1, secondary_y=True
            )
            if len(div_full) >= 2 and pd.notna(div_full[-1]):
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-2], x_full_plot[-1]], y=[div_full[-2], div_full[-1]],
                        mode="lines", name="配当（予）接続",
                        line=dict(dash='dot'), showlegend=False
                    ),
                    row=1, col=1, secondary_y=True
                )
                fig.add_trace(
                    go.Scatter(
                        x=[x_full_plot[-1]], y=[div_full[-1]], mode="markers",
                        name="配当（予）",
                        marker=dict(symbol="x-open"),
                        customdata=[df["決算期"].iloc[-1]],
                        hovertemplate='<b>%{customdata}</b><br>配当(予): %{y:.1f}円<extra></extra>',
                        showlegend=False
                    ),
                    row=1, col=1, secondary_y=True
                )

        # 軸設定など
        try:
            max_sales_op = df[["売上高", "営業益"]].apply(lambda s: s/1e2).stack().max()
        except Exception:
            max_sales_op = None
        max_sales_op_range = (max_sales_op * 1.1) if (max_sales_op and max_sales_op > 0.5) else None
        try:
            max_eps_div = df[["修正1株益", "修正1株配"]].stack().max()
        except Exception:
            max_eps_div = None
        max_eps_div_range = (max_eps_div * 1.1) if (max_eps_div and max_eps_div > 0.5) else None

        fig.update_xaxes(title_text="決算期", tickangle=0, type='category',
                         ticktext=x_full_plot, tickvals=x_full_plot, row=1, col=1)
        fig.update_yaxes(title_text="金額（億円）", range=[None, max_sales_op_range], row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="EPS / 配当（円）", range=[None, max_eps_div_range], row=1, col=1, secondary_y=True)

        # 成長率
        x_full_growth_plot = x_full_plot[1:]
        if not df_growth.empty:
            if "売上高" in df_growth.columns and df_growth["売上高"].notna().any():
                sales_growth_full = df_growth["売上高"].tolist()
                fig.add_trace(
                    go.Scatter(
                        x=x_full_growth_plot[:-1], y=sales_growth_full[:-1], mode="lines+markers",
                        name="売上高成長率（%）",
                        hovertemplate='売上高成長率: %{y:.1f}%%<extra></extra>', showlegend=True
                    ),
                    row=2, col=1
                )
                if len(sales_growth_full) >= 2 and pd.notna(sales_growth_full[-1]):
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[sales_growth_full[-2], sales_growth_full[-1]],
                            mode="lines", name="売上高（予）接続（成長率）",
                            line=dict(dash='dot'),
                            hovertemplate='売上高(予)接続 (成長率)<extra></extra>', showlegend=False
                        ),
                        row=2, col=1
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-1]], y=[sales_growth_full[-1]], mode="markers",
                            name="売上高成長率（予）", marker_symbol="square-open",
                            customdata=[df["決算期"].iloc[-1]],
                            hovertemplate=f'<b>%{{customdata}}</b><br>売上高成長率(予): %{{y:.1f}}%%<extra></extra>',
                            showlegend=True
                        ),
                        row=2, col=1
                    )

            if "営業益" in df_growth.columns and df_growth["営業益"].notna().any():
                op_growth_full = df_growth["営業益"].tolist()
                fig.add_trace(
                    go.Scatter(
                        x=x_full_growth_plot[:-1], y=op_growth_full[:-1], mode="lines+markers",
                        name="営業益成長率（%）",
                        hovertemplate='営業益成長率: %{y:.1f}%%<extra></extra>', showlegend=True
                    ),
                    row=2, col=1
                )
                if len(op_growth_full) >= 2 and pd.notna(op_growth_full[-1]):
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[op_growth_full[-2], op_growth_full[-1]],
                            mode="lines", name="営業益（予）接続（成長率）",
                            line=dict(dash='dot'),
                            hovertemplate='営業益(予)接続 (成長率)<extra></extra>', showlegend=False
                        ),
                        row=2, col=1
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-1]], y=[op_growth_full[-1]], mode="markers",
                            name="営業益成長率（予）", marker_symbol="circle-open",
                            customdata=[df["決算期"].iloc[-1]],
                            hovertemplate=f'<b>%{{customdata}}</b><br>営業益成長率(予): %{{y:.1f}}%%<extra></extra>',
                            showlegend=True
                        ),
                        row=2, col=1
                    )

            if "修正1株益" in df_growth.columns and df_growth["修正1株益"].notna().any():
                eps_growth_full = df_growth["修正1株益"].tolist()
                fig.add_trace(
                    go.Scatter(
                        x=x_full_growth_plot[:-1], y=eps_growth_full[:-1], mode="lines+markers",
                        name="EPS成長率（%）",
                        hovertemplate='EPS成長率: %{y:.1f}%%<extra></extra>', showlegend=True
                    ),
                    row=2, col=1
                )
                if len(eps_growth_full) >= 2 and pd.notna(eps_growth_full[-1]):
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[eps_growth_full[-2], eps_growth_full[-1]],
                            mode="lines", name="EPS（予）接続（成長率）",
                            line=dict(dash='dot'),
                            hovertemplate='EPS(予)接続 (成長率)<extra></extra>', showlegend=False
                        ),
                        row=2, col=1
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=[x_full_growth_plot[-1]], y=[eps_growth_full[-1]], mode="markers",
                            name="EPS成長率（予）", marker_symbol="diamond-open",
                            customdata=[df["決算期"].iloc[-1]],
                            hovertemplate=f'<b>%{{customdata}}</b><br>EPS成長率(予): %{{y:.1f}}%%<extra></extra>',
                            showlegend=True
                        ),
                        row=2, col=1
                    )

        fig.update_xaxes(title_text="決算期", tickangle=0, type='category',
                         ticktext=x_full_growth_plot, tickvals=x_full_growth_plot, row=2, col=1)
        fig.update_yaxes(title_text="成長率（%）", row=2, col=1)
        fig.update_layout(height=900, legend=dict(orientation="v"), margin=dict(l=60, r=40, t=60, b=100))
        body = fig.to_html(include_plotlyjs="cdn", full_html=False)

    verdict_html_content = (verdict_str or "").replace('\n', '<br>')
    verdict_html = (
        f'<div id="verdict_placeholder" style="margin-top:20px; color:#c9302c; font-weight:600; '
        f'text-align:left; white-space:pre-wrap; border:1px solid #ccc; padding:10px; '
        f'margin-left:10%; margin-right:10%; background-color:#f9f9f9;">'
        f'{verdict_html_content}'
        f'</div>'
    )
    html = f"""<!doctype html><html lang="ja"><meta charset="utf-8">
    <title>Fundamentals {code}</title>
    <style>body{{font-family:system-ui,-apple-system,'Noto Sans JP',sans-serif;margin:16px;}}</style>
    {body}
    {verdict_html}
    </html>"""
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[INFO] saved HTML: {out_html}")

# ===== ワーカー =====
async def process_single_code(code: str, out_dir: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> dict:
    code_full = str(code).strip()
    if not code_full or not re.match(r'^[0-9A-Za-z]{4,}$', code_full):
        print(f"[ERROR] {code_full}: 銘柄コード形式不正。スキップ。")
        return {'status': 'ERROR', 'code': code_full, 'message': "invalid code", 'progress_percent': None}
    # ---- 市場がETF/指数ならスキップ（株探URLを作らない）----
    market = _get_market_from_db(code_full)
    if _is_skip_market(market):
        print(f"[SKIP(MARKET)] {code_full}: 市場='{market}' のため株探URL生成をスキップします。")
        return {'status': 'SKIP', 'code': code_full, 'message': f"skip by market ({market})", 'progress_percent': None}
    
    opt = _ARGS
    # DBスキップは定数で既定ON。--no-db-skip を付けた時だけ無効化。
    use_db_skip = ENABLE_DB_SKIP and (not bool(getattr(opt, 'no_db_skip', False)))
    # 閾値：指定なければ定数を使う
    skip_days = getattr(opt, 'skip_recent_days', None)
    if skip_days is None:
        skip_days = SKIP_RECENT_DAYS_DEFAULT
    force_refresh = bool(getattr(opt, 'force_refresh', False))

    # ---- DBキャッシュ事前スキップ（HTTP前）----
    if use_db_skip and skip_days is not None and not force_refresh:
        cached_dt, cached_status = db_get_cached_announce(code_full)
        if cached_dt is not None:
            if cached_status in BAD_LAST_STATUSES:
                print(f"[DB-SKIP-BYPASS] {code_full}: last_status={cached_status} のためスキップせず取得へ。")
            else:
                # 3か月/6か月スキップ（成功銘柄だけ6か月拡張）
                SKIP_3M = int(skip_days or 84)
                SKIP_6M = 168
                delta_days = (datetime.now().date() - cached_dt.date()).days

                if cached_status in {"OK", "FETCHED"}:
                    effective = SKIP_6M if delta_days >= SKIP_3M else SKIP_3M
                elif cached_status in {"ANNOUNCE_ONLY", "EMPTY_QUARTERLY"}:
                    effective = SKIP_3M
                else:
                    effective = SKIP_3M

                if delta_days < effective:
                    policy = "6m" if (cached_status in {"OK","FETCHED"} and effective == SKIP_6M) else "3m"
                    print(f"[SKIP(DB)] {code_full}: last_status={cached_status}, latest={cached_dt.date()}, "
                          f"{delta_days}d < {effective}d (policy={policy})")
                    return {'status': 'SKIP', 'code': code_full,
                            'message': f"db-recent-{cached_status}-{policy}", 'progress_percent': None}

    async with semaphore:
        try:
            df_full_year, df_quarterly = await asyncio.gather(
                fetch_full_year_financials(code_full, session),
                fetch_quarterly_financials(code_full, session)
            )

            # 取得できたらキャッシュ更新（正常印 or announce-only）
            latest_ad = get_latest_announce_date(df_quarterly)
            if latest_ad is None:
                # 四半期が空でもページから発表日だけ拾う
                latest_ad = await fetch_latest_announce_date_only(session, code_full)

            if latest_ad is not None:
                st = "OK" if (df_quarterly is not None and not df_quarterly.empty) else "ANNOUNCE_ONLY"
                db_upsert_cached_announce(code_full, latest_ad, st)
            else:
                db_upsert_cached_announce(code_full, None, "EMPTY_QUARTERLY")

            df_forecast_row = pd.Series()
            if not df_full_year.empty and "決算期" in df_full_year.columns:
                _m = df_full_year["決算期"].astype(str).str.contains("予", na=False)
                if _m.any():
                    df_forecast_row = df_full_year[_m].iloc[-1]

            out_html = os.path.join(out_dir, f"finance_{code_full}.html")
            verdict, score = judge_and_score_performance(df_full_year)
            overall_alpha = _get_overall_alpha(score)  # ★ スコアからアルファを確定
            qp = calc_progress_from_df_op(df_quarterly, df_forecast_row)
            progress_percent_db = qp[3] if qp is not None else None

            # 進捗式ログ
            _log_on = bool(getattr(opt, 'log_progress', False))
            _log_file = getattr(opt, 'log_file', 'progress_debug.csv')
            details = compute_progress_details(df_quarterly, df_forecast_row)
            _force_log = (progress_percent_db == -1.0)
            if _log_on or _force_log:
                log_progress_details(code_full, details, (_log_file if _log_on else None))

            formatted_v = _format_verdict_with_progress(qp, verdict, score)
            export_html(df_full_year, code_full, out_html=out_html, qp_result=qp, verdict_str=formatted_v)
            print(f"[OK] {code_full} done. (Score: {score} points, Alpha: {overall_alpha}, Progress: {progress_percent_db}%)")

            return {
                'status': 'OK',
                'code': code_full,
                'score': score,
                'overall_alpha': overall_alpha,     # ★ 返却
                'progress_percent': progress_percent_db,
                'formatted_verdict': formatted_v,
                'out_html': out_html,
            }

        except aiohttp.ClientResponseError as e:
            status_code = getattr(e, "status", None)
            # 例外時も announce-only フォールバック
            latest_ad = await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "ANNOUNCE_ONLY")
            else:
                err_key = f"ERROR_{status_code}" if status_code in (429, 500, 502, 503, 504) else "HTTP_ERROR"
                db_upsert_cached_announce(code_full, None, err_key)
            print(f"[ERROR] {code_full}: HTTPエラー - {status_code}: {e.message}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"HTTPエラー: {status_code}", 'progress_percent': None}

        except aiohttp.ClientConnectorError as e:
            latest_ad = await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "ANNOUNCE_ONLY")
            else:
                db_upsert_cached_announce(code_full, None, "CONN_ERROR")
            print(f"[ERROR] {code_full}: 接続エラー - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"接続エラー: {type(e).__name__}", 'progress_percent': None}

        except asyncio.TimeoutError as e:
            latest_ad = await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "ANNOUNCE_ONLY")
            else:
                db_upsert_cached_announce(code_full, None, "TIMEOUT")
            print(f"[ERROR] {code_full}: タイムアウト - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': "TIMEOUT", 'progress_percent': None}

        except RuntimeError as e:
            latest_ad = await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "ANNOUNCE_ONLY")
            else:
                db_upsert_cached_announce(code_full, None, "PARSE_ERROR")
            print(f"[ERROR] {code_full}: 処理エラー - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"処理エラー: {type(e).__name__}", 'progress_percent': None}

        except Exception as e:
            latest_ad = await fetch_latest_announce_date_only(session, code_full)
            if latest_ad is not None:
                db_upsert_cached_announce(code_full, latest_ad, "ANNOUNCE_ONLY")
            else:
                db_upsert_cached_announce(code_full, None, "UNKNOWN_ERROR")
            print(f"[ERROR] {code_full}: 予期せぬエラー - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"予期せぬエラー: {type(e).__name__}", 'progress_percent': None}

# ===== メイン =====
async def main_async(target_code: str | None = None):
    out_dir = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data\graph"
    os.makedirs(out_dir, exist_ok=True)

    if target_code:
        codes = {target_code}
        print(f"[INFO] 指定コード {target_code} のみ処理します")
    else:
        try:
            master = pd.read_csv(MASTER_CODES_PATH, encoding="utf8", sep=",", engine="python", dtype={'コード': str})
            codes = set(master["コード"].astype(str))
            print(f"[INFO] {len(codes)} 銘柄を処理します（非同期処理開始）")
        except FileNotFoundError:
            print(f"[CRITICAL ERROR] マスターファイルが見つかりません: {MASTER_CODES_PATH}")
            return

    concurrency = getattr(_ARGS, 'workers', ASYNC_CONCURRENCY_LIMIT) or ASYNC_CONCURRENCY_LIMIT
    semaphore = asyncio.Semaphore(concurrency)
    print(f"[INFO] aiohttp + asyncioで最大 {concurrency} の並列リクエストを行います。")

    async with aiohttp.ClientSession(headers=HEADERS, trust_env=True) as session:
        tasks = [process_single_code(code, out_dir, session, semaphore) for code in codes]
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_results, error_count, skip_count = [], 0, 0
    for result in all_results:
        if isinstance(result, dict):
            st = result.get('status')
            if st == 'OK':
                successful_results.append(result)
            elif st == 'SKIP':
                skip_count += 1
            else:
                error_count += 1
        elif isinstance(result, Exception):
            error_count += 1
            print(f"[SUMMARY] 予期せぬ例外: {type(result).__name__}: {result}")

    # DB保存（--no-db が無い時だけ）
    if successful_results and not getattr(_ARGS, 'no_db', False):
        batch_record_to_sqlite(successful_results)
    elif successful_results:
        print(f"[DB][SKIP] --no-db 指定のため {len(successful_results)} 件のDB書き込みをスキップしました。")

    print("=" * 30)
    print(f"[DONE] 処理完了。成功: {len(successful_results)}件, スキップ: {skip_count}件, エラー: {error_count}件.")
    print("=" * 30)

def main(target_code: str | None = None):
    try:
        main_async_coro = main_async(target_code=target_code)
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_async_coro)
    except Exception as e:
        print(f"[CRITICAL ERROR] メイン処理中に予期せぬエラー: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='株探から財務データを取得し、グラフを生成します。（非同期・高速化対応）')
    parser.add_argument('code', nargs='?', default=None, help='処理する銘柄コード (省略時はマスターファイル内の全コード)')
    parser.add_argument('--workers', type=int, default=ASYNC_CONCURRENCY_LIMIT, help='同時並列数（デフォルト: 15）')
    parser.add_argument('--log-progress', action='store_true', help='進捗の式と使った値を標準出力に出す')
    parser.add_argument('--log-file', type=str, default='progress_debug.csv', help='進捗ログのCSV出力先')
    parser.add_argument('--skip-recent-days', type=int, default=None, help='最新の決算発表日からこの日数以内なら処理をスキップ（未指定なら定数を使用）')
    parser.add_argument('--force-refresh', action='store_true', help='DBキャッシュを無視して必ず取得する')
    parser.add_argument('--no-db-skip', action='store_true', help='DBスキップ機能を無効化（デフォルトは有効）')
    parser.add_argument('--no-db', action='store_true', help='DB書き込みを無効化（finance_notes へ保存しない）')

    args, _unknown = parser.parse_known_args()
    _GLOBALS = globals()
    _GLOBALS['_ARGS'] = args

    try:
        main(target_code=args.code)
    except Exception:
        sys.exit(1)
