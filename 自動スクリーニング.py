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

import subprocess, os

# JS スクリプトと出力ファイルのパス
KARAURI_JS_PATH   = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\空売り無しリスト出しスクリプト_k.js"
KARAURI_OUTPUT_TXT= r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\空売り無しリスト.txt"

def run_karauri_script():
    """Node.js の puppeteer スクリプトを起動して、空売り無しリストを更新する"""
    if not os.path.exists(KARAURI_JS_PATH):
        print(f"[karauri] JS スクリプトが見つかりません: {KARAURI_JS_PATH}")
        return

    try:
        print("[karauri] 空売り無しリスト抽出を開始...")
        # node 実行。スキップ条件（日付チェック）は JS 側に組み込み済み
        subprocess.run(["node", KARAURI_JS_PATH], check=True)
        print("[karauri] 抽出処理 完了")
    except subprocess.CalledProcessError as e:
        print(f"[karauri][WARN] スクリプト実行に失敗しました: {e}")
    except Exception as e:
        print(f"[karauri][WARN] 予期せぬエラー: {e}")


from string import Template
import os
import math
import time
import json
import warnings
import sqlite3
from datetime import datetime, date, timedelta, time as _t

import pandas as pd
from string import Template

# ======== 実行モード ========
RUN_SESSION = "EOD"      # "EOD" or "MIDDAY"
AUTO_MODE = True         # True: 11:30–12:30 は MIDDAY, それ以外は EOD（JST判定）

# ======== 送信設定（Gmail） ========
GMAIL_USER = "tomoyupo9@gmail.com"        # ←あなたのGmail
GMAIL_APP_PASSWORD = "pzunutpfqophuoae"      # ←アプリパスワード16桁（スペース無し）
GMAIL_TO   = "tomoyupo9@gmail.com"       # ←スマホで受け取る宛先
GMAIL_SUBJ = "スクリーニング レポート"
GMAIL_BODY = "index.html を添付します（オフラインで開けます）。"
SEND_HTML_AS_ZIP = False   # Trueにすると index.html を zip圧縮して送ります（HTML添付がブロックされる環境向け）
# =====================================

# ======== パス ========
DB_PATH = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\kani2.db"
CSV_INPUT_PATH = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\screen_data\screener_result.csv"
KARA_URI_NASHI_PATH = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\空売り無しリスト.txt"
MASTER_CODES_PATH = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\株コード番号.txt"
OUTPUT_DIR = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\screen_data"
EXTRA_CLOSED_PATH = r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\market_closed_extra.txt"

# ======== オプション ========
USE_CSV = True
TEST_MODE = False
TEST_LIMIT = 50

# 速度チューニング
YQ_MAX_WORKERS = 16
YQ_BATCH_EOD = 400
YQ_BATCH_MID = 400
YQ_SLEEP_EOD = 0.15
YQ_SLEEP_MID = 0.10

# 履歴取得
HISTORY_PERIOD_DAILY = "10d"    # 2回目以降の増分
HISTORY_PERIOD_FULL  = "12mo"   # 初回は12ヶ月（52週高値用）

# シグナル計算に読み込む過去日数（DBから）
SIGNAL_LOOKBACK_DAYS = 300      # 52週高値に余裕を持たせる

# MIDDAYの対象を絞る（Trueで速い）
MIDDAY_FILTER_BY_FLAGS = False

# ===== フェイルセーフ =====
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
        def networkdays(start: date, end: date, holidays=None):
            if holidays is None: holidays = []
            if start > end: start, end = end, start
            d, cnt = start, 0
            while d <= end:
                if d.weekday() < 5 and d not in holidays:
                    cnt += 1
                d += timedelta(days=1)
            return cnt
        @staticmethod
        def workday(start: date, days: int, holidays=None):
            if holidays is None: holidays = []
            step = 1 if days >= 0 else -1
            d, moved = start, 0
            while moved < abs(days):
                d += timedelta(days=step)
                if d.weekday() < 5 and d not in holidays:
                    moved += 1
            return d
    workdays = _WorkdaysShim()

from yahooquery import Ticker as YQ
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
    return datetime.now().strftime("%Y-%m-%d")
    
# ================== 表示整形ヘルパ ==================
def _format_flag_html(flag: str, start_date: str | None) -> str:
    """
    '候補' などのフラグを、崩れないHTMLに整形して返す。
    start_date は 'YYYY-MM-DD' などを想定（None可）
    """
    if flag is None:
        flag = ""
    flag = str(flag).strip()

    if flag == "候補":
        date_html = f"<br><span style='font-size:10px;color:gray'>{start_date}〜</span>" if (start_date and str(start_date).strip()) else ""
        return f"<div style='line-height:1.1'>候補{date_html}</div>"
    # 空やその他は空表示
    return ""

def _safe_jsonable(val):
    """
    JSONに安全に落とし込むための変換（bytes, NaN, Timestamp 等を処理）
    """
    import math
    import pandas as pd
    import numpy as np
    from datetime import datetime, date

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
    if isinstance(val, (pd.Timestamp, datetime, date)):
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

def is_jp_market_holiday(d: date, extra_closed: set = None) -> bool:
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

def next_business_day_jp(d: date, extra_closed: set = None) -> date:
    cur = d
    while True:
        cur += timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

def prev_business_day_jp(d: date, extra_closed: set = None) -> date:
    cur = d
    while True:
        cur -= timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

# ===== DB =====
def open_conn(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    return conn

def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, decl: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
            conn.commit()
        except Exception:
            pass
    cur.close()

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
          コード   TEXT NOT NULL,
          日付     TEXT NOT NULL,
          始値     REAL,
          高値     REAL,
          安値     REAL,
          終値     REAL,
          出来高   INTEGER,
          PRIMARY KEY (コード, 日付)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_code_date ON price_history(コード, 日付)")
    conn.commit()
    cur.close()

    for col, decl in [
        ("初動フラグ", "TEXT"),
        ("底打ちフラグ", "TEXT"),
        ("上昇余地スコア", "REAL"),
        ("右肩上がりスコア", "REAL"),
        ("右肩上がりフラグ", "TEXT"),
        ("シグナル更新日", "TEXT"),
        ("現在値", "REAL"),
        ("前日終値比率", "REAL"),
        ("出来高", "INTEGER"),
        ("時価総額億円", "REAL"),
        ("更新日", "TEXT"),
        ("登録日", "TEXT"),
        ("UPDOWN", "TEXT"),
        ("UP継続回数", "INTEGER"),
        ("DOWN継続回数", "INTEGER"),
        ("初動株価", "REAL"),
        ("初動株価倍率", "REAL"),
        ("初動出来高", "INTEGER"),
        ("初動出来高倍率", "REAL"),
        ("機関組入時価総額", "TEXT"),
        ("空売り機関", "TEXT"),
        ("残日", "INTEGER"),
        ("経過日数", "INTEGER"),
        ("初動検知成功", "TEXT"),
    ]:
        add_column_if_missing(conn, "screener", col, decl)

def ensure_signal_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals_log (
          コード            TEXT NOT NULL,
          日時              TEXT NOT NULL,
          種別              TEXT NOT NULL,
          終値              REAL,
          高値              REAL,
          安値              REAL,
          出来高            INTEGER,
          スコア            REAL,
          検証済み          INTEGER,
          次日始値          REAL,
          次日終値          REAL,
          次日高値          REAL,
          次日安値          REAL,
          リターン終値pct    REAL,
          フォロー高値pct    REAL,
          最大逆行pct        REAL,
          判定               TEXT,
          理由               TEXT,
          PRIMARY KEY (コード, 日時, 種別)
        )
    """)
    conn.commit()
    cur.close()
    
# ==== 追記：BLOB/TEXTが混入した数値列を正規化（DB側を恒久修正） ====
def normalize_blob_numeric(conn: sqlite3.Connection):
    cur = conn.cursor()
    # 初動出来高：INTEGERへ
    cur.execute("""
        UPDATE screener
        SET 初動出来高 = CAST(REPLACE(CAST(初動出来高 AS TEXT), ',', '') AS INTEGER)
        WHERE typeof(初動出来高) IN ('text','blob')
           OR (typeof(初動出来高)='null' AND 初動出来高 IS NOT NULL)
    """)

    # 出来高：INTEGERへ（保険）
    cur.execute("""
        UPDATE screener
        SET 出来高 = CAST(REPLACE(CAST(出来高 AS TEXT), ',', '') AS INTEGER)
        WHERE typeof(出来高) IN ('text','blob')
    """)

    # 倍率系：REALへ（小数あり）
    cur.execute("""
        UPDATE screener
        SET 初動出来高倍率 = CAST(REPLACE(CAST(初動出来高倍率 AS TEXT), ',', '') AS REAL)
        WHERE typeof(初動出来高倍率) IN ('text','blob')
    """)
    cur.execute("""
        UPDATE screener
        SET 初動株価倍率 = CAST(REPLACE(CAST(初動株価倍率 AS TEXT), ',', '') AS REAL)
        WHERE typeof(初動株価倍率) IN ('text','blob')
    """)

    # 価格系：REALへ（保険）
    cur.execute("""
        UPDATE screener
        SET 初動株価 = CAST(REPLACE(CAST(初動株価 AS TEXT), ',', '') AS REAL)
        WHERE typeof(初動株価) IN ('text','blob')
    """)

    conn.commit()
    cur.close()


# ===== 初回判定 =====
def need_full_history(conn: sqlite3.Connection, lookback_days: int = 360) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM price_history")
    total = (cur.fetchone() or [0])[0]

    since = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    cur.execute("SELECT COUNT(*) FROM price_history WHERE 日付 >= ?", (since,))
    recent = (cur.fetchone() or [0])[0]
    cur.close()
    return total < 5000 or recent < 2000

# ===== 任意：CSV取り込み =====

def phase_csv_import(conn, csv_path=None, overwrite_registered_date=False):
    """
    CSV からは「コード・銘柄名・市場・登録日」だけを取り込む最小実装。
    - 他カラムは一切触らない（Yahoo や内部計算で更新される想定）
    - overwrite_registered_date=False の場合、登録日は既存値があれば温存し、空/NULLのときのみCSV値を入れる
    """

    if not os.path.isfile(CSV_INPUT_PATH):
        print("CSVがないのでスキップ:", CSV_INPUT_PATH)
        return
    print("CSVを読み込み:", CSV_INPUT_PATH)
    cur = conn.cursor()
    csv_input = pd.read_csv(CSV_INPUT_PATH, encoding="utf8", sep=",", engine="python")


    # 必須列のみ読み込み
    df = pd.read_csv(csv_input, dtype=str)
    needed = ["コード", "銘柄名", "市場", "登録日"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"[csv-import] CSVに必須列がありません: {missing}")

    df = df[needed].copy()

    # 正規化
    df["コード"] = df["コード"].astype(str).str.strip().str.zfill(4)
    df["銘柄名"] = df["銘柄名"].astype(str).str.strip()
    df["市場"]   = df["市場"].astype(str).str.strip()
    # 登録日は空文字/NaNなら None に
    df["登録日"] = df["登録日"].where(df["登録日"].notna() & (df["登録日"].str.strip() != ""), None)

    # 必要ならカラムを保証（通常は存在済み）
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(screener)")
    cols = {r[1] for r in cur.fetchall()}
    if "銘柄名" not in cols:
        cur.execute('ALTER TABLE screener ADD COLUMN "銘柄名" TEXT')
    if "市場" not in cols:
        cur.execute('ALTER TABLE screener ADD COLUMN "市場" TEXT')
    if "登録日" not in cols:
        cur.execute('ALTER TABLE screener ADD COLUMN "登録日" TEXT')
    conn.commit()

    # UPSERT（登録日の扱いのみポリシー分岐）
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
        # 既存の登録日を温存。NULL/空のときだけCSVで補完
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


# ===== 任意：上場廃止反映 =====
def phase_delist_cleanup(conn: sqlite3.Connection):
    if not os.path.isfile(MASTER_CODES_PATH):
        print("上場廃止の基準CSVが見つからないためスキップ:", MASTER_CODES_PATH)
        return
    master = pd.read_csv(MASTER_CODES_PATH, encoding="utf8", sep=",", engine="python")
    valid = set(master["コード"].astype(str))
    cur = conn.cursor()
    cur.execute("SELECT コード FROM screener")
    todo = [db_code for (db_code,) in cur.fetchall() if str(db_code) not in valid]
    for code in todo:
        print("上場廃止による削除:", code)
        cur.execute("DELETE FROM screener WHERE コード=?", (code,))
    conn.commit()
    cur.close()

# ===== 任意：空売り無し反映 =====
def phase_mark_karauri_nashi(conn: sqlite3.Connection):
    if not os.path.isfile(KARA_URI_NASHI_PATH):
        print("空売り無しリストが見つからないためスキップ:", KARA_URI_NASHI_PATH)
        return
    df = pd.read_csv(KARA_URI_NASHI_PATH, encoding="utf8", sep=",", engine="python")
    cur = conn.cursor()
    for row in df.itertuples():
        code = getattr(row, "コード")
        cur.execute('UPDATE screener SET 空売り機関="なし" WHERE コード=?', (code,))
    conn.commit()
    cur.close()

# ===== Yahoo（EOD 最新） =====
# 必要: pip install yfinance pandas
import yfinance as yf
import pandas as pd
from datetime import datetime


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
    """
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
            float(close_t) if pd.notna(close_t) else None,     # 現在値
            float(prev) if prev is not None else None,         # 前日終値
            float(yen) if yen is not None else None,           # 前日円差
            float(pct) if pct is not None else None,           # 前日終値比率(％)
            int(vol_t) if pd.notna(vol_t) else None,           # 出来高
            datetime.today().strftime("%Y-%m-%d"),              # シグナル更新日
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
    apply_atr14_pct(conn)
    print(f"[refresh] 追記 {total_added} 行 / 銘柄 {len(codes)} 件（既存{len(exist_codes)}・新規{len(new_codes)}）")

# ==== 時価総額取得

# ===== 全銘柄の時価総額を一括更新 =====
import pandas as pd
import yfinance as yf  # フォールバック用（基本は使わない想定）

def _to_symbol(c: str) -> str:
    s = str(c).strip()
    return s if "." in s else s + ".T"   # 4桁数字コード想定：.T 付与

def _normalize_map(obj):
    """
    yahooquery.Ticker(...).summary_detail / price の戻りを
    {symbol: { ... }} 形式の dict に正規化。文字列はスキップ。
    """
    import pandas as pd
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
    import pandas as pd
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
        from yahooquery import Ticker
        for i in range(0, len(codes), batch_size):
            chunk = codes[i:i+batch_size]
            symbols = [_to_symbol(c) for c in chunk]

            tq = Ticker(symbols, asynchronous=True, max_workers=max_workers)
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
                rows.append((mcap_oku, code))

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
                fi = yf.Ticker(_to_symbol(c)).fast_info
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

def r0(x):
    """整数に丸め（None安全）"""
    try:
        return None if x is None else round(float(x), 0)
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
            zika_oku = math.floor(((mcap or 0.0) / 100_000_000) * 10) / 10

            # ← tupleの順序を変更：前日終値・前日円差・前日終値比率を全部入れる
            up_screener.append((last, prev, yen, pct, vol or 0, zika_oku, today, code))

            o1 = ffloat(q.get("regularMarketOpen"), None)
            h1 = ffloat(q.get("regularMarketDayHigh"), None)
            l1 = ffloat(q.get("regularMarketDayLow"), None)
            c1 = last
            up_hist.append((code, today, o1, h1, l1, c1, vol))

        time.sleep(YQ_SLEEP_MID)

    if up_screener:
        cur = conn.cursor()
        cur.executemany(
            "UPDATE screener SET 現在値=?, 前日終値=?, 前日円差=?, 前日終値比率=?, 出来高=?, 時価総額億円=?, 更新日=? WHERE コード=?",
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
        apply_atr14_pct(conn)
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
    cal_today = date.today()
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


import pandas as pd
import numpy as np
from datetime import timedelta

# === パラメータ（必要なら調整） ===
MID_MA = 25          # 中期移動平均の期間
BREAKOUT_LOOKBACK = 60  # 高値更新の参照期間（過去N日）
EPS = 0.0            # 高値更新の余白（0.01=1%上抜きのみを採用、0なら同値以上OK）





import pandas as pd, numpy as np

# ===== パラメータ（好みで微調整） =====
LOOKBACK = 160        # 判定窓（営業日）
MIN_DAYS = 90         # 必要最小日数
SLOPE_MIN_ANN = 0.12  # 回帰傾き(年率)の最低ライン（12%/年）
R2_MIN = 0.45         # 回帰R^2最低
MDD_MAX = 0.30        # 最大ドローダウン許容（30%）
RIBBON_KEEP_DAYS = 30 # 直近この日数のうち 20>50>100 がどれだけ維持されたか
WEEK_UP_MIN = 0.55    # 週足の“上昇週”比率の下限
HL_WIN = 5            # ピボット検出用の窓（高値/安値の山谷幅）
THRESH_SCORE = 70     # フラグ閾値

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
import pandas as pd
import numpy as np

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
def _ensure_early_schema(conn):
    """
    - screener に 右肩早期{フラグ/種別/スコア} が無ければ追加
    - signals_log が無ければ '日時/コード/種別/詳細' の最小構成で作成
    - (コード,日時,種別) の UNIQUE を張って ON CONFLICT を有効化
    - 速度向上のための推奨インデックスを作成
    """
    cur = conn.cursor()
    # screener 追加列
    sc_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    if "右肩早期フラグ" not in sc_cols:
        cur.execute("ALTER TABLE screener ADD COLUMN 右肩早期フラグ TEXT")
    if "右肩早期種別" not in sc_cols:
        cur.execute("ALTER TABLE screener ADD COLUMN 右肩早期種別 TEXT")
    if "右肩早期スコア" not in sc_cols:
        cur.execute("ALTER TABLE screener ADD COLUMN 右肩早期スコア REAL")

    # signals_log（最小構成）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals_log(
          日時   TEXT NOT NULL,
          コード TEXT,
          種別   TEXT,
          詳細   TEXT
        )
    """)
    # ON CONFLICT を効かせるための UNIQUE
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_log
        ON signals_log(コード, 日時, 種別)
    """)

    # 推奨インデックス（速度向上）
    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_price_history_code_date
        ON price_history(コード, 日付)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_screener_code
        ON screener(コード)
    """)
    conn.commit(); cur.close()

# ---- 指標計算の小物 ----
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
    _ensure_early_schema(conn)

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
    start_cut = (date.today() - timedelta(days=SIGNAL_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

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

        for code, g in df.groupby("コード"):
            g = g.copy()
            # 移動平均・RSI
            g["終値_ma5"] = g["終値"].rolling(5).mean()
            g["終値_ma20"] = g["終値"].rolling(20).mean()
            g["出来高_ma5"] = g["出来高"].rolling(5).mean()
            g["MA13"] = g["終値"].rolling(13).mean()
            g["MA26"] = g["終値"].rolling(26).mean()
            g["ATR20"] = (g["高値"] - g["安値"]).rolling(20).mean()

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
    today = date.today()
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
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup, escape
import os, math, pandas as pd, json

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

def _safe_jsonable(val):
    """bytes/NaN/Timestamp等を安全にJSON化"""
    import math, numpy as np
    import pandas as pd
    from datetime import datetime, date
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try: return val.decode("utf-8", errors="ignore")
        except Exception: return str(val)
    if (isinstance(val, float) and math.isnan(val)) or (hasattr(pd, "isna") and pd.isna(val)):
        return None
    if isinstance(val, (np.floating, np.integer)):
        return val.item()
    if isinstance(val, (pd.Timestamp, datetime, date)):
        return str(val)[:19]
    return val

# ---------- タグ生成 ----------
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

def _derive_recommendation(d):
    """
    候補フラグ + 流動性(RVOL/売買代金) + 合成S + 上昇率レンジで推奨を自動決定。
    返り値: (推奨アクション, 推奨比率)  ※比率は数値（%ではない）
    """
    turn = _to_float(d.get("売買代金(億)"))
    rvol = _to_float(d.get("RVOL代金"))
    rate = _to_float(d.get("前日終値比率"))
    comp = _to_float(d.get("合成スコア"))

    # しきい値（必要に応じて調整）
    TURN_MIN     = 5.0      # 売買代金(億) ≥ 5
    RVOL_MIN     = 2.0      # RVOL代金 ≥ 2
    COMP_BASE    = 70.0     # 小口の目安
    COMP_STRONG  = 80.0     # 有力の目安
    RATE_SOFT    = (-2.0, 3.0)  # 小口：-2%〜+3%
    RATE_STRONG  = ( 0.0, 2.0)  # 有力： 0%〜+2%

    is_setup = any((d.get(k) or "").strip() == "候補"
                   for k in ("右肩早期フラグ", "右肩上がりフラグ", "初動フラグ"))
    liquid   = (turn is not None and turn >= TURN_MIN) and (rvol is not None and rvol >= RVOL_MIN)
    good     = (comp is not None and comp >= COMP_BASE)
    strong   = (comp is not None and comp >= COMP_STRONG)

    def _in(v, lohi): 
        return (v is not None) and (lohi[0] <= v <= lohi[1])

    # 強い条件 → エントリー有力（1.0%）
    if is_setup and liquid and strong and _in(rate, RATE_STRONG):
        return "エントリー有力", 1.0

    # 緩い条件 → 小口提案（0.5%）
    if is_setup and liquid and good and _in(rate, RATE_SOFT):
        return "小口提案", 0.5

    return "", None


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
        from urllib.parse import quote as _q
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

        # 付加フラグ
        d["空売り機関なし_flag"] = _noshor_from_agency(d.get("空売り機関"))
        d["営利対時価_flag"]     = _op_ratio_flag(d)  # ← チェックボックス用（割安）

        # 判定/理由
        pct = _to_float(d.get("前日終値比率"))
        d["判定"] = "当たり！" if (pct is not None and pct > 0) else ""
        d["判定理由"] = _build_reason(d)
        
                # --- 推奨/比率（DB列が無い or 空なら自動付与） ---
        if not (d.get("推奨アクション") or "").strip():
            rec, ratio = _derive_recommendation(d)
            if rec:
                d["推奨アクション"] = rec
            if ratio is not None:
                d["推奨比率"] = ratio

        # --- 営利対時価_flag（DB列が無い/空なら導出） ---
        if not (d.get("営利対時価_flag") or "").strip():
            d["営利対時価_flag"] = _derive_opratio_flag(d)

        # 念のため：空売り機関なし_flag を維持
        d["空売り機関なし_flag"] = _noshor_from_agency(d.get("空売り機関"))


        rows.append(d)
    return rows


# =================== HTMLテンプレ ===================


DASH_TEMPLATE_STR = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>スクリーニング ダッシュボード</title>
<style>
  :root{
    --ink:#1f2937; --muted:#6b7280; --bg:#f9fafb; --line:#e5e7eb;
    --blue:#0d3b66; --green:#15803d; --orange:#b45309; --yellow:#a16207;
    --hit:#ffe6ef; --rowhover:#f6faff;
  }
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;margin:16px;color:var(--ink);background:#fff}
  nav{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
  nav a{padding:6px 10px;border-radius:8px;text-decoration:none;background:#e1e8f0;color:#1f2d3d;font-weight:600}
  nav a.active{background:var(--blue);color:#fff}
  .toolbar{display:flex;gap:14px;align-items:center;margin:8px 0 10px;flex-wrap:wrap}
  .toolbar input[type="text"],.toolbar input[type="number"]{padding:6px 10px;border:1px solid #ccd;border-radius:8px;background:#fff}
  .toolbar input[type="number"]{width:80px}
  .btn{background:var(--blue);color:#fff;border:none;border-radius:8px;padding:6px 12px;cursor:pointer;font-weight:600}

  /* ▼ テーブルまわり（角丸クリップはラッパで管理） */
  .tbl-wrap{
    border-radius:10px;
    overflow:visible;
    background:#fff;
    box-shadow:0 0 0 1px var(--line) inset;
  }
  .tbl{ border-collapse:collapse; width:100%; background:#fff; }
  .tbl th,.tbl td{border-bottom:1px solid var(--line);padding:8px 10px;vertical-align:top}
  .tbl tbody tr:nth-child(even){background:#fcfdff}
  .tbl tbody tr:hover{background:var(--rowhover)}
  .tbl th.sortable{cursor:pointer;user-select:none}
  .tbl th.sortable .arrow{margin-left:6px;font-size:11px;color:#666}
  .num{text-align:right}
  .muted{color:var(--muted)} .hidden{display:none} .count{margin-left:6px;color:var(--muted)}
  .pager{display:flex;gap:8px;align-items:center}
  tr.hit>td{background:var(--hit)}
  .badge{display:inline-flex;gap:6px;align-items:center;padding:2px 8px;border-radius:999px;font-size:12px;line-height:1;font-weight:700}
  .b-green{background:#e7f6ed;color:var(--green);border:1px solid #cceedd}
  .b-orange{background:#fff4e6;color:#b45309;border:1px solid #ffe2c2}
  .b-yellow{background:#fff9db;color:#a16207;border:1px solid #ffe9a8}

  /* 推奨バッジ */
  .rec-badge{
    display:inline-flex; align-items:center; gap:6px;
    padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700;
    line-height:1; white-space:nowrap;
  }
  .rec-strong{ background:#e7f6ed; color:#166534; border:1px solid #cceedd; }
  .rec-small { background:#fff4e6; color:#9a3412; border:1px solid #ffe2c2; }
  .rec-watch { background:#eef2f7; color:#475569; border:1px solid #dbe4ef; }
  .rec-dot{ display:inline-block; width:6px; height:6px; border-radius:50%; background:currentColor;}

  /* ▼ 候補一覧テーブルだけヘッダー固定（Safari対応） */
  #tbl-candidate thead th{
    position: sticky;
    position: -webkit-sticky;
    top: 0;
    background:#f3f6fb;
    z-index: 5;
    border-bottom:2px solid #ccc;
  }
  /* ▼ 全カラムテーブルも候補一覧と同じヘッダー固定 */
  #tbl-allcols thead th{
    position: sticky;
    position: -webkit-sticky; /* Safari */
    top: 0;
    background:#f3f6fb;
    z-index: 5;
    border-bottom:2px solid #ccc;
  }

  /* ===== ヘルプ（小窓＋暗幕） ===== */
  .help-backdrop{
    position: fixed; inset: 0;
    background: rgba(17,24,39,.45);
    z-index: 9998;
    display: none;               /* ← 初期は非表示 */
  }
  .help-pop{
    position: absolute;
    z-index: 9999;
    max-width: 360px;
    background: #fff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    box-shadow: 0 12px 32px rgba(0,0,0,.18);
    padding: 12px 14px 14px;
    font-size: 13px;
    line-height: 1.55;
    display: none;               /* ← 初期は非表示 */
  }
  .help-pop .help-head{
    display:flex; align-items:center; justify-content:space-between;
    gap:12px; margin-bottom:6px; font-weight:700;
  }
  .help-pop .help-close{
    display:inline-flex; align-items:center; justify-content:center;
    width:22px; height:22px; border-radius:6px; cursor:pointer;
    user-select:none; font-weight:700;
  }
  .help-pop .help-close:hover{ background:#f3f4f6; }

  /* ？アイコン（テーブル/ツールバー共通） */
  .qhelp{
    display:inline-flex; align-items:center; justify-content:center;
    width:18px; height:18px; margin-left:6px;
    border-radius:50%; border:1px solid #cbd5e1;
    font-size:12px; cursor:pointer; background:#eef2ff; color:#334155; font-weight:700; line-height:1;
  }
  .qhelp:hover{ background:#e0e7ff; }
</style>
</head>

<body>
  <nav>
    <a href="#" id="lnk-cand" class="active">候補一覧</a>
    <a href="#" id="lnk-tmr">明日用</a>
    <a href="#" id="lnk-all">全カラム</a>
    {% if include_log %}<a href="#" id="lnk-log">signals_log</a>{% endif %}
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

    <label>上昇率≥ <input type="number" id="th_rate" placeholder="3" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label>売買代金≥ <input type="number" id="th_turn" placeholder="5" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label>RVOL代金≥ <input type="number" id="th_rvol" placeholder="2" step="0.1" inputmode="decimal" autocomplete="off"></label>
    <label><input type="checkbox" id="f_defaultset"> 規定</label>

    <input type="text" id="q" placeholder="全文検索（コード/銘柄/判定理由など）" style="min-width:240px">
    <button class="btn" id="btn-stats">傾向グラフ</button>
    <button class="btn" id="btn-ts">推移グラフ</button>

    <span class="pager">
      <label>表示件数
        <select id="perpage">
          <option value="200">200</option><option value="500" selected>500</option>
          <option value="1000">1000</option><option value="2000">2000</option>
        </select>
      </label>
      <button class="btn" id="prev">前へ</button>
      <button class="btn" id="next">次へ</button>
      <span id="pageinfo" class="muted">- / -</span>
    </span>
    <span class="count">件数: <b id="count">-</b></span>
  </div>

  <!-- ヘルプ文言（キーはヘッダーと完全一致） -->
  <script>
    window.HELP_TEXT = {
      "規定": "既定セット（前日終値比率 降順 × RVOL>2 × 売買代金(億)の下限）を一括適用。",
      "コード": "東証の銘柄コード（4桁、ETF等は例外あり）。",
      "銘柄": "銘柄名。",
      "現在値": "最新の株価（終値/スナップショット）。",
      "前日終値": "その銘柄の前日の終値。基準価格となる。",
      "前日比(円)": "当日の株価が前日終値から何円動いたか。",
      "前日終値比率（％）": "【勢い】値動きの強さ。+10%以上は短期資金集中の証拠。",
      "出来高": "売買された株数。売買代金やRVOLと併用が望ましい。",
      "売買代金(億)": "【流動性】最重要。デイトレ狙いなら最低 5–10 億以上が目安。",
      "初動": "【シグナル】資金流入の初動。短期資金の動きの兆候。",
      "底打ち": "【シグナル】安値圏からの反転兆候。リバ狙いの候補。",
      "右肩": "【シグナル】右肩上がりスコアに基づくトレンド持続性の判定。",
      "早期": "【シグナル】右肩の“早期”局面。詳細は『早期種別』参照。",
      "早期S": "【勢い+シグナル】RVOL/代金/値動きの合成スコア。80+ 強い、90+ 主役級。",
      "早期種別": "当日最有力のエントリー種別（ブレイク/ポケット/20MAリバ/200MAリクレイム等）。",
      "判定": "最終判定（候補/監視/非該当など）。",
      "判定理由": "アルゴが候補にした根拠の要約。",
      "推奨": "自動分類の推奨ラベル（有力/小口/監視など）。",
      "推奨比率%": "推奨の強さ（%）。",
      "更新": "シグナル最終更新日。",

      /* 行内の値ヘルプ */
      "ブレイク": "過去高値更新＋出来高伴う上抜け。",
      "ポケット": "10MA上で直近の下げ日最大出来高を上回るなどの“押し目買い”有利域。",
      "20MAリバ": "20MAを下から上へ再突入。出来高は20日平均以上が望ましい。",
      "200MAリクレイム": "200MAを回復し上で維持。50MA上向き/100MA横ばい以上が理想。",

      /* まとめ */
      "まとめ（優先度順）": "・売買代金 × RVOL（まず流動性）\n・前日比％ と 合成S（勢い）\n・フラグ（右肩/早期/初動/底打ち）\n・ATR14%（許容リスク）\n👉 実務は「代金 ≥10億、RVOL ≥2、合成S ≥80」かつ「右肩 or 早期」を優先。"
    };
    // data-col → HELP_TEXT マップ（ヘッダーの data-col 用）
    window.DATACOL_TO_HELPKEY = {
      "コード":"コード","銘柄名":"銘柄","現在値":"現在値",
      "前日終値":"前日終値","前日円差":"前日比(円)","前日終値比率":"前日終値比率（％）",
      "出来高":"出来高","売買代金(億)":"売買代金(億)",
      "初動フラグ":"初動","底打ちフラグ":"底打ち","右肩上がりフラグ":"右肩","右肩早期フラグ":"早期",
      "右肩早期スコア":"早期S","右肩早期種別":"早期種別",
      "判定":"判定","判定理由":"判定理由",
      "推奨アクション":"推奨","推奨比率":"推奨比率%",
      "シグナル更新日":"更新"
    };
  </script>

  <section id="tab-candidate" class="tab">
    <div class="tbl-wrap">
      <table id="tbl-candidate" class="tbl">
        <thead>
          <tr>
            <th class="sortable" data-col="コード" data-type="text">コード<span class="arrow"></span></th>
            <th class="sortable" data-col="銘柄名" data-type="text">銘柄<span class="arrow"></span></th>
            <th>Yahoo</th>
            <th>X</th>
            <th class="num sortable" data-col="現在値" data-type="num">現在値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値" data-type="num">前日終値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日円差" data-type="num">前日比(円)<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値比率" data-type="num">前日終値比率（％）<span class="arrow"></span></th>
            <th class="num sortable" data-col="出来高" data-type="num">出来高<span class="arrow"></span></th>
            <th class="num sortable" data-col="売買代金(億)" data-type="num">売買代金(億)<span class="arrow"></span></th>
            <th class="sortable" data-col="初動フラグ" data-type="flag">初動<span class="arrow"></span></th>
            <th class="sortable" data-col="底打ちフラグ" data-type="flag">底打ち<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩上がりフラグ" data-type="flag">右肩<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期フラグ" data-type="flag">早期<span class="arrow"></span></th>
            <th class="num sortable" data-col="右肩早期スコア" data-type="num">早期S<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期種別" data-type="text">早期種別<span class="arrow"></span></th>
            <th class="sortable" data-col="判定" data-type="text">判定<span class="arrow"></span></th>
            <th class="sortable" data-col="判定理由" data-type="text">判定理由<span class="arrow"></span></th>
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
            <th>Yahoo</th>
            <th>X</th>
            <th class="num sortable" data-col="現在値" data-type="num">現在値<span class="arrow"></span></th>
            <th class="num sortable" data-col="前日終値比率" data-type="num">前日終値比率（％）<span class="arrow"></span></th>
            <th class="num sortable" data-col="売買代金(億)" data-type="num">売買代金(億)<span class="arrow"></span></th>
            <th class="num sortable" data-col="右肩早期スコア" data-type="num">早期S<span class="arrow"></span></th>
            <th class="sortable" data-col="右肩早期種別" data-type="text">早期種別<span class="arrow"></span></th>
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


  {% if include_log %}
  <section id="tab-log" class="tab hidden">
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
  </section>
  {% endif %}

  <!-- 直埋めデータ(JSON) -->
  <script id="__DATA__" type="application/json">{{ data_json|safe }}</script>

  <!-- ===== ここからクライアントJS（整頓済み） ===== -->
  <script>
  (function(){
    "use strict";

    /* ---------- データ読込 ---------- */
    const RAW = (()=>{ try{ return JSON.parse(document.getElementById("__DATA__").textContent||"{}"); }catch(_){ return {}; } })();
    const DATA_CAND = Array.isArray(RAW.cand)? RAW.cand: [];
    const DATA_ALL  = Array.isArray(RAW.all) ? RAW.all : [];
    const DATA_LOG  = Array.isArray(RAW.logs)? RAW.logs: [];

    /* ---------- util ---------- */
    const $  = (s,r=document)=>r.querySelector(s);
    const $$ = (s,r=document)=>Array.from(r.querySelectorAll(s));
    const num = (v)=>{ const s=String(v??"").replace(/[,\s円％%]/g,""); const n=parseFloat(s); return Number.isFinite(n)?n:NaN; };
    const cmp = (a,b)=>{ if(a==null&&b==null) return 0; if(a==null) return -1; if(b==null) return 1;
      const na=+a, nb=+b, da=new Date(a), db=new Date(b);
      if(!Number.isNaN(na)&&!Number.isNaN(nb)) return na-nb;
      if(!Number.isNaN(da)&&!Number.isNaN(db)) return da-db;
      return String(a).localeCompare(String(b),"ja"); };
    const hasKouho = (v)=> String(v||"").includes("候補");
    // === 汎用DOMソート（全カラム / 明日用） ===
    function _parseNum(s){
      const t = String(s).replace(/[,\s円％%]/g,'');
      const n = parseFloat(t);
      return Number.isFinite(n) ? n : NaN;
    }
    function _sortKeyByType(text, typ){
      if(typ === 'num'){ const n=_parseNum(text); return Number.isNaN(n) ? -Infinity : n; }
      if(typ === 'date'){ const t=Date.parse(text); return Number.isNaN(t) ? -Infinity : t; }
      if(typ === 'flag'){ return /候補/.test(text) ? 1 : 0; }
      return text; // text
    }
    function wireDomSort(tableSelector){
      const table = document.querySelector(tableSelector); if(!table) return;
      const ths = Array.from(table.querySelectorAll('thead th.sortable'));
      ths.forEach((th, idx)=>{
        if(th.__wiredSort) return;
        th.__wiredSort = true;
        th.addEventListener('click', ()=>{
          // 方向トグル
          const prev = th.dataset.dir;
          ths.forEach(h=>{ h.dataset.dir=''; const a=h.querySelector('.arrow'); if(a) a.textContent=''; });
          const dir = (prev === 'asc') ? 'desc' : 'asc';
          th.dataset.dir = dir;

          // 並べ替え
          const typ = th.dataset.type || 'text';
          const rows = Array.from(table.querySelectorAll('tbody tr'));
          rows.sort((r1,r2)=>{
            const a = _sortKeyByType((r1.children[idx]?.textContent||'').trim(), typ);
            const b = _sortKeyByType((r2.children[idx]?.textContent||'').trim(), typ);
            if(a < b) return dir==='asc' ? -1 : 1;
            if(a > b) return dir==='asc' ?  1 : -1;
            return 0;
          });
          const tb = table.querySelector('tbody');
          rows.forEach(r=>tb.appendChild(r));
          const arrow = th.querySelector('.arrow'); if(arrow) arrow.textContent = (dir==='asc'?'▲':'▼');
        });
      });
    }


    /* ---------- state ---------- */
    const state = { tab:"cand", page:1, per:parseInt($("#perpage")?.value||"500",10), sortKey:null, sortDir:1, q:"", data: DATA_CAND.slice() };
    window.state = state;

    /* ---------- 既定セット ---------- */
    const DEFAULTS = { rate:3, turn:5, rvol:2 };
    function applyDefaults(on){
      const ia=$("#th_rate"), it=$("#th_turn"), ir=$("#th_rvol");
      if(!ia||!it||!ir) return;
      if(on){ ia.value=DEFAULTS.rate; it.value=DEFAULTS.turn; ir.value=DEFAULTS.rvol; }
      else{ ia.value=""; it.value=""; ir.value=""; ia.removeAttribute("value"); it.removeAttribute("value"); ir.removeAttribute("value"); }
      state.page=1; render();
    }
    function forceClearThresholds(){ const cb=$("#f_defaultset"); if(cb) cb.checked=false; applyDefaults(false); }

    /* ---------- フィルタ ---------- */
    function thRate(){ const v=num($("#th_rate")?.value); return Number.isNaN(v)?null:v; }
    function thTurn(){ const v=num($("#th_turn")?.value); return Number.isNaN(v)?null:v; }
    function thRvol(){ const v=num($("#th_rvol")?.value); return Number.isNaN(v)?null:v; }
    function applyFilter(rows){
      const q = ($("#q")?.value||"").trim();
      return rows.filter(r=>{
        const sh=hasKouho(r["初動フラグ"]), te=hasKouho(r["底打ちフラグ"]), ru=hasKouho(r["右肩上がりフラグ"]), ea=hasKouho(r["右肩早期フラグ"]);
        const etp=(String(r["右肩早期種別"]||"").trim().length>0);
        const ns=String(r["空売り機関なし_flag"]||"0")==="1";
        const op=String(r["営利対時価_flag"]||"0")==="1";
        const ht=String(r["判定"]||"")==="当たり！";

        if($("#f_shodou")?.checked && !sh) return false;
        if($("#f_tei")?.checked    && !te) return false;
        if($("#f_both")?.checked   && !(sh && ru)) return false;
        if($("#f_rightup")?.checked&& !ru) return false;
        if($("#f_early")?.checked  && !ea) return false;
        if($("#f_etype")?.checked  && !etp) return false;
        if($("#f_noshor")?.checked && !ns) return false;
        if($("#f_opratio")?.checked&& !op) return false;
        if($("#f_hit")?.checked    && !ht) return false;

        const rate=num(r["前日終値比率"]), turn=num(r["売買代金(億)"]), rvol=num(r["RVOL代金"]);
        const tr=thRate(), tt=thTurn(), tv=thRvol();
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

    /* ---------- ソート ---------- */
    function sortRows(rows){ return state.sortKey? rows.slice().sort((a,b)=>state.sortDir*cmp(a[state.sortKey],b[state.sortKey])) : rows; }

    /* ---------- ヘルプ（小窓） ---------- */
    const HELP_MAP = window.HELP_TEXT || {};
    const ALIAS = window.DATACOL_TO_HELPKEY || {};

    let _helpBackdrop=null, _helpPop=null, _helpAnchor=null;
    function ensureHelpDom(){
      if(!_helpBackdrop){
        _helpBackdrop=document.createElement("div");
        _helpBackdrop.className="help-backdrop";
        document.body.appendChild(_helpBackdrop);
        _helpBackdrop.addEventListener("click", closeHelp, {passive:true});
      }
      if(!_helpPop){
        _helpPop=document.createElement("div");
        _helpPop.className="help-pop";
        document.body.appendChild(_helpPop);
      }
    }
    function norm(s){ return String(s||"").replace(/[ \t\u3000]/g,"").replace(/\r?\n/g,"").trim(); }
    function thToKey(th){
      const dc=th?.dataset?.col, raw=(th?.textContent||"").trim();
      return (dc && ALIAS[dc]) || ALIAS[raw] || raw;
    }
    function openHelpAt(anchor){
      ensureHelpDom();
      _helpAnchor = anchor;
      const th = anchor.closest("th");
      const key = (anchor.dataset.help || thToKey(th) || "").trim();
      const title = key || (th?.textContent?.trim() || "ヘルプ");
      const html = HELP_MAP[key] || "説明準備中";

      _helpPop.innerHTML = `
        <div class="help-head">
          <div>${escapeHtml(title)}</div>
          <div class="help-close" aria-label="close">×</div>
        </div>
        <div class="help-body">${html}</div>
      `;
      _helpPop.querySelector(".help-close")?.addEventListener("click", closeHelp);

      _helpBackdrop.style.display="block";
      _helpPop.style.display="block";
      placeNearAnchor(anchor);

      window.addEventListener("scroll", onHelpMove, {passive:true});
      window.addEventListener("resize", onHelpMove);
      document.addEventListener("keydown", onHelpKeydown);
    }
    function closeHelp(){
      if(_helpPop) _helpPop.style.display="none";
      if(_helpBackdrop) _helpBackdrop.style.display="none";
      _helpAnchor=null;
      window.removeEventListener("scroll", onHelpMove);
      window.removeEventListener("resize", onHelpMove);
      document.removeEventListener("keydown", onHelpKeydown);
    }
    function onHelpKeydown(e){ if(e.key==="Escape") closeHelp(); }
    function onHelpMove(){ if(_helpAnchor) placeNearAnchor(_helpAnchor); }
    function placeNearAnchor(anchor){
      const r = anchor.getBoundingClientRect();
      const sx = window.scrollX || document.documentElement.scrollLeft;
      const sy = window.scrollY || document.documentElement.scrollTop;
      const vw = document.documentElement.clientWidth;
      const vh = document.documentElement.clientHeight;
      const gap = 10;
      const pw = _helpPop.offsetWidth, ph = _helpPop.offsetHeight;
      const spaceBottom = vh - r.bottom;
      const top = (spaceBottom > ph + gap) ? (r.bottom + gap + sy) : (r.top - ph - gap + sy);
      let left = r.left + r.width/2 - pw/2 + sx;
      left = Math.max(8 + sx, Math.min(left, sx + vw - pw - 8));
      _helpPop.style.top = `${top}px`; _helpPop.style.left = `${left}px`;
    }
    function attachHeaderHelps(tableSelector){
      document.querySelectorAll(`${tableSelector} thead th`).forEach(th=>{
        if(th.querySelector(".qhelp")) return;
        const col = th.dataset.col || th.textContent.trim();
        const key = ALIAS[col] || col;
        if(!HELP_MAP[key]) return;
        const s=document.createElement("span"); s.className="qhelp"; s.textContent="?"; s.title="ヘルプ";
        s.addEventListener("click",(e)=>{ e.stopPropagation(); openHelpAt(s); });
        s.dataset.help = key;  // 明示キー
        th.appendChild(s);
      });
    }
    function attachToolbarHelps(){
      const map = [
        ["上昇率≥",  document.getElementById("th_rate")],
        ["売買代金≥", document.getElementById("th_turn")],
        ["RVOL代金≥", document.getElementById("th_rvol")],
        ["規定",      document.getElementById("f_defaultset")]
      ];
      map.forEach(([key, el])=>{
        if(!el) return;
        if(el.parentElement.querySelector(".qhelp")) return;
        const s=document.createElement("span"); s.className="qhelp"; s.textContent="?"; s.title="ヘルプ";
        s.addEventListener("click", ()=> openHelpAt(s));
        s.dataset.help = key;
        el.parentElement.appendChild(s);
      });
    }
    function escapeHtml(s){ return String(s).replace(/[&<>"']/g, m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m])); }

    /* ---------- 描画 ---------- */
    function renderCand(){
      const body = $("#tbl-candidate tbody"); if(!body) return;
      const rows = sortRows(applyFilter(state.data));
      const total = rows.length, per = state.per, maxPage = Math.max(1, Math.ceil(total/per));
      state.page = Math.min(state.page, maxPage);
      const s = (state.page-1)*per, e = Math.min(s+per, total);

      let html = "";
      for (let i = s; i < e; i++) {
        const r = rows[i] || {};
        const hit = String(r["判定"] || "") === "当たり！";

        // 早期種別バッジ
        const et = (r["右肩早期種別"] || "").trim();
        let etBadge = et;
        if (et === "ブレイク") etBadge = '<span class="badge b-green">● ブレイク</span>';
        else if (et === "20MAリバ") etBadge = '<span class="badge b-green">● 20MAリバ</span>';
        else if (et === "ポケット") etBadge = '<span class="badge b-orange">● ポケット</span>';
        else if (et === "200MAリクレイム") etBadge = '<span class="badge b-yellow">● 200MAリクレイム</span>';

        // 推奨バッジ
        const rec = (r["推奨アクション"] || "").trim();
        let recBadge = "";
        if (rec === "エントリー有力")      recBadge = '<span class="rec-badge rec-strong" title="エントリー有力"><span class="rec-dot"></span>有力</span>';
        else if (rec === "小口提案")        recBadge = '<span class="rec-badge rec-small"  title="小口提案"><span class="rec-dot"></span>小口</span>';
        else if (rec)                       recBadge = `<span class="rec-badge rec-watch"  title="${rec.replace(/"/g,'&quot;')}"><span class="rec-dot"></span>${rec}</span>`;

        html += `<tr${hit ? " class='hit'" : ""}>
          <td>${r["コード"] ?? ""}</td>
          <td>${r["銘柄名"] ?? ""}</td>
          <td><a href="${r["yahoo_url"] ?? "#"}" target="_blank" rel="noopener">Yahoo</a></td>
          <td><a href="${r["x_url"] ?? "#"}" target="_blank" rel="noopener">X検索</a></td>
          <td class="num">${r["現在値"] ?? ""}</td>
          <td class="num">${r["前日終値"] ?? ""}</td>
          <td class="num">${r["前日円差"] ?? ""}</td>
          <td class="num">${r["前日終値比率"] ?? ""}</td>
          <td class="num">${r["出来高"] ?? ""}</td>
          <td class="num">${r["売買代金(億)"] ?? ""}</td>
          <td>${r["初動フラグ"] || ""}</td>
          <td>${r["底打ちフラグ"] || ""}</td>
          <td>${r["右肩上がりフラグ"] || ""}</td>
          <td>${r["右肩早期フラグ"] || ""}</td>
          <td class="num">${r["右肩早期スコア"] ?? ""}</td>
          <td>${etBadge}${r["右肩早期種別_mini"] || ""}</td>
          <td>${r["判定"] || ""}</td>
          <td>${r["判定理由"] || ""}</td>
          <td>${recBadge}</td>
          <td class="num">${r["推奨比率"] ?? ""}</td>
          <td>${r["シグナル更新日"] || ""}</td>
        </tr>`;
      }

      body.innerHTML = html;
      $("#count").textContent = String(total);
      $("#pageinfo").textContent = `${state.page} / ${Math.max(1, Math.ceil(total/per))}`;

      $$("#tbl-candidate thead th.sortable").forEach(th=>{
        th.querySelector(".arrow").textContent =
          (th.dataset.col === state.sortKey ? (state.sortDir > 0 ? "▲" : "▼") : "");
      });
      $$("#tbl-candidate tbody tr").forEach(tr=>{
        tr.addEventListener("click",(e)=>{ if (e.target.closest("a")) return; openRowModal(tr); });
      });

      // ヘルプ
      attachHeaderHelps("#tbl-candidate");
      attachToolbarHelps();
    }

    function renderAll(){
      const head=$("#all-head"), body=$("#all-body"); if(!head||!body) return;
      head.innerHTML=body.innerHTML="";
      const rows=DATA_ALL; if(!rows.length) return;
      const cols=Object.keys(rows[0]);
      head.innerHTML=cols.map(c=>{
        const typ=(c.includes("フラグ")?"flag":(c.includes("日")||c.includes("更新")||c==="日時"?"date":(["現在値","出来高","売買代金(億)","時価総額億円","右肩早期スコア","推奨比率"].includes(c)?"num":"text")));
        return `<th class="sortable ${typ==='num'?'num':''}" data-col="${c}" data-type="${typ}">${c}<span class="arrow"></span></th>`;
      }).join("");
      body.innerHTML=rows.slice(0,2000).map(r=>`<tr>${cols.map(c=>`<td class="${['現在値','出来高','売買代金(億)','時価総額億円','右肩早期スコア','推奨比率'].includes(c)?'num':''}">${r[c]??""}</td>`).join("")}</tr>`).join("");
      attachHeaderHelps("#tbl-allcols");
      wireDomSort("#tbl-allcols");
    }

    function render(){ if(state.tab==="cand") renderCand(); else if(state.tab==="all") renderAll(); }
    window.render=render;

    /* ---------- 行モーダル ---------- */
    function ensureModal(){
      let back=$("#__row_back__"); if(back) return back;
      back=document.createElement("div"); back.id="__row_back__"; back.className="help-backdrop";
      const box=document.createElement("div");
      box.className="help-pop";
      box.innerHTML=`<div class="help-head"><div>詳細</div><div class="help-close">×</div></div><div id="__row_body__"></div>`;
      document.body.appendChild(back); document.body.appendChild(box);
      back.addEventListener("click",()=>{ box.style.display="none"; back.style.display="none"; });
      box.querySelector(".help-close").addEventListener("click",()=>{ box.style.display="none"; back.style.display="none"; });
      return back;
    }
    function openRowModal(tr){
      const back=ensureModal(), body=$("#__row_body__");
      const headers=Array.from($("#tbl-candidate thead").querySelectorAll("th"));
      const tds=Array.from(tr.children);
      let html='<div style="display:grid;grid-template-columns:160px 1fr;gap:8px 12px;">';
      tds.forEach((td,i)=>{ const h=headers[i]?.dataset?.col||headers[i]?.innerText||""; html+=`<div style="color:#6b7280">${h}</div><div>${(td.innerHTML||"").trim()}</div>`; });
      html+='</div>'; body.innerHTML=html;
      back.style.display="block";
      const box=document.querySelector(".help-pop"); box.style.display="block"; // 直近のhelp-popを流用
      const vw=document.documentElement.clientWidth, sx=window.scrollX||0, sy=window.scrollY||0;
      box.style.top = `${sy+80}px`; box.style.left=`${sx+Math.max(20,(vw-940)/2)}px`;
    }

    /* ---------- 簡易グラフ ---------- */
    function ensureChartModal(){
      let back=$("#__chart_back__"); if(back) return back;
      back=document.createElement("div"); back.id="__chart_back__"; back.className="help-backdrop";
      const box=document.createElement("div");
      box.className="help-pop";
      box.innerHTML=`<div class="help-head"><div>グラフ</div><div class="help-close">×</div></div><div id="__chart_body__"></div>`;
      document.body.appendChild(back); document.body.appendChild(box);
      back.addEventListener("click",()=>{ box.style.display="none"; back.style.display="none"; });
      box.querySelector(".help-close").addEventListener("click",()=>{ box.style.display="none"; back.style.display="none"; });
      return back;
    }
    function drawAxes(ctx,W,H,pad){ ctx.strokeStyle="#ccc"; ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(pad,H-pad); ctx.lineTo(W-pad,H-pad); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(pad,H-pad); ctx.lineTo(pad,pad); ctx.stroke(); }
    function drawBar(canvas,labels,values,title){
      const ctx=canvas.getContext("2d"), W=canvas.width, H=canvas.height, pad=40;
      ctx.clearRect(0,0,W,H); ctx.fillStyle="#000"; ctx.font="14px system-ui"; ctx.fillText(title,pad,24); drawAxes(ctx,W,H,pad);
      if(!values.length) return; const max=Math.max(1,Math.max(...values)); const bw=(W-pad*2)/values.length*0.7;
      labels.forEach((lb,i)=>{ const x=pad+(i+0.15)*(W-pad*2)/labels.length; const h=(H-pad*2)*(values[i]/max);
        ctx.fillStyle="#4a90e2"; ctx.fillRect(x,H-pad-h,bw,h);
        ctx.fillStyle="#333"; ctx.font="12px system-ui"; ctx.fillText(lb,x,H-pad+14); ctx.fillText(String(values[i]),x,H-pad-h-4); });
    }
    function drawLine(canvas,labels,values,title){
      const ctx=canvas.getContext("2d"), W=canvas.width, H=canvas.height, pad=40;
      ctx.clearRect(0,0,W,H); ctx.fillStyle="#000"; ctx.font="14px system-ui"; ctx.fillText(title,pad,24); drawAxes(ctx,W,H,pad);
      if(!values.length) return; const max=Math.max(1,Math.max(...values)), min=Math.min(0,Math.min(...values)); const step=(W-pad*2)/Math.max(1,values.length-1);
      ctx.strokeStyle="#4a90e2"; ctx.lineWidth=2; ctx.beginPath();
      values.forEach((v,i)=>{ const x=pad+i*step; const y=H-pad-(H-pad*2)*((v-min)/(max-min||1)); if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke();
      ctx.fillStyle="#333"; ctx.font="12px system-ui"; labels.forEach((lb,i)=>{ const x=pad+i*step; ctx.fillText(lb,x-10,H-pad+14); });
    }
    function openStatsChart(){
      const back=ensureChartModal(), body=$("#__chart_body__");
      const rows=applyFilter(state.data);
      const rvolBuckets=["<1","1-2","2-3","3-5","5+"], rvolCnt=[0,0,0,0,0];
      const turnBuckets=["<5","5-10","10-50","50-100","100+"], turnCnt=[0,0,0,0,0];
      rows.forEach(r=>{ const rvol=num(r["RVOL代金"]); if(!Number.isNaN(rvol)){ if(rvol<1)rvolCnt[0]++; else if(rvol<2)rvolCnt[1]++; else if(rvol<3)rvolCnt[2]++; else if(rvol<5)rvolCnt[3]++; else rvolCnt[4]++; }
                        const turn=num(r["売買代金(億)"]); if(!Number.isNaN(turn)){ if(turn<5)turnCnt[0]++; else if(turn<10)turnCnt[1]++; else if(turn<50)turnCnt[2]++; else if(turn<100)turnCnt[3]++; else turnCnt[4]++; } });
      body.innerHTML=`<h3>傾向グラフ（表示中データ）</h3><canvas id="cv1" width="940" height="320"></canvas><canvas id="cv2" width="940" height="320" style="margin-top:16px;"></canvas>`;
      const c1=body.querySelector("#cv1"), c2=body.querySelector("#cv2");
      drawBar(c1,rvolBuckets,rvolCnt,"RVOL代金の分布"); drawBar(c2,turnBuckets,turnCnt,"売買代金(億)の分布");
      back.style.display="block"; const box=back.nextElementSibling; box.style.display="block"; const sy=window.scrollY||0; box.style.top=`${sy+80}px`; box.style.left=`${Math.max(20,(document.documentElement.clientWidth-940)/2)}px`;
    }
    function openTrendChart(){
      const back=ensureChartModal(), body=$("#__chart_body__");
      const rows=applyFilter(state.data); const byDay=new Map();
      rows.forEach(r=>{ const d=String(r["シグナル更新日"]||"").slice(0,10); if(!d) return; const hit=(String(r["判定"]||"")==="当たり！")?1:0; const o=byDay.get(d)||{tot:0,hit:0}; o.tot++; o.hit+=hit; byDay.set(d,o); });
      const days=Array.from(byDay.keys()).sort(); const rate=days.map(d=>{ const o=byDay.get(d); return o&&o.tot?Math.round(1000*o.hit/o.tot)/10:0; });
      body.innerHTML=`<h3>推移グラフ（日別 当たり率 %）</h3><canvas id="cv3" width="940" height="320"></canvas>`;
      drawLine(body.querySelector("#cv3"),days,rate,"当たり率（%）");
      back.style.display="block"; const box=back.nextElementSibling; box.style.display="block"; const sy=window.scrollY||0; box.style.top=`${sy+80}px`; box.style.left=`${Math.max(20,(document.documentElement.clientWidth-940)/2)}px`;
    }

    /* ---------- イベント ---------- */
    $$("#tbl-candidate thead th.sortable").forEach(th=>{
      th.style.cursor="pointer";
      th.addEventListener("click",()=>{ const key=th.dataset.col; if(state.sortKey===key) state.sortDir*=-1; else{ state.sortKey=key; state.sortDir=1; } state.page=1; render(); });
    });
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

    /* ---------- タブ ---------- */
    function switchTab(to){
      state.tab=to; $$(".tab").forEach(x=>x.classList.add("hidden"));
      if(to==="cand"){ $("#tab-candidate").classList.remove("hidden"); state.data=DATA_CAND.slice(); }
      if(to==="all"){  $("#tab-all").classList.remove("hidden"); }
      if(to==="log"){  $("#tab-log").classList.remove("hidden"); if(!$("#log-body").dataset.inited){ $("#log-body").innerHTML=DATA_LOG.map(r=>`<tr><td>${r["日時"]||""}</td><td>${r["コード"]||""}</td><td>${r["種別"]||""}</td><td>${r["詳細"]||""}</td></tr>`).join(""); $("#log-body").dataset.inited="1"; } }
      state.page=1; render();
      document.querySelectorAll("nav a").forEach(a=>a.classList.remove("active"));
      if(to==="cand") $("#lnk-cand")?.classList.add("active");
      if(to==="all")  $("#lnk-all") ?.classList.add("active");
      if(to==="log")  $("#lnk-log") ?.classList.add("active");
    }
    $("#lnk-cand")?.addEventListener("click",(e)=>{ e.preventDefault(); switchTab("cand"); });
    $("#lnk-all") ?.addEventListener("click",(e)=>{ e.preventDefault(); switchTab("all");  });
    $("#lnk-log") ?.addEventListener("click",(e)=>{ e.preventDefault(); switchTab("log");  });
    
    
    // 直近日（EOD実行日）を求める
    function latestUpdateDate(rows){
      const ds = rows.map(r=>String(r["シグナル更新日"]||"").slice(0,10)).filter(Boolean);
      return ds.sort().pop() || null;
    }

    // 「明日用」抽出（直近日に更新 & 初動/右肩/早期のいずれかが候補）
    function toTomorrowRows(src){
      if(!src.length) return [];
      const d0 = latestUpdateDate(src);
      if(!d0) return [];
      return src.filter(r=>{
        const d = String(r["シグナル更新日"]||"").slice(0,10);
        if(d !== d0) return false;
        const sh = String(r["初動フラグ"]||"").includes("候補");
        const ru = String(r["右肩上がりフラグ"]||"").includes("候補");
        const ea = String(r["右肩早期フラグ"]||"").includes("候補");
        return sh || ru || ea;
      });
    }

    function renderTomorrow(){
      const body = document.querySelector("#tbl-tmr tbody");
      if(!body) return;

      // ① 見出し（📅 YYYY-MM-DD 向け）
      const md = (RAW.meta || {});
      const baseStr = md.base_day || latestUpdateDate(DATA_CAND) || null;
      let targetStr = md.next_business_day || null;

      // フォールバック（Python側で祝日計算できない場合）：土日のみスキップ
      if(!targetStr && baseStr){
        const dt = new Date(baseStr);
        dt.setDate(dt.getDate() + 1);
        while([0,6].includes(dt.getDay())) dt.setDate(dt.getDate() + 1);
        targetStr = dt.toISOString().slice(0,10);
      }
      const lbl = document.getElementById("tmr-label");
      if(lbl) lbl.textContent = targetStr ? `📅 ${targetStr} 向け` : "📅 明日用（日付未取得）";

      // ② データ並び：推奨 > 早期S > 売買代金(億)
      const rows = toTomorrowRows(DATA_CAND).sort((a,b)=>{
        const rank = (x)=> x==="エントリー有力" ? 2 : (x==="小口提案" ? 1 : 0);
        const r  = rank((b["推奨アクション"]||"").trim()) - rank((a["推奨アクション"]||"").trim());
        if(r!==0) return r;
        const s  = (+b["右肩早期スコア"]||0) - (+a["右肩早期スコア"]||0);
        if(s!==0) return s;
        return (+b["売買代金(億)"]||0) - (+a["売買代金(億)"]||0);
      });

      body.innerHTML = rows.map(r=>{
        const et = (r["右肩早期種別"]||"").trim();
        const etBadge =
          et==="ブレイク" ? '<span class="badge b-green">● ブレイク</span>' :
          et==="20MAリバ" ? '<span class="badge b-green">● 20MAリバ</span>' :
          et==="ポケット" ? '<span class="badge b-orange">● ポケット</span>' :
          et==="200MAリクレイム" ? '<span class="badge b-yellow">● 200MAリクレイム</span>' : et;

        const rec = (r["推奨アクション"]||"").trim();
        const recBadge =
          rec==="エントリー有力" ? '<span class="rec-badge rec-strong"><span class="rec-dot"></span>有力</span>' :
          rec==="小口提案"       ? '<span class="rec-badge rec-small"><span class="rec-dot"></span>小口</span>' :
          (rec ? `<span class="rec-badge rec-watch"><span class="rec-dot"></span>${rec}</span>` : "");

        return `<tr>
          <td>${r["コード"]??""}</td>
          <td>${r["銘柄名"]??""}</td>
          <td><a href="${r["yahoo_url"]??"#"}" target="_blank" rel="noopener">Yahoo</a></td>
          <td><a href="${r["x_url"]??"#"}" target="_blank" rel="noopener">X検索</a></td>
          <td class="num">${r["現在値"]??""}</td>
          <td class="num">${r["前日終値比率"]??""}</td>
          <td class="num">${r["売買代金(億)"]??""}</td>
          <td class="num">${r["右肩早期スコア"]??""}</td>
          <td>${etBadge}${r["右肩早期種別_mini"]||""}</td>
          <td>${r["判定"]||""}</td>
          <td>${recBadge}</td>
        </tr>`;
      }).join("");

      // 行クリックでモーダル（既存の openRowModal がある前提）
      document.querySelectorAll("#tbl-tmr tbody tr").forEach(tr=>{
        tr.addEventListener("click",(e)=>{ if (e.target.closest("a")) return; openRowModal(tr); });
      });

      // ヘッダーに ? 付与（関数がある場合だけ）
      if (typeof attachHeaderHelps === "function") attachHeaderHelps("#tbl-tmr");
      wireDomSort("#tbl-tmr");
    }

    // タブ切替に「tmr」を追加
    const _oldSwitchTab = switchTab;
    switchTab = function(to){
      _oldSwitchTab(to);
      if(to==="tmr"){
        document.querySelectorAll(".tab").forEach(x=>x.classList.add("hidden"));
        document.getElementById("tab-tmr")?.classList.remove("hidden");
        renderTomorrow();
        document.querySelectorAll("nav a").forEach(a=>a.classList.remove("active"));
        document.getElementById("lnk-tmr")?.classList.add("active");
      }
    };
    // クリックハンドラ
    document.getElementById("lnk-tmr")?.addEventListener("click",(e)=>{
      e.preventDefault(); switchTab("tmr");
    });



    /* ---------- 初期化 ---------- */
    switchTab("cand");
    forceClearThresholds();
    setTimeout(forceClearThresholds,150);
    window.addEventListener("pageshow",(ev)=>{ if(ev.persisted) forceClearThresholds(); });

    // 初回：ヘッダー/ツールバーに ? を付与
    attachHeaderHelps("#tbl-candidate");
    attachToolbarHelps();

    // 表DOM変化に追従（再描画で ? が消えるのを防ぐ）
    const mo = new MutationObserver(()=>{ attachHeaderHelps("#tbl-candidate"); attachToolbarHelps(); });
    mo.observe(document.body, {childList:true, subtree:true});

  })();
  </script>
</body>
</html>"""




# =================== HTMLテンプレートの保存 ===================
def _ensure_template_file(template_dir: str, overwrite=True):
    import os
    os.makedirs(template_dir, exist_ok=True)
    path = os.path.join(template_dir, "dashboard.html")
    if overwrite or not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(DASH_TEMPLATE_STR)
    return path


# =================== ダッシュボード書き出し（完全版） ===================
def phase_export_html_dashboard_offline(conn, html_path, template_dir="templates",
                                        include_log: bool=False, log_limit: int=2000):
    """
    ・候補/全カラム/（任意）signals_log を 1ファイルHTMLで出力
    ・表描画はフロント側JS。ここでは “JSONを __DATA__ に直埋め” する
    ・DBに存在しない列（推奨アクション等）は参照しないので、DB差異でも落ちない
    """
    import os, json, pandas as pd
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    # ---------- 1) DBから候補用の必要最小列だけ取得（存在しない列は触らない） ----------
    df_cand = pd.read_sql_query("""
      SELECT
        コード, 銘柄名,
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
        営業利益
      FROM screener
      ORDER BY COALESCE(時価総額億円,0) DESC, COALESCE(出来高,0) DESC, コード
    """, conn)

    # 全カラムタブは素直に全件（DB差異を吸収したいので * でOK）
    df_all = pd.read_sql_query("""
      SELECT * FROM screener
      ORDER BY COALESCE(時価総額億円,0) DESC, COALESCE(出来高,0) DESC, コード
    """, conn)

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
        return f"<br><span style='font-size:10px;color:gray'>{text}</span>"

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
    from datetime import datetime  # 既に import 済みなら不要

    def _to_date10(s):
        s = str(s or '')[:10]
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
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
    from datetime import date, datetime, timedelta

    try:
        import jpholiday
        def _is_holiday(d: date) -> bool:
            return (d.weekday() >= 5) or jpholiday.is_holiday(d)
    except Exception:
        # jpholiday が無い環境では土日のみ補正
        def _is_holiday(d: date) -> bool:
            return d.weekday() >= 5

    def _to_date(s) -> date | None:
        if not s:
            return None
        s = str(s)[:10]  # "YYYY-MM-DD ..." 形式にも対応
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _latest_update_day(rows) -> date | None:
        try:
            if hasattr(rows, "iterrows"):  # pandas.DataFrame
                dates = [_to_date(r.get("シグナル更新日")) for _, r in rows.iterrows()]
            else:                           # list[dict]
                dates = [_to_date(r.get("シグナル更新日")) for r in rows]
            dates = [d for d in dates if d]
            return max(dates) if dates else None
        except Exception:
            return None

    def _next_business_day(d: date) -> date:
        d = d + timedelta(days=1)
        while _is_holiday(d):
            d += timedelta(days=1)
        return d

    _base = _latest_update_day(cand_rows)  # ← cand_rows は既存の候補リスト/DF
    meta = {"base_day": None, "next_business_day": None}
    if _base:
        _next = _next_business_day(_base)
        meta["base_day"] = _base.strftime("%Y-%m-%d")
        meta["next_business_day"] = _next.strftime("%Y-%m-%d")

    # ---------- 4) JSON を __DATA__ に直埋め ----------
    data_obj = {"cand": cand_rows, "all": all_rows, "logs": log_rows, "meta": meta}  # ← 追加
    data_json = json.dumps(data_obj, ensure_ascii=False, separators=(",", ":"))



    # ---------- 5) テンプレート描画 ----------
    _ensure_template_file(template_dir, overwrite=True)
    env = Environment(loader=FileSystemLoader(template_dir, encoding="utf-8"),
                      autoescape=select_autoescape(["html"]))
    # Jinjaのフィルタは今回は未使用だが、他テンプレ互換のため残す
    env.filters["fmt_cell"] = _fmt_cell

    tpl = env.get_template("dashboard.html")
    html = tpl.render(
        include_log=include_log,
        data_json=data_json,                         # ← ここが重要：JSが読むJSON
        generated_at=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # ---------- 6) 書き出し ----------
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8", newline="") as f:
        f.write(html)
    print(f"[export] HTML書き出し: {html_path} (logs={'ON' if include_log else 'OFF'})")

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
        from yahooquery import Ticker
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

        tq = Ticker(symbols, asynchronous=True, max_workers=max_workers)

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
        from email.message import EmailMessage
        import smtplib, ssl, zipfile
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

# ===== 任意CSV出力 =====
def phase_output_and_notify(conn: sqlite3.Connection):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 銘柄名, 初動フラグ, 底打ちフラグ, 右肩上がりフラグ, 上昇余地スコア, 右肩上がりスコア
        FROM screener
        WHERE 初動フラグ='候補' OR 底打ちフラグ='候補' OR 右肩上がりフラグ='候補'
        ORDER BY COALESCE(右肩上がりスコア,0) DESC, COALESCE(上昇余地スコア,0) DESC
        LIMIT 300
    """)
    rows = cur.fetchall(); cur.close()
    if not rows: return
    out_path = os.path.join(OUTPUT_DIR, f"signals_{today_str()}.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        f.write("コード,銘柄名,初動,底打ち,右肩上がり,上昇余地スコア,右肩上がりスコア\n")
        for code, name, sh, bt, tob, sc, ts in rows:
            name_q = f"\"{name}\"" if name and "," in str(name) else str(name or "")
            f.write(f"{code},{name_q},{sh or ''},{bt or ''},{tob or ''},{sc if sc is not None else ''},{ts if ts is not None else ''}\n")
    try:
        notification.notify(title="シグナル抽出完了", message=f"{len(rows)}件を出力", timeout=5)
    except Exception:
        pass

# --- ここから: 前日終値アップデータ（履歴ベース／唯一の定義） ---
# ---- 前日終値比率(%)＋現在値から前日終値を逆算してDB更新 ----

def ensure_prevclose_columns(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(screener)")
    cols = {r[1] for r in cur.fetchall()}

    # 必須列だけ保証
    if "前日終値" not in cols:
        cur.execute('ALTER TABLE screener ADD COLUMN "前日終値" REAL')
    if "前日円差" not in cols:
        cur.execute('ALTER TABLE screener ADD COLUMN "前日円差" REAL')

    conn.commit(); cur.close()


def _to_raw(v):
    if isinstance(v, dict):
        return v.get("raw", v.get("fmt"))
    return v

def update_prevclose_from_quotes(conn, codes, batch_size=200, max_workers=8):
    """
    quotes.regularMarketPrice（現在値）と quotes.regularMarketChangePercent（前日終値比率％）
    から 前日終値 を逆算し、DB: 前日終値 / 前日円差 / 前日終値比率(％:小数2桁) を更新する。
    - ％が無い場合は regularMarketChange（円）→ now - change で計算
    - それも無い場合のみ previousClose を保険で使用
    """
    try:
        from yahooquery import Ticker
    except ImportError:
        print("[prevclose-derive] yahooquery 未導入のためスキップ")
        return

    ensure_prevclose_columns(conn)

    code4s = [str(c).zfill(4) for c in codes if str(c).strip()]
    if not code4s:
        print("[prevclose-derive] 対象なし")
        return

    total = 0
    cur = conn.cursor()

    for i in range(0, len(code4s), batch_size):
        chunk = code4s[i:i+batch_size]
        syms = [f"{c}.T" for c in chunk]

        tq = Ticker(syms, formatted=False, asynchronous=True, max_workers=max_workers)
        q = tq.quotes if isinstance(tq.quotes, dict) else {}

        updates = []
        for code in chunk:
            sym = f"{code}.T"
            data = q.get(sym, {}) or {}

            now  = _to_raw(data.get("regularMarketPrice"))
            pct  = _to_raw(data.get("regularMarketChangePercent"))
            chg  = _to_raw(data.get("regularMarketChange"))  # 円
            pcls = _to_raw(data.get("regularMarketPreviousClose"))  # 保険

            # 数値化
            try: now  = float(now)  if now  is not None else None
            except: now = None
            try: pct  = float(pct)  if pct  is not None else None
            except: pct = None
            try: chg  = float(chg)  if chg  is not None else None
            except: chg = None
            try: pcls = float(pcls) if pcls is not None else None
            except: pcls = None

            if now is None:
                continue

            # 逆算ロジック
            if pct is not None:
                prev = now / (1.0 + pct/100.0)
            elif chg is not None:
                prev = now - chg
            else:
                prev = pcls  # 最後の保険

            if prev is None or prev == 0:
                continue

            # 丸め（前日終値は円単位に丸め、％は小数第2位まで）
            prev_rounded = float(int(round(prev)))
            diff_yen = float(int(round(now - prev_rounded)))
            pct_rounded = round(((now - prev_rounded) / prev_rounded) * 100.0, 2)

            updates.append((prev_rounded, diff_yen, pct_rounded, code))

        if updates:
            cur.executemany(
                'UPDATE screener SET "前日終値"=?, "前日円差"=?, 前日終値比率=? WHERE コード=?',
                updates
            )
            conn.commit()
            total += len(updates)
            print(f"[prevclose-derive] updated {len(updates)} / {len(chunk)} (chunk {i//batch_size+1})")
        else:
            print(f"[prevclose-derive] no updates in chunk ({len(chunk)})")

    cur.close()
    print(f"[prevclose-derive] 合計更新 {total} 件")


# --- ここまで ---



# ==== 簡易正規化（_normalize_map が無い場合の保険）====
def _normalize_map_simple(sd_raw):
    """
    yahooquery の返却（dict / list / DataFrame）を
    { 'XXXX.T': { ... } } 形式に寄せる簡易版。
    """
    try:
        # DataFrameパターン (pandas)
        if hasattr(sd_raw, "reset_index"):
            df = sd_raw.reset_index()
            m = {}
            if "symbol" in df.columns:
                # asOfDate があれば最新順、無ければそのまま
                key = "asOfDate" if "asOfDate" in df.columns else None
                for sym, g in df.groupby("symbol"):
                    gg = g.dropna(subset=[key]) if key else g
                    gg = gg.sort_values(key) if key else gg
                    if len(gg):
                        m[sym] = gg.iloc[-1].to_dict()
            return m

        # dict（銘柄→辞書 or 銘柄→リスト[辞書...]）パターン
        if isinstance(sd_raw, dict):
            m = {}
            for k, v in sd_raw.items():
                if isinstance(v, list) and v:
                    # 末尾を優先
                    vv = v[-1] if isinstance(v[-1], dict) else v[0]
                    if isinstance(vv, dict):
                        m[k] = vv
                elif isinstance(v, dict):
                    m[k] = v
            return m
    except Exception:
        pass
    return {}


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
import sqlite3
from datetime import datetime, timedelta, timezone

def _ensure_turnover_cols(conn: sqlite3.Connection):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)")}
    cur = conn.cursor()
    if "売買代金億" not in cols:
        cur.execute("ALTER TABLE screener ADD COLUMN 売買代金億 REAL")
    if "売買代金20日平均億" not in cols:
        cur.execute("ALTER TABLE screener ADD COLUMN 売買代金20日平均億 REAL")
    if "RVOL代金" not in cols:
        cur.execute("ALTER TABLE screener ADD COLUMN RVOL代金 REAL")
    conn.commit()
    cur.close()

def _jp_session_progress(dt: datetime | None = None) -> float:
    """場中の進捗率(0.0〜1.0)。東証 9:00–11:30 / 12:30–15:00 を300分=1.0で換算。"""
    if dt is None:
        dt = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=9)  # JST
    m = dt.hour * 60 + dt.minute
    s1, e1, s2, e2 = 9*60, 11*60+30, 12*60+30, 15*60
    if m < s1: return 0.0
    if s1 <= m <= e1: return (m - s1) / 300.0
    if e1 < m < s2:  return 150 / 300.0
    if s2 <= m <= e2: return (150 + (m - s2)) / 300.0
    return 1.0

def apply_auto_metrics_midday(conn: sqlite3.Connection,
                              use_time_progress: bool = True,
                              denom_floor: float = 1.0,       # ← ここが“分母の床”
                              progress_floor: float = 0.33):   # ← 進捗の下限（寄り直後の暴れ抑制）
    """
    現在値×出来高→売買代金億、RVOL代金を更新。
    RVOLの分母= max(売買代金20日平均億, denom_floor) * progress
    """
    _ensure_turnover_cols(conn)
    cur = conn.cursor()

    # 売買代金（億）= 現在値×出来高/1e8
    cur.execute("""
        UPDATE screener
        SET 売買代金億 =
          CASE WHEN 現在値 IS NOT NULL AND 出来高 IS NOT NULL
               THEN (現在値 * 出来高) / 100000000.0 END
    """)

    # RVOL = 当日代金 / max(20日平均代金, denom_floor) / progress
    if use_time_progress:
        f = max(_jp_session_progress(), progress_floor)
        cur.execute("""
            WITH p AS (SELECT ? AS f, ? AS dmin)
            UPDATE screener
            SET RVOL代金 =
              CASE
                WHEN 売買代金億 IS NOT NULL AND 売買代金20日平均億 IS NOT NULL
                THEN ROUND(
                  売買代金億 /
                  ((CASE WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                         THEN (SELECT dmin FROM p)
                         ELSE 売買代金20日平均億 END) * (SELECT f FROM p)),
                  3
                )
              END
        """, (f, denom_floor))
    else:
        cur.execute("""
            WITH p AS (SELECT ? AS dmin)
            UPDATE screener
            SET RVOL代金 =
              CASE
                WHEN 売買代金億 IS NOT NULL AND 売買代金20日平均億 IS NOT NULL
                THEN ROUND(
                  売買代金億 /
                  (CASE WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                        THEN (SELECT dmin FROM p)
                        ELSE 売買代金20日平均億 END),
                  3
                )
              END
        """, (denom_floor,))
    conn.commit()
    cur.close()

def apply_auto_metrics_eod(conn: sqlite3.Connection,
                           denom_floor: float = 1.0):
    """
    終値×出来高で当日代金を確定 → 20日平均を更新 → RVOLを更新。
    RVOLの分母= max(20日平均代金, denom_floor)（progress=1.0）
    """
    _ensure_turnover_cols(conn)
    cur = conn.cursor()

    #-- 当日確定 代金（億）
    cur.execute("""
        UPDATE screener AS s
        SET 売買代金億 = (
          SELECT (ph.終値 * COALESCE(ph.出来高,0)) / 100000000.0
          FROM price_history ph
          WHERE ph.コード = s.コード
          ORDER BY 日付 DESC
          LIMIT 1
        )
    """)

    #-- 20日平均（直近20本の単純平均）
    cur.execute("""
        WITH lastdate AS (
          SELECT コード, MAX(日付) AS mx FROM price_history GROUP BY コード
        ),
        hist AS (
          SELECT ph.コード,
                 (ph.終値 * COALESCE(ph.出来高,0)) / 100000000.0 AS 代金億,
                 ROW_NUMBER() OVER (PARTITION BY ph.コード ORDER BY ph.日付 DESC) AS rn
          FROM price_history ph
          JOIN lastdate ld ON ph.コード = ld.コード AND ph.日付 <= ld.mx
        ),
        avg20 AS (
          SELECT コード, AVG(代金億) AS avg20
          FROM hist
          WHERE rn <= 20
          GROUP BY コード
        )
        UPDATE screener
        SET 売買代金20日平均億 = (SELECT avg20 FROM avg20 WHERE avg20.コード = screener.コード)
    """)

    #-- RVOL（分母に床）
    cur.execute("""
        WITH p AS (SELECT ? AS dmin)
        UPDATE screener
        SET RVOL代金 =
          CASE
            WHEN 売買代金億 IS NOT NULL AND 売買代金20日平均億 IS NOT NULL
            THEN ROUND(
              売買代金億 /
              (CASE WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                    THEN (SELECT dmin FROM p)
                    ELSE 売買代金20日平均億 END),
              3
            )
          END
    """, (denom_floor,))

    conn.commit()
    cur.close()



# ===== /RVOL/売買代金 自動更新ユーティリティ =====
import os
import time
import webbrowser
from pathlib import Path

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
        time.sleep(wait_sec)

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

import numpy as np, pandas as pd, sqlite3

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



import sqlite3

def apply_atr14_pct(conn: sqlite3.Connection) -> int:
    """
    price_history から TR→ATR14→ATR14% を算出し、screener.ATR14_PCT を更新する。
      TR  = max(高値-安値, |高値-前日終値|, |安値-前日終値|)
      ATR14% = ATR14 / 直近終値 * 100
    戻り値: 更新対象になった銘柄数（目安値）
    """
    # 1) カラムが無ければ追加
    cur = conn.cursor()
    cols = {r[1] for r in cur.execute("PRAGMA table_info(screener)")}
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
    """Add 'since date' columns for flags and early-type history if missing."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    add = []
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

    from datetime import datetime
    def dstr(d): return d.strftime("%Y-%m-%d")
    try:
        latest = datetime.strptime(rows[0][:10], "%Y-%m-%d").date()
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

    from datetime import datetime
    cur_tag = None
    cur_dates = []
    prev_tag = None
    for dt_s, detail in rows:
        tag = _parse_early_tag(detail)
        d = datetime.strptime(dt_s[:10], "%Y-%m-%d").date()
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



# ===== タイマーユーティリティ =====
from datetime import datetime, time as dtime

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
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Asia/Tokyo")).time()
    except Exception:
        now = datetime.now().time()

    return "MIDDAY" if _t(11,30) <= now < _t(12,30) else "EOD"


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

    # (1) DB open & スキーマ保証
    conn = open_conn(DB_PATH)
    _timed("ensure_schema", ensure_schema, conn)
    _timed("ensure_signal_schema", ensure_signal_schema, conn)
    _timed("ensure_since_schema", ensure_since_schema, conn)

    # (2) 休場日チェック（必要なら有効化）
    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    # if is_jp_market_holiday(date.today(), extra_closed):
    #     print(f"日本の休場日（{date.today()}）のためスキップします。")
    #     print("=== 終了 ==="); return

    # (3) CSV取り込み（コード・銘柄名・市場・登録日のみ）
    if USE_CSV:
        try:
            _timed("phase_csv_import", phase_csv_import, conn, overwrite_registered_date=False)
        except Exception as e:
            print("[csv-import][WARN]", e)

    # (4) 上場廃止/空売り無しの反映
    try:
        _timed("phase_delist_cleanup", phase_delist_cleanup, conn)
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
            now = datetime.now().time()
            if now < dtime(12,30):
                _timed("update_market_cap_all", update_market_cap_all, conn, batch_size=150, max_workers=8)
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

        # (7) 数値の正規化
        try:
            _timed("normalize_blob_numeric", normalize_blob_numeric, conn)
        except Exception as e:
            print("[normalize][WARN]", e)

        # (8) ダッシュボード出力
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        _timed("export_html_dashboard", phase_export_html_dashboard_offline, conn, html_path)

        # (9) メール送信（任意）
        try:
            _timed("send_index_html_via_gmail", send_index_html_via_gmail, html_path)
            # クールダウンなしで強制オープン
            ok = open_html_locally(r"H:\desctop\株攻略\twitter_code\スクリーニング自動化プログラム\screen_data\index.html", cool_min=0, force=True)
            print("opened:", ok)

        except Exception as e:
            print("[gmail][WARN]", e)
            
        

    finally:
        conn.close()
    print(f"実行時間： {time.time() - t0:.2f}s")
    print("=== 終了 ===")


# ===== エントリーポイント =====
if __name__ == "__main__":
    main()
