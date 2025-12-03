"""
チャートパターンパイプライン_v3.py

v3 変更点：
- N_CLUSTERS=30 に増加
- クラスタ別「主な遷移先」をトップ5まで％付きで表示
- ダッシュボードのヘッダ文言に日付(YYYYMMDD)を埋め込み
- 出力ファイルを ./csv, ./html, ./model 配下に整理
- 旧 pattern_vectors_2m.csv が同階層にある場合は読み込んで ./csv に移行し、60日フル再取得を避ける
"""

import os
import re
import sqlite3
import datetime as dt
import pickle
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import yfinance as yf


# ==============================
# パス設定（スクリプトと同階層をベースに）
# ==============================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CSV_DIR = os.path.join(BASE_DIR, "csv")
HTML_DIR = os.path.join(BASE_DIR, "html")
MODEL_DIR = os.path.join(BASE_DIR, "model")

OUTPUT_2M_DIR = os.path.join(BASE_DIR, "output_2m_patterns")
CLUSTER_OUTPUT_DIR = os.path.join(BASE_DIR, "cluster_patterns_2m")

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(OUTPUT_2M_DIR, exist_ok=True)
os.makedirs(CLUSTER_OUTPUT_DIR, exist_ok=True)

# ★ kani2.db のパス（必要なら変更）
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

# 旧（v2まで）の CSV パス（同階層直下）
LEGACY_VECTORS_CSV_PATH = os.path.join(BASE_DIR, "pattern_vectors_2m.csv")

# v3 で使う CSV パス（./csv 配下）
VECTORS_CSV_PATH = os.path.join(CSV_DIR, "pattern_vectors_2m.csv")
WITH_CLUSTER_CSV_PATH = os.path.join(CSV_DIR, "pattern_with_clusters_2m.csv")
WITH_NEXT_CSV_PATH = os.path.join(CSV_DIR, "pattern_with_next_2m.csv")
STATS_CSV_PATH = os.path.join(CSV_DIR, "pattern_stats_by_cluster_2m.csv")
THRESHOLD_CSV_PATH = os.path.join(CSV_DIR, "kmeans_2m_threshold.csv")
TODAY_PREDICTIONS_CSV_PATH = os.path.join(CSV_DIR, "today_predictions_2m.csv")

# モデル・HTML
KMEANS_MODEL_PATH = os.path.join(MODEL_DIR, "kmeans_2m.pkl")
DASHBOARD_HTML_PATH = os.path.join(HTML_DIR, "chart_pattern_dashboard.html")

# 2分足の「最大保持営業日数」（スライディングウィンドウ）
MAX_DAYS_WINDOW = 60

# yfinanceでの初回取得期間（日数）
INITIAL_FETCH_DAYS = 60

# 差分更新時に yfinance から取る期間（日数）
UPDATE_FETCH_DAYS = 1

# 日本時間
TZ = "Asia/Tokyo"

# ザラ場時間（9:00〜15:30）
INTRADAY_START = dt.time(9, 0)
INTRADAY_END = dt.time(15, 30)

# 9:00〜15:30（390分）を2分刻み → 390/2 = 195 区間 + 開始点=196
EXPECTED_LEN = 196

# クラスタリング用パラメータ
N_CLUSTERS = 30      # ★ v3: パターン数アップ
CLUSTER_DAYS = 40    # クラスタ学習に使う直近営業日数
STATS_DAYS = 20      # 翌日統計に使う直近営業日数

# 「差分がないときにそれでもクラスタリング等を実行するか」
FORCE_RECLUSTER_IF_NO_UPDATE = False


# ==============================
# 共通ユーティリティ
# ==============================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def get_codes_names_markets_from_screener(db_path: str):
    """
    screenerテーブルから『コード』『銘柄名』『市場系カラム』を取得する。
    戻り値:
        codes: list[str]
        names: dict[code] = 銘柄名
        markets: dict[code] = 市場名(またはNone)
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(screener)")
        cols_info = cur.fetchall()
        col_names = [c[1] for c in cols_info]

        code_col = None
        name_col = None
        for c in col_names:
            if c in ("コード", "code", "証券コード"):
                code_col = c
            if c in ("銘柄名", "name", "銘柄"):
                name_col = c
        if code_col is None:
            code_col = "コード"
        if name_col is None:
            name_col = "銘柄名"

        market_candidates = [
            "市場", "市場名", "市場区分", "market", "Market", "section",
            "exchange", "上市市場", "市場コード", "market_raw"
        ]
        market_col = None
        for c in col_names:
            if c in market_candidates:
                market_col = c
                break

        if market_col:
            query = f"SELECT {code_col}, {name_col}, {market_col} FROM screener"
        else:
            query = f"SELECT {code_col}, {name_col} FROM screener"

        cur.execute(query)
        rows = cur.fetchall()
    finally:
        conn.close()

    codes: list[str] = []
    names: dict[str, str] = {}
    markets: dict[str, str | None] = {}
    seen = set()

    for row in rows:
        code = row[0]
        name = row[1] if len(row) > 1 else ""
        market = row[2] if (market_col and len(row) > 2) else None

        if code is None:
            continue
        s = str(code).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            codes.append(s)

        names[s] = str(name) if name is not None else ""
        if market is not None:
            markets[s] = str(market)
        else:
            markets[s] = None

    return codes, names, markets


def resolve_yahoo_symbol(code: str, market: str | None = None) -> str:
    """
    code + 市場名 から Yahoo シンボルを決定する。

    優先順位：
      1) yahoo_symbol_override (コード, 問い合わせシンボル) に登録があれば最優先
      2) TOPIX の別名吸収 (^TOPIX, TOPIX, 998405.T → ^TOPX)
      3) 指数(^)はそのまま
      4) 既に拡張子 .X (Xは1文字の英字) が付いていればそのまま
      5) 市場名からサフィックス (.T/.N/.S/.F) を決定し、4桁数字・ETFコード等に付与
      6) それ以外は大文字化して返す
    """
    s = (code or "").strip()
    if not s:
        return s

    S = s.upper()

    # 1) 明示オーバーライド（最優先）
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS yahoo_symbol_override (
              コード TEXT PRIMARY KEY,
              問い合わせシンボル TEXT NOT NULL
            )
        """)
        row = conn.execute(
            "SELECT 問い合わせシンボル FROM yahoo_symbol_override WHERE コード=?",
            (s,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception:
        pass

    # 2) TOPIX の簡易別名吸収
    alias = {
        "^TOPIX": "^TOPX",
        "TOPIX": "^TOPX",
        "998405.T": "^TOPX",
    }
    if S in alias:
        return alias[S]

    # 3) 指数 (^N225 等) はそのまま
    if S.startswith("^"):
        return S

    # 4) 既に拡張子 .X (1文字の英字) が付いている場合はそのまま
    if re.search(r"\.[A-Z]$", S):
        return S

    # 5) 市場名からサフィックス決定
    suffix = ".T"  # デフォルト：東証系
    if market:
        M = str(market).upper()
        if ("名証" in M) or ("NAGOYA" in M):
            suffix = ".N"
        elif ("札証" in M) or ("SAPPORO" in M):
            suffix = ".S"
        elif ("福証" in M) or ("FUKUOKA" in M):
            suffix = ".F"
        else:
            suffix = ".T"

    # 4桁数字 → XXXX + suffix
    if len(S) == 4 and S.isdigit():
        return S + suffix

    # ETF/指数系：3桁数字+英字 (例: 162A)
    if re.fullmatch(r"\d{3}[A-Z]", S):
        return S + suffix

    # 4桁英数字ミックス
    if len(S) == 4 and re.fullmatch(r"[A-Z0-9]{4}", S):
        return S + suffix

    # その他はとりあえず大文字で返す
    return S


def to_yahoo_symbol(code: str, market: str | None = None) -> str:
    return resolve_yahoo_symbol(code, market)


def fetch_2m_last_ndays(symbol: str, days: int, tz: str = TZ) -> pd.DataFrame | None:
    """
    指定シンボルの「直近 N日分の2分足」を取得して、日本時間に変換した DataFrame を返す。
    取れなかった場合は None。
    """
    try:
        df = yf.download(
            symbol,
            period=f"{days}d",
            interval="2m",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        print(f"[skip] {symbol}: yfinance error: {e}")
        return None

    if df is None or df.empty:
        print(f"[skip] {symbol}: empty 2m data")
        return None

    if df.index.tz is None:
        df = df.tz_localize("UTC")
    df = df.tz_convert(tz)

    return df


def intraday_close_series(df_2m: pd.DataFrame) -> pd.Series:
    s = df_2m["Close"].copy()
    s = s.between_time(INTRADAY_START, INTRADAY_END)
    return s


def build_daily_vector_full_day(series_2m) -> tuple[pd.Series, float, float] | None:
    """
    1日分の 9:00〜15:30 の2分足Seriesから196点の変化率ベクトルを作る。
    """
    import pandas as pd

    if isinstance(series_2m, pd.DataFrame):
        if "Close" in series_2m.columns:
            series_2m = series_2m["Close"]
        else:
            series_2m = series_2m.iloc[:, 0]

    if series_2m is None or series_2m.empty:
        return None

    day = series_2m.index[0].date()
    tzinfo = series_2m.index.tz

    start_dt = dt.datetime.combine(day, INTRADAY_START)
    end_dt = dt.datetime.combine(day, INTRADAY_END)
    idx = pd.date_range(start=start_dt, end=end_dt, freq="2min", tz=tzinfo)

    s = series_2m.reindex(idx)

    if s.isna().all():
        return None

    s = s.ffill().bfill()

    if len(s) != EXPECTED_LEN:
        print(f"[warn] unexpected length ({len(s)} != {EXPECTED_LEN}) for {day}")
        return None

    open_raw = float(s.iloc[0])
    close_raw = float(s.iloc[-1])

    if open_raw == 0 or pd.isna(open_raw):
        return None

    s_rel = (s / open_raw) - 1.0

    return s_rel, open_raw, close_raw


def build_partial_vector_until_now(series_2m) -> tuple[pd.Series, float] | None:
    """
    当日用：現時点までの2分足Seriesから、196点の変化率ベクトルを作る。
    未来部分は直近値で横ばい埋め。
    """
    import pandas as pd

    if isinstance(series_2m, pd.DataFrame):
        if "Close" in series_2m.columns:
            series_2m = series_2m["Close"]
        else:
            series_2m = series_2m.iloc[:, 0]

    if series_2m is None or series_2m.empty:
        return None

    day = series_2m.index[0].date()
    tzinfo = series_2m.index.tz

    start_dt = dt.datetime.combine(day, INTRADAY_START)
    end_dt = dt.datetime.combine(day, INTRADAY_END)
    idx = pd.date_range(start=start_dt, end=end_dt, freq="2min", tz=tzinfo)

    s = series_2m.reindex(idx)
    if s.notna().sum() == 0:
        return None

    s = s.ffill().bfill()

    if len(s) != EXPECTED_LEN:
        print(f"[warn] partial vector unexpected len ({len(s)} != {EXPECTED_LEN}) for {day}")
        return None

    open_raw = float(s.iloc[0])
    if open_raw == 0 or pd.isna(open_raw):
        return None

    s_rel = (s / open_raw) - 1.0
    return s_rel, open_raw


# ==============================
# 1) ベクトルCSVの更新（初回60日 or 差分）
# ==============================

def update_vectors() -> bool:
    """
    pattern_vectors_2m.csv を更新：
    - ./csv/pattern_vectors_2m.csv が無ければ：
      - 旧版の BASE_DIR/pattern_vectors_2m.csv があればそれを読み込んで ./csv に移行
      - それも無ければ全コード60日分をフルビルド
    - ファイルがあれば：差分のみ追加＋直近MAX_DAYS_WINDOW営業日に絞って保存
    戻り値:
        True  = 新しい日足を追加した
        False = 差分なし（＝クラスタ以降をスキップしてもOK）
    """
    start = dt.datetime.now()
    print("\n===== [1] ベクトルCSV更新 (update_vectors) 開始 =====")
    print(f"[timer] 開始時刻: {start}")

    ensure_dir(OUTPUT_2M_DIR)

    codes, _name_map, market_map = get_codes_names_markets_from_screener(DB_PATH)
    print(f"[update_vectors] screenerからコード取得: {len(codes)}件")

    df_existing = None
    last_dates: dict[str, dt.date] = {}

    # 1) v3用CSVがあるか？
    if os.path.exists(VECTORS_CSV_PATH):
        df_existing = pd.read_csv(VECTORS_CSV_PATH)
        if df_existing.empty:
            df_existing = None
        else:
            df_existing["date"] = pd.to_datetime(df_existing["date"]).dt.date
            last_dates = df_existing.groupby("code")["date"].max().to_dict()
    # 2) なければ旧ファイルを探す
    elif os.path.exists(LEGACY_VECTORS_CSV_PATH):
        print(f"[update_vectors] 旧ファイル({LEGACY_VECTORS_CSV_PATH})を読み込み、./csv に移行します。")
        df_existing = pd.read_csv(LEGACY_VECTORS_CSV_PATH)
        if df_existing.empty:
            df_existing = None
        else:
            df_existing["date"] = pd.to_datetime(df_existing["date"]).dt.date
            last_dates = df_existing.groupby("code")["date"].max().to_dict()
            # とりあえず ./csv に保存し直す
            df_existing.to_csv(VECTORS_CSV_PATH, index=False, encoding="utf-8-sig")

    # 既存があれば本日分まであるかチェック
    if df_existing is not None:
        max_date = df_existing["date"].max()
        today_jst = dt.datetime.now(ZoneInfo(TZ)).date()
        if max_date >= today_jst:
            print(f"[update_vectors] 既に本日({today_jst})分まで反映済み。差分が無いためベクトル更新をスキップします。")
            end = dt.datetime.now()
            secs = (end - start).total_seconds()
            print(f"[timer] update_vectors 終了時刻: {end}")
            print(f"[timer] update_vectors 実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
            print("===== [1] ベクトルCSV更新 完了（スキップ） =====\n")
            return False

    rows_new: list[list] = []
    is_initial = df_existing is None

    for idx, code in enumerate(codes, start=1):
        market = market_map.get(code)
        sym = to_yahoo_symbol(code, market)
        if not sym:
            print(f"[skip] {code}: invalid symbol (market={market})")
            continue

        if is_initial:
            print(f"[{idx}/{len(codes)}] 初回取得 {code}({market}) -> {sym} ({INITIAL_FETCH_DAYS}d)")
            df_2m = fetch_2m_last_ndays(sym, INITIAL_FETCH_DAYS, tz=TZ)
        else:
            print(f"[{idx}/{len(codes)}] 差分取得 {code}({market}) -> {sym} ({UPDATE_FETCH_DAYS}d)")
            df_2m = fetch_2m_last_ndays(sym, UPDATE_FETCH_DAYS, tz=TZ)

        if df_2m is None:
            continue

        df_2m = df_2m.sort_index()
        df_2m["date"] = df_2m.index.date
        grouped = df_2m.groupby("date")

        last_date = last_dates.get(code)
        code_dir = os.path.join(OUTPUT_2M_DIR, code)
        ensure_dir(code_dir)

        for day, df_day in grouped:
            if (not is_initial) and (last_date is not None) and (day <= last_date):
                continue

            s_intraday = intraday_close_series(df_day)
            out = build_daily_vector_full_day(s_intraday)
            if out is None:
                continue

            s_rel, open_raw, close_raw = out
            date_str = dt.date.fromisoformat(str(day)).isoformat()
            vec = s_rel.values
            row = [code, date_str, open_raw, close_raw] + vec.tolist()
            rows_new.append(row)

            png_name = f"{date_str}.png"
            out_path = os.path.join(code_dir, png_name)

            plt.figure(figsize=(8, 3))
            s_rel.plot()
            plt.title(f"{code} {date_str}  (2m, norm by open)")
            plt.xlabel("Time (JST)")
            plt.ylabel("Return from open")
            plt.tight_layout()
            plt.savefig(out_path)
            plt.close()

            print(f"[ok] {code} {date_str}: vector+png saved")

    if rows_new:
        col_names = ["code", "date", "open_raw", "close_raw"]
        col_names += [f"v_{i:03d}" for i in range(EXPECTED_LEN)]
        df_new = pd.DataFrame(rows_new, columns=col_names)
        df_new["date"] = pd.to_datetime(df_new["date"]).dt.date

        if df_existing is not None and not df_existing.empty:
            df_all = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_all = df_new

        unique_dates = sorted(df_all["date"].unique())
        if len(unique_dates) > MAX_DAYS_WINDOW:
            cutoff_date = unique_dates[-MAX_DAYS_WINDOW]
            df_all = df_all[df_all["date"] >= cutoff_date]

        df_all.sort_values(["code", "date"], inplace=True)
        df_all.to_csv(VECTORS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"[update_vectors] ベクトルCSV更新: {VECTORS_CSV_PATH} (行数: {len(df_all)})")
        updated = True
    else:
        print("[update_vectors] 新規に追加された日足はありません。")
        updated = False

    end = dt.datetime.now()
    secs = (end - start).total_seconds()
    print(f"[timer] update_vectors 終了時刻: {end}")
    print(f"[timer] update_vectors 実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
    print("===== [1] ベクトルCSV更新 完了 =====\n")

    return updated


# ==============================
# 2) クラスタリング＋モデル・閾値・図鑑PNG
# ==============================

def run_clustering():
    """
    ./csv/pattern_vectors_2m.csv から：
    - 直近 CLUSTER_DAYS 営業日で k-means 学習 (N_CLUSTERS=30)
    - 全データに cluster_id を付与
    - 当日→翌日の next_cluster_id, next_day_return を付与
    - クラスタ別統計 (STATS_DAYS) を作成（トップ5遷移も文字列で保存）
    - クラスタ中心の図鑑PNGを ./cluster_patterns_2m に出力
    - k-means モデルと距離の閾値を保存
    """
    start = dt.datetime.now()
    print("\n===== [2] クラスタリング (run_clustering) 開始 =====")
    print(f"[timer] 開始時刻: {start}")

    ensure_dir(CLUSTER_OUTPUT_DIR)

    if not os.path.exists(VECTORS_CSV_PATH):
        print(f"[run_clustering] ベクトルCSVがありません: {VECTORS_CSV_PATH}")
        return

    df = pd.read_csv(VECTORS_CSV_PATH)
    if df.empty:
        print("[run_clustering] ベクトルCSVが空です。")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    vec_cols = [c for c in df.columns if c.startswith("v_")]
    vec_dim = len(vec_cols)
    print(f"[run_clustering] vector dimension: {vec_dim}")

    unique_dates = sorted(df["date"].unique())
    if not unique_dates:
        print("[run_clustering] 日付がありません。")
        return

    unique_dates_arr = np.array(unique_dates)
    n_days = len(unique_dates_arr)

    cluster_days = min(CLUSTER_DAYS, n_days)
    stats_days = min(STATS_DAYS, n_days)

    cluster_cutoff_date = unique_dates_arr[-cluster_days]
    stats_cutoff_date = unique_dates_arr[-stats_days]

    print(f"[run_clustering] total trading days in data  : {n_days}")
    print(f"[run_clustering] cluster_days (used)        : {cluster_days} (cutoff={cluster_cutoff_date})")
    print(f"[run_clustering] stats_days (used)          : {stats_days} (cutoff={stats_cutoff_date})")

    df_cluster_train = df[df["date"] >= cluster_cutoff_date].copy()
    X_train = df_cluster_train[vec_cols].to_numpy()
    print(f"[run_clustering] cluster train samples: {len(X_train)}")

    kmeans = KMeans(
        n_clusters=N_CLUSTERS,
        random_state=42,
        n_init="auto",
    )
    kmeans.fit(X_train)

    dists_train = kmeans.transform(X_train)
    assigned = kmeans.labels_
    min_dists = dists_train[np.arange(len(X_train)), assigned]
    threshold = float(np.quantile(min_dists, 0.95))

    pd.DataFrame({"threshold": [threshold]}).to_csv(THRESHOLD_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"[run_clustering] 新規パターン判定用閾値 (95%ile) = {threshold:.6f}")
    print(f"[run_clustering] 閾値CSV保存: {THRESHOLD_CSV_PATH}")

    with open(KMEANS_MODEL_PATH, "wb") as f:
        pickle.dump(kmeans, f)
    print(f"[run_clustering] k-means モデル保存: {KMEANS_MODEL_PATH}")

    # 全データにクラスタ付与
    X_all = df[vec_cols].to_numpy()
    cluster_all = kmeans.predict(X_all)
    df["cluster_id"] = cluster_all

    # 翌日情報
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    df["next_date"] = df.groupby("code")["date"].shift(-1)
    df["next_cluster_id"] = df.groupby("code")["cluster_id"].shift(-1)
    df["next_close_raw"] = df.groupby("code")["close_raw"].shift(-1)
    df["next_day_return"] = df["next_close_raw"] / df["close_raw"] - 1.0

    df_with_cluster = df.copy()
    df_with_cluster.to_csv(WITH_CLUSTER_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"[run_clustering] pattern_with_clusters -> {WITH_CLUSTER_CSV_PATH}")

    df_with_next = df.drop(columns=["next_close_raw"])
    df_with_next.to_csv(WITH_NEXT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"[run_clustering] pattern_with_next -> {WITH_NEXT_CSV_PATH}")

    # クラスタ統計（直近 stats_days ベース）
    df_stats_base = df[df["date"] >= stats_cutoff_date].copy()
    df_stats_base = df_stats_base[~df_stats_base["next_day_return"].isna()].copy()

    stats_rows = []
    for cid, g in df_stats_base.groupby("cluster_id"):
        cnt = len(g)
        if cnt == 0:
            continue

        avg_ret = g["next_day_return"].mean()
        med_ret = g["next_day_return"].median()
        p_up_0 = (g["next_day_return"] > 0).mean()
        p_up_1 = (g["next_day_return"] > 0.01).mean()
        p_down_1 = (g["next_day_return"] < -0.01).mean()

        next_counts = g["next_cluster_id"].value_counts(dropna=True)
        most_common_next = int(next_counts.idxmax()) if not next_counts.empty else None

        # ★ トップ5遷移先を％付き文字列にする
        top5 = next_counts.head(5)
        top_list = []
        total = top5.sum()
        for nxt, cnt_n in top5.items():
            if pd.isna(nxt):
                continue
            pct = cnt_n / total * 100.0
            top_list.append(f"C{int(nxt)}({pct:.1f}%)")
        top_next_str = ", ".join(top_list)

        stats_rows.append(
            [
                int(cid),
                cnt,
                avg_ret,
                med_ret,
                p_up_0,
                p_up_1,
                p_down_1,
                most_common_next,
                top_next_str,
            ]
        )

    if stats_rows:
        df_stats = pd.DataFrame(
            stats_rows,
            columns=[
                "cluster_id",
                "count",
                "avg_next_day_return",
                "med_next_day_return",
                "p_up_0",
                "p_up_1",
                "p_down_1",
                "most_common_next_cluster_id",
                "top5_next_clusters_text",
            ],
        ).sort_values("cluster_id")
        df_stats.to_csv(STATS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"[run_clustering] pattern_stats_by_cluster -> {STATS_CSV_PATH}")
    else:
        print("[run_clustering] クラスタ統計行がありません。")

    # クラスタ中心パターンPNG
    centers = kmeans.cluster_centers_
    t = np.arange(vec_dim)

    for cid in range(N_CLUSTERS):
        center = centers[cid]

        plt.figure(figsize=(8, 3))
        plt.plot(t, center)
        plt.title(f"Cluster {cid}  (mean pattern, 2m normalized)")
        plt.xlabel("index (2-min steps from 09:00)")
        plt.ylabel("Return from open")
        plt.tight_layout()

        out_path = os.path.join(CLUSTER_OUTPUT_DIR, f"cluster_{cid:02d}.png")
        plt.savefig(out_path)
        plt.close()

        print(f"[run_clustering] cluster center png -> {out_path}")

    end = dt.datetime.now()
    secs = (end - start).total_seconds()
    print(f"[timer] run_clustering 終了時刻: {end}")
    print(f"[timer] run_clustering 実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
    print("===== [2] クラスタリング 完了 =====\n")


# ==============================
# 3) 当日14:30までのパターン判定
# ==============================

def predict_today():
    """
    当日14:30までの2分足データから当日パターン＆翌日予想を算出し、
    ./csv/today_predictions_2m.csv に保存。
    """
    start = dt.datetime.now()
    print("\n===== [3] 当日パターン判定 (predict_today) 開始 =====")
    print(f"[timer] 開始時刻: {start}")

    if not os.path.exists(KMEANS_MODEL_PATH) or not os.path.exists(THRESHOLD_CSV_PATH) or not os.path.exists(STATS_CSV_PATH):
        print("[predict_today] 必要なモデル/閾値/統計ファイルがありません。先に run_clustering を実行してください。")
        return

    with open(KMEANS_MODEL_PATH, "rb") as f:
        kmeans = pickle.load(f)

    threshold = pd.read_csv(THRESHOLD_CSV_PATH)["threshold"].iloc[0]
    df_stats = pd.read_csv(STATS_CSV_PATH)

    stats_by_cluster = {}
    for _, row in df_stats.iterrows():
        stats_by_cluster[int(row["cluster_id"])] = row

    codes, _names, market_map = get_codes_names_markets_from_screener(DB_PATH)
    print(f"[predict_today] screenerからコード取得: {len(codes)}件")

    rows: list[list] = []

    for idx, code in enumerate(codes, start=1):
        market = market_map.get(code)
        sym = to_yahoo_symbol(code, market)
        if not sym:
            print(f"[skip] {code}: invalid symbol (market={market})")
            continue

        print(f"[{idx}/{len(codes)}] 当日取得 {code}({market}) -> {sym} (1d)")
        df_2m = fetch_2m_last_ndays(sym, 1, tz=TZ)
        if df_2m is None:
            continue

        df_2m = df_2m.sort_index()
        df_2m["date"] = df_2m.index.date
        dates = sorted(df_2m["date"].unique())
        if not dates:
            continue
        day = dates[-1]
        df_day = df_2m[df_2m["date"] == day].copy()

        s_intraday = intraday_close_series(df_day)
        out = build_partial_vector_until_now(s_intraday)
        if out is None:
            continue

        s_rel, open_raw = out
        vec = s_rel.values

        dists = kmeans.transform([vec])[0]
        cid = int(np.argmin(dists))
        min_dist = float(dists[cid])
        is_new = bool(min_dist > threshold)

        pred_next_cluster = None
        pred_next_return = None
        if not is_new and cid in stats_by_cluster:
            st = stats_by_cluster[cid]
            if not pd.isna(st["most_common_next_cluster_id"]):
                pred_next_cluster = int(st["most_common_next_cluster_id"])
            if not pd.isna(st["avg_next_day_return"]):
                pred_next_return = float(st["avg_next_day_return"])

        date_str = dt.date.fromisoformat(str(day)).isoformat()
        rows.append(
            [
                code,
                date_str,
                cid,
                min_dist,
                is_new,
                pred_next_cluster,
                pred_next_return,
            ]
        )
        print(f"[predict_today] {code} {date_str}: cluster={cid}, dist={min_dist:.6f}, is_new={is_new}")

    if rows:
        df_today = pd.DataFrame(
            rows,
            columns=[
                "code",
                "date",
                "cluster_today",
                "distance",
                "is_new_pattern",
                "pred_next_cluster_today",
                "pred_next_return_today",
            ],
        )
        df_today.to_csv(TODAY_PREDICTIONS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"[predict_today] today_predictions -> {TODAY_PREDICTIONS_CSV_PATH} (行数: {len(df_today)})")
    else:
        print("[predict_today] 当日パターン判定の結果がありません。")

    end = dt.datetime.now()
    secs = (end - start).total_seconds()
    print(f"[timer] predict_today 終了時刻: {end}")
    print(f"[timer] predict_today 実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
    print("===== [3] 当日パターン判定 完了 =====\n")


# ==============================
# 4) ダッシュボードHTML生成
# ==============================
def build_dashboard():
    """
    ./html/chart_pattern_dashboard.html を生成。
    上部：クラスタ図鑑（トップ5遷移＋リターン色分け）
    下部：銘柄一覧（前日＆当日パターンと翌日予想）
    """
    start = dt.datetime.now()
    print("\n===== [4] ダッシュボード生成 (build_dashboard) 開始 =====")
    print(f"[timer] 開始時刻: {start}")

    if not os.path.exists(STATS_CSV_PATH):
        print(f"[build_dashboard] クラスタ統計がありません: {STATS_CSV_PATH}")
        return
    if not os.path.exists(WITH_CLUSTER_CSV_PATH):
        print(f"[build_dashboard] pattern_with_clusters がありません: {WITH_CLUSTER_CSV_PATH}")
        return

    df_stats = pd.read_csv(STATS_CSV_PATH)
    df_clusters = pd.read_csv(WITH_CLUSTER_CSV_PATH)
    df_clusters["date"] = pd.to_datetime(df_clusters["date"]).dt.date

    if not os.path.exists(TODAY_PREDICTIONS_CSV_PATH):
        df_today = pd.DataFrame(
            columns=[
                "code",
                "date",
                "cluster_today",
                "distance",
                "is_new_pattern",
                "pred_next_cluster_today",
                "pred_next_return_today",
            ]
        )
        print("[build_dashboard] today_predictions が無いので、当日パターンは空になります。")
    else:
        df_today = pd.read_csv(TODAY_PREDICTIONS_CSV_PATH)
        if not df_today.empty:
            df_today["date"] = pd.to_datetime(df_today["date"]).dt.date

    codes, name_map, _market_map = get_codes_names_markets_from_screener(DB_PATH)

    if df_clusters.empty:
        print("[build_dashboard] pattern_with_clusters が空です。")
        return

    all_dates = sorted(df_clusters["date"].unique())
    latest_full_date = all_dates[-1]
    print(f"[build_dashboard] 最新フル日付: {latest_full_date}")

    if not df_today.empty:
        today_date = df_today["date"].max()
    else:
        today_date = latest_full_date

    prev_label = latest_full_date.strftime("%Y%m%d")
    today_label = today_date.strftime("%Y%m%d")

    # 前日パターン
    yesterday_by_code = {}
    for code in codes:
        sub = df_clusters[(df_clusters["code"] == code) & (df_clusters["date"] == latest_full_date)]
        if not sub.empty:
            row = sub.iloc[0]
            yesterday_by_code[code] = int(row["cluster_id"])

    stats_by_cluster = {int(row["cluster_id"]): row for _, row in df_stats.iterrows()}

    # 当日パターン
    today_by_code = {}
    if not df_today.empty:
        for _, row in df_today.iterrows():
            today_by_code[row["code"]] = row

    # 銘柄一覧データ
    table_rows = []
    for code in codes:
        name = name_map.get(code, "")

        cid_y = yesterday_by_code.get(code)
        if cid_y is None:
            prev_pattern = ""
            prev_next_pattern = ""
        else:
            prev_pattern = f"C{cid_y}"
            prev_next_pattern = ""
            st = stats_by_cluster.get(cid_y)
            if st is not None and not pd.isna(st["most_common_next_cluster_id"]):
                prev_next_pattern = f"C{int(st['most_common_next_cluster_id'])}"

        row_t = today_by_code.get(code)
        if row_t is None:
            today_pattern = ""
            today_next_pattern = ""
        else:
            cid_t = int(row_t["cluster_today"])
            is_new = bool(row_t["is_new_pattern"])
            if is_new:
                today_pattern = "NEW"
                today_next_pattern = ""
            else:
                today_pattern = f"C{cid_t}"
                st2 = stats_by_cluster.get(cid_t)
                if st2 is not None and not pd.isna(st2["most_common_next_cluster_id"]):
                    today_next_pattern = f"C{int(st2['most_common_next_cluster_id'])}"
                else:
                    today_next_pattern = ""

        table_rows.append(
            {
                "code": code,
                "name": name,
                "prev_pattern": prev_pattern,
                "prev_next_pattern": prev_next_pattern,
                "today_pattern": today_pattern,
                "today_next_pattern": today_next_pattern,
            }
        )

    # クラスタ図鑑データ（トップ5遷移テキスト＋色クラス）
    cluster_cards = []
    for _, row in df_stats.sort_values("cluster_id").iterrows():
        cid = int(row["cluster_id"])
        count = int(row["count"])
        avg_ret = float(row["avg_next_day_return"]) * 100.0
        med_ret = float(row["med_next_day_return"]) * 100.0
        top5_text = row.get("top5_next_clusters_text", "")
        img_rel = f"../cluster_patterns_2m/cluster_{cid:02d}.png"  # html からの相対パス

        # ★ リターンの大きさでカードの色クラスを決める
        #   avg_ret（％）を使う。好みで med_ret に切り替えてもOK。
        if avg_ret >= 1.0:
            card_class = "cluster-card cluster-pos-strong"
        elif avg_ret >= 0.2:
            card_class = "cluster-card cluster-pos"
        elif avg_ret <= -1.0:
            card_class = "cluster-card cluster-neg-strong"
        elif avg_ret <= -0.2:
            card_class = "cluster-card cluster-neg"
        else:
            card_class = "cluster-card cluster-flat"

        cluster_cards.append(
            {
                "cid": cid,
                "count": count,
                "avg_ret": avg_ret,
                "med_ret": med_ret,
                "top5": top5_text,
                "img_rel": img_rel,
                "card_class": card_class,
            }
        )

    # HTML生成
    ensure_dir(os.path.dirname(DASHBOARD_HTML_PATH))

    html_lines: list[str] = []

    html_lines.append("<!DOCTYPE html>")
    html_lines.append("<html lang='ja'>")
    html_lines.append("<head>")
    html_lines.append("  <meta charset='UTF-8'>")
    html_lines.append("  <title>チャートパターン ダッシュボード</title>")
    html_lines.append("  <style>")
    html_lines.append("    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin:16px; background:#111; color:#eee; }")
    html_lines.append("    h1 { font-size:20px; margin-bottom:8px; }")
    html_lines.append("    h2 { font-size:16px; margin-top:24px; margin-bottom:8px; }")
    html_lines.append("    .cluster-gallery { display:flex; flex-wrap:wrap; gap:8px; }")
    html_lines.append("    .cluster-card { background:#1e1e1e; border-radius:8px; padding:8px; width:230px; box-sizing:border-box; border:1px solid #333; }")
    html_lines.append("    .cluster-card img { width:100%; border-radius:4px; background:#000; }")
    html_lines.append("    .cluster-meta { font-size:11px; margin-top:4px; line-height:1.4; }")
    # ★ 色クラス
    html_lines.append("    .cluster-pos-strong { border-color:#2ecc71; box-shadow:0 0 6px rgba(46,204,113,0.7); }")
    html_lines.append("    .cluster-pos { border-color:#27ae60; }")
    html_lines.append("    .cluster-flat { border-color:#555; }")
    html_lines.append("    .cluster-neg { border-color:#c0392b; }")
    html_lines.append("    .cluster-neg-strong { border-color:#e74c3c; box-shadow:0 0 6px rgba(231,76,60,0.7); }")
    html_lines.append("    table { width:100%; border-collapse:collapse; margin-top:8px; font-size:11px; }")
    html_lines.append("    th, td { border-bottom:1px solid #333; padding:4px 6px; }")
    html_lines.append("    th { background:#222; position:sticky; top:0; z-index:1; }")
    html_lines.append("    tr:nth-child(even) { background:#151515; }")
    html_lines.append("    tr:hover { background:#262626; }")
    html_lines.append("    .code { font-family:'Consolas','Menlo',monospace; }")
    html_lines.append("    .pattern { font-weight:bold; }")
    html_lines.append("  </style>")
    html_lines.append("</head>")
    html_lines.append("<body>")

    html_lines.append("  <h1>チャートパターン ダッシュボード (2分足)</h1>")

    html_lines.append("  <h2>パターン図鑑（クラスタ中心・トップ5遷移）</h2>")
    html_lines.append("  <div class='cluster-gallery'>")
    for c in cluster_cards:
        html_lines.append(f"    <div class='{c['card_class']}'>")
        html_lines.append(f"      <img src='{c['img_rel']}' alt='cluster {c['cid']}'>")
        html_lines.append("      <div class='cluster-meta'>")
        html_lines.append(f"        <div>Cluster {c['cid']}</div>")
        html_lines.append(f"        <div>件数: {c['count']}</div>")
        html_lines.append(f"        <div>翌日平均: {c['avg_ret']:+.2f}% / 中央: {c['med_ret']:+.2f}%</div>")
        html_lines.append(f"        <div>主な遷移先(Top5): {c['top5']}</div>")
        html_lines.append("      </div>")
        html_lines.append("    </div>")
    html_lines.append("  </div>")

    html_lines.append("  <h2>銘柄一覧（前日＆当日パターン）</h2>")
    html_lines.append("  <table>")
    html_lines.append("    <thead>")
    html_lines.append("      <tr>")
    html_lines.append("        <th>コード</th>")
    html_lines.append("        <th>銘柄名</th>")
    html_lines.append(f"        <th>前日({prev_label})パターン</th>")
    html_lines.append(f"        <th>前日({prev_label})時点の翌日予想パターン</th>")
    html_lines.append(f"        <th>当日({today_label})パターン</th>")
    html_lines.append(f"        <th>当日({today_label})時点の翌日予想パターン</th>")
    html_lines.append("      </tr>")
    html_lines.append("    </thead>")
    html_lines.append("    <tbody>")

    for r in table_rows:
        html_lines.append("      <tr>")
        html_lines.append(f"        <td class='code'>{r['code']}</td>")
        html_lines.append(f"        <td>{r['name']}</td>")
        html_lines.append(f"        <td class='pattern'>{r['prev_pattern']}</td>")
        html_lines.append(f"        <td>{r['prev_next_pattern']}</td>")
        html_lines.append(f"        <td class='pattern'>{r['today_pattern']}</td>")
        html_lines.append(f"        <td>{r['today_next_pattern']}</td>")
        html_lines.append("      </tr>")

    html_lines.append("    </tbody>")
    html_lines.append("  </table>")
    html_lines.append("</body>")
    html_lines.append("</html>")

    with open(DASHBOARD_HTML_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))

    print(f"[build_dashboard] ダッシュボードHTML出力: {DASHBOARD_HTML_PATH}")

    end = dt.datetime.now()
    secs = (end - start).total_seconds()
    print(f"[timer] build_dashboard 終了時刻: {end}")
    print(f"[timer] build_dashboard 実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
    print("===== [4] ダッシュボード生成 完了 =====\n")


# ==============================
# メインパイプライン
# ==============================

def main():
    pipeline_start = dt.datetime.now()
    print("############################################")
    print("# チャートパターン パイプライン v3 開始")
    print(f"# 開始時刻: {pipeline_start}")
    print("############################################")

    updated = update_vectors()

    if (not updated) and (not FORCE_RECLUSTER_IF_NO_UPDATE):
        print("[main] ベクトルの更新が無かったため、クラスタリング・当日判定・ダッシュボード生成をスキップします。")
        pipeline_end = dt.datetime.now()
        secs = (pipeline_end - pipeline_start).total_seconds()
        print("############################################")
        print("# チャートパターン パイプライン v3 完了（差分なしスキップ）")
        print(f"# 終了時刻: {pipeline_end}")
        print(f"# 総実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
        print("############################################")
        return

    run_clustering()
    predict_today()
    build_dashboard()

    pipeline_end = dt.datetime.now()
    secs = (pipeline_end - pipeline_start).total_seconds()
    print("############################################")
    print("# チャートパターン パイプライン v3 完了")
    print(f"# 終了時刻: {pipeline_end}")
    print(f"# 総実行時間: {secs:.1f} 秒（約 {secs/60:.1f} 分）")
    print("############################################")


if __name__ == "__main__":
    main()
