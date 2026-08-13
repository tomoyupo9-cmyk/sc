import argparse
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

# ============================================================
# 設定
# ============================================================
DEFAULT_DB_PATH = os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
)
DEFAULT_MODEL_DIR = os.environ.get(
    "KABU_MODEL_DIR",
    r"D:\kabu\main\1-スクリーニング自動化プログラム\main\model",
)
DEFAULT_MODEL_NAME = "stock_predictor_lv3.pkl"

DEFAULT_TERM = 10
DEFAULT_TARGET_PCT = 1.10
DEFAULT_STOP_PCT = 0.93
DEFAULT_MIN_PRICE = 300.0
DEFAULT_VAL_RATIO = 0.10
DEFAULT_TEST_RATIO = 0.20

RANDOM_SEED = 42

FEATURES = [
    "return_1d", "range", "body", "upper_shadow",
    "kairi_5", "kairi_25", "kairi_75",
    "rsi_14", "bb_pos", "vol_ratio",
    "perfect_order", "trend_strong",
    "market_return", "market_sentiment", "relative_strength",
    "stop_hunt_reversal", "dist_from_poc", "turnover_20d_avg", "liquidity_class",
]

CAT_FEATURES = ["liquidity_class"]


# ============================================================
# 汎用
# ============================================================
def _safe_div(num, den):
    """0除算とinfを抑制したSeries除算。"""
    den = den.replace(0, np.nan)
    return num / den


def _backup_existing(path: str):
    """既存モデルを上書きする前に日時付きバックアップを作る。"""
    p = Path(path)
    if not p.exists():
        return None

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = p.with_name(f"{p.stem}.bak_{stamp}{p.suffix}")
    shutil.copy2(p, backup)
    return str(backup)


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=period).mean()

    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    # 上昇だけが続いた場合は100、変化ゼロだけなら50
    only_gain = (loss == 0) & (gain > 0)
    flat = (loss == 0) & (gain == 0)
    rsi = rsi.mask(only_gain, 100.0)
    rsi = rsi.mask(flat, 50.0)
    return rsi


def _true_rolling_poc_close(df: pd.DataFrame, window: int = 20) -> np.ndarray:
    """
    「直近window日で出来高が最大だった日の終値」を正しく返す。

    旧実装は「その日が20日最大出来高だった時だけ更新→以後ffill」だったため、
    最大出来高日が20日窓から抜けても古い価格が残り続ける問題があった。
    """
    out = np.full(len(df), np.nan, dtype=float)

    for _, positions in df.groupby("コード", sort=False).indices.items():
        pos = np.asarray(positions, dtype=int)
        vol = df.iloc[pos]["volume"].to_numpy(dtype=float)
        close = df.iloc[pos]["close"].to_numpy(dtype=float)
        n = len(pos)

        if n < window:
            continue

        # 各20日窓で出来高最大日の位置を求める
        try:
            from numpy.lib.stride_tricks import sliding_window_view
            windows = sliding_window_view(vol, window_shape=window)
            argmax = np.argmax(windows, axis=1)
            starts = np.arange(len(windows))
            poc_values = close[starts + argmax]
            out[pos[window - 1:]] = poc_values
        except Exception:
            # 古いNumPy等へのフォールバック
            for i in range(window - 1, n):
                start = i - window + 1
                j = start + int(np.argmax(vol[start:i + 1]))
                out[pos[i]] = close[j]

    return out


# ============================================================
# 特徴量
# ============================================================
def build_features(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    df = df.rename(
        columns={
            "始値": "open",
            "高値": "high",
            "安値": "low",
            "終値": "close",
            "出来高": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["日付"], errors="coerce")

    # 重要:
    # 300円未満や出来高0を「特徴量計算前」に消すと、
    # rolling 5/25/75日や未来10日が飛び飛びになる。
    # ここでは明らかな壊れ値だけ落とし、学習対象フィルタは後で行う。
    df = df.dropna(subset=["コード", "date", "open", "high", "low", "close", "volume"])
    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["volume"] >= 0)
    ].copy()

    # groupby rollingのため銘柄→日付順に固定
    df = df.sort_values(["コード", "date"]).reset_index(drop=True)

    g = df.groupby("コード", sort=False)

    # Lv.1
    df["return_1d"] = g["close"].pct_change(1, fill_method=None)
    df["range"] = _safe_div(df["high"] - df["low"], df["close"])
    df["body"] = _safe_div(df["close"] - df["open"], df["open"])
    df["upper_shadow"] = _safe_div(
        df["high"] - df[["close", "open"]].max(axis=1),
        df["close"],
    )

    for window in [5, 25, 75]:
        col_ma = f"ma_{window}"
        col_kairi = f"kairi_{window}"
        df[col_ma] = g["close"].transform(
            lambda x, w=window: x.rolling(w, min_periods=w).mean()
        )
        df[col_kairi] = _safe_div(df["close"] - df[col_ma], df[col_ma])

    df["rsi_14"] = g["close"].transform(calc_rsi)

    df["ma_20"] = g["close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    df["std_20"] = g["close"].transform(lambda x: x.rolling(20, min_periods=20).std())
    df["bb_pos"] = _safe_div(df["close"] - df["ma_20"], 2 * df["std_20"])

    df["vol_ma5"] = g["volume"].transform(lambda x: x.rolling(5, min_periods=5).mean())
    df["vol_ratio"] = _safe_div(df["volume"], df["vol_ma5"])

    # Lv.2
    df["perfect_order"] = (
        (df["ma_5"] > df["ma_25"]) & (df["ma_25"] > df["ma_75"])
    ).astype(int)
    df["trend_strong"] = (
        (df["close"] > df["ma_5"]) & (df["close"] > df["ma_25"])
    ).astype(int)

    # Lv.3 市場環境
    market_return = df.groupby("date")["return_1d"].mean().rename("market_return")
    up_counts = df[df["return_1d"] > 0].groupby("date")["コード"].count()
    total_counts = df.groupby("date")["コード"].count()
    market_sentiment = (up_counts / total_counts).fillna(0).rename("market_sentiment")

    df = df.merge(market_return, on="date", how="left")
    df = df.merge(market_sentiment, on="date", how="left")
    df["relative_strength"] = df["return_1d"] - df["market_return"]

    # ========================================================
    # アルゴ対策＆流動性
    # ========================================================

    # 1. ストップ狩り完了フラグ
    # 旧実装:
    #   groupby.shift(1).rolling(5).min()
    # は shift 後のSeries全体にrollingしてしまうため、
    # 銘柄境界をまたいで別銘柄の安値が混入する。
    df["min_low_5d"] = df.groupby("コード", sort=False)["low"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=5).min()
    )
    df["stop_hunt_reversal"] = (
        (df["low"] < df["min_low_5d"])
        & (df["close"] > df["open"])
        & ((df["open"] - df["low"]) > (df["close"] - df["open"]))
    ).astype(int)

    # 2. 出来高の壁までの距離
    # 「直近20日で出来高最大の日の終値」を毎日再計算する
    df["poc_20d_close"] = _true_rolling_poc_close(df, window=20)
    df["dist_from_poc"] = _safe_div(
        df["close"] - df["poc_20d_close"],
        df["poc_20d_close"],
    )

    # 3. 流動性
    df["turnover"] = df["close"] * df["volume"]
    df["turnover_20d_avg"] = df.groupby("コード", sort=False)["turnover"].transform(
        lambda x: x.rolling(20, min_periods=20).mean()
    )

    conditions = [
        df["turnover_20d_avg"] < 100_000_000,
        (df["turnover_20d_avg"] >= 100_000_000)
        & (df["turnover_20d_avg"] < 3_000_000_000),
        df["turnover_20d_avg"] >= 3_000_000_000,
    ]
    df["liquidity_class"] = np.select(conditions, [0, 1, 2], default=1).astype(int)

    # 数値異常を欠損へ
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df


# ============================================================
# 正解ラベル
# ============================================================
def build_target(
    df: pd.DataFrame,
    term: int = 10,
    target_pct: float = 1.10,
    label_mode: str = "touch",
    stop_pct: float = 0.93,
) -> pd.DataFrame:
    """
    label_mode:
      touch:
        従来意図を維持。
        「翌日からterm営業日以内に高値が現在終値×target_pctへ到達」

      tp_before_sl:
        より売買向け。
        +targetに到達する前に-stopへ到達したものは0。
        同日にTP/SL両方へ触れた場合は順序不明なので保守的に0。

    重要修正:
      term日分すべて未来データが存在する行だけを教師データにする。
      旧コードでは末尾1～9日が短い観測期間のまま0扱いされ得た。
    """
    out = df.copy()
    g = out.groupby("コード", sort=False)

    high_cols = []
    low_cols = []

    for i in range(1, term + 1):
        h = f"__next_high_{i}"
        l = f"__next_low_{i}"
        out[h] = g["high"].shift(-i)
        out[l] = g["low"].shift(-i)
        high_cols.append(h)
        low_cols.append(l)

    # 正確なラベル利用終了日。時系列分割のpurgeにも使う。
    out["label_end_date"] = g["date"].shift(-term)

    # term営業日すべて存在する行のみ
    full_horizon = out["label_end_date"].notna()

    future_high = out[high_cols]
    max_high_term = future_high.max(axis=1)

    if label_mode == "touch":
        target = (max_high_term >= out["close"] * target_pct).astype(float)

    elif label_mode == "tp_before_sl":
        tp_level = out["close"] * target_pct
        sl_level = out["close"] * stop_pct

        tp_day = np.full(len(out), np.nan)
        sl_day = np.full(len(out), np.nan)

        for i, (h, l) in enumerate(zip(high_cols, low_cols), start=1):
            tp_hit = out[h] >= tp_level
            sl_hit = out[l] <= sl_level

            tp_day = np.where(np.isnan(tp_day) & tp_hit, i, tp_day)
            sl_day = np.where(np.isnan(sl_day) & sl_hit, i, sl_day)

        # TPに到達し、SL未到達 or TPが先
        target = (
            (~np.isnan(tp_day))
            & (np.isnan(sl_day) | (tp_day < sl_day))
        ).astype(float)

    else:
        raise ValueError(f"未知の label_mode: {label_mode}")

    # 未来10日を満たさない末尾は教師データにしない
    out["target"] = np.where(full_horizon, target, np.nan)

    out.drop(columns=high_cols + low_cols, inplace=True)
    return out


# ============================================================
# 時系列分割（Purged split）
# ============================================================
def purged_time_split(
    df: pd.DataFrame,
    val_ratio: float = 0.10,
    test_ratio: float = 0.20,
):
    """
    日付で train / validation / test に分割し、
    trainラベルの未来期間がvalidationへ、
    validationラベルの未来期間がtestへ食い込む行を除外する。

    これにより「学習ラベルがテスト期間の株価を見ている」
    という境界リークを防止する。
    """
    if not (0 < val_ratio < 0.5 and 0 < test_ratio < 0.5):
        raise ValueError("val_ratio/test_ratio が不正です")
    if val_ratio + test_ratio >= 0.5:
        raise ValueError("val_ratio + test_ratio は0.5未満を推奨します")

    dates = np.array(sorted(pd.unique(df["date"])))
    if len(dates) < 50:
        raise ValueError("学習日数が少なすぎます")

    val_idx = int(len(dates) * (1.0 - val_ratio - test_ratio))
    test_idx = int(len(dates) * (1.0 - test_ratio))

    val_start = pd.Timestamp(dates[val_idx])
    test_start = pd.Timestamp(dates[test_idx])

    train = df[
        (df["date"] < val_start)
        & (df["label_end_date"] < val_start)
    ].copy()

    valid = df[
        (df["date"] >= val_start)
        & (df["date"] < test_start)
        & (df["label_end_date"] < test_start)
    ].copy()

    test = df[df["date"] >= test_start].copy()

    if train.empty or valid.empty or test.empty:
        raise ValueError("時系列分割後に空データが発生しました")

    # 安全確認
    assert train["label_end_date"].max() < val_start
    assert valid["label_end_date"].max() < test_start

    return train, valid, test, val_start, test_start


# ============================================================
# 評価
# ============================================================
def choose_threshold_by_f1(y_true, prob) -> float:
    """
    validationデータだけでF1最大の閾値を決める。
    testを使って閾値調整しないことでテスト汚染を防ぐ。
    """
    precision, recall, thresholds = precision_recall_curve(y_true, prob)
    if len(thresholds) == 0:
        return 0.5

    p = precision[:-1]
    r = recall[:-1]
    f1 = 2 * p * r / np.maximum(p + r, 1e-12)

    idx = int(np.nanargmax(f1))
    return float(thresholds[idx])


def precision_at_fraction(y_true: pd.Series, prob: np.ndarray, frac: float):
    if len(y_true) == 0:
        return np.nan, np.nan

    n = max(1, int(len(y_true) * frac))
    order = np.argsort(prob)[::-1][:n]
    y_arr = np.asarray(y_true, dtype=int)
    precision = float(y_arr[order].mean())
    base = float(y_arr.mean())
    lift = precision / base if base > 0 else np.nan
    return precision, lift


def evaluate(name: str, y_true, prob, threshold: float):
    y_true = pd.Series(y_true).astype(int).reset_index(drop=True)
    prob = np.asarray(prob, dtype=float)
    pred = (prob >= threshold).astype(int)

    base_rate = float(y_true.mean())
    result = {
        "name": name,
        "n": int(len(y_true)),
        "positive_rate": base_rate,
        "threshold": float(threshold),
        "accuracy": float(accuracy_score(y_true, pred)),
        "precision": float(precision_score(y_true, pred, zero_division=0)),
        "recall": float(recall_score(y_true, pred, zero_division=0)),
        "f1": float(f1_score(y_true, pred, zero_division=0)),
        "brier": float(brier_score_loss(y_true, prob)),
        "confusion_matrix": confusion_matrix(y_true, pred).tolist(),
    }

    if y_true.nunique() >= 2:
        result["roc_auc"] = float(roc_auc_score(y_true, prob))
        result["pr_auc"] = float(average_precision_score(y_true, prob))
    else:
        result["roc_auc"] = None
        result["pr_auc"] = None

    for frac in [0.01, 0.05, 0.10]:
        p, lift = precision_at_fraction(y_true, prob, frac)
        result[f"precision_top_{int(frac*100)}pct"] = p
        result[f"lift_top_{int(frac*100)}pct"] = lift

    print(f"\n[{name}]")
    print(f"  件数          : {result['n']:,}")
    print(f"  陽性率        : {base_rate:.2%}")
    print(f"  判定閾値      : {threshold:.4f}")
    print(f"  Accuracy      : {result['accuracy']:.2%}")
    print(f"  Precision     : {result['precision']:.2%}")
    print(f"  Recall        : {result['recall']:.2%}")
    print(f"  F1            : {result['f1']:.4f}")
    if result["roc_auc"] is not None:
        print(f"  ROC-AUC       : {result['roc_auc']:.4f}")
        print(f"  PR-AUC        : {result['pr_auc']:.4f}")
    print(f"  Brier         : {result['brier']:.5f}")

    for frac in [1, 5, 10]:
        p = result[f"precision_top_{frac}pct"]
        lift = result[f"lift_top_{frac}pct"]
        print(f"  上位{frac:>2}% Precision: {p:.2%} / Lift: {lift:.2f}x")

    print(f"  混同行列      : {result['confusion_matrix']}")
    return result


# ============================================================
# 学習
# ============================================================
def train(
    db_path: str,
    model_path: str,
    term: int = DEFAULT_TERM,
    target_pct: float = DEFAULT_TARGET_PCT,
    stop_pct: float = DEFAULT_STOP_PCT,
    label_mode: str = "touch",
    min_price: float = DEFAULT_MIN_PRICE,
    val_ratio: float = DEFAULT_VAL_RATIO,
    test_ratio: float = DEFAULT_TEST_RATIO,
):
    started = time.time()

    model_path = str(Path(model_path))
    model_dir = str(Path(model_path).parent)
    os.makedirs(model_dir, exist_ok=True)

    print("============================================================")
    print("CatBoost 株価モデル学習 v2")
    print("============================================================")
    print(f"DB           : {db_path}")
    print(f"MODEL        : {model_path}")
    print(f"期間         : {term}営業日")
    print(f"目標         : +{(target_pct - 1) * 100:.1f}%")
    print(f"ラベル方式   : {label_mode}")
    if label_mode == "tp_before_sl":
        print(f"損切り判定   : {(stop_pct - 1) * 100:.1f}%")
    print()

    # --------------------------------------------------------
    # 1. DB
    # --------------------------------------------------------
    print("1. データベースから【全期間】を読み込み中...")
    conn = sqlite3.connect(db_path)
    try:
        query = """
        SELECT
            コード, 日付,
            始値, 高値, 安値, 終値, 出来高
        FROM price_history
        ORDER BY コード, 日付
        """
        raw = pd.read_sql(query, conn)
    finally:
        conn.close()

    if raw.empty:
        raise RuntimeError("price_history が空です")

    print(f"   DB取得件数: {len(raw):,} 行")

    # --------------------------------------------------------
    # 2. 特徴量
    # --------------------------------------------------------
    print("2. 特徴量エンジニアリング中...")
    df = build_features(raw)

    # --------------------------------------------------------
    # 3. 正解ラベル
    # --------------------------------------------------------
    print("3. 正解ラベル作成中...")
    df = build_target(
        df,
        term=term,
        target_pct=target_pct,
        label_mode=label_mode,
        stop_pct=stop_pct,
    )

    # 重要:
    # 価格/出来高による「学習対象の選別」は特徴量・ラベル作成後に行う。
    df = df[(df["close"] >= min_price) & (df["volume"] > 0)].copy()

    # 必要列のみ欠損除去
    required = FEATURES + ["target", "label_end_date", "date"]
    before = len(df)
    df = df.dropna(subset=required).copy()
    df["target"] = df["target"].astype(int)
    df["liquidity_class"] = df["liquidity_class"].astype(int)

    df = df.sort_values(["date", "コード"]).reset_index(drop=True)

    print(f"   学習候補件数: {len(df):,} 行")
    print(f"   欠損/末尾10日など除外: {before - len(df):,} 行")
    print(f"   全体陽性率: {df['target'].mean():.2%}")

    # --------------------------------------------------------
    # 4. Purged split
    # --------------------------------------------------------
    print("4. 時系列Purged splitを実行中...")
    train_df, val_df, test_df, val_start, test_start = purged_time_split(
        df,
        val_ratio=val_ratio,
        test_ratio=test_ratio,
    )

    print(f"   TRAIN: {len(train_df):,} 行 / ～ {train_df['date'].max().date()}")
    print(f"   VALID: {len(val_df):,} 行 / {val_start.date()} ～ {val_df['date'].max().date()}")
    print(f"   TEST : {len(test_df):,} 行 / {test_start.date()} ～ {test_df['date'].max().date()}")
    print(f"   TRAIN陽性率: {train_df['target'].mean():.2%}")
    print(f"   VALID陽性率: {val_df['target'].mean():.2%}")
    print(f"   TEST 陽性率: {test_df['target'].mean():.2%}")

    X_train = train_df[FEATURES].copy()
    y_train = train_df["target"].copy()
    X_val = val_df[FEATURES].copy()
    y_val = val_df["target"].copy()
    X_test = test_df[FEATURES].copy()
    y_test = test_df["target"].copy()

    train_pool = Pool(X_train, y_train, cat_features=CAT_FEATURES)
    val_pool = Pool(X_val, y_val, cat_features=CAT_FEATURES)
    test_pool = Pool(X_test, y_test, cat_features=CAT_FEATURES)

    # --------------------------------------------------------
    # 5. CatBoost
    # --------------------------------------------------------
    print("5. CatBoost学習開始...")
    model = CatBoostClassifier(
        iterations=3000,
        learning_rate=0.03,
        depth=7,
        loss_function="Logloss",
        eval_metric="AUC",
        l2_leaf_reg=5.0,
        random_strength=1.0,
        bootstrap_type="Bernoulli",
        subsample=0.85,
        random_seed=RANDOM_SEED,
        verbose=100,
        allow_writing_files=False,
    )

    model.fit(
        train_pool,
        eval_set=val_pool,
        use_best_model=True,
        early_stopping_rounds=150,
    )

    best_iteration = model.get_best_iteration()
    print(f"   best_iteration: {best_iteration}")

    # --------------------------------------------------------
    # 6. 評価
    # --------------------------------------------------------
    print("6. Validationで閾値決定 → Testで最終評価...")

    val_prob = model.predict_proba(val_pool)[:, 1]
    threshold = choose_threshold_by_f1(y_val, val_prob)

    test_prob = model.predict_proba(test_pool)[:, 1]

    val_metrics = evaluate("VALIDATION", y_val, val_prob, threshold)
    test_metrics = evaluate("TEST", y_test, test_prob, threshold)

    # --------------------------------------------------------
    # 7. Feature importance
    # --------------------------------------------------------
    importances = (
        pd.Series(model.get_feature_importance(), index=FEATURES, name="importance")
        .sort_values(ascending=False)
    )

    print("\n[AIが重要視した指標 TOP10]")
    print(importances.head(10).to_string())

    importance_path = str(Path(model_path).with_suffix(".feature_importance.csv"))
    importances.to_csv(importance_path, encoding="utf-8-sig", header=True)

    # --------------------------------------------------------
    # 8. 保存
    # --------------------------------------------------------
    print("\n7. モデル保存中...")

    backup = _backup_existing(model_path)
    if backup:
        print(f"   既存モデルをバックアップ: {backup}")

    joblib.dump(model, model_path)

    # CatBoostネイティブ形式も保存（joblib版との互換性は維持）
    cbm_path = str(Path(model_path).with_suffix(".cbm"))
    model.save_model(cbm_path)

    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": db_path,
        "model_path": model_path,
        "features": FEATURES,
        "cat_features": CAT_FEATURES,
        "term": term,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "label_mode": label_mode,
        "min_price": min_price,
        "val_ratio": val_ratio,
        "test_ratio": test_ratio,
        "validation_start": str(val_start.date()),
        "test_start": str(test_start.date()),
        "train_rows": int(len(train_df)),
        "validation_rows": int(len(val_df)),
        "test_rows": int(len(test_df)),
        "best_iteration": int(best_iteration) if best_iteration is not None else None,
        "decision_threshold": float(threshold),
        "validation_metrics": val_metrics,
        "test_metrics": test_metrics,
    }

    meta_path = str(Path(model_path).with_suffix(".metadata.json"))
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - started

    print("\n============================================================")
    print("完了")
    print("============================================================")
    print(f"MODEL      : {model_path}")
    print(f"CATBOOST   : {cbm_path}")
    print(f"METADATA   : {meta_path}")
    print(f"IMPORTANCE : {importance_path}")
    print(f"閾値       : {threshold:.4f}")
    print(f"所要時間   : {elapsed:.1f}秒")
    print()
    print("※重要:")
    print("  stop_hunt_reversal と dist_from_poc の定義を修正しているため、")
    print("  推論側でも同じ特徴量計算式に合わせてください。")


def parse_args():
    parser = argparse.ArgumentParser(
        description="CatBoost 株価予測モデル学習（時系列リーク修正版）"
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--model",
        default=os.path.join(DEFAULT_MODEL_DIR, DEFAULT_MODEL_NAME),
    )
    parser.add_argument("--term", type=int, default=DEFAULT_TERM)
    parser.add_argument("--target-pct", type=float, default=DEFAULT_TARGET_PCT)
    parser.add_argument("--stop-pct", type=float, default=DEFAULT_STOP_PCT)
    parser.add_argument(
        "--label-mode",
        choices=["touch", "tp_before_sl"],
        default="touch",
        help="touch=10日以内+10%%到達 / tp_before_sl=損切りより先に目標到達",
    )
    parser.add_argument("--min-price", type=float, default=DEFAULT_MIN_PRICE)
    parser.add_argument("--val-ratio", type=float, default=DEFAULT_VAL_RATIO)
    parser.add_argument("--test-ratio", type=float, default=DEFAULT_TEST_RATIO)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train(
        db_path=args.db,
        model_path=args.model,
        term=args.term,
        target_pct=args.target_pct,
        stop_pct=args.stop_pct,
        label_mode=args.label_mode,
        min_price=args.min_price,
        val_ratio=args.val_ratio,
        test_ratio=args.test_ratio,
    )
