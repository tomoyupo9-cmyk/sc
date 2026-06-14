import sqlite3
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
import joblib
import time
import os
import numpy as np

# ★設定
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"
MODEL_DIR = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\model"
MODEL_PATH = os.path.join(MODEL_DIR, "stock_predictor_lv3.pkl")

def train():
    start_time = time.time()
    
    if not os.path.exists(MODEL_DIR):
        os.makedirs(MODEL_DIR, exist_ok=True)

    print("1. データベースから【全期間】のデータを読み込んでいます...")
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT 
        コード, 日付, 
        始値, 高値, 安値, 終値, 出来高
    FROM price_history
    ORDER BY 日付, コード
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"エラー: {e}")
        return
    finally:
        conn.close()

    if df.empty:
        return

    # --- ノイズ除去 ---
    df = df[df['終値'] >= 300]
    df = df[df['出来高'] > 0]

    print(f"   取得件数(300円未満カット後): {len(df)} 行")
    print("2. 特徴量エンジニアリング（10日10%仕様）を実行中...")

    df = df.rename(columns={'始値': 'open', '高値': 'high', '安値': 'low', '終値': 'close', '出来高': 'volume'})
    df['date'] = pd.to_datetime(df['日付'])

    # Lv.1
    df['return_1d'] = df.groupby('コード')['close'].pct_change(1)
    df['range'] = (df['high'] - df['low']) / df['close']
    df['body']  = (df['close'] - df['open']) / df['open']
    df['upper_shadow'] = (df['high'] - df[['close', 'open']].max(axis=1)) / df['close']

    for window in [5, 25, 75]:
        col_ma = f'ma_{window}'
        col_kairi = f'kairi_{window}'
        df[col_ma] = df.groupby('コード')['close'].transform(lambda x: x.rolling(window).mean())
        df[col_kairi] = (df['close'] - df[col_ma]) / df[col_ma]

    def calc_rsi(series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        return 100 - (100 / (1 + gain/loss))

    df['rsi_14'] = df.groupby('コード')['close'].transform(lambda x: calc_rsi(x))

    df['ma_20'] = df.groupby('コード')['close'].transform(lambda x: x.rolling(20).mean())
    df['std_20'] = df.groupby('コード')['close'].transform(lambda x: x.rolling(20).std())
    df['bb_pos'] = (df['close'] - df['ma_20']) / (2 * df['std_20'])

    df['vol_ma5'] = df.groupby('コード')['volume'].transform(lambda x: x.rolling(5).mean())
    df['vol_ratio'] = df['volume'] / df['vol_ma5']

    # Lv.2
    df['perfect_order'] = ((df['ma_5'] > df['ma_25']) & (df['ma_25'] > df['ma_75'])).astype(int)
    df['trend_strong'] = ((df['close'] > df['ma_5']) & (df['close'] > df['ma_25'])).astype(int)

    # Lv.3
    market_return = df.groupby('date')['return_1d'].mean().rename('market_return')
    up_counts = df[df['return_1d'] > 0].groupby('date')['コード'].count()
    total_counts = df.groupby('date')['コード'].count()
    market_sentiment = (up_counts / total_counts).fillna(0).rename('market_sentiment')

    df = df.merge(market_return, on='date', how='left')
    df = df.merge(market_sentiment, on='date', how='left')
    df['relative_strength'] = df['return_1d'] - df['market_return']

    # 正解ラベル
    term = 10
    target_pct = 1.10

    cols = []
    for i in range(1, term + 1):
        col_name = f'next_high_{i}'
        df[col_name] = df.groupby('コード')['high'].shift(-i)
        cols.append(col_name)

    df['max_high_term'] = df[cols].max(axis=1)
    df['target'] = (df['max_high_term'] >= df['close'] * target_pct).astype(int)

    df = df.drop(columns=cols + ['max_high_term']).dropna()

    features = [
        'return_1d', 'range', 'body', 'upper_shadow',
        'kairi_5', 'kairi_25', 'kairi_75',
        'rsi_14', 'bb_pos', 'vol_ratio',
        'perfect_order', 'trend_strong',
        'market_return', 'market_sentiment', 'relative_strength'
    ]

    print(f"3. AI学習開始（学習データ数: {len(df)}）...")
    X = df[features]
    y = df['target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # ★★★ CatBoost に置き換え ★★★
    model = CatBoostClassifier(
        iterations=500,
        learning_rate=0.05,
        depth=8,
        loss_function='Logloss',
        verbose=False,
        random_seed=42
    )

    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    print(f"   テストデータでの正解率: {score:.2%}")

    # 重要度
    importances = pd.Series(model.get_feature_importance(), index=features).sort_values(ascending=False)
    print("\n   [AIが重要視した指標トップ5]")
    print(importances.head(5))

    print(f"モデルを保存しています... -> {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)

    elapsed = time.time() - start_time
    print(f"\n完了！ CatBoost版モデルを保存しました。（所要時間: {elapsed:.1f}秒）")

if __name__ == "__main__":
    train()
