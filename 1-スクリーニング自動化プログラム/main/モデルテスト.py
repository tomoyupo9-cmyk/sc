import sqlite3
import pandas as pd
import lightgbm as lgb
from datetime import datetime, timedelta
import os
import numpy as np

# ★設定
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

def verify():
    print("1. 検証用データを読み込んでいます...")
    if not os.path.exists(DB_PATH):
        print(f"エラー: DBが見つかりません {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # 全期間取得
    query = """
    SELECT コード, 日付, 始値, 高値, 安値, 終値, 出来高
    FROM price_history
    ORDER BY 日付, コード
    """
    
    query_names = "SELECT コード, 銘柄名 FROM screener"
    
    try:
        df = pd.read_sql(query, conn)
        df_names = pd.read_sql(query_names, conn)
    except Exception as e:
        print(f"データ読み込みエラー: {e}")
        return
    finally:
        conn.close()

    if df.empty: return

    # --- ノイズ除去フィルタ ---
    # ★変更: 300円未満は除外
    df = df[df['終値'] >= 300]
    df = df[df['出来高'] > 0]

    name_map = dict(zip(df_names['コード'], df_names['銘柄名']))
    df['銘柄名'] = df['コード'].map(name_map).fillna("不明")

    # --- 特徴量エンジニアリング ---
    print("2. データを加工中 (厳選仕様)...")
    df = df.rename(columns={'始値': 'open', '高値': 'high', '安値': 'low', '終値': 'close', '出来高': 'volume'})
    df['date'] = pd.to_datetime(df['日付'])

    # Lv.1
    df['return_1d'] = df.groupby('コード')['close'].pct_change(1)
    df['range'] = (df['high'] - df['low']) / df['close']
    df['body']  = (df['close'] - df['open']) / df['open']
    df['upper_shadow'] = (df['high'] - df[['close', 'open']].max(axis=1)) / df['close']

    for w in [5, 25, 75]:
        col_ma = f'ma_{w}'
        col_kairi = f'kairi_{w}'
        df[col_ma] = df.groupby('コード')['close'].transform(lambda x: x.rolling(w).mean())
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

    # Lv.3 (Market)
    market_return = df.groupby('date')['return_1d'].mean().rename('market_return')
    up_counts = df[df['return_1d'] > 0].groupby('date')['コード'].count()
    total_counts = df.groupby('date')['コード'].count()
    market_sentiment = (up_counts / total_counts).fillna(0).rename('market_sentiment')
    
    df = df.merge(market_return, on='date', how='left')
    df = df.merge(market_sentiment, on='date', how='left')
    df['relative_strength'] = df['return_1d'] - df['market_return']

    # --- 正解データ (10日以内 +10%) ---
    term = 10
    target_pct = 1.10

    cols = []
    for i in range(1, term + 1):
        col_name = f'next_high_{i}'
        df[col_name] = df.groupby('コード')['high'].shift(-i)
        cols.append(col_name)

    df['max_high_term'] = df[cols].max(axis=1)
    df['target'] = (df['max_high_term'] >= df['close'] * target_pct).astype(int)

    full_df = df.copy() # 追跡用
    df = df.drop(columns=cols + ['max_high_term']).dropna()

    # --- 検証 ---
    features = [
        'return_1d', 'range', 'body', 'upper_shadow',
        'kairi_5', 'kairi_25', 'kairi_75', 'rsi_14', 'bb_pos', 'vol_ratio',
        'perfect_order', 'trend_strong',
        'market_return', 'market_sentiment', 'relative_strength'
    ]

    last_date = df['date'].max()
    split_date = last_date - timedelta(days=60)
    print(f"分割日: {split_date.date()} (これより後ろをテストします)")

    train_df = df[df['date'] < split_date]
    test_df  = df[df['date'] >= split_date]

    if test_df.empty: return

    print("3. 学習中...")
    model = lgb.LGBMClassifier(random_state=42, n_jobs=-1, learning_rate=0.05, n_estimators=500, verbose=-1)
    model.fit(train_df[features], train_df['target'])

    print("4. 予測＆検証中...")
    preds = model.predict_proba(test_df[features])[:, 1]
    result_df = test_df.copy()
    result_df['AIスコア'] = preds

    print("\n" + "="*110)
    print("      ★ スイング予測(10日10% / 300円未満カット) 検証ログ ★")
    print("="*110)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 1000)

    def track_performance(row):
        code = row['コード']
        entry_date = row['date']
        entry_price = row['close']
        target_price = entry_price * 1.10 # ★目標+10%
        
        # 20営業日追跡
        future_data = full_df[
            (full_df['コード'] == code) & 
            (full_df['date'] > entry_date)
        ].sort_values('date').head(20)
        
        reach_date = None
        days_needed = None
        status = "LOSE"

        for i, f_row in enumerate(future_data.itertuples()):
            # 高値が目標を超えたら
            if f_row.high >= target_price:
                reach_date = f_row.date
                days_needed = i + 1
                if days_needed <= 10:
                    status = "WIN"   # 10日以内達成
                else:
                    status = "LATER" # 11日〜20日後に達成
                break
        
        return pd.Series([reach_date, days_needed, status])

    for th in [0.7, 0.8]:
        picks = result_df[result_df['AIスコア'] >= th].copy()
        count = len(picks)
        
        print(f"\n\n● AI自信度 {th*100:.0f}% 以上 (エントリー数: {count})")
        
        if count > 0:
            print("   ...未来の値動きを追跡中...")
            track_results = picks.apply(track_performance, axis=1)
            track_results.columns = ['達成日', '所要日数', '追跡判定']
            
            picks = pd.concat([picks, track_results], axis=1)
            
            # 整数変換
            picks['目標値'] = (picks['close'] * 1.10).fillna(0).astype(int)
            picks['close'] = picks['close'].fillna(0).astype(int)
            
            picks['日付'] = picks['date'].dt.strftime('%m/%d')
            picks['達成日'] = picks['達成日'].apply(lambda x: x.strftime('%m/%d') if pd.notnull(x) else '-')
            picks['所要日数'] = picks['所要日数'].apply(lambda x: f"{int(x)}日" if pd.notnull(x) else '-')
            picks['自信度'] = (picks['AIスコア'] * 100).round(1).astype(str) + '%'
            
            cols = ['日付', 'コード', '銘柄名', '自信度', 'close', '目標値', '追跡判定', '達成日', '所要日数']
            rename = {'close': '株価', '追跡判定': '判定', '目標値': '目標'}
            
            view = picks.sort_values(['date', 'AIスコア'], ascending=[True, False])[cols].rename(columns=rename)
            
            print("-" * 105)
            print(f"{'GOサイン':<8} {'コード':<6} {'銘柄名':<10} {'自信度':<8} {'株価':>7} {'目標(+10%)':>10} {'判定':<6} {'達成日':<8} {'所要'}")
            print("-" * 105)
            
            win_count = 0
            later_count = 0
            
            for _, r in view.iterrows():
                judge = r['判定']
                if judge == 'WIN': win_count += 1
                if judge == 'LATER': later_count += 1
                
                name = str(r['銘柄名'])[:8]
                print(f"{r['日付']:<8} {r['コード']:<6} {name:<10} {r['自信度']:<8} {r['株価']:>7} {r['目標']:>10} {judge:<6} {r['達成日']:<8} {r['所要日数']}")
            
            print("-" * 105)
            print(f"10日以内勝利: {win_count}回")
            print(f"20日以内達成: {win_count + later_count}回")
            print(f"勝率: {win_count}/{count} = {win_count/count:.1%}")

        else:
            print("該当なし")

if __name__ == "__main__":
    verify()