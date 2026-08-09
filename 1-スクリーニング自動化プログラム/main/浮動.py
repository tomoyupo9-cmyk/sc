import sqlite3
import time
import yfinance as yf

# データベースのパスを設定
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 浮動株数・発行済株式数のカラムが存在しない場合は追加
    for col in ["浮動株数", "発行済株式数"]:
        try:
            cur.execute(f'ALTER TABLE screener ADD COLUMN "{col}" REAL')
        except sqlite3.OperationalError:
            pass
            
    # ★ 改良1: すでに取得済みの銘柄はスキップし、欠損している銘柄だけを対象にする（途中再開）
    cur.execute("SELECT コード FROM screener WHERE 浮動株数 IS NULL OR 発行済株式数 IS NULL")
    rows = cur.fetchall()
    
    total = len(rows)
    if total == 0:
        print("すべての銘柄の浮動株数が取得済みです！")
        return

    print(f"未取得の {total} 銘柄の浮動株数取得を開始します...")
    
    start_time = time.time()
    updates = []
    
    for i, (code,) in enumerate(rows, 1):
        code_str = str(code).strip().zfill(4)
        ticker_symbol = f"{code_str}.T"
        
        float_shares = None
        shares_out = None
        
        # ★ 改良2: レート制限に引っかかった場合のリトライ処理
        max_retries = 3
        for attempt in range(max_retries):
            try:
                info = yf.Ticker(ticker_symbol).info
                float_shares = info.get("floatShares")
                shares_out = info.get("sharesOutstanding")
                break # 成功したらループを抜ける
            except Exception as e:
                error_msg = str(e)
                if "Too Many Requests" in error_msg or "Rate limited" in error_msg:
                    wait_time = 15 * (attempt + 1) # 15秒, 30秒, 45秒と徐々に待機時間を増やす
                    print(f"\n[{i:04d}/{total}] {ticker_symbol} レート制限です。{wait_time}秒待機してリトライします({attempt+1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    print(f"\n[{i:04d}/{total}] {ticker_symbol} 取得エラー: {e}")
                    break # レート制限以外のエラーはリトライしない
        
        updates.append((float_shares, shares_out, code))
        
        if i % 10 == 0 or i == total:
            elapsed = time.time() - start_time
            print(f"[{i:04d}/{total}] 処理中... 経過時間: {elapsed:.1f}秒")
            
        # ★ 改良3: APIへの負荷を軽減するため、リクエスト間に必ず0.5秒の待機を入れる
        time.sleep(0.5)
        
        # ★ 改良4: 100件ごとにこまめにDBへ保存する（途中で強制終了しても進捗を失わないため）
        if len(updates) >= 100 or i == total:
            cur.executemany("""
                UPDATE screener 
                SET 浮動株数 = ?, 発行済株式数 = ?
                WHERE コード = ?
            """, updates)
            conn.commit()
            updates = [] # リセット
            
    end_time = time.time()
    total_time = end_time - start_time
    
    print("\nデータベースの更新が完了しました。")
    print("-" * 30)
    print(f"総所要時間: {total_time:.1f}秒 ({total_time/60:.2f}分)")
    print("-" * 30)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()