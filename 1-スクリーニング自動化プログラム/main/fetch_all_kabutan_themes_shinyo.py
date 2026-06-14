import sqlite3
import requests
from bs4 import BeautifulSoup
import time
from datetime import date
import sys
import re  # ★追加：正規表現モジュール

# 既存のデータベースのパスに合わせて変更してください
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

def get_all_stock_codes(conn):
    cur = conn.cursor()
    try:
        # 自動スクリーニングのメインテーブル(screener)から全銘柄コードを取得
        cur.execute("SELECT DISTINCT コード FROM screener")
        
        # 取得したコードを4桁の文字列に整形してリスト化（Noneは弾く）
        return [str(row[0]).zfill(4) for row in cur.fetchall() if row[0] is not None]
        
    except Exception as e:
        print(f"コード一覧の取得に失敗しました: {e}")
        return []
        
def scrape_kabutan_all_themes():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- テーブル作成：テーマ用 ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS theme_master (
            theme_id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_theme_kabutan (
            コード TEXT NOT NULL,
            theme_id INTEGER NOT NULL,
            取得日 TEXT,
            PRIMARY KEY (コード, theme_id)
        )
    """)
    
    # --- ★追加 テーブル作成：信用取引用 ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_credit_margin (
            コード TEXT PRIMARY KEY,
            基準日 TEXT,
            売り残 INTEGER,
            買い残 INTEGER,
            倍率 REAL,
            取得日 TEXT
        )
    """)
    conn.commit()

    codes = get_all_stock_codes(conn)
    if not codes:
        print("処理対象の銘柄コードがありません。")
        sys.exit()

    today = date.today().isoformat()
    print(f"全 {len(codes)} 銘柄のデータ(テーマ ＆ 信用残)収集を開始します...")

    session = requests.Session()
    
    # User-Agentを追加してブラウザからのアクセスに見せかける
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    for i, code in enumerate(codes):
        url = f"https://kabutan.jp/stock/?code={code}"
        
        try:
            res = session.get(url, timeout=10)
            res.raise_for_status()
            # 文字化け対策（株探は utf-8）
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, "html.parser")

            # ==========================================
            # 1. テーマの取得・保存ロジック
            # ==========================================
            extracted_themes = set()
            theme_th = soup.find("th", string="テーマ")
            
            if theme_th:
                theme_td = theme_th.find_next_sibling("td")
                if theme_td:
                    theme_links = theme_td.find_all("a", href=lambda h: h and h.startswith("/themes/?theme="))
                    
                    for a_tag in theme_links:
                        theme_name = a_tag.text.strip()
                        if theme_name:
                            extracted_themes.add(theme_name)

            for tname in extracted_themes:
                cur.execute("INSERT OR IGNORE INTO theme_master (theme_name) VALUES (?)", (tname,))
                cur.execute("SELECT theme_id FROM theme_master WHERE theme_name = ?", (tname,))
                row = cur.fetchone()
                if row:
                    theme_id = row[0]
                    cur.execute("""
                        INSERT OR IGNORE INTO stock_theme_kabutan(コード, theme_id, 取得日) 
                        VALUES (?, ?, ?)
                    """, (code, theme_id, today))

            # ==========================================
            # 2. ★追加：信用取引の取得・保存ロジック
            # ==========================================
            credit_h2 = soup.find("h2", string=re.compile(r"信用取引"))
            if credit_h2:
                credit_table = credit_h2.find_next_sibling("table")
                if credit_table:
                    tbody = credit_table.find("tbody")
                    if tbody:
                        # 最初の行(最新の日付)を取得
                        first_row = tbody.find("tr")
                        if first_row:
                            time_tag = first_row.find("time")
                            margin_date = time_tag["datetime"] if time_tag else None

                            tds = first_row.find_all("td")
                            if len(tds) >= 3:
                                # カンマを除去
                                sell_str = tds[0].text.replace(",", "").strip()
                                buy_str = tds[1].text.replace(",", "").strip()
                                ratio_str = tds[2].text.replace(",", "").strip()

                                # --- ★変更：安全に数値変換する関数を定義 ---
                                def safe_float(s):
                                    try:
                                        return float(s)
                                    except ValueError:
                                        return None  # 全角ハイフン等で変換失敗したらNoneを返す

                                sell_f = safe_float(sell_str)
                                buy_f = safe_float(buy_str)
                                ratio = safe_float(ratio_str)

                                # 値があれば1000倍して整数化、なければNone
                                sell_bal = int(sell_f * 1000) if sell_f is not None else None
                                buy_bal = int(buy_f * 1000) if buy_f is not None else None

                                # DBへ保存 (INSERT OR REPLACEで常に最新の1行で上書き)
                                cur.execute("""
                                    INSERT OR REPLACE INTO stock_credit_margin
                                    (コード, 基準日, 売り残, 買い残, 倍率, 取得日) 
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (code, margin_date, sell_bal, buy_bal, ratio, today))

            # 変更を確定
            conn.commit()
            print(f"[{i+1}/{len(codes)}] {code} : テーマ({len(extracted_themes)}件) ＆ 信用残 更新完了")

        except Exception as e:
            print(f"[{i+1}/{len(codes)}] {code} : エラー発生 -> {e}")

        # サーバー負荷軽減（IPBAN回避）のための待機
        time.sleep(1.5)

    conn.close()
    print("すべての収集が完了しました！")

if __name__ == "__main__":
    scrape_kabutan_all_themes()