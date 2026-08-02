import asyncio
import os
import time
import sqlite3
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page

# ==== 設定 ====
INPUT_CODES = Path('H:/desctop/株攻略/1-スクリーニング自動化プログラム/main/input_data/株コード番号.txt')
DB_PATH = Path('H:/desctop/株攻略/1-スクリーニング自動化プログラム/main/db/kani2.db')
# 1日1回実行を管理するマーカーファイルのパス
MARKER_FILE = Path('H:/desctop/株攻略/1-スクリーニング自動化プログラム/main/input_data/.karauri_executed_today')
BASE_URL = 'https://karauri.net'
HEADLESS = True  # ヘッドレスモード (True/False)
REQUEST_INTERVAL_MS = 800  # アクセス間隔 (ミリ秒)
MAX_RETRY = 5  # 最大リトライ回数
CONCURRENT_LIMIT = 5 # 最大同時実行数

# ==== データベース初期化 ====
def init_db(db_path):
    """機関の空売り残高情報を保存するテーブルを準備します"""
    os.makedirs(db_path.parent, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS institution_short_sales (
            code TEXT,
            calc_date TEXT,
            institution_name TEXT,
            ratio REAL,
            ratio_change REAL,
            shares INTEGER,
            shares_change INTEGER,
            note TEXT,
            PRIMARY KEY (code, calc_date, institution_name)
        )
    """)
    # 検索用インデックス
    cur.execute("CREATE INDEX IF NOT EXISTS idx_iss_code ON institution_short_sales(code)")
    conn.commit()
    conn.close()

def check_and_skip_today():
    """マーカーファイルの更新日時をチェックし、今日すでに実行済みならTrueを返します"""
    if not MARKER_FILE.exists():
        return False

    try:
        mtime_ts = os.path.getmtime(MARKER_FILE)
        mtime = datetime.fromtimestamp(mtime_ts).date()
        today = datetime.now().date()
        
        if mtime == today:
            print(f"[karauri] 本日はすでにスクリプトを実行済みのためスキップします。")
            return True
    except Exception as e:
        print(f"警告: 実行日時のチェック中にエラーが発生: {e}")
        
    return False

def touch_marker():
    """本日の実行完了を示すマーカーファイルを更新（作成）します"""
    try:
        MARKER_FILE.parent.mkdir(parents=True, exist_ok=True)
        MARKER_FILE.touch()
    except Exception as e:
        print(f"警告: マーカーファイルの更新に失敗しました: {e}")

# ==== ユーティリティ ====
async def sleep(ms):
    """非同期で指定された時間だけ待機します"""
    await asyncio.sleep(ms / 1000.0)

async def retry(func, *args, retry_count=MAX_RETRY):
    """指定された関数をリトライします（指数バックオフ付き）"""
    last_err = None
    for i in range(retry_count + 1):
        try:
            return await func(*args)
        except Exception as e:
            last_err = e
            wait_time = 0.5 * (2 ** i)
            print(f"警告: リトライ({i+1}/{retry_count+1}) 待機 {wait_time:.1f}秒...")
            await asyncio.sleep(wait_time)
    raise last_err

def load_codes(file_path):
    """ファイルから4桁の株コードを読み込みます"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"エラー: 入力ファイルが見つかりません: {file_path}")
        return []

    return [
        s.strip()
        for s in raw.splitlines()
        if len(s.strip()) == 4 and s.strip().isdigit()
    ]

def parse_num(text, is_float=False):
    """文字列から数値（float/int）を安全に抽出します"""
    if not text:
        return 0.0 if is_float else 0
    clean = re.sub(r'[^\d\.\-\+]', '', text)
    try:
        return float(clean) if is_float else int(clean)
    except ValueError:
        return 0.0 if is_float else 0


# ==== 並行処理ワーカー関数 ====
async def fetch_karauri_info(
    browser: Browser, 
    code: str, 
    all_rows_to_insert: list, 
    semaphore: asyncio.Semaphore, 
    request_interval_ms: float, 
    base_url: str
):
    """
    単一の株コードに対して Playwright でアクセスし、詳細な機関の空売り表を解析するワーカー関数
    """
    url = f"{base_url}/{code}/"
    print(f'GET {code}: {url}')

    async with semaphore:
        await sleep(request_interval_ms) 
        page: Page = await browser.new_page()
        page.set_default_timeout(23_000)

        try:
            async def fetch_page_and_parse():
                await page.goto(url, wait_until="domcontentloaded", timeout=23_000)
                
                # 1. 空売り無しメッセージのチェック
                has_message = await page.query_selector('.message_c')
                if has_message:
                    print(f' → 空売り無し: {code} (発見)')
                    return True

                # 2. 空売り残高の明細テーブル（行）を全て取得
                rows = await page.query_selector_all('table tr')
                parsed_count = 0

                for row in rows:
                    cols = await row.query_selector_all('td')
                    if len(cols) >= 6:
                        date_str = (await cols[0].inner_text()).strip()
                        inst_name = (await cols[1].inner_text()).strip()
                        ratio_str = (await cols[2].inner_text()).strip()
                        ratio_chg_str = (await cols[3].inner_text()).strip()
                        shares_str = (await cols[4].inner_text()).strip()
                        shares_chg_str = (await cols[5].inner_text()).strip()
                        
                        note_str = ""
                        if len(cols) >= 7:
                            note_str = (await cols[6].inner_text()).strip()

                        # 日付フォーマットの簡易チェック (YYYY/MM/DD)
                        if re.match(r'^\d{4}/\d{2}/\d{2}$', date_str):
                            calc_date = date_str.replace('/', '-')
                            ratio = parse_num(ratio_str, is_float=True)
                            ratio_chg = parse_num(ratio_chg_str, is_float=True)
                            shares = parse_num(shares_str, is_float=False)
                            shares_chg = parse_num(shares_chg_str, is_float=False)

                            all_rows_to_insert.append((
                                code, calc_date, inst_name, ratio, ratio_chg, shares, shares_chg, note_str
                            ))
                            parsed_count += 1

                if parsed_count > 0:
                    print(f' → 空売り情報あり: {code} ({parsed_count}件の明細取得)')
                return True

            await retry(fetch_page_and_parse)

        except Exception as err:
            error_msg = str(err)
            if hasattr(err, 'message'):
                error_msg = err.message
            print(f" × 失敗: {code} {error_msg}")
        
        finally:
            await page.close()


# ==== 本体（並行処理対応） ====
async def main():
    # 1日1回実行済みかチェック（今日すでに走っていたらここで終了）
    if check_and_skip_today():
        return

    codes = load_codes(INPUT_CODES)
    if not codes:
        return

    print(f"対象コード: {len(codes)}件")
    init_db(DB_PATH)
    
    all_rows_to_insert = []
    
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    adjusted_interval = REQUEST_INTERVAL_MS / CONCURRENT_LIMIT 

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
        ])
        
        tasks = [
            fetch_karauri_info(
                browser, 
                code, 
                all_rows_to_insert, 
                semaphore, 
                adjusted_interval,
                BASE_URL
            )
            for code in codes
        ]

        print(f"--- 並行処理開始 (同時最大 {CONCURRENT_LIMIT} 件) ---")
        await asyncio.gather(*tasks)
        print("--- 全てのタスク完了 ---")

        await browser.close()

    # まとめてデータベースに一括保存 (UPSERT)
    if all_rows_to_insert:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO institution_short_sales (code, calc_date, institution_name, ratio, ratio_change, shares, shares_change, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code, calc_date, institution_name) DO UPDATE SET
                ratio = excluded.ratio,
                ratio_change = excluded.ratio_change,
                shares = excluded.shares,
                shares_change = excluded.shares_change,
                note = excluded.note
        """, all_rows_to_insert)
        conn.commit()
        conn.close()
        print(f'完了: 機関空売りデータ {len(all_rows_to_insert)} 件をDBに保存しました。')

    # 最後に本日の実行完了マーカーを更新
    touch_marker()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print('致命的エラー:', e)
        exit(1)