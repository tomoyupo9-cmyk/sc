import asyncio
import os
import time
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page

# ==== 設定 ====
# CHROME_PATH の代わりに Playwright が自動でブラウザを管理します
INPUT_CODES = Path('H:/desctop/株攻略/1-スクリーニング自動化プログラム/main/input_data/株コード番号.txt')
OUTPUT_TXT = Path('H:/desctop/株攻略/1-スクリーニング自動化プログラム/main/input_data/空売り無しリスト.txt')
BASE_URL = 'https://karauri.net'
HEADLESS = True  # ヘッドレスモード (True/False)
REQUEST_INTERVAL_MS = 800  # アクセス間隔 (ミリ秒) - ★並行処理の総量制御に使用
MAX_RETRY = 5  # 最大リトライ回数
CONCURRENT_LIMIT = 5 # ★最大同時実行数（並行で開くページ数の上限）

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
            # Puppeteerのバックオフ: 500 * (2^i) ms
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

def ensure_output(file_path):
    """出力ファイルを初期化し、ヘッダ行を書き込みます"""
    # ファイルが存在しても、常にヘッダを上書き（初期化）する
    file_path.write_text('コード\n', encoding='utf-8')

def check_and_skip(file_path):
    """出力ファイルの更新日時をチェックし、今日の日付ならTrueを返します"""
    if not file_path.exists():
        return False

    try:
        mtime_ts = os.path.getmtime(file_path)
        mtime = datetime.fromtimestamp(mtime_ts).date()
        today = datetime.now().date()
    except Exception as e:
        print(f"警告: ファイル日時のチェック中にエラーが発生: {e}")
        return False

    if mtime == today:
        print(f"[karauri] 本日分の結果が既に存在するためスキップします: {file_path}")
        return True
    return False

# ----------------------------------------------------

# ==== 並行処理ワーカー関数 ====
async def fetch_karauri_info(
    browser: Browser, 
    code: str, 
    results: list, 
    semaphore: asyncio.Semaphore, 
    request_interval_ms: float, 
    base_url: str
):
    """
    単一の株コードに対して Playwright アクセスと判定を行うワーカー関数
    """
    url = f"{base_url}/{code}/"
    print(f'GET {code}: {url}')

    # ★セマフォで同時実行数を制限
    async with semaphore:
        # ★アクセス間隔の待機 (並行実行数を考慮した調整)
        await sleep(request_interval_ms) 
        
        # ★単一のブラウザインスタンス内に新しいページを作成し、処理後に閉じる
        page: Page = await browser.new_page()
        # タイムアウトの設定
        page.set_default_timeout(23_000) # 個別のページ操作タイムアウト

        try:
            # リトライ処理
            async def fetch_page_and_check():
                # Playwrightの page.goto
                await page.goto(url, wait_until="domcontentloaded", timeout=23_000)
                
                # 要素の存在チェック
                has_message = await page.query_selector('.message_c')
                
                if has_message:
                    results.append(code)
                    print(f' → 空売り無し: {code} (発見)')
                # else:
                    # print(f' → 空売りあり: {code}')
                return True # 成功

            await retry(fetch_page_and_check)

        except Exception as err:
            error_msg = str(err)
            if hasattr(err, 'message'):
                error_msg = err.message
            print(f" × 失敗: {code} {error_msg}")
        
        finally:
            # ページを閉じてリソースを解放
            await page.close()


# ==== 本体（並行処理対応） ====
async def main():
    if check_and_skip(OUTPUT_TXT):
        return

    codes = load_codes(INPUT_CODES)
    if not codes:
        return

    print(f"対象コード: {len(codes)}件")
    ensure_output(OUTPUT_TXT)
    results = []
    
    # ★セマフォを初期化し、同時実行数の上限を設定
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    # ★並行処理に合わせて調整されたアクセス間隔
    # 例: 800ms / 5 = 160ms (全体で800msあたり5リクエスト程度を目指す)
    adjusted_interval = REQUEST_INTERVAL_MS / CONCURRENT_LIMIT 

    async with async_playwright() as p:
        # ブラウザ起動
        browser = await p.chromium.launch(headless=HEADLESS, args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
        ])
        
        # ★全てのタスクを作成
        tasks = [
            fetch_karauri_info(
                browser, 
                code, 
                results, 
                semaphore, 
                adjusted_interval,
                BASE_URL
            )
            for code in codes
        ]

        # ★タスクを並行実行し、全て完了するのを待機
        print(f"--- 並行処理開始 (同時最大 {CONCURRENT_LIMIT} 件) ---")
        await asyncio.gather(*tasks)
        print("--- 全てのタスク完了 ---")

        await browser.close()

    if results:
        # fs.appendFileSync と似た処理
        # ヘッダーは ensure_output で書き込み済みのため、結果のみ追記
        with open(OUTPUT_TXT, 'a', encoding='utf-8') as f:
            f.write('\n'.join(results) + '\n')
            
    print(f'完了: 空売り無し = {len(results)} 件')

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print('致命的エラー:', e)
        exit(1)