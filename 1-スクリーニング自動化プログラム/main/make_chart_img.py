import os
import time
import logging
import io
from concurrent.futures import ProcessPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image

# ==========================================
# 設定項目
# ==========================================
INPUT_FILE = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt"
OUTPUT_DIR = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\chart_img"

# 同時に起動するブラウザの数（PCスペックに合わせて調整）
MAX_WORKERS = 6 

# チャート切り替え後の待機秒数（描画が間に合わない場合は増やしてください）
CHART_WAIT_SEC = 5 

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_ticker_list(path):
    """銘柄リストをファイルから取得"""
    tickers = []
    if not os.path.exists(path):
        return tickers
    # Windowsの標準的なテキストファイル（Shift-JIS）を想定。エラーなら utf-8 に変更。
    try:
        with open(path, "r", encoding="cp932") as f:
            for line in f:
                code = line.strip()
                if code and code.isdigit() and code != "コード":
                    tickers.append(code)
    except UnicodeDecodeError:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                code = line.strip()
                if code and code.isdigit() and code != "コード":
                    tickers.append(code)
    return tickers

def process_single_stock(code):
    """1つの銘柄を処理するコア関数（並列実行）"""
    file_path = os.path.join(OUTPUT_DIR, f"{code}.png")
    
    # すでに存在する場合はスキップ
    if os.path.exists(file_path):
        return f"{code}: Skiped"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1600,1600") # 切り抜き用に広めに設定
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)
    
    try:
        url = f"https://www.sbisec.co.jp/ETGate/?_ControlID=WPLETsiR001Control&_PageID=WPLETsiR001Idtl30&_DataStoreID=DSWPLETsiR001Control&_ActionID=DefaultAID&getFlg=on&stock_sec_code_mul={code}&exchange_code=JPN"
        driver.get(url)
        
        # 1. ページ読み込み待機とiframeへの潜入
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # 切り抜き用の要素（銘柄ヘッダー）を確保
        name_header = driver.find_element(By.CSS_SELECTOR, "div.mgt15 > div.trHead01")
        loc_a = name_header.location
        size_a = name_header.size

        wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        chart_frame = driver.find_element(By.TAG_NAME, "iframe")
        loc_b = chart_frame.location
        size_b = chart_frame.size

        # 2. iframeに切り替えて「1年」に変更
        driver.switch_to.frame(chart_frame)
        year_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[text()='1年']")))
        driver.execute_script("arguments[0].click();", year_link)
        
        # 3. 描画待ち
        time.sleep(CHART_WAIT_SEC)
        
        # 4. 親ウィンドウに戻ってからメモリ内スクショ
        driver.switch_to.default_content()
        png_data = driver.get_screenshot_as_png()
        
        # 5. Pillowで切り抜き加工
        img = Image.open(io.BytesIO(png_data))
        left = min(loc_a['x'], loc_b['x'])
        right = max(loc_a['x'] + size_a['width'], loc_b['x'] + size_b['width'])
        top = loc_a['y']
        bottom = loc_b['y'] + size_b['height']
        
        cropped_img = img.crop((left, top, right, bottom))
        cropped_img.save(file_path)
        
        return f"{code}: Success"

    except Exception as e:
        return f"{code}: Failed ({str(e)})"
    finally:
        driver.quit()

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ticker_list = get_ticker_list(INPUT_FILE)
    if not ticker_list:
        logging.error(f"銘柄リストが空か、ファイルが見つかりません: {INPUT_FILE}")
        return

    total = len(ticker_list)
    logging.info(f"全 {total} 件の処理を開始（並列数: {MAX_WORKERS} / 待機: {CHART_WAIT_SEC}秒）...")

    # 並列実行の開始
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_single_stock, ticker_list))

    # 結果の集計
    success = sum(1 for r in results if "Success" in r)
    skipped = sum(1 for r in results if "Skiped" in r)
    logging.info(f"完了！ 成功: {success} / スキップ: {skipped} / 全体: {total}")

if __name__ == "__main__":
    main()