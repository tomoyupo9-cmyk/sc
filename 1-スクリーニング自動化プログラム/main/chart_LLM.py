import os
import time
import logging
import ollama
import re
import csv
from PIL import Image, ImageDraw, ImageFont

# ==========================================
# 設定項目
# ==========================================
MODEL_ID = "llava" 
SRC_DIR = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\chart_img"
DST_DIR = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\chart_img_LLM"
CSV_PATH = os.path.join(DST_DIR, "stock_analysis_results.csv")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# AIに「本気のプロ分析」を英語でさせる（精度優先）
ANALYZE_PROMPT = """
Analyze this stock chart as an expert trader. 
1. Rank: 【最高】, 【注目】, 【期待】, or 【見送り】.
2. Reason: Detailed logic on price action, trend, and volume.
"""

# AIに「意味の通る日本語」へ翻訳・要約させる
TRANSLATE_PROMPT = """
以下の英文の株価分析を、プロの視点で「意味の通る自然な日本語」に翻訳してください。
短くまとめる必要はありませんが、論理的で読みやすい文章にしてください。
文頭には必ず判定（【最高】など）をつけてください。
Input:
"""

def translate_to_japanese(raw_text):
    try:
        response = ollama.chat(
            model=MODEL_ID,
            messages=[{'role': 'user', 'content': TRANSLATE_PROMPT + raw_text}]
        )
        return response['message']['content'].strip()
    except Exception as e:
        logging.error(f"翻訳エラー: {e}")
        return "【エラー】翻訳に失敗しました。"

def add_rank_only_stamp(img_path, comment, save_path):
    """画像には【判定】のみを刻印する。チャートを一切隠さない"""
    try:
        img = Image.open(img_path).convert("RGB")
        draw = ImageDraw.Draw(img)
        
        # 判定ラベル（【最高】など）だけを抽出
        match = re.search(r'【(最高|注目|期待|見送り)】', comment)
        display_text = match.group(0) if match else "【解析済】"
            
        font_size = 32 # 判定のみなので、少し大きくして見やすく
        try:
            font = ImageFont.truetype("msjh.ttc", font_size)
        except:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), display_text, font=font)
        tw, th = bbox[2]-bbox[0], bbox[3]-bbox[1]
        
        # 右下の邪魔にならない位置
        x = img.width - tw - 20
        y = img.height - th - 20
        
        draw.rectangle([x-5, y-5, x+tw+5, y+th+5], fill=(0, 0, 0))
        draw.text((x, y), display_text, font=font, fill=(255, 255, 0)) 
        
        img.save(save_path)
    except Exception as e:
        logging.error(f"描画エラー: {e}")

def main():
    if not os.path.exists(DST_DIR): os.makedirs(DST_DIR)
    
    # CSVのヘッダー：短文を廃止し、長文のみに
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'w', newline='', encoding='utf_8_sig') as f:
            writer = csv.writer(f)
            writer.writerow(["コード", "判定", "分析詳細（日本語長文）"])

    all_files = [f for f in os.listdir(SRC_DIR) if f.endswith(".png")]
    processed_codes = [re.search(r'(\d{4})', f).group(1) for f in os.listdir(DST_DIR) if re.search(r'(\d{4})', f)]
    target_files = [f for f in all_files if os.path.splitext(f)[0] not in processed_codes]
    
    logging.info(f"実戦スクリーニング開始（残り {len(target_files)} 枚）")

    for f in target_files:
        src_path = os.path.join(SRC_DIR, f)
        code = os.path.splitext(f)[0]
        logging.info(f"[{code}] 解析中...")

        try:
            # 1. Llavaによる英語解析
            res_analyze = ollama.chat(model=MODEL_ID, messages=[{'role': 'user', 'content': ANALYZE_PROMPT, 'images': [src_path]}])
            raw_analysis = res_analyze['message']['content']

            # 2. 日本語翻訳（長文で意味が通る形）
            full_japanese = translate_to_japanese(raw_analysis)
            
            # 3. 判定抽出
            match = re.search(r'【(最高|注目|期待|見送り)】', full_japanese)
            label = match.group(0) if match else "【解析済】"
            category = label.replace('【', '').replace('】', '')
            
            # 4. 画像保存（判定のみ！）
            dst_path = os.path.join(DST_DIR, f"{label}{f}")
            add_rank_only_stamp(src_path, full_japanese, dst_path)
            
            # 5. CSVに長文を保存
            with open(CSV_PATH, 'a', newline='', encoding='utf_8_sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([code, category, full_japanese.replace('\n', ' ')])
            
            logging.info(f"  -> {label} 完了")

        except Exception as e:
            logging.error(f"エラー [{code}]: {e}")
            time.sleep(1)

    logging.info(f"全件完了！ CSVパス: {CSV_PATH}")

if __name__ == "__main__":
    main()