import logging
import json
import requests
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import subprocess
from fastapi.responses import JSONResponse
import os
from fastapi import BackgroundTasks 

# ======== ロギング設定 ========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("server_debug.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# ======== CORS設定 ========
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class StockRequest(BaseModel):
    data: dict

def call_ollama_qwen(prompt: str, model: str = "qwen2.5:14b"):
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        return response.json().get("response", "")
    except Exception as e:
        logger.error(f"Ollama API呼び出しエラー: {e}")
        return None

import feedparser

def fetch_google_news(query: str):
    # GoogleニュースRSS（完全無料・無制限）
    url = f"https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
    feed = feedparser.parse(url)

    news_list = []
    for entry in feed.entries[:5]:  # 最新5件だけ使う
        news_list.append({
            "title": entry.title,
            "link": entry.link,
            "published": entry.get("published", "")
        })
    return news_list


def build_prompt(stock_json: dict):
    system_prompt = """
あなたは日本株の短期トレードに特化したAIアナリストです。
ユーザーは「初動・テーマ性・需給・抵抗帯・財務」を重視する短期トレーダーです。
あなたは stock_json に含まれる情報をもとに、短期トレードの意思決定に役立つ
“攻めるロジック” で評価・推論・自然文説明を行います。

【重要ルール】
- 自然文で説明してよい。
- 推論してよい。ただし stock_json に基づく範囲に限る。
- stock_json に無い外部情報（ニュース、業界推測、地合い推測、銘柄名からのテーマ推測）は禁止。
- stock_json に無い情報を創作しない。
- stock_json の値をもとに短期トレードの判断を積極的に行う。
- 出力は必ず JSON のみ。

【ニュースの扱いルール】
- latest_news に含まれる GoogleニュースRSS の内容は stock_json の一部として扱う。
- ニュースの「タイトル」「概要」「公開日時」から短期材料を抽出してよい。
- ただし、ニュース本文の推測は禁止（RSSには本文が含まれないため）。
- ニュース内容から以下の要素を判断してよい：
  - 短期テーマ性の強化（設備投資、受注、業績、政策関連など）
  - 需給改善の兆候（信用残減少、機関の買い戻し示唆など）
  - 短期の材料性（急騰理由、イベント、決算、提携など）
- ニュースが弱材料の場合はリスク要因として扱う。
- ニュースが強材料の場合は key_points に反映してよい。


【攻めるスコアリングロジック】
1. 初動・勢い（0〜25点）
- reverse_strong = 1 → +10, reverse_weak = 1 → -10
- price_diff_rate > +2% → +5, volume > 1000000 → +5, price_now > price_prev → +5
2. テーマ鮮度（0〜20点）
- themes が10個以上 → +5, latest_theme が直近30日 → +10
- themes に「蓄電池」「再生可能エネルギー」「脱炭素」が含まれる → +5
3. 財務・増資リスク（0〜20点）
- finance_rank = A+ → +10, progress > 80 → +5, dilution_risk = 0 → +5
4. テクニカル（0〜20点）
- price_now > ma5 → +5, price_now > ma25 → +5
- bb_signal = -1 → -3, rs5 > 0 → +3, rs20 > 0 → +3
5. 抵抗帯・支持帯（0〜15点）
- resist_near が price_now より十分遠い → +5, support_near が近い → +5, resist_near が近い → -5
6. ニュース材料性（0〜10点）
- latest_news のタイトルに以下の強材料が含まれる場合 → +5
  「決算」「上方修正」「増益」「黒字化」「受注」「提携」「新製品」「設備投資」
- 株価に直接影響する短期材料が含まれる場合 → +3
  「急騰」「ストップ高」「買い材料」「好材料」「需給改善」
- リスク材料が含まれる場合 → -5
  「不祥事」「下方修正」「赤字」「業績悪化」「大量売り」「ストップ安」
- ニュースが1件もない場合 → ±0（評価対象外）

【テーマ連動加点（最大 +5）】
- ニュースタイトルに stock_json["themes"] と関連する語が含まれる場合 → +3
  （例：テーマに「蓄電池」があり、ニュースに「電池」「バッテリー」「EV」など）
- ニュースタイトルが最新テーマ（latest_theme）と直接関連する場合 → +2
  （例：latest_theme が「再生可能エネルギー」で、ニュースに「太陽光」「風力」など）
  
【ニュース鮮度スコア（0〜5点）】
- 公開日時が24時間以内 → +5
- 公開日時が3日以内 → +3
- 公開日時が7日以内 → +1
- 7日より古いニュース → 0（材料性として弱い）
- published が取得できない場合 → 0

【最終判定ロジック】
85〜100 → A / 強気 / 買い / 0.5〜1.0
70〜84 → B / やや強気 / 押し目買い / 0.2〜0.4
55〜69 → C / 中立 / 様子見 / 0〜0.1
40〜54 → D / 弱気 / 触らない / 0
0〜39 → E / 危険 / 撤退 / 0

【出力フォーマット】
以下のJSON形式で出力すること:
{
  "rating": "", "stance": "", "position_ratio": 0.0, "time_horizon_days": 0,
  "target_price": null, "invalidation_price": null, "key_points": [],
  "risk_factors": [], "short_tag": "",
  "score_breakdown": {"initial":0,"themes":0,"fundamentals":0,"technical":0,"support_resist":0},
  "total_score": 0, "action": "", "reason": []
}
"""
    user_prompt = f"以下が stock_json です。このデータのみを使って評価してください。\n\nstock_json:\n{json.dumps(stock_json, ensure_ascii=False, indent=2)}"
    return system_prompt + "\n" + user_prompt

@app.post("/analyze")
async def analyze_stock(req: StockRequest):
    code = req.data.get('code', '不明')
    logger.info(f"Analyze request received: {code}")
    req.data["latest_news"] = fetch_google_news(code)

    prompt = build_prompt(req.data)
    result = call_ollama_qwen(prompt)

    if result is None:
        return {"error": "AI response failed"}

    try:
        start = result.find('{')
        end = result.rfind('}') + 1
        json_str = result[start:end]
        parsed = json.loads(json_str, strict=False)
        logger.info(f"Analysis success for {code}. Score: {parsed.get('total_score')}")
        return parsed
    except Exception as e:
        logger.error(f"JSONパースエラー: {e} | Raw出力: {result}")
        return {"raw": result, "error": "JSON parse failed"}
        
# ======== ヤフ板分析の実行 ========
def execute_yabb_script(code: str):
    script_path = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\ヤフ板分析.py"
    try:
        subprocess.run([sys.executable, script_path, code.upper()], check=True)
        logger.info(f"ヤフ板分析スクリプト正常終了: {code}")
    except Exception as e:
        logger.error(f"ヤフ板分析スクリプト実行失敗: {e}")

@app.get("/run_yabb")
async def run_yahoo_bbs_analysis(code: str, background_tasks: BackgroundTasks):
    report_path = rf"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\report_{code.upper()}.html"
    
    # ★修正箇所：新しく分析を回す前に、もし古いレポートが存在していれば確実に削除する
    if os.path.exists(report_path):
        try:
            os.remove(report_path)
            logger.info(f"古いレポートを削除しました: {report_path}")
        except Exception as e:
            logger.error(f"古いレポートの削除に失敗: {e}")

    # 即座にレスポンスを返し、実際の分析は裏側で開始
    background_tasks.add_task(execute_yabb_script, code.upper())
    return {"status": "started"}

@app.get("/check_yabb_status")
async def check_yabb_status(code: str):
    # レポートファイルが生成された時点で「完了」とみなす
    report_path = rf"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\report_{code.upper()}.html"
    if os.path.exists(report_path):
        return {"status": "completed"}
    return {"status": "processing"}