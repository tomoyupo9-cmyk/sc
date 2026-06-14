from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import json

app = FastAPI()

# ======== 【追加】CORS設定 ========
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 許可するオリジン（開発中は "*" で全て許可）
    allow_credentials=True,
    allow_methods=["*"],  # 全てのHTTPメソッド（GET, POST, OPTIONSなど）を許可
    allow_headers=["*"],  # 全てのHTTPヘッダーを許可
)
# ==================================

# ======== 入力モデル ========
class StockRequest(BaseModel):
    data: dict


# ======== Qwen 呼び出し関数 ========
def call_ollama_qwen(prompt: str, model: str = "qwen2.5:14b"):
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    response = requests.post(url, json=payload)
    return response.json().get("response", "")


# ======== 攻めるロジック入りプロンプト生成 ========
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

【攻めるスコアリングロジック】

1. 初動・勢い（0〜25点）
- reverse_strong = 1 → +10
- reverse_weak = 1 → -10
- price_diff_rate > +2% → +5
- volume > 1000000 → +5
- price_now > price_prev → +5

2. テーマ鮮度（0〜20点）
- themes が10個以上 → +5
- latest_theme が直近30日 → +10
- themes に「蓄電池」「再生可能エネルギー」「脱炭素」が含まれる → +5

3. 財務・増資リスク（0〜20点）
- finance_rank = A+ → +10
- progress > 80 → +5
- dilution_risk = 0 → +5

4. テクニカル（0〜20点）
- price_now > ma5 → +5
- price_now > ma25 → +5
- bb_signal = -1 → -3
- rs5 > 0 → +3
- rs20 > 0 → +3

5. 抵抗帯・支持帯（0〜15点）
- resist_near が price_now より十分遠い → +5
- support_near が近い → +5
- resist_near が近い → -5

【最重要】
スコア計算は必ず stock_json の値を使い、上記ルールを厳密に機械的に適用すること。
自然文生成や推論は「reason(reason)」の説明にのみ使用し、
スコア計算には一切使用しない。

【最終判定ロジック】
Score → rating / stance / action / position_ratio

85〜100 → A / 強気 / 買い / 0.5〜1.0  
70〜84 → B / やや強気 / 押し目買い / 0.2〜0.4  
55〜69 → C / 中立 / 様子見 / 0〜0.1  
40〜54 → D / 弱気 / 触らない / 0  
0〜39 → E / 危険 / 撤退 / 0  

【出力フォーマット】
必ず以下の JSON 形式で出力する：

{
  "rating": "",
  "stance": "",
  "position_ratio": 0.0,
  "time_horizon_days": 0,
  "target_price": null,
  "invalidation_price": null,
  "key_points": [],
  "risk_factors": [],
  "short_tag": "",
  "score_breakdown": {
    "initial": 0,
    "themes": 0,
    "fundamentals": 0,
    "technical": 0,
    "support_resist": 0
  },
  "total_score": 0,
  "action": "",
  "reason": []
}

"""

    user_prompt = f"""
以下が stock_json です。このデータのみを使って評価してください。

stock_json:
{json.dumps(stock_json, ensure_ascii=False, indent=2)}
"""

    return system_prompt + "\n" + user_prompt


# ======== API エンドポイント ========
@app.post("/analyze")
def analyze_stock(req: StockRequest):
    stock_json = req.data

    prompt = build_prompt(stock_json)
    result = call_ollama_qwen(prompt)


    try:
        parsed = json.loads(result)
        return parsed
    except:
        return {"raw": result}
