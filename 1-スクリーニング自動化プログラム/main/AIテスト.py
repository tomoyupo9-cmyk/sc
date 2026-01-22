import requests
import json
import time

# --- 設定（LM Studioが稼働していることを前提） ---
# LM StudioのAPIエンドポイント
LMSTUDIO_API_URL = "http://localhost:1234/v1/chat/completions"
MODEL_NAME = "local-model" 
# 推論タスクに適したパラメータ
TEMPERATURE = 0.3 
MAX_TOKENS = 1024 

# --- 1. 診断させたい銘柄データ ---
# 実際のデータフレームの1行を辞書形式に変換したもの
# 銘柄ごとにこの辞書をループで作成・更新してAPIに送ります。
stock_data_dict = {
    "コード": 7203,
    "銘柄名": "トヨタ自動車",
    "株価": 3500,
    "PER": 10.5,
    "PBR": 1.1,
    "自己資本比率": "40.0%",
    "直近売上成長率": "5.5%",
    "配当利回り": "2.8%"
}

# --- 2. プロンプト設計（最も重要） ---
# システムプロンプト: AIの役割と、出力すべき形式を厳格に指示
# 出力形式をより明確にし、AIが余計な文章を付け加えないよう指示を強化しています。
system_prompt = (
    "あなたは経験豊富な株式アナリストです。以下のJSON形式の株データに基づき、"
    "『成長性』、『割安性』、『財務健全性』の3観点から評価し、"
    "総合診断を150文字以内の日本語で出力してください。"
    "**絶対に、以下のJSONスキーマ以外のテキスト、説明、コメント、マークダウン記法(```json)は出力に含めないでください。**\n"
    "**出力はJSONオブジェクトそのもののみです。**\n"
    "JSONスキーマ: {\"成長性評価\": \"A|B|C\", \"割安性評価\": \"A|B|C\", \"財務評価\": \"A|B|C\", \"総合診断コメント\": \"...\"}"
)

# ユーザープロンプト: 処理させるデータをAIに渡す
user_content = f"銘柄データ: {json.dumps(stock_data_dict, ensure_ascii=False)}"

# --- 3. APIリクエストの実行関数 ---
def get_ai_diagnosis(stock_data: dict, system_p: str, user_c: str) -> dict:
    """LM Studio APIにリクエストを送信し、診断結果JSONを返す"""
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_p},
            {"role": "user", "content": user_c},
        ],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "stream": False
    }

    try:
        # リクエスト実行 (タイムアウトを設定)
        response = requests.post(LMSTUDIO_API_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status() 

        response_data = response.json()
        
        # 応答の本文を取得
        ai_diagnosis_text = response_data['choices'][0]['message']['content']
        
        # LLMの出力はクリーンなJSONであることを期待しているため、そのままパースを試みる
        diagnosis_json = json.loads(ai_diagnosis_text)
        
        # 元のデータと診断結果を結合して返す
        return {**stock_data, **diagnosis_json}

    except requests.exceptions.RequestException as e:
        print(f"❌ APIリクエストエラー（{stock_data.get('銘柄名', '不明')}）: {e}")
        return {**stock_data, "AI診断エラー": str(e)}
    except json.JSONDecodeError:
        print(f"❌ JSON形式エラー（{stock_data.get('銘柄名', '不明')}）。AIの生の出力: {ai_diagnosis_text}")
        return {**stock_data, "AI診断エラー": "JSON形式エラー"}
    except Exception as e:
        print(f"❌ 予期せぬエラー（{stock_data.get('銘柄名', '不明')}）: {e}")
        return {**stock_data, "AI診断エラー": str(e)}


# --- 4. 実行部分 ---
if __name__ == "__main__":
    
    print("--- AI株診断（LM Studio Local API）開始 ---")
    start_time = time.time()
    
    # 仮のデータリスト（実際にはデータフレームからループで抽出）
    sample_stock_data_list = [
        stock_data_dict, # トヨタ
        { "コード": 9984, "銘柄名": "ソフトバンクG", "株価": 8000, "PER": 50.0, "PBR": 1.5, "自己資本比率": "25.0%", "直近売上成長率": "10.0%", "配当利回り": "0.5%"},
        { "コード": 4502, "銘柄名": "武田薬品", "株価": 4000, "PER": 12.0, "PBR": 0.8, "自己資本比率": "55.0%", "直近売上成長率": "-2.0%", "配当利回り": "4.5%"},
    ]
    
    results = []
    
    for stock_data in sample_stock_data_list:
        print(f"\n[処理中] 銘柄コード: {stock_data['コード']} ({stock_data['銘柄名']})")
        
        # 銘柄ごとにユーザープロンプトを更新
        current_user_content = f"銘柄データ: {json.dumps(stock_data, ensure_ascii=False)}"
        
        # 診断実行
        diagnosis_result = get_ai_diagnosis(stock_data, system_prompt, current_user_content)
        
        results.append(diagnosis_result)
        
        # 結果の表示
        print(f"  ✅ 総合診断: {diagnosis_result.get('総合診断コメント', 'エラー発生')}")
        
    end_time = time.time()
    
    print("\n--- 全ての診断が完了しました ---")
    print(f"処理時間: {end_time - start_time:.2f}秒")
    # 最終的な結果リスト (Pandasなどでデータフレームに変換して利用)
    # print("\n最終結果リスト:")
    # print(json.dumps(results, indent=2, ensure_ascii=False))