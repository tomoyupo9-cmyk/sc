import os
import sys
import requests
import json
import time
import pandas as pd
import numpy as np
import webbrowser  # 生成完了後の自動オープン用
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re # ★銘柄名の抽出・高度な辞書に使う正規表現ライブラリ
from curl_cffi import requests

# =========================================================================
# ★ 実行設定（BAN回避のためのスリープ・スレッド設定）
# =========================================================================
# マルチスレッドの最大同時実行数（多いほど高速ですがBANリスクが跳ね上がります。推奨: 2〜4）
MAX_THREADS_CONFIG = 5

# 1銘柄の処理を開始する前のランダム待機時間（秒）の最小・最大
# 複数スレッドが一斉にアクセスを開始するのを防ぎます
THREAD_START_SLEEP_MIN = 2.0
THREAD_START_SLEEP_MAX = 5.0

# 掲示板のページネーション（過去の投稿へ遡る）ごとの待機時間（秒）
# APIへの連続攻撃と判定されないための最重要インターバルです（推奨: 2.0〜3.0）
PAGE_FETCH_SLEEP = 0.5

# ★ 追加：バッチ処理（チャンク）設定（ALLモード時のBAN回避用）
CHUNK_SIZE = 10         # 1度に処理する銘柄数（10〜15程度を推奨）
LONG_SLEEP_MIN = 180.0  # バッチ間の休憩（秒）最小（推奨: 180 = 3分）
LONG_SLEEP_MAX = 300.0  # バッチ間の休憩（秒）最大（推奨: 300 = 5分）
# =========================================================================

# ★Pandas/Numpyの特殊な数値をJSON保存できるようにするためのエンコーダー
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

# ★認証情報の使い回し（BAN回避・高速化）とスレッド排他ロックのためのグローバル変数
_AUTH_CACHE = {'jwt': None, 'cookie_b': None, 'timestamp': 0}
_AUTH_LOCK = threading.Lock()
_FILE_LOCK = threading.Lock() # 途中経過のファイル書き込み衝突を防ぐロック

def get_stock_name(code):
    """
    ブラウザを立ち上げず、軽量かつ高速に銘柄名だけを確実に取得する専用関数
    User-Agentを付与してブロックを回避するよう強化
    """
    try:
        url = f"https://finance.yahoo.co.jp/quote/{code}.T/forum"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        #res = requests.get(url, headers=headers, timeout=10)
        res = requests.get(url, headers=headers, timeout=10, impersonate="chrome120")
        res.encoding = 'utf-8'
        
        # HTMLの <title>タグ から「【」の手前までを抽出
        match = re.search(r'<title>(.*?)【', res.text)
        if match:
            return match.group(1).replace('(株)', '').strip()
        else:
            title_match = re.search(r'<title>(.*?)</title>', res.text)
            return title_match.group(1).split('(')[0].strip() if title_match else "不明"
    except Exception:
        return "不明"

def get_latest_auth_info(code):
    """
    Playwrightを人間モードに偽装し、認証情報を取得。
    マルチスレッド衝突を防ぐためのロック機構を搭載し、
    30分以内であればキャッシュを再利用してBANを防ぐ。
    """
    global _AUTH_CACHE
    
    with _AUTH_LOCK: # ここから下は1度に1つのスレッドしか実行できない
        # 最後の取得から1800秒（30分）以内ならキャッシュを再利用
        if time.time() - _AUTH_CACHE['timestamp'] < 1800 and _AUTH_CACHE['jwt']:
            return {'cookie_b': _AUTH_CACHE['cookie_b'], 'jwt_token': _AUTH_CACHE['jwt']}

        print(f"[{code}] ブラウザを起動して最新の認証情報を自動更新中...")
        auth_info = {'cookie_b': None, 'jwt_token': None}
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 800},
                locale='ja-JP',
                timezone_id='Asia/Tokyo',
                java_script_enabled=True
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
            page = context.new_page()
            
            # 画像やメディアの読み込みをブロックして高速化＆タイムアウト防止
            page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
            
            def handle_request(request):
                if "yahoo.co.jp" in request.url and 'x-jwt-token' in request.headers:
                    auth_info['jwt_token'] = request.headers['x-jwt-token']

            page.on("request", handle_request)
            
            try:
                # タイムアウトを60秒(60000ms)に延長してエラー落ちを防ぐ
                page.goto(f"https://finance.yahoo.co.jp/quote/{code}.T/forum", wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)
                
                for cookie in context.cookies():
                    if cookie['name'] == 'B':
                        auth_info['cookie_b'] = cookie['value']
                        break
                        
                # 取得に成功したらキャッシュを更新
                if auth_info['jwt_token'] and auth_info['cookie_b']:
                    _AUTH_CACHE = {'jwt': auth_info['jwt_token'], 'cookie_b': auth_info['cookie_b'], 'timestamp': time.time()}
                    
            except Exception as e:
                print(f"⚠️ Playwright通信エラー: {e}")
            finally:
                browser.close()
                
        return auth_info

def calculate_target_period(base_datetime=None):
    """
    実行時間が平日の8:00〜23:59なら「本日の8:00〜現在」、
    それ以外は「前営業日15:30〜現在」を計算する新ルール
    """
    if base_datetime is None:
        base_datetime = datetime.now()
        
    current_weekday = base_datetime.weekday() # 0=月, 4=金
    current_hour = base_datetime.hour
    current_minute = base_datetime.minute
    
    # 時刻を整数化 (例: 8時00分 -> 800, 23時59分 -> 2359)
    time_val = current_hour * 100 + current_minute
    
    end_target = base_datetime
    
    # ★ 新ルール: 平日(月〜金)の 08:00 〜 23:59 の場合は、当日の08:00〜現在を取得する
    if 0 <= current_weekday <= 4 and 800 <= time_val <= 2359:
        start_target = base_datetime.replace(hour=8, minute=0, second=0, microsecond=0)
        return start_target, end_target
    
    # 従来通りのロジック (平日00:00〜07:59、または土日の場合)
    current_date = base_datetime.date()
    
    if current_weekday == 0:    # 月曜日 -> 前日（日曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 1:  # 火曜日 -> 前日（月曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 2:  # 水曜日 -> 前日（火曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 3:  # 木曜日 -> 前日（水曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 4:  # 金曜日 -> 前日（木曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 5:  # 土曜日 -> 前日（金曜）
        start_date = current_date - timedelta(days=1)
    elif current_weekday == 6:  # 日曜日 -> 前々日（金曜）
        start_date = current_date - timedelta(days=2)
        
    start_target = datetime.combine(start_date, datetime.strptime('15:30', '%H:%M').time())
    return start_target, end_target

def fetch_yahoo_bbs_market_adaptive(code, verbose=False):
    """
    相場カレンダーを考慮し、必要な期間だけの投稿をrequestsで高速収集する関数
    固定投稿（お知らせ等）のトラップを回避する改修版
    """
    start_target, end_target = calculate_target_period()
    if verbose:
        print(f"\n[相場カレンダー連動フィルター発動]")
        print(f"◆ 対象銘柄: {code}")
        print(f"◆ 現在日時: {end_target.strftime('%Y/%m/%d %H:%M:%S')}")
        print(f"◆ 収集範囲: {start_target}  〜  {end_target}")
    
    # ★ここで確実に銘柄名を取得
    stock_name = get_stock_name(code)
    
    auth = get_latest_auth_info(code)
    if not auth['cookie_b'] or not auth['jwt_token']:
        if verbose: print(f"【エラー】[{code}] 認証情報の自動取得に失敗しました。")
        return pd.DataFrame(), stock_name
        
    url = 'https://finance.yahoo.co.jp/bff-quote-stocks/v1/ajax/bbs/comment'
    cookies = {'B': auth['cookie_b']}
    headers = {
        'accept': '*/*',
        'referer': f'https://finance.yahoo.co.jp/quote/{code}.T/forum',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-jwt-token': auth['jwt_token'],
    }
    
    filtered_comments = []
    current_mid = ''
    
    for loop in range(1000):
        # ★BAN回避：sizeを3000から人間らしい数値(100)に修正
        params = {'code': code, 'size': '3000', 'sort': '1'}
        if current_mid:
            params['mid'] = current_mid
            
        try:
            proxies = {
                'http': 'socks5h://127.0.0.1:9050',
                'https': 'socks5h://127.0.0.1:9050'
            }

            # リクエスト時に適用する
            #response = requests.get(url, params=params, cookies=cookies, headers=headers, proxies=proxies, timeout=10)
            #response = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=10)
            response = requests.get(url, params=params, cookies=cookies, headers=headers, impersonate="chrome120", timeout=10)
            
            if response.status_code != 200: break
                
            items = response.json().get("response", {}).get("items", [])
            if not items: break
            
            # 条件に合うものだけを黙々と抽出する
            for item in items:
                post_time = datetime.strptime(item['postDate'], '%Y/%m/%d %H:%M')
                if start_target <= post_time <= end_target:
                    filtered_comments.append(item)
            
            # 自動停止の判定は「取得したページの最後の投稿」で行う
            last_item_time = datetime.strptime(items[-1]['postDate'], '%Y/%m/%d %H:%M')
            if last_item_time < start_target:
                if verbose: print(f" -> ページ末尾の投稿（{last_item_time}）がターゲット期間より過去に到達。収集を自動停止します。")
                break
                
            if verbose: print(f" ループ {loop + 1}: 過去へ巡回中... (最古: {items[-1]['postDate']})")
            current_mid = str(items[-1]["part"])
            
            # ★BAN回避のためのスリープ
            time.sleep(PAGE_FETCH_SLEEP)
            
        except Exception as e:
            if verbose: print(f"[{code}] リクエスト中にエラーが発生しました: {e}")
            break
            
    if verbose: print(f"⇒ 【結果】条件に適合する書き込みが {len(filtered_comments)} 件抽出されました。")
    return pd.DataFrame(filtered_comments), stock_name

def analyze_bbs_manipulation(df, stock_name="不明"):
    """
    【究極版マルチ複合判定】全16パターン＋文脈理解（セマンティック処理）＋引用・アンケートスルー＋ブラックリスト/スパイク最適化
    """
    if df.empty:
        return {"is_manipulated": False, "intent": "データなし", "risk_level": "Low", "summary": ["対象枠内のデータが0件です。"], "anomalies": [], "max_flow": 0, "avg_len": 0, "fact_rate": 0, "blacklist": [], "extra_analysis_results": {}, "bullish_score": 0, "stock_name": stock_name}
        
    report = {
        'is_manipulated': False, 'intent': '正常（個人の雑談・ノイズベース）', 'risk_level': 'Low',
        'summary': [], 'total_posts': len(df), 'unique_users': df['userId'].nunique(),
        'anomalies': [], 'max_flow': 0, 'avg_len': 0, 'fact_rate': 0.0, 'blacklist': [], 'bullish_score': 0
    }
    
    # ===============================================================
    # ★精度向上①：テキストの前処理（引用返信の除去とタグ消去）
    # ===============================================================
    df['temp_body'] = df['body'].astype(str).str.replace('<br />', '\n', regex=False).str.replace('&gt;', '>', regex=False)
    # > または ＞ で始まる「引用行」を完全に削除する
    df['temp_body'] = df['temp_body'].apply(lambda x: re.sub(r'^[>＞].*$', '', x, flags=re.MULTILINE))
    
    # ===============================================================
    # ★精度向上②：アンケート（Poll）の検知フラグ
    # ===============================================================
    def check_poll(text):
        if ('はい' in text and 'いいえ' in text) or ('👍' in text and '👎' in text):
            return 1
        if '投票' in text and ('お願いします' in text or 'クリック' in text):
            return 1
        return 0
    df['is_poll'] = df['temp_body'].apply(check_poll)

    # ===============================================================
    # ★精度向上③：意味の反転（文脈を読むセマンティック処理）
    # ===============================================================
    def preprocess_semantics(text):
        # 1. 誤検知しやすい単語（買い煽り、売り残など）を透明化
        text = re.sub(r'買い煽|売り煽|買い残|売り残|信用買|信用売|買戻|買い戻', '', text)
        # 2. 「ガラッと」「ガラガラ」などの副詞・擬音語を透明化（「ガラ」暴落誤検知を防ぐ）
        text = re.sub(r'ガラッと|ガラガラ', '', text)
        # 3. 強力な打ち消し表現の反転
        text = re.sub(r'(上が|高騰|爆上げ|反発|ストップ高|S高).{0,6}(ない|ません|訳ない|わけない|状況では|厳しい|無理)', '下がる', text)
        text = re.sub(r'(下が|暴落|下落|ガラ|ストップ安|S安).{0,6}(ない|ません|訳ない|わけない|状況では|厳しい|無理)', '上がる', text)
        return text

    # HTMLタグと改行を消し去り、純粋な「本人の言葉」だけにする
    df['clean_body'] = df['temp_body'].str.replace(r'<[^>]+>', '', regex=True).str.replace('\n', '').str.replace(' ', '')
    df['analyzed_body'] = df['clean_body'].apply(preprocess_semantics)
    df['body_len'] = df['analyzed_body'].str.len()

    # ★精度向上④：疑問形（？や?や「か」）の後に絵文字等があっても正確に質問と判定する
    def check_question(text):
        if re.search(r'[？\?か]([wWｗＷ草笑\s。、！\!\.😂😭🥺🙏💦🙇]*)$', text):
            return 1
        return 0
    df['is_question'] = df['analyzed_body'].apply(check_question)

    # ===============================================================
    # 正規表現を用いた高度な辞書群（表記揺れや活用を網羅）
    # ===============================================================
    pat_buy = re.compile(
        r'ストップ高|S高|Ｓ高|[買か]い気配|上が[るっり]|上げ[るてた]|爆上げ|ばくあげ|プラ転|反発|上昇|高騰|底打ち|'
        r'買い増|買[おえ]う|買っ[てた]|仕込|リバ|ホールド|握力|ガチホ|上値|青天井|割安|お宝|'
        r'期待|強い|最高|自社株買い|🚀|📈|助かる|救われる|耐え[ろる]|戻[るっ]|プラス|勝たん|買い時|追加|強気|ナンピン|押し目', 
        re.IGNORECASE
    )
    pat_sell = re.compile(
        r'ストップ安|S安|Ｓ安|[下さ]が[るっり]|下げ[るてた]|ガラ|ナイアガラ|暴落|急落|下落|マイ転|調整|'
        r'逃げ[ろてた]|空売り|売[ろお]う|売っ[てた]|損切り|損切|天井|下値|割高|'
        r'オワタ|終わった|クソ株|くそ株|紙切れ|弱い|最悪|ゴミ|退場|ヤバい|やばい|売ら|ヤラれ|焼かれ|全滅|減っ|📉|利確|逃避', 
        re.IGNORECASE
    )
    pat_squeeze = re.compile(r'踏み上げ|売り豚|燃料|丸焼け|担がれ|踏む|買い戻し', re.IGNORECASE)
    pat_fact = re.compile(r'決算|業績|四半期|進捗|コンセンサス|需給|PER|PBR|時価総額|中長期|指標|ファンダ|\d+日線|日足|月足', re.IGNORECASE)
    pat_noise = re.compile(r'脳死|全力|仕手|イナゴ', re.IGNORECASE)
    pat_despair = re.compile(r'ふざけんな|しね|氏ね|死ね|ゴミ|クソ|助けて|含み損|もう無理|退場|紙切れ|眠れ|南無|アーメン', re.IGNORECASE)
    pat_cult = re.compile(r'信じて|ついていき|神|流石|さすが|先生|天才', re.IGNORECASE)
    pat_migrating = re.compile(r'こっち|あっち|乗り換え|\d{4}.*?[行い]く', re.IGNORECASE)

    # 打ち消し表現
    pat_neg_sell = re.compile(r'下がらない|暴落しない|心配.*ない|問題.*ない|売らない|売るな|下げない|売らん|手離さない|手放さない|ガチホ', re.IGNORECASE)

    # 各投稿のスコアを正規表現で計算
    df['raw_buy_count'] = df['analyzed_body'].apply(lambda x: len(pat_buy.findall(x)))
    df['raw_sell_count'] = df['analyzed_body'].apply(lambda x: len(pat_sell.findall(x)))
    df['neg_sell_count'] = df['analyzed_body'].apply(lambda x: len(pat_neg_sell.findall(x)))

    # 打ち消しを考慮し、さらにアンケートや疑問形ならスコアを無効化する
    df['buy_count'] = (df['raw_buy_count'] + df['neg_sell_count']) * (1 - df['is_question']) * (1 - df['is_poll'])
    df['sell_count'] = (df['raw_sell_count'] - df['neg_sell_count']).clip(lower=0) * (1 - df['is_question']) * (1 - df['is_poll'])

    df['squeeze_count'] = df['analyzed_body'].apply(lambda x: len(pat_squeeze.findall(x)))
    df['fact_count'] = df['analyzed_body'].apply(lambda x: len(pat_fact.findall(x)))
    df['noise_count'] = df['analyzed_body'].apply(lambda x: len(pat_noise.findall(x)))
    df['despair_count'] = df['analyzed_body'].apply(lambda x: len(pat_despair.findall(x)))
    df['cult_count'] = df['analyzed_body'].apply(lambda x: len(pat_cult.findall(x)))
    df['mig_count'] = df['analyzed_body'].apply(lambda x: len(pat_migrating.findall(x)))
    
    # 時間帯フラグ
    df['post_dt'] = pd.to_datetime(df['postDate'])
    df['hour'] = df['post_dt'].dt.hour
    df['minute'] = df['post_dt'].dt.minute
    df['is_morning'] = (df['hour'] == 8) & (df['minute'] >= 30)
    df['is_midnight'] = (df['hour'] >= 1) & (df['hour'] <= 5)

    # ===============================================================
    # ユーザー分析（ブラックリスト判定の高度化）
    # ===============================================================
    for uid, u_df in df.groupby('userId'):
        u_posts_count = len(u_df)
        unique_names = u_df['dispname'].dropna().unique().tolist()
        
        u_buy = u_df['buy_count'].sum()
        u_sell = u_df['sell_count'].sum()
        u_avg_len = u_df['body_len'].mean()
        u_fact_total = u_df['fact_count'].sum()
        
        direction, dir_class = "方向不明", "badge-watch"
        if u_buy > u_sell: direction, dir_class = "買い誘導 (上)", "badge-buy"
        elif u_sell > u_buy: direction, dir_class = "売り誘導 (下)", "badge-sell"
        
        reasons = []
        if len(unique_names) > 1: reasons.append(f"【ネーム多重偽装】同一IDで複数名使用（{ ' -> '.join(map(str, unique_names)) }）")
        
        is_spammer = False
        if u_posts_count >= 5 or (u_posts_count / len(df) > 0.10):
            if u_avg_len < 40 and u_fact_total < 2:
                is_spammer = True 
            elif u_posts_count >= 15:
                is_spammer = True 
                
        if is_spammer:
            reasons.append(f"【高密度連投工作】短期間に {u_posts_count} 件の視覚占有工作")
            
        if reasons:
            report['blacklist'].append({'userId': str(uid), 'names': ", ".join(map(str, unique_names)), 'count': int(u_posts_count), 'direction': direction, 'dir_class': dir_class, 'reason': " / ".join(reasons)})

    report['user_occupancy'] = float(report['unique_users'] / report['total_posts']) if report['total_posts'] > 0 else 1.0
    time_resampled = df.set_index('post_dt').resample('10min').size()
    report['max_flow'] = int(time_resampled.max()) if not time_resampled.empty else 0
    report['avg_len'] = float(df['body_len'].mean())
    report['fact_rate'] = float(df['fact_count'].sum() / len(df))
    
    df['total_reactions'] = df['good'] + df['bad']
    mean_react = df['total_reactions'].mean()
    std_react = df['total_reactions'].std()
    threshold = mean_react + (3 * std_react) if std_react > 0 else 50
    anomaly_df = df[df['total_reactions'] > threshold].copy()
    
    buy_score, sell_score, squeeze_score, fake_bear_score = 0, 0, 0, 0
    flag_hidden_fact, flag_sakura_good = False, False
    
    # ===============================================================
    # 異常スパイクの判定（秀逸な考察と荒らしの保護）
    # ===============================================================
    if not anomaly_df.empty:
        real_manipulation_count = 0 
        
        for _, row in anomaly_df.iterrows():
            if row['is_poll'] == 1:
                bias = "📊 掲示板アンケート（世論調査）"
            elif row['is_question'] == 1:
                bias = "🤔 狼狽・質問（投資家の迷い）"
            elif row['body_len'] >= 50 and row['fact_count'] >= 1 and row['good'] > row['bad']:
                bias = "💡 秀逸な考察（投資家からの高い支持）"
            elif row['body_len'] > 40 and row['fact_count'] > 0 and row['bad'] > row['good'] * 2:
                flag_hidden_fact = True
                bias = "🙈 隠蔽工作（的確な分析をBadで潰す）"
                real_manipulation_count += 1
            elif row['body_len'] < 10 and row['good'] > row['bad'] * 5 and row['good'] > 20:
                flag_sakura_good = True
                bias = "🤖 サクラGood爆撃（中身ゼロの押し上げ）"
                real_manipulation_count += 1
            elif row['good'] >= row['bad']:
                buy_score += (row['buy_count'] + 1) * (row['good'] - row['bad'])
                squeeze_score += row['squeeze_count'] * (row['good'] - row['bad'])
                bias = "買い嵌め込み（強気の心理操作）"
                real_manipulation_count += 1
            else:
                # ★追加：Badが多い場合、短文で中身がないならただの「荒らし」としてスルーする
                if row['body_len'] < 40 and row['fact_count'] == 0:
                    bias = "🤬 荒らし・不快な投稿への反発（ノイズ）"
                else:
                    sell_score += (row['sell_count'] + 1) * (row['bad'] - row['good'])
                    if row['sell_count'] > 0:
                        fake_bear_score += (row['bad'] - row['good'])
                        bias = "サクラ売り煽り（逆張りの安心ハメ工作）"
                    else:
                        bias = "売り崩し（弱気の心理工作）"
                    real_manipulation_count += 1
                
            report['anomalies'].append({
                'part': str(row['part']), 'dispname': str(row['dispname']), 'postDate': str(row['postDate']), 
                'good': int(row['good']), 'bad': int(row['bad']), 'bias': bias, 'body': str(row['body']),
                'buy_count': int(row['buy_count']), 'sell_count': int(row['sell_count']),
                'is_poll': int(row.get('is_poll', 0)), 'is_question': int(row.get('is_question', 0))
            })

        if real_manipulation_count > 0:
            report['is_manipulated'] = True
            report['risk_level'] = 'High'
            report['summary'].append(f"【警告】ボタン統計に異常スパイク（しきい値 {threshold:.1f}票超）の工作疑い投稿を {real_manipulation_count} 件検知。")

    morning_rate = len(df[df['is_morning']]) / len(df) if len(df) > 0 else 0
    midnight_rate = len(df[df['is_midnight']]) / len(df) if len(df) > 0 else 0
    despair_rate = df['despair_count'].sum() / len(df) if len(df) > 0 else 0
    cult_rate = df['cult_count'].sum() / len(df) if len(df) > 0 else 0
    mig_rate = df['mig_count'].sum() / len(df) if len(df) > 0 else 0

    if report['max_flow'] >= 40:
        report['is_manipulated'] = True
        report['risk_level'] = 'High'
        if report['user_occupancy'] < 0.20:
            report['intent'] = '⑦【ステルス絨毯爆撃】少数の複垢による超高速連投ジャック'
            report['summary'].append(f"【超高流速】10分間に最大 {report['max_flow']} 件の組織的連投。大口によるステルス世論操作の疑い。")
        else:
            report['intent'] = '⑧【イナゴパニックお祭り相場】制御不能の殺到状態'
            report['summary'].append(f"【需給過熱】10分間に最大 {report['max_flow']} 件の書き込み。週明けは値幅が極限まで荒れるためエントリー要注意。")
            
    elif flag_hidden_fact:
        report['is_manipulated'] = True
        report['intent'] = '⑫【不都合なファクトの隠蔽工作】大口や空売り機関が的確な分析をBadで潰しにかかっています。'
        report['summary'].append("【情報統制】個人に気づかれたくない都合の悪い考察が意図的に叩かれています。逆張りの大ヒントです。")
    elif flag_sakura_good:
        report['is_manipulated'] = True
        report['intent'] = '⑬【中身ゼロのサクラGood爆撃】BOT等を使った雑な買い煽り。'
        report['summary'].append("【工作の焦り】中身のない短文に不自然なGoodがついています。天井からの投げ売りに警戒。")
        
    elif report['is_manipulated']:
        if fake_bear_score > 0 and fake_bear_score > buy_score:
            report['intent'] = '⑤【巧妙な売り抜け（逆張り罠）】サクラの売り煽りで安心させる嵌め込み'
            report['summary'].append("【トラップ検知】個人を安心させて月曜に上値を買わせようとしています。寄り天警戒レベル最大。")
        elif squeeze_score > buy_score and squeeze_score > sell_score:
            report['intent'] = '⑥【踏み上げ誘導（売り豚焼き）】空売り勢の損切り巻き込み狙い'
            report['summary'].append("【需給仕掛け】「売り豚」を煽って踏み上げを加速させる意図的な買い誘導。週明け朝イチの急騰トリガー。")
        elif report['user_occupancy'] < 0.25 and buy_score > sell_score:
            report['intent'] = '①【異常な買いスパイク発生】チャート位置により大チャンスか罠'
            report['summary'].append("🚨【最重要アラート：必ずチャートの現在地を確認してください】大口による意図的な買い空気の醸成を検知しました。")
            report['summary'].append("📈 ➡️ 【現在の株価が『底値圏・揉み合い』の場合】: 大口の玉集めが完了し、上に飛ばす発射準備（初動）の可能性が極めて高いです。打診買い監視レベルMAXです。")
            report['summary'].append("📉 ➡️ 【現在の株価が『高値圏・急騰後』の場合】: 大口が利確して逃げるため、個人のイナゴ買いを誘っている完全なトラップです。絶対に見送り（寄り天警戒）です。")
        elif report['user_occupancy'] < 0.25 and sell_score > buy_score:
            report['intent'] = '②【安値集め（仕込み型売り煽り）】パニックを誘い下値で拾う狙い'
            report['summary'].append("【仕込みの売り煽り】個人を恐怖させて株を吐き出させようとしています。パニック安した局面はリバウンドの押し目候補。")
        else:
            report['intent'] = '③【短期世論コントロール】気配値の小幅誘導'
            
    elif morning_rate > 0.2:
        report['intent'] = '⑩【寄付前・気配値コントロール工作】8:30〜の気配値操作'
        report['summary'].append("【見せ板連動】寄付き前の成り行き注文を誘発する狙いがあります。")
    elif midnight_rate > 0.2 and report['user_occupancy'] < 0.3:
        report['intent'] = '⑪【深夜のステルス世論形成】閑散期にシナリオを仕込んでいる状態'
        report['summary'].append("【深層工作】朝起きた個人投資家を錯覚させるための書き込みが行われています。")
    elif cult_rate > 0.05:
        report['intent'] = '⑯【教祖降臨・盲信モード】煽り屋への同調者が集結'
        report['summary'].append("【チキンレース】インフルエンサー等が介入中。彼らが売り抜けた瞬間に暴落します。")
    elif mig_rate > 0.05:
        report['intent'] = '⑭【他銘柄へのイナゴ誘導（資金抜き）】別銘柄へ資金を向かわせる宣伝'
        report['summary'].append("【資金移動】別の祭りにイナゴを誘導し、ここの資金を抜こうとしています。")
    elif despair_rate > 0.1:
        report['intent'] = '⑮【総悲観・振るい落とし完了サイン】握力限界の投げ売り完了'
        report['summary'].append("【大底候補】罵倒と悲観が蔓延し、セリングクライマックスを迎えている可能性があります。")
            
    else:
        if report['user_occupancy'] >= 0.4 and report['avg_len'] >= 45 and df['fact_count'].sum() > df['noise_count'].sum():
            report['intent'] = '⑨【健全なファクト考察相場】中長期ホールド勢主導の優良相場'
            report['risk_level'] = 'Low'
            report['summary'].append(f"【超優良】平均文字数 {report['avg_len']:.1f}文字 の質の高い長文考察がメインです。")
            report['summary'].append("感情的な煽りやパニックがなく、需給とファンダメンタルズが淡々と現実的に評価されています。大口のハメ込みリスクが極めて低い『安全圏の上昇トレンド』としてスクリーニング評価を【加点】します。")
        elif report['user_occupancy'] < 0.3:
            report['intent'] = '④【不気味な過疎集め】少人数で静かに集める狙い'
            report['summary'].append("【過疎工作】派手なスパイクはありませんが、特定の勢力が居座り空気を重くコントロールしています。")
        else:
            report['intent'] = '正常（個人の自然な雑談ベース）'
            report['summary'].append("特筆すべき歪みや、際立った長文考察は見られません。標準的な個人の雑談ベースです。")
            
    meta_map = {
        '①': {'action': '要確認', 'phase': '初動 or 天井', 'class': 'action-watch'},
        '②': {'action': '買い', 'phase': '仕込み/押し目', 'class': 'action-buy'},
        '③': {'action': '様子見', 'phase': '様子見', 'class': 'action-watch'},
        '④': {'action': '様子見', 'phase': '仕込み', 'class': 'action-watch'},
        '⑤': {'action': '売り', 'phase': '天井圏警戒', 'class': 'action-sell'},
        '⑥': {'action': '買い', 'phase': 'モメンタム', 'class': 'action-buy'},
        '⑦': {'action': 'スルー', 'phase': '異常事態', 'class': 'action-watch'},
        '⑧': {'action': 'スルー', 'phase': 'カオス', 'class': 'action-watch'},
        '⑨': {'action': '買い', 'phase': '優良トレンド', 'class': 'action-buy'},
        '⑩': {'action': '要確認', 'phase': '寄付前', 'class': 'action-watch'},
        '⑪': {'action': '要確認', 'phase': '閑散期工作', 'class': 'action-watch'},
        '⑫': {'action': '買い', 'phase': '逆張り候補', 'class': 'action-buy'},
        '⑬': {'action': '売り', 'phase': '天井圏警戒', 'class': 'action-sell'},
        '⑭': {'action': '売り', 'phase': '資金抜け', 'class': 'action-sell'},
        '⑮': {'action': '買い', 'phase': '大底・セリクラ', 'class': 'action-buy'},
        '⑯': {'action': '売り', 'phase': 'イナゴ天井', 'class': 'action-sell'}
    }
    report['action'], report['phase'], report['class'] = '様子見', '様子見', 'action-watch'
    for key, val in meta_map.items():
        if report['intent'].startswith(key):
            report['action'], report['phase'], report['class'] = val['action'], val['phase'], val['class']
            break
            
    def extra_analysis_modules(analysis_df):
        results = {'topic_list': [], 'stealth_sync': [], 'sentiment_timeline': []}
        topic_map = {
            'IR・公式発表': ['増資', '決算', '配当', '自社株買い', '上方修正', '下方修正', '適時開示', '中期経営計画', '中計', 'IR発表'],
            '個別材料・思惑': ['M&A', '業務提携', '資本提携', '買収', '新製品', '特許', '承認', '思惑', '材料出た', 'TOB', '株式分割'],
            'ニュース・市場': ['日経', '報道', '速報', 'ニュース', '株探', 'ロイター', '経済指標'],
            '政治・国策': ['政府', '自民', '野党', '総理', '規制', '法案', '補助金', '国策', '防衛'],
            '為替・金融': ['ドル円', '円安', '円高', '為替', '金利', '日銀', '植田', '政策金利']
        }
        for _, row in analysis_df.iterrows():
            tags = [t for t, ws in topic_map.items() if any(w in str(row['analyzed_body']) for w in ws)]
            if tags: 
                results['topic_list'].append({'postDate': str(row['postDate']), 'dispname': str(row['dispname']), 'part': str(row['part']), 'tags': tags, 'body': str(row['body']), 'buy_count': int(row['buy_count']), 'sell_count': int(row['sell_count']), 'is_poll': int(row['is_poll']), 'is_question': int(row['is_question'])})
        
        df_sorted = analysis_df.sort_values('post_dt')
        def get_bigrams(text):
            t = str(text)
            return set([t[i:i+2] for i in range(len(t)-1)]) if len(t) > 1 else set()

        best_matches = {}

        for i, row1 in df_sorted.iterrows():
            if len(str(row1['clean_body'])) < 25: continue
            
            window_end = row1['post_dt'] + pd.Timedelta(minutes=15)
            mask = (df_sorted['post_dt'] > row1['post_dt']) & (df_sorted['post_dt'] <= window_end)
            nearby_posts = df_sorted[mask]
            
            s1 = get_bigrams(row1['clean_body'])
            for j, row2 in nearby_posts.iterrows():
                if row1['userId'] == row2['userId']: continue
                if len(str(row2['clean_body'])) < 25: continue
                
                s2 = get_bigrams(row2['clean_body'])
                if not s1 or not s2: continue
                
                similarity = len(s1 & s2) / len(s1 | s2)
                if similarity >= 0.65:
                    target_part = str(row2['part'])
                    if target_part not in best_matches or best_matches[target_part]['similarity'] < similarity:
                        best_matches[target_part] = {
                            'postDate': str(row2['postDate']), 'dispname': str(row2['dispname']), 'part': str(row2['part']),
                            'similarity': float(similarity), 'source_id': str(row1['userId']), 'body': str(row2['body']),
                            'source_body': str(row1['body']), 'source_dispname': str(row1['dispname']),
                            'source_postDate': str(row1['postDate']), 'source_part': str(row1['part']),
                            'buy_count': int(row2['buy_count']), 'sell_count': int(row2['sell_count']),
                            'is_poll': int(row2.get('is_poll', 0)), 'is_question': int(row2.get('is_question', 0))
                        }
        
        results['stealth_sync'] = list(best_matches.values())

        temp_df = analysis_df.copy()
        temp_df['score'] = temp_df['buy_count'] - temp_df['sell_count']
        if not temp_df.empty:
            resampled = temp_df.set_index('post_dt').resample('30min')['score'].sum().fillna(0)
            results['sentiment_timeline'] = [{'time': ts.strftime('%m/%d %H:%M'), 'score': float(sc)} for ts, sc in resampled.items()]
        return results

    report['extra_analysis_results'] = extra_analysis_modules(df)
    
    score = 0
    inte = report['intent']
    if inte.startswith('⑨'): score += 10000    
    elif inte.startswith('⑥'): score += 9000    
    elif inte.startswith('②') or inte.startswith('⑮') or inte.startswith('⑫'): score += 8000    
    elif inte.startswith('①'): score += 5000    
    elif inte.startswith('③'): score += 3000    
    elif inte.startswith('④') or inte.startswith('⑪'): score += 2000    
    elif inte.startswith('⑧') or inte.startswith('⑩'): score += -1000   
    elif inte.startswith('⑦'): score += -2000   
    elif inte.startswith('⑤') or inte.startswith('⑬') or inte.startswith('⑭') or inte.startswith('⑯'): score += -5000   
    
    tl = report['extra_analysis_results'].get('sentiment_timeline', [])
    recent_sentiment = tl[-1]['score'] if tl else 0
    score += (report['fact_rate'] * 1000) + report['max_flow'] + (recent_sentiment * 10)        
    
    report['bullish_score'] = float(score)
    report['stock_name'] = stock_name
    return report

def generate_html_report(df, report, code, filepath):
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    stock_name = report.get('stock_name', '不明')
    
    if report['risk_level'] == 'High': status_color, status_border, status_text = "#fff1f2", "#f43f5e", "#9f1239"
    elif '健全' in report['intent']: status_color, status_border, status_text = "#eff6ff", "#3b82f6", "#1e3a8a"
    else: status_color, status_border, status_text = "#f0fdf4", "#22c55e", "#166534"

    def get_dir_html(b_count, s_count, is_poll=0, is_question=0):
        if is_poll == 1:
            return "<span style='background-color:#fef3c7; color:#d97706; border:1px solid #fcd34d; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📊 アンケート</span>"
        elif is_question == 1:
            return "<span style='background-color:#e0f2fe; color:#0369a1; border:1px solid #7dd3fc; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>🤔 質問・迷い</span>"
        elif b_count > s_count:
            return "<span style='background-color:#dcfce7; color:#15803d; border:1px solid #86efac; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📈 買い誘導</span>"
        elif s_count > b_count:
            return "<span style='background-color:#fee2e2; color:#b91c1c; border:1px solid #fca5a5; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📉 売り誘導</span>"
        else:
            return "<span style='background-color:#f1f5f9; color:#64748b; border:1px solid #cbd5e1; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>➖ 様子見/中立</span>"

    unified_list = []
    
    for a in report['anomalies']:
        bc = "badge-trap" if any(x in a['bias'] for x in ["サクラ", "隠蔽", "買い嵌め", "売り崩"]) else "badge-watch"
        if "秀逸な考察" in a['bias']: bc = "badge-buy"
        if "荒らし" in a['bias']: bc = "badge-watch"
            
        unified_list.append({
            'date': a['postDate'], 'id_info': f"<b>{a['dispname']}</b><br><small style='color:#888;'>ID: {a['part']}</small>",
            'direction_html': get_dir_html(a.get('buy_count', 0), a.get('sell_count', 0), a.get('is_poll', 0), a.get('is_question', 0)),
            'badge': f"<span class='badge {bc}'>🚨 {a['bias']}</span>", 'body': a['body'],
            'link': f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{a['part']}"
        })
        
    for s in report['extra_analysis_results'].get('stealth_sync', []):
        sl = f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{s.get('source_part', '')}"
        tl = f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{s.get('part', '')}"
        ch = f"""
        <div style='background: #f8fafc; border-left: 4px solid #94a3b8; padding: 8px 12px; margin-bottom: 8px; border-radius: 4px;'>
            <div style='color: #475569; font-weight: bold; font-size: 0.85em; margin-bottom: 4px;'>📝 発信元の投稿 ({s.get('source_postDate', '')}) <a href='{sl}' target='_blank' style='margin-left:10px;'>🔗元へ</a></div>
            <div style='color: #475569; font-size: 0.9em;'>{s.get('source_body', '')}</div>
        </div>
        <div style='color: #7e22ce; font-weight: bold; font-size: 0.85em; margin-bottom: 4px;'>⬇️ 追従・コピペ投稿<a href='{tl}' target='_blank' style='margin-left:10px;'>🔗コピペへ</a></div>
        <div>{s['body']}</div>
        """
        unified_list.append({
            'date': s['postDate'], 'id_info': f"<b>{s['dispname']}</b><br><small style='color:#888;'>ID: {s['part']}</small>",
            'direction_html': get_dir_html(s.get('buy_count', 0), s.get('sell_count', 0), s.get('is_poll', 0), s.get('is_question', 0)),
            'badge': f"<span class='badge badge-stealth'>同期工作({s['similarity']*100:.0f}%)</span>", 'body': ch, 'link': tl
        })
        
    for t in report['extra_analysis_results'].get('topic_list', []):
        ts = "".join([f"<span class='badge badge-topic'>{tag}</span>" for tag in t['tags']])
        unified_list.append({
            'date': t['postDate'], 'id_info': f"<b>{t['dispname']}</b><br><small style='color:#888;'>ID: {t['part']}</small>",
            'direction_html': get_dir_html(t.get('buy_count', 0), t.get('sell_count', 0), t.get('is_poll', 0), t.get('is_question', 0)),
            'badge': ts, 'body': t['body'], 'link': f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{t['part']}"
        })
        
    for item in unified_list: item['dt_obj'] = datetime.strptime(item['date'], '%Y/%m/%d %H:%M')
    unified_list.sort(key=lambda x: x['dt_obj'], reverse=True)
    
    integrated_rows = ""
    for item in unified_list:
        integrated_rows += f"<tr><td>{item['date']}</td><td>{item['id_info']}</td><td style='text-align:center; vertical-align:middle;'>{item['direction_html']}</td><td>{item['badge']}</td><td class='comment-body'>{item['body']}</td><td style='text-align:center; vertical-align:middle;'><a href=\"{item['link']}\" target=\"_blank\" style=\"text-decoration:none; font-size: 1.4em;\">🔗</a></td></tr>"
    if not integrated_rows: integrated_rows = "<tr><td colspan='6' style='text-align:center; color:#888; padding: 30px;'>異常検知や重要トピックはありませんでした。</td></tr>"

    blacklist_rows = ""
    for target in report['blacklist']:
        blacklist_rows += f"<tr><td><code style='background:#f1f5f9; padding:2px 6px; border-radius:4px; font-weight:bold; color:#0f172a;'>{target['userId']}</code></td><td><b>{target['names']}</b></td><td style='text-align:center; font-weight:bold; color:#e11d48;'>{target['count']} 件</td><td style='text-align:center;'><span class='badge {target['dir_class']}'>{target['direction']}</span></td><td style='color:#7f1d1d; font-size:12.5px; font-weight:600; background:#fef2f2;'>⚠️ {target['reason']}</td></tr>"
    if not blacklist_rows: blacklist_rows = "<tr><td colspan='5' style='text-align:center; color:#64748b; padding: 25px;'>検出基準に該当する組織的な要注意人物・自作自演アカウントはいません。</td></tr>"

    timeline = report['extra_analysis_results'].get('sentiment_timeline', [])
    if not timeline: chart_html, chart_labels, chart_scores = "<div style='text-align:center; width:100%; color:#888; padding: 30px;'>グラフ描画用データがありません</div>", "[]", "[]"
    else:
        chart_html = """<div style="position: relative; height: 260px; width: 100%;"><canvas id="sentimentChart"></canvas></div>"""
        chart_labels = json.dumps([item['time'] for item in timeline])
        chart_scores = json.dumps([item['score'] for item in timeline])

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>{stock_name}({code}) マルチ分析レポート</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            html {{ scroll-behavior: smooth; }}
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; background-color: #f0f2f5; color: #1e293b; margin: 0; padding: 20px; }}
            .header-main {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 4px solid #1e3a8a; padding-bottom: 12px; margin-bottom: 15px; }}
            .header-main h1 {{ color: #1e3a8a; margin: 0; font-size: 24px; }}
            .meta-info {{ color: #64748b; font-size: 0.9em; }}
            .toc-container {{ position: sticky; top: 0; background: rgba(240, 242, 245, 0.95); padding: 12px 0; z-index: 1000; border-bottom: 1px solid #cbd5e1; margin-bottom: 25px; display: flex; gap: 12px; flex-wrap: wrap; backdrop-filter: blur(5px); }}
            .toc-link {{ text-decoration: none; color: #1e3a8a; background: white; border: 1px solid #94a3b8; padding: 8px 16px; border-radius: 20px; font-size: 13px; font-weight: bold; transition: all 0.2s; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }}
            .toc-link:hover {{ background: #1e3a8a; color: white; border-color: #1e3a8a; }}
            .dashboard-top {{ display: flex; gap: 20px; margin-bottom: 25px; align-items: stretch; }}
            .col-left {{ flex: 1.2; display: flex; flex-direction: column; }}
            .col-right {{ flex: 1; display: flex; flex-direction: column; justify-content: space-between; gap: 12px; }}
            .summary-box {{ padding: 22px; border-radius: 8px; line-height: 1.7; box-shadow: inset 0 2px 4px rgba(0,0,0,0.02); height: 100%; box-sizing: border-box; }}
            .dynamic-status {{ background-color: {status_color}; border-left: 8px solid {status_border}; color: {status_text}; }}
            .intent-label {{ font-size: 0.85em; font-weight: bold; text-transform: uppercase; opacity: 0.8; margin-bottom: 6px; }}
            .intent-title {{ font-size: 1.35em; font-weight: 800; line-height: 1.4; }}
            .metrics-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; flex-grow: 1; }}
            .card {{ background: white; border: 1px solid #e2e8f0; padding: 14px; border-radius: 8px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.02); display: flex; flex-direction: column; justify-content: center; }}
            .card.full-width {{ grid-column: span 2; background: #f8fafc; border-style: dashed; }}
            .card .title {{ font-size: 0.82em; color: #64748b; font-weight: 600; margin-bottom: 4px; }}
            .card .value {{ font-size: 20px; font-weight: bold; color: #0f172a; }}
            .highlight-orange {{ color: #ea580c !important; }}
            .highlight-blue {{ color: #2563eb !important; }}
            .content-wrapper {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.04); margin-bottom: 25px; scroll-margin-top: 70px; }}
            h2 {{ font-size: 17px; color: #1e3a8a; margin-top: 0; margin-bottom: 15px; border-left: 5px solid #1e3a8a; padding-left: 10px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 6px; overflow: hidden; margin-top: 5px; }}
            th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #e2e8f0; font-size: 13px; vertical-align: top; }}
            th {{ background-color: #f1f5f9; color: #475569; font-weight: bold; font-size: 0.9em; }}
            tr:hover {{ background-color: #f8fafc; }}
            .badge {{ padding: 5px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; display: inline-block; margin-bottom: 4px; }}
            .badge-buy {{ background-color: #dcfce7; color: #15803d; border: 1px solid #86efac; }}
            .badge-sell {{ background-color: #fee2e2; color: #b91c1c; border: 1px solid #fca5a5; }}
            .badge-trap {{ background-color: #fef3c7; color: #d97706; border: 1px dashed #d97706; }}
            .badge-topic {{ background-color: #e0f2fe; color: #0369a1; border: 1px solid #7dd3fc; margin-right: 4px; }}
            .badge-stealth {{ background-color: #f3e8ff; color: #7e22ce; border: 1px solid #d8b4fe; }}
            .badge-watch {{ background-color: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; }}
            .comment-body {{ font-size: 0.95em; max-width: 550px; word-wrap: break-word; line-height: 1.5; }}
            .guide-table th {{ background-color: #1e3a8a; color: white; }}
            .bl-table th {{ background-color: #7f1d1d; color: white; }}
            a[title="Yahoo!ファイナンスの実際の投稿へ飛ぶ"]:hover {{ opacity: 0.7; transform: scale(1.1); display: inline-block; transition: all 0.2s; }}
        </style>
    </head>
    <body>
        <div style="max-width: 1250px; margin: 0 auto;">
            <div class="header-main">
                <h1>📊 {stock_name}({code}) 大口戦術＆書き込みの質 マルチ分析レポート</h1>
                <div class="meta-info">生成日時: {now_str} \| 対象期間内の総スキャン数: {report['total_posts']} 件</div>
            </div>
            
            <div class="toc-container">
                <a href="#sec-dashboard" class="toc-link">🎯 大口の狙いサマリー</a>
                <a href="#sec-timeline" class="toc-link">📈 時系列心理グラフ</a>
                <a href="#sec-integrated" class="toc-link">🔍 統合検知リスト</a>
                <a href="#sec-blacklist" class="toc-link">💀 工作員ブラックリスト</a>
                <a href="#sec-matrix" class="toc-link">📊 判定マトリクス早見表</a>
            </div>
            
            <div class="dashboard-top" id="sec-dashboard">
                <div class="col-left">
                    <div class="summary-box dynamic-status">
                        <div class="intent-label">🎯 解析された掲示板の現状・大口の狙い</div>
                        <div style="margin: 12px 0; display: flex; gap: 8px;">
                            <span class="{report['class']}" style="padding: 5px 12px; border-radius: 4px; font-weight: bold; border: 2px solid currentColor; background: rgba(255,255,255,0.5);">
                                売買：{report['action']}
                            </span>
                            <span style="padding: 5px 12px; border-radius: 4px; font-weight: bold; border: 2px solid #64748b; background: rgba(255,255,255,0.5); color: #64748b;">
                                需給フェーズ：{report['phase']}
                            </span>
                        </div>
                        <div class="intent-title">{report['intent']}</div>
                        <hr style="border: 0; border-top: 1px solid rgba(0,0,0,0.1); margin: 15px 0;">
                        <div class="intent-label">📋 スクリーニングシステム戦略分析ログ</div>
                        <ul style="margin: 5px 0 0 0; padding-left: 18px; font-size: 13.5px;">
                            {"".join([f"<li style='margin-bottom: 4px;'>{line}</li>" for line in report['summary']])}
                        </ul>
                    </div>
                </div>
                <div class="col-right">
                    <div class="metrics-grid">
                        <div class="card"><div class="title">期間内総投稿数</div><div class="value">{report['total_posts']} 件</div></div>
                        <div class="card"><div class="title">最大流速 (10分ピーク)</div><div class="value {'highlight-orange' if report['max_flow'] >= 40 else ''}">{report['max_flow']} 件</div></div>
                        <div class="card"><div class="title">平均文字数 (テキスト密度)</div><div class="value {'highlight-blue' if report['avg_len'] >= 45 else ''}">{report['avg_len']:.1f} 文字</div></div>
                        <div class="card"><div class="title">ファクト分析密度スコア</div><div class="value">{report['fact_rate']:.2f}</div></div>
                        <div class="card full-width"><div class="title">ユーザー占有率 (多様性)</div><div class="value">{report['user_occupancy']:.2f} （ユニーク数: {report['unique_users']} 人）</div></div>
                    </div>
                </div>
            </div>
            
            <div class="content-wrapper" id="sec-timeline">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h2 style="margin-bottom: 0;">📈 時系列心理分析トレンド（買い・売り優勢 / 30分間隔）</h2>
                    <div style="display: flex; gap: 12px; font-size: 12px; font-weight: bold; color: #475569; background: #f1f5f9; padding: 6px 12px; border-radius: 6px;">
                        <div style="display: flex; align-items: center; gap: 4px;"><span style="display:inline-block; width:12px; height:12px; background:#22c55e; border-radius:50%;"></span>買い優勢</div>
                        <div style="display: flex; align-items: center; gap: 4px;"><span style="display:inline-block; width:12px; height:12px; background:#ef4444; border-radius:50%;"></span>売り優勢</div>
                    </div>
                </div>
                <div style="background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0;">
                    {chart_html}
                </div>
            </div>
            
            <div class="content-wrapper" id="sec-integrated">
                <h2 style="color: #0369a1; border-left-color: #0369a1; margin-bottom: 5px;">🔍 統合検知リスト（スパイク・ステルス同期・マクロトピック）</h2>
                <p style="font-size: 12px; color: #64748b; margin-top: 0; margin-bottom: 15px;">※「スパイク」は異常投票数、その他のフラグは工作アカウント等による世論操作の可能性を示唆します。</p>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 120px;">投稿日時</th>
                            <th style="width: 140px;">アカウント / ID</th>
                            <th style="width: 100px; text-align: center;">誘導方向</th>
                            <th style="width: 180px;">検知フラグ</th>
                            <th>投稿本文</th>
                            <th style="width: 60px; text-align: center;">リンク</th>
                        </tr>
                    </thead>
                    <tbody>{integrated_rows}</tbody>
                </table>
            </div>

            <div class="content-wrapper" id="sec-blacklist">
                <h2 style="color: #7f1d1d; border-left-color: #7f1d1d;">💀 掲示板占有・アカウント偽装工作員ブラックリスト</h2>
                <table class="bl-table">
                    <thead><tr><th style="width: 140px;">userId（生ID）</th><th style="width: 180px;">使用ニックネーム</th><th style="width: 80px; text-align: center;">連投数</th><th style="width: 140px; text-align: center;">誘導ベクトル</th><th>🚨 工作・マーク指定された具体的な理由</th></tr></thead>
                    <tbody>{blacklist_rows}</tbody>
                </table>
            </div>
            
            <div class="content-wrapper" id="sec-matrix">
                <h2>🎯 ヤフ板工作・大口の狙い判定マトリクス（戦略・行動早見表）</h2>
                <table class="guide-table">
                    <thead><tr><th style="width: 260px;">パターン番号とタイトル</th><th style="width: 100px; text-align: center;">売買・行動</th><th style="width: 160px; text-align: center;">需給フェーズ</th><th>トレード戦略</th><th>相場の需給状態・裏の本音</th></tr></thead>
                    <tbody>
                        <tr><td><b>①【異常な買いスパイク発生】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">要確認</span></td><td style="text-align: center;"><b>初動 or 寄り天</b></td><td><b>【チャートで判断】</b><br>底値圏：大口の玉集め完了、発射準備（監視レベルMAX）。<br>高値圏：個人のイナゴ買いを誘う罠（絶対見送り）。</td><td>少数のアカウント等で意図的なポジティブ世論を形成。現在地によって「これからの上昇」か「売り抜け」かが真逆になるため要チャート確認。</td></tr>
                        <tr><td><b>②【安値集め（仕込み型売り煽り）】</b></td><td style="text-align: center;"><span style="color:#16a34a; font-weight:bold;">買い</span></td><td style="text-align: center;"><b>仕込み / 寄り底</b></td><td><b>【逆張り監視】</b><br>パニック狼狽売りが一巡し、需給が軽くなった後の反発リバ（押し目）を狙う。</td><td>掲示板に恐怖を植え付けることで個人に投げ売りをさせ、大口が下の安値で美味しく拾おうとしているフェーズ。</td></tr>
                        <tr><td><b>③【短期世論コントロール】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">様子見</span></td><td style="text-align: center;"><b>様子見</b></td><td><b>【様子見・通常雑談】</b><br>大きな歪みはないため、標準的な個人の雑談ベースとして静観。</td><td>寄付き気配値を、大口にとって少しでも有利な方向に小幅に誘導したい思惑が交錯している状態。</td></tr>
                        <tr><td><b>④【不気味な過疎集め】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">様子見</span></td><td style="text-align: center;"><b>仕込みフェーズ</b></td><td><b>【様子見・過疎工作】</b><br>派手なスパイクはないが、特定の勢力が居座り空気感を重く支配している。</td><td>ボタン過熱はさせず、少人数でネガティブノイズを等間隔で流す。新規の買い手を遠ざけ、下で静かに集める狙い。</td></tr>
                        <tr><td><b>⑤【巧妙な売り抜け（逆張り罠）】</b></td><td style="text-align: center;"><span style="color:#dc2626; font-weight:bold;">売り</span></td><td style="text-align: center;"><b>寄り天</b></td><td><b>【危険・絶対に見送り】</b><br>個人を安心させて上値を買わせようとする悪質なトラップ。寄り天警戒レベル最大.</td><td>あえて拙い売り煽りを自作自演し、ボットのBadで全否定する。個人に「買いの安心感」を誤認させて嵌め込む高等戦術。</td></tr>
                        <tr><td><b>⑥【踏み上げ誘導（売り豚焼き）】</b></td><td style="text-align: center;"><span style="color:#16a34a; font-weight:bold;">買い</span></td><td style="text-align: center;"><b>買い</b></td><td><b>【短期買い】</b><br>需給の燃料（踏み上げ）が溜まった急騰期待。初動のモメンタムに乗る。</td><td>空売り勢（売り豚）の損切り（買い戻し）を巻き込んだ、需給の歪みによる強制的な急騰を狙って仕掛けているフェーズ。</td></tr>
                        <tr><td><b>⑦【ステルス絨毯爆撃】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">様子見</span></td><td style="text-align: center;"><b>様子見</b></td><td><b>【触るな危険・スルー】</b><br>大口が必死に洗脳・世論操作を仕掛けている異常状態。ボラティリティがバグる前に足切り。</td><td>ボタン連打を封印し、少数の複垢による超高速連投を敢行。文字の「量」だけで掲示板の画面全体を強制ジャックする工作。</td></tr>
                        <tr><td><b>⑧【イナゴパニックお祭り相場】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">様子見</span></td><td style="text-align: center;"><b>様子見</b></td><td><b>【触るな危険・スルー】</b><br>板の流速が速すぎて完全にカオス状態。上下どちらにも激しく乱高下するため見送り。</td><td>突発的な材料（PTS急騰やIR）に驚いた一般投資家（イナゴ）が群がり、完全に理性を失って殺到している制御不能状態。</td></tr>
                        <tr><td><b>⑨【健全なファクト考察相場】</b></td><td style="text-align: center;"><span style="color:#16a34a; font-weight:bold;">買い</span></td><td style="text-align: center;"><b>仕込みフェーズ</b></td><td><b>【加点・押し目買い】</b><br>株主の握力が非常に強く、ハメ込みリスクが極めて低い安全圏の上昇トレンド。</td><td>仕手・機関の介入や感情的な煽り工作が一切ない。地に足のついた個人投資家が、業績やファンダを淡々と長文で論理分析。</td></tr>
                        <tr><td><b>⑩【寄付前・気配値コントロール工作】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">要確認</span></td><td style="text-align: center;"><b>寄付前</b></td><td><b>【寄り凸注意】</b><br>見せ板に連動した煽りの可能性。寄付直後の値動きを確認してからエントリー。</td><td>8:30以降の気配値に合わせて意図的な誘導を行い、一般投資家の成り行き注文を誘発する狙い。</td></tr>
                        <tr><td><b>⑪【深夜のステルス世論形成】</b></td><td style="text-align: center;"><span style="color:#64748b; font-weight:bold;">要確認</span></td><td style="text-align: center;"><b>閑散期工作</b></td><td><b>【罠の可能性】</b><br>朝イチでその話題に飛びつくのは危険。まずは静観。</td><td>誰も見ていない深夜帯に掲示板を自作のシナリオで埋め尽くし、朝起きた個人投資家を錯覚させる罠。</td></tr>
                        <tr><td><b>⑫【不都合なファクトの隠蔽工作】</b></td><td style="text-align: center;"><span style="color:#16a34a; font-weight:bold;">買い</span></td><td style="text-align: center;"><b>逆張り候補</b></td><td><b>【超加点・逆張り買い】</b><br>隠された事実こそが正解。的確な分析を信じてエントリー。</td><td>大口や空売り機関にとって「一般人に気づかれたくない的確な考察」を、大量のBadで「的外れな意見」に見せかけて隠蔽している状態。</td></tr>
                        <tr><td><b>⑬【中身ゼロのサクラGood爆撃】</b></td><td style="text-align: center;"><span style="color:#dc2626; font-weight:bold;">売り</span></td><td style="text-align: center;"><b>天井圏警戒</b></td><td><b>【危険・見送り】</b><br>質の低い工作が露呈している状態。騙されてはいけない。</td><td>BOT等を使った雑な押し上げ工作。大口の資金力はあるが工作の質が低く、売り抜けのために焦って買い煽っている。</td></tr>
                        <tr><td><b>⑭【他銘柄へのイナゴ誘導（資金抜き）】</b></td><td style="text-align: center;"><span style="color:#dc2626; font-weight:bold;">売り</span></td><td style="text-align: center;"><b>資金抜け</b></td><td><b>【下落警戒】</b><br>資金が抜ける可能性大。ホールドしている場合は利確を検討。</td><td>煽り屋が自分が仕掛けている別の銘柄に資金を移動させるため、あえて他所の板で宣伝活動（資金抜き）をしている。</td></tr>
                        <tr><td><b>⑮【総悲観・振るい落とし完了サイン】</b></td><td style="text-align: center;"><span style="color:#16a34a; font-weight:bold;">買い</span></td><td style="text-align: center;"><b>大底・セリクラ</b></td><td><b>【超加点・大底拾い】</b><br>総悲観は買い。打診買いの絶好のチャンス。</td><td>個人の握力が尽きて投げ売り（セリング・クライマックス）が完了したサイン。ここから大口が拾って反転する可能性が高い。</td></tr>
                        <tr><td><b>⑯【教祖降臨・盲信モード】</b></td><td style="text-align: center;"><span style="color:#dc2626; font-weight:bold;">売り</span></td><td style="text-align: center;"><b>イナゴ天井</b></td><td><b>【チキンレース警戒】</b><br>短期的な急騰はあっても、教祖が逃げた瞬間に即死する相場。</td><td>インフルエンサー等が参戦し、信者が盲信している状態。教祖の売り抜けを合図に大暴落する。</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <script>
        const timelineScores = {chart_scores};
        if (timelineScores && timelineScores.length > 0) {{
            const ctx = document.getElementById('sentimentChart').getContext('2d');
            const labels = {chart_labels};
            const pointColors = timelineScores.map(val => val > 0 ? '#22c55e' : (val < 0 ? '#ef4444' : '#94a3b8'));
            new Chart(ctx, {{
                type: 'line',
                data: {{ labels: labels, datasets: [{{ label: '心理スコア', data: timelineScores, borderColor: '#94a3b8', backgroundColor: 'rgba(148, 163, 184, 0.1)', borderWidth: 2, pointBackgroundColor: pointColors, pointBorderColor: '#ffffff', pointBorderWidth: 1.5, pointRadius: 5, pointHoverRadius: 8, fill: true, tension: 0.4 }}] }},
                options: {{ responsive: true, maintainAspectRatio: false, scales: {{ y: {{ grid: {{ color: (ctx) => ctx.tick.value === 0 ? '#475569' : '#e2e8f0', lineWidth: (ctx) => ctx.tick.value === 0 ? 2 : 1 }} }} }}, plugins: {{ legend: {{ display: false }}, tooltip: {{ padding: 10, callbacks: {{ label: function(context) {{ let val = context.parsed.y; return val > 0 ? '📈 買い優勢 (スコア: +' + val + ')' : (val < 0 ? '📉 売り優勢 (スコア: ' + val + ')' : '➖ 中立 (スコア: 0)'); }} }} }} }} }}
            }});
        }}
        </script>
    </body>
    </html>
    """
    with open(filepath, 'w', encoding='utf-8') as f: f.write(html_content)
    print(f"📄 HTMLレポートを完全生成しました: {filepath}")

def generate_ranking_html(ranking_list, filepath):
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    table_rows = ""
    for i, rep in enumerate(ranking_list):
        code, stock_name = rep.get('code', 'N/A'), rep.get('stock_name', '不明')
        score, posts = float(rep.get('bullish_score', 0)), int(rep.get('total_posts', 0))
        action, phase = rep.get('action', ''), rep.get('phase', '')
        intent = rep.get('intent', '')
        fact_rate, max_flow = float(rep.get('fact_rate', 0)), int(rep.get('max_flow', 0))
        
        target_aim = intent.split('】')[0].replace('【', '') if '】' in intent else intent
        
        if action == '買い': action_style = "background-color:#dcfce7; color:#15803d; border:1px solid #86efac"
        elif action == '要確認': action_style = "background-color:#fef3c7; color:#d97706; border:1px solid #fcd34d"
        elif action == '売り': action_style = "background-color:#fee2e2; color:#b91c1c; border:1px solid #fca5a5"
        else: action_style = "background-color:#f1f5f9; color:#64748b; border:1px solid #cbd5e1"
        
        row_style = "background-color: #f8fafc;" if posts < 10 else ""
        table_rows += f"""<tr style="{row_style}"><td style="text-align:center;">{i+1}</td><td style="text-align:center;">{stock_name}</td><td style="text-align:center;"><a href="report_{code}.html" target="_blank" style="font-weight:bold; color:#2563eb; text-decoration:none;">{code}</a></td><td style="text-align:center; font-weight:bold; color:#ea580c;">{score:.1f}</td><td style="text-align:center;">{posts}</td><td style="text-align:center;"><span style="{action_style}; padding:4px 8px; border-radius:4px; font-weight:bold; font-size:12px;">{action}</span></td><td style="text-align:center; font-size:13px; font-weight:bold;">{phase}</td><td style="font-size:13px; color:#334155; font-weight:bold;">{target_aim}</td><td style="text-align:center;">{fact_rate:.2f}</td><td style="text-align:center;">{max_flow}</td></tr>"""
        
    if not table_rows: table_rows = "<tr><td colspan='10' style='text-align:center; padding:30px; color:#64748b;'>解析可能なデータがありませんでした。</td></tr>"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>上昇期待値 スクリーニング全件ランキング</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/js/jquery.tablesorter.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/js/jquery.tablesorter.widgets.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/css/theme.default.min.css">
        <style>
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; background-color: #f0f2f5; color: #1e293b; margin: 0; padding: 30px; }} 
            .header-main {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 4px solid #ea580c; padding-bottom: 12px; margin-bottom: 20px; }} 
            .header-main h1 {{ color: #ea580c; margin: 0; font-size: 26px; }} 
            .meta-info {{ color: #64748b; font-size: 0.9em; }} 
            .content-wrapper {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.04); }} 
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }} 
            th {{ background-color: #1e3a8a !important; color: white !important; cursor: pointer; padding: 12px !important; text-align:center; }} 
            td {{ padding: 10px; border-bottom: 1px solid #e2e8f0; }} 
            tr:hover {{ background-color: #f8fafc; }}
            .tablesorter-filter {{ width: 95%; padding: 4px; border-radius: 4px; border: 1px solid #ccc; font-size: 12px; box-sizing: border-box; }}
            select.tablesorter-filter {{ height: 26px; }}
        </style>
    </head>
    <body>
        <div style="max-width: 1400px; margin: 0 auto;">
            <div class="header-main"><h1>🏆 ヤフ板・上昇期待値 スクリーニング全件ランキング</h1><div class="meta-info">生成日時: {now_str} | 対象: {len(ranking_list)}銘柄</div></div>
            <div class="content-wrapper"><p style="font-size:14px; color:#475569;">※ <b>各列の検索ボックスに「①」や「買い」と入力すると絞り込みができます。</b></p>
                <table id="rankTable" class="tablesorter">
                    <thead>
                        <tr>
                            <th data-filter="false">順位</th>
                            <th>銘柄名</th>
                            <th>コード</th>
                            <th data-filter="false">期待スコア🔥</th>
                            <th data-filter="false">投稿数</th>
                            <th class="filter-select">判定</th>
                            <th class="filter-select">需給フェーズ</th>
                            <th>大口の狙い</th>
                            <th data-filter="false">ファクト密度</th>
                            <th data-filter="false">最大流速</th>
                        </tr>
                    </thead>
                    <tbody>{table_rows}</tbody>
                </table>
            </div>
        </div>
        <script>
            $(document).ready(function() {{
                $("#rankTable").tablesorter({{
                    theme: 'default',
                    widthFixed: true,
                    widgets: ['zebra', 'filter'],
                    widgetOptions: {{
                        filter_cssFilter: 'tablesorter-filter',
                        filter_columnFilters: true,
                        filter_placeholder: {{ search: '絞り込み...' }},
                        filter_saveFilters: false,
                        filter_reset: '.reset'
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    """
    with open(filepath, 'w', encoding='utf-8') as f: f.write(html_content)
    print(f"🏆 ランキングダッシュボードを生成しました: {filepath}")
    try: webbrowser.open(f"file:///{os.path.abspath(filepath)}")
    except: pass

if __name__ == "__main__":
    target_code = sys.argv[1].upper() if len(sys.argv) > 1 else '285A'
    
    # ★実行環境に依存しないよう、スクリプト自身のディレクトリを基準にパスを設定
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output_data")
    input_dir = os.path.join(base_dir, "input_data")
    
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    if not os.path.exists(input_dir): os.makedirs(input_dir)

    # all または ALL-RESET コマンド等の吸収
    if target_code.startswith('ALL'):
        input_file = os.path.join(input_dir, "ヤフ板コード番号.txt")
        cache_file = os.path.join(output_dir, 'all_reports_cache.jsonl')
        
        # ★エラー落ち防止：ファイルがない場合はダミーを自動生成して案内を出す
        if not os.path.exists(input_file):
            print(f"【エラー】銘柄リストファイルが見つかりません。")
            print(f"👉 以下の場所に新規作成しました:\n   {input_file}")
            print(f"監視したい銘柄コードを1行ずつ入力して、再度実行してください。")
            with open(input_file, 'w', encoding='utf-8') as f:
                f.write("9984\n7974\n6920\n")  # ダミーでソフトバンクG、任天堂、レーザーテック等を入れておく
            sys.exit()

        with open(input_file, 'r', encoding='utf-8') as f: codes = [line.strip() for line in f if line.strip()]
        
        all_reports, processed_codes = [], set()
        
        # ★コマンドライン引数で 'reset' や 'clear' が指定されたか判定
        force_clean = (target_code == 'ALL-RESET' or (len(sys.argv) > 2 and sys.argv[2].upper() in ['RESET', 'CLEAR', '-F']))
        
        if os.path.exists(cache_file):
            if force_clean:
                print("\n🗑️ 【強制初期化】既存のキャッシュファイルを削除し、最初からクリーン実行します。")
                os.remove(cache_file)
            else:
                # ★インタラクティブモード（コマンドプロンプト実行時）ならユーザーに選択させる
                if sys.stdin.isatty():
                    ans = input(f"\n🔄 前回の中断データ ({os.path.basename(cache_file)}) があります。\n続きから再開しますか？ [Y: 再開 / N: 最初からやり直す] (デフォルト: Y): ").strip().upper()
                    if ans == 'N':
                        backup_file = cache_file.replace('.jsonl', f'_backup_{int(time.time())}.jsonl')
                        os.rename(cache_file, backup_file)
                        print(f"\n🗑️ 古いキャッシュを退避しました。最初から実行します。")
                    else:
                        with open(cache_file, 'r', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    try:
                                        rep = json.loads(line)
                                        all_reports.append(rep)
                                        processed_codes.add(rep['code'])
                                    except: pass
                        print(f"\n✅ {len(processed_codes)} 銘柄をスキップし、続きから再開します。")
                else:
                    # cron等の自動実行時はデフォルトで再開させる
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    rep = json.loads(line)
                                    all_reports.append(rep)
                                    processed_codes.add(rep['code'])
                                except: pass
                    print(f"\n🔄 【レジューム機能】前回の中断データを発見しました。{len(processed_codes)} 銘柄をスキップして再開します。")
        
        codes_to_process = [c for c in codes if c not in processed_codes]
        if not codes_to_process: print(f"\n✅ 全 {len(codes)} 銘柄がすでに処理済みです。ランキングを再生成します。")
        else: print(f"\n🚀 ALLモード起動：残り {len(codes_to_process)} 銘柄のマルチスレッドスクリーニングを開始します。")

        def process_stock(code):
            time.sleep(random.uniform(THREAD_START_SLEEP_MIN, THREAD_START_SLEEP_MAX))
            try:
                df_market, stock_name = fetch_yahoo_bbs_market_adaptive(code=code)
                if not df_market.empty:
                    report = analyze_bbs_manipulation(df_market, stock_name=stock_name)
                    report['code'] = code 
                    html_path = os.path.join(output_dir, f'report_{code}.html')
                    generate_html_report(df_market, report, code, html_path)
                    with _FILE_LOCK:
                        with open(cache_file, 'a', encoding='utf-8') as cf: cf.write(json.dumps(report, ensure_ascii=False, cls=NpEncoder) + '\n')
                    return report
                else:
                    empty_report = {'code': code, 'stock_name': stock_name, 'is_empty': True, 'bullish_score': -9999, 'total_posts': 0}
                    with _FILE_LOCK:
                        with open(cache_file, 'a', encoding='utf-8') as cf: cf.write(json.dumps(empty_report, ensure_ascii=False, cls=NpEncoder) + '\n')
                    return empty_report
            except Exception as e: print(f"⚠️ [{code}] 予期せぬエラーでスキップされました: {e}"); return None

        if codes_to_process:
            print(f"⚡ {MAX_THREADS_CONFIG} スレッドでバッチ分割処理を実行します...")
            
            total_target = len(codes_to_process)
            completed_count = 0

            for i in range(0, total_target, CHUNK_SIZE):
                chunk = codes_to_process[i:i + CHUNK_SIZE]
                print(f"\n🚀 バッチ処理開始: {i+1} 〜 {min(i+CHUNK_SIZE, total_target)} / {total_target} 銘柄")

                with ThreadPoolExecutor(max_workers=MAX_THREADS_CONFIG) as executor:
                    future_to_code = {executor.submit(process_stock, code): code for code in chunk}
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        result = future.result()
                        completed_count += 1
                        
                        if result:
                            all_reports.append(result)
                            if result.get('is_empty'): 
                                print(f"➖ [{completed_count}/{total_target}] データなし/スキップ: {code}")
                            else: 
                                print(f"✅ [{completed_count}/{total_target}] 解析完了: {code} (Score: {result.get('bullish_score', 0):.0f})")
                        else: 
                            print(f"❌ [{completed_count}/{total_target}] エラーによる失敗: {code}")
                        time.sleep(0.5)

                # 最後のバッチ以外はWAF検知回避の長時間スリープを入れる
                if i + CHUNK_SIZE < total_target:
                    sleep_time = random.uniform(LONG_SLEEP_MIN, LONG_SLEEP_MAX)
                    print(f"☕ WAF検知回避のため {sleep_time:.0f} 秒間の待機に入ります...")
                    time.sleep(sleep_time)
                
        print("\n🏆 全銘柄のスクリーニングが完了しました。上昇期待値全件ランキングを計算・生成します...")
        valid_reports = [r for r in all_reports if not r.get('is_empty', False)]
        valid_reports.sort(key=lambda x: x['bullish_score'], reverse=True)
        generate_ranking_html(valid_reports, os.path.join(output_dir, 'report_ALL_Ranking.html'))

    else:
        df_market, stock_name = fetch_yahoo_bbs_market_adaptive(code=target_code, verbose=True)
        if not df_market.empty:
            report = analyze_bbs_manipulation(df_market, stock_name=stock_name)
            print("\n" + "="*60)
            print(f" 📊  YABB ADVANCED COMPOSITE ANALYSIS REPORT [{target_code}]")
            print("="*60)
            print(f"▶ 総合工作フラグ : {'【 TRUE (要警戒) 】' if report['is_manipulated'] else '【 FALSE (正常) 】'}")
            print(f"▶ 掲示板現状判定 : {report['intent']}")
            print(f"▶ 上昇期待スコア : {report['bullish_score']:.1f} 点")
            print(f"▶ 統計流速情報   : 総数 {report['total_posts']}件 / 最大流速 {report['max_flow']}件(10分)")
            print(f"▶ テキスト質評価 : 平均 {report['avg_len']:.1f}文字 / ファクト密度 {report['fact_rate']:.2f}")
            print(f"▶ ユーザー多様性 : 実人数 {report['unique_users']}人 / 占有比率 {report['user_occupancy']:.2f}")
            print("-"*60)
            print("▶ スクリーニング戦略ログ:")
            for line in report['summary']: print(f"  {line}")
            print("="*60 + "\n")
            html_path = os.path.join(output_dir, f'report_{target_code}.html')
            generate_html_report(df_market, report, target_code, html_path)
            try: webbrowser.open(f"file:///{os.path.abspath(html_path)}")
            except: pass
        else: print("\n対象時間枠内に投稿データがありませんでした。")