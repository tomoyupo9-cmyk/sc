from typing import Sequence, Dict, Any
from pytrends.request import TrendReq
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    from requests.packages.urllib3.util.retry import Retry  # 古い環境用

def fetch_trends(aliases: Sequence[str], timeframe: str, geo: str) -> Dict[str, Any]:
    """
    Google Trends を取得。ネットワーク障害時は空を返して呼び出し側を止めない。
    返り値: {"latest": float, "series": [{"timestamp": ISO8601, "score": float}, ...]}
    """
    if not aliases:
        return {"latest": 0.0, "series": []}

    # ---- リトライ付きセッション ----
    sess = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adpt = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=retry)
    sess.mount("http://", adpt)
    sess.mount("https://", adpt)

    # ---- pytrends に timeout と session を渡す（初回Cookie取得にも効く）----
    try:
        pytrends = TrendReq(
            hl="ja-JP", tz=540,
            requests_args={"timeout": (5, 20), "session": sess}  # (connect, read)
        )
    except Exception:
        return {"latest": 0.0, "series": []}

    # 5語まで（重複除去後）
    kw = list(dict.fromkeys(aliases))[:5]

    try:
        pytrends.build_payload(kw_list=kw, timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return {"latest": 0.0, "series": []}

        # isPartial 列があれば除外
        cols = [c for c in df.columns if c.lower() != "ispartial"]
        if not cols:
            return {"latest": 0.0, "series": []}

        # タイムシリーズ
        series = []
        for idx, row in df.iterrows():
            try:
                series.append({"timestamp": idx.to_pydatetime().isoformat(), "score": float(row[cols].mean())})
            except Exception:
                continue

        latest = float(df.tail(1)[cols].mean(axis=1).iloc[0]) if series else 0.0
        return {"latest": latest, "series": series}

    except Exception as e:
        print(f"[trends][WARN] {e}")
        return {"latest": 0.0, "series": []}
