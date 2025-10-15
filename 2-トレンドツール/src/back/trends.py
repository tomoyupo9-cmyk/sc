from pytrends.request import TrendReq

def fetch_trends(aliases, timeframe, geo):
    if not aliases:
        return {"latest": 0.0, "series": []}
    pytrends = TrendReq(hl="ja-JP", tz=540)
    kw = list(dict.fromkeys(aliases))[:5]
    try:
        pytrends.build_payload(kw_list=kw, timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return {"latest": 0.0, "series": []}
        latest_row = df.tail(1)[kw].mean(axis=1).iloc[0]
        series = [
            {"timestamp": idx.isoformat(), "score": float(row[kw].mean())}
            for idx, row in df.iterrows()
        ]
        return {"latest": float(latest_row), "series": series}
    except Exception:
        return {"latest": 0.0, "series": []}
