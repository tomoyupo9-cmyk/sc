# -*- coding: utf-8 -*-
"""
決算後上昇スクリーニング_v32_値幅低リスク.py

直近に決算を終えた銘柄から、決算後に上昇を維持している候補を高速抽出し、
以下を追加して CSV + HTML ダッシュボードを出力する。

- 強さスコア   : 決算後トレンドそのものの強さ
- 今からスコア : 伸び切りすぎていない入りやすさ
- 過熱度       : どれだけ伸び切っているか
- 短期低リスクスコア : 今から入りやすい・強い・高値維持・低過熱を価格だけで合成
- 今ならまだ間に合う順位 : 短期低リスク条件を通過した銘柄内の順位
- 値幅×低リスクスコア / 順位 : 値幅実績と下落耐性を両立した銘柄を抽出
- 今のページCSV : ダッシュボードで現在表示中のページだけを、既存スクリーナー互換形式で保存
- タイプ       : 条件に当てはまるものを複数タグで併記（再評価 / 今から候補 / 継続上昇 / 伸び切り等）
- サマリー     : 何が良いか・何に注意かをルールベースで文章化

重要:
- 伸び切っている銘柄も削除しない。全候補を残す。
- タイプは排他的にせず、複数条件に当てはまれば複数タグを付ける。
- D3/D5 は決算後営業日数が足りない場合は無効扱いにする。
- DBは read-only で開き、書き換えない。
- 短期低リスク順位・値幅×低リスク順位には信用・出来高・機関空売りを一切使わない。

例:
  python 決算後上昇スクリーニング_v12_dashboard.py --days 60 --min-return 5 --max-drawdown 4
  python 決算後上昇スクリーニング_v12_dashboard.py --days 60 --min-return 5 --max-drawdown 4 --open
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import sqlite3
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np


DB_CANDIDATES = [
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
    r"D:\kabu\main\1-スクリーニング自動化プログラム\main\db\kani2.db",
]

# 出力先は固定。実行カレントディレクトリには依存しない。
OUTPUT_DIR = Path(r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data")


def log(msg: str) -> None:
    print(msg, flush=True)


def clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        return max(lo, min(hi, float(v)))
    except Exception:
        return lo


def resolve_db_path(cli_path: str | None) -> Path:
    if cli_path:
        p = Path(cli_path)
        if not p.exists():
            raise FileNotFoundError(f"DBが見つかりません: {p}")
        return p
    for s in DB_CANDIDATES:
        p = Path(s)
        if p.exists():
            return p
    raise FileNotFoundError("kani2.db を自動検出できません。--db で指定してください。")


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.execute("PRAGMA query_only=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-50000;")
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone() is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def normalize_code(v) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4) if s.isdigit() else s


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def pct(new, old):
    try:
        a, b = float(new), float(old)
        if not math.isfinite(a) or not math.isfinite(b) or b == 0:
            return None
        return (a / b - 1.0) * 100.0
    except Exception:
        return None


def n0(v, default=0.0) -> float:
    try:
        if v is None or pd.isna(v):
            return float(default)
        return float(v)
    except Exception:
        return float(default)


# ============================================================
# DB取得
# ============================================================

def _guess_earnings_type(title: str = "", quarter_no=None, fallback: str = "") -> str:
    """決算短信タイトル / quarter_no から 1Q・2Q/H1・3Q・FY を推定。"""
    t = str(title or "")
    z = t.translate(str.maketrans("１２３４", "1234"))
    if re.search(r"(第\s*1\s*四半期|1Q)", z, re.I):
        return "1Q"
    if re.search(r"(第\s*2\s*四半期|2Q|中間|半期)", z, re.I):
        return "2Q/H1"
    if re.search(r"(第\s*3\s*四半期|3Q)", z, re.I):
        return "3Q"
    try:
        q = int(quarter_no)
        return {1:"1Q", 2:"2Q/H1", 3:"3Q", 4:"FY"}.get(q, fallback or "")
    except Exception:
        return fallback or ""


def _normalize_event_dt_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(
        s.astype(str)
         .str.replace("/", "-", regex=False)
         .str.replace("T", " ", regex=False)
         .str.replace("+09:00", "", regex=False),
        errors="coerce",
        format="mixed"
    )


def fetch_latest_earnings(conn: sqlite3.Connection, days: int) -> pd.DataFrame:
    """
    v23 真の発表済み母集団。

    優先ソース:
      1) tdnet_documents          : 決算短信そのもの
      2) earnings_events          : TDnet決算レーン
      3) tdnet_xbrl_metrics       : 決算短信XBRL
      4) quarterly_actual_history : 株探3ヵ月実績
      5) earnings_reaction_labels : 従来ラベルのfallback

    reaction_labels は母集団ではなく、同じ発表日のイベントへ
    D1/D3/D5を後付けする用途に変更する。
    """
    start_day = (date.today() - timedelta(days=int(days))).isoformat()
    events = []

    def add_frame(df, source, priority, time_precision="時刻"):
        if df is None or df.empty:
            return
        x = df.copy()
        x["母集団ソース単体"] = source
        x["_source_priority"] = priority
        x["発表時刻精度"] = time_precision
        events.append(x)

    # 1) TDnet本文
    if table_exists(conn, "tdnet_documents"):
        c = columns(conn, "tdnet_documents")
        if {"コード","提出時刻","タイトル"}.issubset(c):
            name_expr = '"銘柄名"' if "銘柄名" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) AS コード,
                       {name_expr} AS 銘柄名,
                       提出時刻 AS 発表日時,
                       タイトル AS 決算タイトル
                FROM tdnet_documents
                WHERE date(提出時刻) >= date(?)
                  AND datetime(提出時刻) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add_frame(
                pd.read_sql_query(q, conn, params=(start_day,)),
                "tdnet_documents", 50, "時刻"
            )

    # 2) earnings_events
    if table_exists(conn, "earnings_events"):
        c = columns(conn, "earnings_events")
        if {"コード","タイトル"}.issubset(c) and ("発表日時" in c or "提出時刻" in c):
            dt_expr = (
                "COALESCE(NULLIF(発表日時,''), 提出時刻)"
                if {"発表日時","提出時刻"}.issubset(c)
                else ('発表日時' if "発表日時" in c else '提出時刻')
            )
            name_expr = '"銘柄名"' if "銘柄名" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) AS コード,
                       {name_expr} AS 銘柄名,
                       {dt_expr} AS 発表日時,
                       タイトル AS 決算タイトル
                FROM earnings_events
                WHERE date({dt_expr}) >= date(?)
                  AND datetime({dt_expr}) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add_frame(
                pd.read_sql_query(q, conn, params=(start_day,)),
                "earnings_events", 45, "時刻"
            )

    # 3) tdnet_xbrl_metrics
    if table_exists(conn, "tdnet_xbrl_metrics"):
        c = columns(conn, "tdnet_xbrl_metrics")
        if {"コード","提出時刻","タイトル"}.issubset(c):
            q = """
                SELECT CAST(コード AS TEXT) AS コード,
                       NULL AS 銘柄名,
                       提出時刻 AS 発表日時,
                       タイトル AS 決算タイトル
                FROM tdnet_xbrl_metrics
                WHERE date(提出時刻) >= date(?)
                  AND datetime(提出時刻) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add_frame(
                pd.read_sql_query(q, conn, params=(start_day,)),
                "tdnet_xbrl_metrics", 40, "時刻"
            )

    # 4) quarterly_actual_history
    if table_exists(conn, "quarterly_actual_history"):
        c = columns(conn, "quarterly_actual_history")
        if {"コード","announcement_date"}.issubset(c):
            qno_expr = "quarter_no" if "quarter_no" in c else "NULL"
            label_expr = "quarter_label" if "quarter_label" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) AS コード,
                       NULL AS 銘柄名,
                       announcement_date AS 発表日時,
                       {label_expr} AS 決算タイトル,
                       {qno_expr} AS quarter_no
                FROM quarterly_actual_history
                WHERE announcement_date IS NOT NULL
                  AND date(announcement_date) >= date(?)
                  AND date(announcement_date) <= date('now','localtime')
            """
            add_frame(
                pd.read_sql_query(q, conn, params=(start_day,)),
                "quarterly_actual_history", 30, "日付"
            )

    # 5) reaction labels
    labels = pd.DataFrame()
    if table_exists(conn, "earnings_reaction_labels"):
        c = columns(conn, "earnings_reaction_labels")
        if {"コード","発表日時"}.issubset(c):
            select_cols = [
                "CAST(コード AS TEXT) AS コード",
                ('銘柄名' if "銘柄名" in c else 'NULL AS 銘柄名'),
                "発表日時",
                ('決算種別' if "決算種別" in c else 'NULL AS 決算種別'),
                ('基準終値' if "基準終値" in c else 'NULL AS 基準終値'),
                ('D1日付' if "D1日付" in c else 'NULL AS D1日付'),
                ('D1終値騰落率' if "D1終値騰落率" in c else 'NULL AS D1終値騰落率'),
                ('D3終値騰落率' if "D3終値騰落率" in c else 'NULL AS D3終値騰落率'),
                ('D5終値騰落率' if "D5終値騰落率" in c else 'NULL AS D5終値騰落率'),
            ]
            labels = pd.read_sql_query(
                f"""
                SELECT {", ".join(select_cols)}
                FROM earnings_reaction_labels
                WHERE date(発表日時) >= date(?)
                  AND datetime(発表日時) <= datetime('now','localtime')
                """,
                conn, params=(start_day,)
            )
            if not labels.empty:
                ev = labels[["コード","銘柄名","発表日時"]].copy()
                ev["決算タイトル"] = ""
                ev["決算種別"] = labels.get("決算種別", "")
                add_frame(ev, "earnings_reaction_labels", 20, "時刻")

    if not events:
        return pd.DataFrame()

    all_ev = pd.concat(events, ignore_index=True, sort=False)
    all_ev["コード"] = all_ev["コード"].map(normalize_code)
    all_ev = all_ev[
        all_ev["コード"].astype(str).str.len().eq(4)
        & all_ev["コード"].ne("0000")
    ].copy()

    all_ev["発表日時_dt"] = _normalize_event_dt_series(all_ev["発表日時"])
    all_ev = all_ev.dropna(subset=["コード","発表日時_dt"])
    if all_ev.empty:
        return all_ev

    all_ev["_event_date"] = all_ev["発表日時_dt"].dt.date.astype(str)

    evidence = (
        all_ev.groupby(["コード","_event_date"])["母集団ソース単体"]
              .agg(lambda s: "+".join(dict.fromkeys(s.astype(str).tolist())))
              .rename("母集団根拠")
              .reset_index()
    )

    all_ev = all_ev.sort_values(
        ["コード","_event_date","発表日時_dt","_source_priority"],
        ascending=[True,True,True,True]
    )
    daily = all_ev.drop_duplicates(["コード","_event_date"], keep="last")
    daily = daily.merge(evidence, on=["コード","_event_date"], how="left")

    daily = (
        daily.sort_values(["コード","発表日時_dt","_source_priority"])
             .drop_duplicates("コード", keep="last")
             .reset_index(drop=True)
    )

    if "決算種別" not in daily.columns:
        daily["決算種別"] = ""
    daily["決算種別"] = [
        _guess_earnings_type(
            title=r.get("決算タイトル",""),
            quarter_no=r.get("quarter_no"),
            fallback=r.get("決算種別","")
        )
        for _, r in daily.iterrows()
    ]

    for c in ("基準終値","D1日付","D1終値騰落率","D3終値騰落率","D5終値騰落率"):
        if c not in daily.columns:
            daily[c] = np.nan if c != "D1日付" else None
    daily["反応ラベル有無"] = 0

    # 同じ発表日だけreaction labelを結合
    if labels is not None and not labels.empty:
        lab = labels.copy()
        lab["コード"] = lab["コード"].map(normalize_code)
        lab["発表日時_dt_lab"] = _normalize_event_dt_series(lab["発表日時"])
        lab = lab.dropna(subset=["コード","発表日時_dt_lab"])
        lab["_event_date"] = lab["発表日時_dt_lab"].dt.date.astype(str)
        lab = (
            lab.sort_values(["コード","_event_date","発表日時_dt_lab"])
               .drop_duplicates(["コード","_event_date"], keep="last")
        )
        keep = [
            "コード","_event_date","基準終値","D1日付",
            "D1終値騰落率","D3終値騰落率","D5終値騰落率","決算種別"
        ]
        keep = [c for c in keep if c in lab.columns]
        lab2 = lab[keep].copy()
        rename = {
            c:f"{c}__lab"
            for c in keep if c not in ("コード","_event_date")
        }
        lab2 = lab2.rename(columns=rename)
        daily = daily.merge(lab2, on=["コード","_event_date"], how="left")

        has_lab = pd.Series(False, index=daily.index)
        if "D1日付__lab" in daily:
            has_lab = has_lab | daily["D1日付__lab"].notna()
        if "基準終値__lab" in daily:
            has_lab = has_lab | pd.to_numeric(
                daily["基準終値__lab"], errors="coerce"
            ).notna()
        daily["反応ラベル有無"] = has_lab.astype(int)

        for c in ("基準終値","D1日付","D1終値騰落率","D3終値騰落率","D5終値騰落率"):
            lc = f"{c}__lab"
            if lc in daily:
                daily[c] = daily[lc].combine_first(daily[c])
                daily.drop(columns=[lc], inplace=True)

        if "決算種別__lab" in daily:
            blank = daily["決算種別"].astype(str).str.strip().eq("")
            daily.loc[blank, "決算種別"] = daily.loc[blank, "決算種別__lab"]
            daily.drop(columns=["決算種別__lab"], inplace=True)

    daily["発表日時"] = daily["発表日時_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    daily["D1日付_dt"] = pd.to_datetime(
        daily["D1日付"], errors="coerce"
    ).dt.normalize()

    for c in ("基準終値","D1終値騰落率","D3終値騰落率","D5終値騰落率"):
        daily[c] = to_num(daily[c])

    daily["母集団ソース"] = daily["母集団ソース単体"].astype(str)
    daily = daily.drop(
        columns=["母集団ソース単体","_source_priority","_event_date"],
        errors="ignore"
    )

    out_cols = [
        "コード","銘柄名","発表日時","発表日時_dt","発表時刻精度",
        "決算種別","決算タイトル","母集団ソース","母集団根拠","反応ラベル有無",
        "基準終値","D1日付","D1日付_dt",
        "D1終値騰落率","D3終値騰落率","D5終値騰落率"
    ]
    for c in out_cols:
        if c not in daily.columns:
            daily[c] = None
    return daily[out_cols].reset_index(drop=True)

def fetch_screener_current(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    if not codes or not table_exists(conn, "screener"):
        return pd.DataFrame(columns=["コード", "銘柄名_s", "現在値"])

    sc = columns(conn, "screener")
    name_expr = '"銘柄名"' if "銘柄名" in sc else "NULL"
    px_expr = '"現在値"' if "現在値" in sc else "NULL"

    parts = []
    for i in range(0, len(codes), 700):
        part = codes[i:i+700]
        ph = ",".join("?" for _ in part)
        sql = f'''SELECT CAST(コード AS TEXT) AS コード,
                         {name_expr} AS 銘柄名_s,
                         {px_expr} AS 現在値
                  FROM screener
                  WHERE コード IN ({ph})'''
        parts.append(pd.read_sql_query(sql, conn, params=part))

    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["コード", "銘柄名_s", "現在値"])
    df["コード"] = df["コード"].map(normalize_code)
    df["現在値"] = to_num(df["現在値"])
    return df.drop_duplicates("コード", keep="last")


def fetch_price_history(conn: sqlite3.Connection, codes: list[str], start_date: str) -> pd.DataFrame:
    have = columns(conn, "price_history")
    miss = {"コード", "日付", "終値"} - have
    if miss:
        raise RuntimeError("price_history の不足列: " + ", ".join(sorted(miss)))

    open_expr = '"始値"' if "始値" in have else '"終値"'
    high_expr = '"高値"' if "高値" in have else '"終値"'
    low_expr = '"安値"' if "安値" in have else '"終値"'

    parts = []
    for i in range(0, len(codes), 700):
        part = codes[i:i+700]
        ph = ",".join("?" for _ in part)
        sql = f'''SELECT コード,日付,{open_expr} AS 始値,{high_expr} AS 高値,{low_expr} AS 安値,終値
                  FROM price_history
                  WHERE コード IN ({ph}) AND 日付 >= ? AND 終値 IS NOT NULL
                  ORDER BY コード,日付'''
        parts.append(pd.read_sql_query(sql, conn, params=[*part, start_date]))

    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        return df

    df["コード"] = df["コード"].map(normalize_code)
    df["日付_dt"] = pd.to_datetime(df["日付"], errors="coerce").dt.normalize()
    for c in ("始値", "高値", "安値", "終値"):
        df[c] = to_num(df[c])
    return (
        df.dropna(subset=["コード", "日付_dt", "終値"])
          .sort_values(["コード", "日付_dt"])
          .drop_duplicates(["コード", "日付_dt"], keep="last")
          .reset_index(drop=True)
    )


def _is_tse_session_date(day: date) -> bool:
    """
    東証営業日判定。
    exchange_calendars があれば XTKS を使う。
    ユーザー環境に無ければ土日だけ除外する軽量fallback。
    """
    try:
        import exchange_calendars as xcals
        cal = xcals.get_calendar("XTKS")
        return bool(cal.is_session(pd.Timestamp(day)))
    except Exception:
        return day.weekday() < 5


def append_live_if_needed(
    prices: pd.DataFrame,
    screen: pd.DataFrame,
    now: datetime | None = None,
) -> pd.DataFrame:
    """
    screener.現在値を price_history に反映する。

    v24重要修正:
      - 00:00～09:00の市場開始前は「今日」の架空行を作らない。
        直近実在営業日の行だけを更新する。
      - 土日/休場日も新しい日付行を作らない。
      - 市場開始後の営業日だけ、今日行が無ければライブ行を追加。
      - 今日のprice_history行が既にあれば、その行の終値を最新現在値へ更新。

    これにより 00:10実行時に
      8/13終値 → 8/14終値
    と複製されて直近1日騰落率が0%になるバグを防ぐ。
    """
    if screen.empty:
        return prices

    now_dt = now or datetime.now()
    today = pd.Timestamp(now_dt.date()).normalize()
    out = prices.copy()

    for c in ["コード", "日付", "日付_dt", "始値", "高値", "安値", "終値"]:
        if c not in out.columns:
            out[c] = pd.NA

    out["コード"] = out["コード"].map(normalize_code)
    out["日付_dt"] = pd.to_datetime(out["日付_dt"], errors="coerce").dt.normalize()

    valid_dates = out["日付_dt"].dropna()
    latest_actual = valid_dates.max() if not valid_dates.empty else pd.NaT
    today_exists_any = bool((out["日付_dt"] == today).any())

    t = now_dt.time()
    market_started = t >= datetime.strptime("09:00", "%H:%M").time()
    is_session = _is_tse_session_date(now_dt.date())

    if today_exists_any:
        target_day = today
        mode = "today_exists"
        allow_add = True
    elif market_started and is_session:
        target_day = today
        mode = "live_today"
        allow_add = True
    else:
        target_day = latest_actual if pd.notna(latest_actual) else None
        mode = "preopen_or_closed_previous"
        allow_add = False

    if target_day is None:
        out.attrs["live_session_mode"] = "no_target"
        out.attrs["live_session_date"] = None
        return out

    add = []
    for r in screen.itertuples(index=False):
        code = normalize_code(getattr(r, "コード"))
        px = getattr(r, "現在値", None)
        try:
            px = float(px)
        except Exception:
            continue
        if not code or not pd.notna(px) or px <= 0:
            continue

        mask_target = (out["コード"] == code) & (out["日付_dt"] == target_day)

        if mask_target.any():
            idx = out.index[mask_target][-1]
            old_open = pd.to_numeric(pd.Series([out.at[idx, "始値"]]), errors="coerce").iloc[0]
            old_high = pd.to_numeric(pd.Series([out.at[idx, "高値"]]), errors="coerce").iloc[0]
            old_low  = pd.to_numeric(pd.Series([out.at[idx, "安値"]]), errors="coerce").iloc[0]

            if pd.isna(old_open) or float(old_open) <= 0:
                out.at[idx, "始値"] = px
            out.at[idx, "高値"] = max(float(old_high), px) if pd.notna(old_high) and float(old_high) > 0 else px
            out.at[idx, "安値"] = min(float(old_low), px) if pd.notna(old_low) and float(old_low) > 0 else px
            out.at[idx, "終値"] = px
            out.at[idx, "日付"] = pd.Timestamp(target_day).strftime("%Y-%m-%d")

        elif allow_add:
            add.append({
                "コード": code,
                "日付": pd.Timestamp(target_day).strftime("%Y-%m-%d"),
                "日付_dt": pd.Timestamp(target_day).normalize(),
                "始値": px,
                "高値": px,
                "安値": px,
                "終値": px,
            })

    if add:
        out = pd.concat([out, pd.DataFrame(add)], ignore_index=True)

    for c in ("始値", "高値", "安値", "終値"):
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = (
        out.sort_values(["コード", "日付_dt"])
           .drop_duplicates(["コード", "日付_dt"], keep="last")
           .reset_index(drop=True)
    )
    out.attrs["live_session_mode"] = mode
    out.attrs["live_session_date"] = pd.Timestamp(target_day).strftime("%Y-%m-%d")
    out.attrs["live_rows_added"] = len(add)
    return out


def reconstruct_reaction_from_prices(
    earnings: pd.DataFrame,
    prices: pd.DataFrame,
    now: datetime | None = None,
) -> pd.DataFrame:
    """
    reaction_labels がまだ無くても price_history から D1/D3/D5 を復元する。

    反応営業日の定義:
      - 発表時刻が分かり、15:30より前 → 発表日が取引日なら同日をD1
      - 15:30以降                  → 次の取引日をD1
      - 発表時刻が日付しか無い      → 保守的に次の取引日をD1

    基準終値:
      D1の1つ前の保存営業日終値。

    既存reaction_labelsがある場合は原則その値を優先し、
    欠損部分だけprice_historyで補完する。

    D1状態:
      ラベル確定   : reaction_labels由来
      価格履歴復元 : D1をprice_historyから生成
      真のD1待ち   : 次の反応営業日がまだ来ていない
      価格不足     : 反応営業日は既に来たはずだが当該コードの価格履歴がない
    """
    if earnings is None or earnings.empty:
        return earnings
    if prices is None or prices.empty:
        out = earnings.copy()
        out["D1ソース"] = np.where(
            pd.to_numeric(out.get("反応ラベル有無", 0), errors="coerce").fillna(0).eq(1),
            "reaction_labels", ""
        )
        out["D1復元精度"] = ""
        out["D1状態"] = np.where(out["D1ソース"].eq("reaction_labels"), "ラベル確定", "価格不足")
        out["D1待ち理由"] = np.where(out["D1状態"].eq("価格不足"), "price_historyなし", "")
        out["基準終値ソース"] = np.where(
            pd.to_numeric(out.get("基準終値"), errors="coerce").notna()
            & out["D1ソース"].eq("reaction_labels"),
            "earnings_reaction_labels", ""
        )
        return out

    now_dt = now or datetime.now()
    out = earnings.copy()

    for c in ("基準終値", "D1終値騰落率", "D3終値騰落率", "D5終値騰落率"):
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    if "D1日付" not in out.columns:
        out["D1日付"] = None
    if "D1日付_dt" not in out.columns:
        out["D1日付_dt"] = pd.NaT
    out["D1日付_dt"] = pd.to_datetime(out["D1日付_dt"], errors="coerce").dt.normalize()

    label_flag = pd.to_numeric(out.get("反応ラベル有無", 0), errors="coerce").fillna(0).astype(int)
    out["D1ソース"] = np.where(label_flag.eq(1), "reaction_labels", "")
    out["D1復元精度"] = np.where(label_flag.eq(1), "確定", "")
    out["D1状態"] = np.where(label_flag.eq(1), "ラベル確定", "")
    out["D1待ち理由"] = ""
    out["基準終値ソース"] = np.where(
        out["基準終値"].notna() & label_flag.eq(1),
        "earnings_reaction_labels", ""
    )

    pgroups = {
        code: g.sort_values("日付_dt").drop_duplicates("日付_dt", keep="last").reset_index(drop=True)
        for code, g in prices.groupby("コード", sort=False)
    }
    global_latest = pd.to_datetime(prices["日付_dt"], errors="coerce").max()
    live_day = prices.attrs.get("live_session_date")
    live_mode = str(prices.attrs.get("live_session_mode") or "")

    cutoff = datetime.strptime("15:30", "%H:%M").time()

    for idx, er in out.iterrows():
        code = normalize_code(er.get("コード"))
        g = pgroups.get(code)
        if g is None or g.empty:
            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1状態"] = "価格不足"
                out.at[idx, "D1待ち理由"] = "当該コードのprice_historyなし"
            continue

        g = g.dropna(subset=["日付_dt", "終値"]).copy()
        if g.empty:
            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1状態"] = "価格不足"
                out.at[idx, "D1待ち理由"] = "終値履歴なし"
            continue

        ann = pd.to_datetime(er.get("発表日時_dt"), errors="coerce")
        if pd.isna(ann):
            ann = pd.to_datetime(er.get("発表日時"), errors="coerce")
        if pd.isna(ann):
            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1状態"] = "価格不足"
                out.at[idx, "D1待ち理由"] = "発表日時不明"
            continue

        ann_day = pd.Timestamp(ann).normalize()
        precision = str(er.get("発表時刻精度") or "")
        exact_time = precision == "時刻"

        session_dates = pd.DatetimeIndex(g["日付_dt"].dropna().unique()).sort_values()

        # 既存D1があればその日を優先。無ければ発表時刻から復元。
        existing_d1 = pd.to_datetime(er.get("D1日付_dt"), errors="coerce")
        if pd.notna(existing_d1):
            d1_day = pd.Timestamp(existing_d1).normalize()
        else:
            if exact_time and pd.Timestamp(ann).time() < cutoff and ann_day in session_dates:
                d1_day = ann_day
            else:
                after = session_dates[session_dates > ann_day]
                d1_day = after[0] if len(after) else pd.NaT

        if pd.isna(d1_day):
            # 本当に次営業日待ちなのか、既に反応日は来たのに価格が無いのかを分離
            should_wait = False
            if pd.isna(global_latest):
                should_wait = True
            elif exact_time and pd.Timestamp(ann).time() < cutoff:
                # 当日反応型。全体では発表日まで価格が進んでいるのに当該コードだけ無ければ価格不足
                should_wait = bool(global_latest < ann_day)
            else:
                # 翌営業日反応型。全体の最新価格日が発表日以下ならまだ次営業日が来ていない
                should_wait = bool(global_latest <= ann_day)

            if int(label_flag.loc[idx]) != 1:
                if should_wait:
                    out.at[idx, "D1状態"] = "真のD1待ち"
                    out.at[idx, "D1待ち理由"] = "次の反応営業日が未到来"
                else:
                    out.at[idx, "D1状態"] = "価格不足"
                    out.at[idx, "D1待ち理由"] = "反応営業日は到来済みだが価格履歴不足"
            continue

        # D1位置
        pos_arr = np.where(session_dates == pd.Timestamp(d1_day).normalize())[0]
        if len(pos_arr) == 0:
            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1状態"] = "価格不足"
                out.at[idx, "D1待ち理由"] = "D1日価格が見つからない"
            continue
        pos = int(pos_arr[0])

        if pos == 0:
            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1状態"] = "価格不足"
                out.at[idx, "D1待ち理由"] = "D1前営業日の基準終値が不足"
            continue

        base_day = session_dates[pos - 1]
        base_rows = g[g["日付_dt"] == base_day]
        if base_rows.empty:
            continue
        base = float(base_rows["終値"].iloc[-1])
        if not np.isfinite(base) or base <= 0:
            continue

        # 既存基準が無い時だけ補完
        if pd.isna(out.at[idx, "基準終値"]) or float(out.at[idx, "基準終値"]) <= 0:
            out.at[idx, "基準終値"] = base
            out.at[idx, "基準終値ソース"] = "price_history_D1前終値"

        # reaction_labelsがある場合は既存値を優先し、欠損だけ埋める
        vals = {}
        for n, col in [(1, "D1終値騰落率"), (3, "D3終値騰落率"), (5, "D5終値騰落率")]:
            p = pos + (n - 1)
            if p < len(session_dates):
                dayn = session_dates[p]
                rr = g[g["日付_dt"] == dayn]
                if not rr.empty:
                    close_n = float(rr["終値"].iloc[-1])
                    vals[n] = (dayn, (close_n / base - 1.0) * 100.0)

        if 1 in vals:
            if pd.isna(out.at[idx, "D1日付_dt"]):
                out.at[idx, "D1日付_dt"] = pd.Timestamp(vals[1][0]).normalize()
                out.at[idx, "D1日付"] = pd.Timestamp(vals[1][0]).strftime("%Y-%m-%d")
            if pd.isna(out.at[idx, "D1終値騰落率"]):
                out.at[idx, "D1終値騰落率"] = round(vals[1][1], 6)
            if pd.isna(out.at[idx, "D3終値騰落率"]) and 3 in vals:
                out.at[idx, "D3終値騰落率"] = round(vals[3][1], 6)
            if pd.isna(out.at[idx, "D5終値騰落率"]) and 5 in vals:
                out.at[idx, "D5終値騰落率"] = round(vals[5][1], 6)

            if int(label_flag.loc[idx]) != 1:
                out.at[idx, "D1ソース"] = "price_history復元"
                d1s = pd.Timestamp(vals[1][0]).strftime("%Y-%m-%d")
                if live_day == d1s and live_mode == "live_today" and now_dt.time() < cutoff:
                    out.at[idx, "D1復元精度"] = "時刻・当日途中"
                elif exact_time:
                    out.at[idx, "D1復元精度"] = "時刻基準"
                else:
                    out.at[idx, "D1復元精度"] = "日付推定(翌営業日)"
                out.at[idx, "D1状態"] = "価格履歴復元"
                out.at[idx, "D1待ち理由"] = ""

    out["D1日付_dt"] = pd.to_datetime(out["D1日付_dt"], errors="coerce").dt.normalize()
    return out

def fetch_gu_stats(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    """GU=当日始値 > 前営業日終値。全期間はprice_history保存全履歴。"""
    cols_out=["コード","全期間GU率","全期間GU回数","全期間GU判定日数",
              "直近10日GU率","直近10日GU回数","直近10日GU判定日数","GU加速差"]
    if not codes or not table_exists(conn,"price_history"):
        return pd.DataFrame(columns=cols_out)
    have=columns(conn,"price_history")
    if not {"コード","日付","始値","終値"}.issubset(have):
        return pd.DataFrame(columns=cols_out)

    out=[]
    for i in range(0,len(codes),400):
        part=codes[i:i+400]
        ph=",".join("?" for _ in part)
        sql=f"""
        WITH x AS (
          SELECT CAST(コード AS TEXT) AS コード,日付,
                 CAST(始値 AS REAL) AS 始値,
                 LAG(CAST(終値 AS REAL)) OVER(
                   PARTITION BY CAST(コード AS TEXT) ORDER BY date(日付)
                 ) AS 前日終値
          FROM price_history
          WHERE CAST(コード AS TEXT) IN ({ph})
            AND 始値 IS NOT NULL AND 終値 IS NOT NULL
        ),
        y AS (
          SELECT コード,日付,
                 CASE WHEN 始値>前日終値 THEN 1 ELSE 0 END AS GU,
                 ROW_NUMBER() OVER(PARTITION BY コード ORDER BY date(日付) DESC) AS rn
          FROM x
          WHERE 前日終値 IS NOT NULL AND 前日終値>0 AND 始値>0
        )
        SELECT コード,
               ROUND(100.0*SUM(GU)/COUNT(*),2) AS 全期間GU率,
               SUM(GU) AS 全期間GU回数,
               COUNT(*) AS 全期間GU判定日数,
               ROUND(100.0*SUM(CASE WHEN rn<=10 THEN GU ELSE 0 END)/
                     NULLIF(SUM(CASE WHEN rn<=10 THEN 1 ELSE 0 END),0),2) AS 直近10日GU率,
               SUM(CASE WHEN rn<=10 THEN GU ELSE 0 END) AS 直近10日GU回数,
               SUM(CASE WHEN rn<=10 THEN 1 ELSE 0 END) AS 直近10日GU判定日数
        FROM y GROUP BY コード
        """
        out.append(pd.read_sql_query(sql,conn,params=part))
    if not out:
        return pd.DataFrame(columns=cols_out)
    df=pd.concat(out,ignore_index=True)
    if df.empty:
        return pd.DataFrame(columns=cols_out)
    df["コード"]=df["コード"].map(normalize_code)
    df["GU加速差"]=pd.to_numeric(df["直近10日GU率"],errors="coerce")-pd.to_numeric(df["全期間GU率"],errors="coerce")
    df["GU加速差"]=df["GU加速差"].round(2)
    return df.drop_duplicates("コード",keep="last")


def fetch_strong_close_stats(conn: sqlite3.Connection, codes: list[str],
                             upper_zone: float = 0.80) -> pd.DataFrame:
    """
    「強い高値圏引け率」を price_history の全保存期間から計算。

    強い高値圏引け:
      1) 終値がその日の値幅の上位20%以内
         (終値-安値)/(高値-安値) >= 0.80
      2) 終値 >= 始値

    完全な「終値=高値」だけに限定せず、
    高値近辺まで買われたまま引けた日を捉える。
    """
    out_cols = [
        "コード",
        "全期間高値圏引け率","全期間高値圏引け回数","全期間高値圏引け判定日数",
        "直近10日高値圏引け率","直近10日高値圏引け回数","直近10日高値圏引け判定日数"
    ]
    if not codes or not table_exists(conn, "price_history"):
        return pd.DataFrame(columns=out_cols)

    have = columns(conn, "price_history")
    if not {"コード","日付","始値","高値","安値","終値"}.issubset(have):
        return pd.DataFrame(columns=out_cols)

    parts=[]
    for i in range(0,len(codes),400):
        part=codes[i:i+400]
        ph=",".join("?" for _ in part)
        sql=f"""
        WITH x AS (
          SELECT
            CAST(コード AS TEXT) AS コード,
            日付,
            CAST(始値 AS REAL) AS 始値,
            CAST(高値 AS REAL) AS 高値,
            CAST(安値 AS REAL) AS 安値,
            CAST(終値 AS REAL) AS 終値
          FROM price_history
          WHERE CAST(コード AS TEXT) IN ({ph})
            AND 始値 IS NOT NULL
            AND 高値 IS NOT NULL
            AND 安値 IS NOT NULL
            AND 終値 IS NOT NULL
        ),
        y AS (
          SELECT
            コード,
            日付,
            CASE
              WHEN 高値 > 安値
               AND ((終値-安値)/(高値-安値)) >= ?
               AND 終値 >= 始値
              THEN 1 ELSE 0
            END AS 強い高値圏引け,
            ROW_NUMBER() OVER(
              PARTITION BY コード ORDER BY date(日付) DESC
            ) AS rn
          FROM x
          WHERE 始値>0 AND 高値>0 AND 安値>0 AND 終値>0
        )
        SELECT
          コード,
          ROUND(100.0*SUM(強い高値圏引け)/COUNT(*),2) AS 全期間高値圏引け率,
          SUM(強い高値圏引け) AS 全期間高値圏引け回数,
          COUNT(*) AS 全期間高値圏引け判定日数,
          ROUND(
            100.0*SUM(CASE WHEN rn<=10 THEN 強い高値圏引け ELSE 0 END)/
            NULLIF(SUM(CASE WHEN rn<=10 THEN 1 ELSE 0 END),0),2
          ) AS 直近10日高値圏引け率,
          SUM(CASE WHEN rn<=10 THEN 強い高値圏引け ELSE 0 END) AS 直近10日高値圏引け回数,
          SUM(CASE WHEN rn<=10 THEN 1 ELSE 0 END) AS 直近10日高値圏引け判定日数
        FROM y
        GROUP BY コード
        """
        params=list(part)+[float(upper_zone)]
        parts.append(pd.read_sql_query(sql,conn,params=params))

    if not parts:
        return pd.DataFrame(columns=out_cols)
    df=pd.concat(parts,ignore_index=True)
    if df.empty:
        return pd.DataFrame(columns=out_cols)
    df["コード"]=df["コード"].map(normalize_code)
    return df.drop_duplicates("コード",keep="last")

def fetch_chart_flags(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    if not codes or not table_exists(conn, "chart_flags"):
        return pd.DataFrame()
    have = columns(conn, "chart_flags")
    wanted = ["コード", "GCフラグ", "三役好転フラグ", "5日線上", "25日線上", "75日線上", "作成日時"]
    actual = [c for c in wanted if c in have]
    if "コード" not in actual:
        return pd.DataFrame()
    cols_sql = ",".join(f'"{c}"' for c in actual)
    parts = []
    for i in range(0, len(codes), 700):
        part = codes[i:i+700]
        ph = ",".join("?" for _ in part)
        parts.append(pd.read_sql_query(
            f'SELECT {cols_sql} FROM chart_flags WHERE コード IN ({ph})',
            conn, params=part,
        ))
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not df.empty:
        df["コード"] = df["コード"].map(normalize_code)
        df = df.drop_duplicates("コード", keep="last")
    return df


# ============================================================
# スコア
# ============================================================

def score_strength(current_ret, high_dd, ma5_gap, ma25_gap, ret5, ret10, d1=None, d3=None, d5=None) -> float:
    """決算後の「右肩上がりの継続性」を重視した強さ。単発急騰だけでは満点にしない。"""
    cr=n0(current_ret); hd=n0(high_dd,-99); m5=n0(ma5_gap); m25=n0(ma25_gap)
    r5=n0(ret5); r10=n0(ret10); q1=n0(d1); q3=n0(d3); q5=n0(d5)

    # 1) 現在の上昇水準 0～32
    score=min(max(cr,0.0),40.0)*0.80

    # 2) 高値維持 0～20
    if hd>=-1: score+=20
    elif hd>=-3: score+=16
    elif hd>=-5: score+=12
    elif hd>=-8: score+=6

    # 3) D1→D3→D5→現在の継続性 0～24
    if q3>=q1: score+=6
    if q5>=q3: score+=7
    if cr>=q5: score+=8
    if q5>0 and cr>0: score+=3

    # 4) 現在もMA上 0～12
    if m5>=0: score+=min(5.0,2.0+m5*0.30)
    if m25>=0: score+=min(7.0,3.0+m25*0.16)

    # 5) 足元の継続 0～12
    if r5>0: score+=min(6.0,r5*0.55)
    if r10>0: score+=min(6.0,r10*0.28)

    # 一発高後にかなり崩れた場合は明確に減点
    if hd < -8: score -= min(18.0, abs(hd+8)*1.2)
    return round(clamp(score),1)

def score_overheat(current_ret, ma5_gap, ma25_gap, ret5, ret10) -> float:
    """0=穏やか、100=かなり伸び切り。"""
    cr = max(n0(current_ret), 0)
    m5 = max(n0(ma5_gap), 0)
    m25 = max(n0(ma25_gap), 0)
    r5 = max(n0(ret5), 0)
    r10 = max(n0(ret10), 0)

    score = 0.0
    score += clamp((cr - 10) * 1.6, 0, 40)
    score += clamp((m5 - 3) * 3.2, 0, 25)
    score += clamp((m25 - 8) * 1.6, 0, 25)
    score += clamp((r5 - 7) * 1.5, 0, 10)
    score += clamp((r10 - 15) * 0.5, 0, 10)
    return round(clamp(score), 1)


def score_entry(current_ret, high_dd, ma5_gap, ma25_gap, ret5, ret10,
                d1, d3, d5, post_days, overheat) -> float:
    """
    今から入りやすいか。
    旧ロジックの条件評価は維持しつつ、最後に再スケーリングして100点飽和を抑える。

    旧raw → 新スコア:
      new = raw * 1.10 - 21
    これにより「100点=かなり稀」にし、85点以上を今から候補の目安にする。
    """
    cr = n0(current_ret)
    hd = n0(high_dd, -99)
    m5 = n0(ma5_gap)
    m25 = n0(ma25_gap)
    r5 = n0(ret5)
    pdays = int(post_days or 0)

    raw = 35.0

    if hd >= -1:
        raw += 18
    elif hd >= -2:
        raw += 15
    elif hd >= -4:
        raw += 11
    elif hd >= -7:
        raw += 5
    else:
        raw -= 8

    if 0 <= m5 <= 2:
        raw += 15
    elif m5 <= 4:
        raw += 12
    elif m5 <= 6:
        raw += 7
    elif m5 <= 9:
        raw += 1
    else:
        raw -= 10

    if 0 <= m25 <= 5:
        raw += 12
    elif m25 <= 10:
        raw += 10
    elif m25 <= 15:
        raw += 5
    elif m25 <= 20:
        raw += 0
    else:
        raw -= 10

    if 5 <= cr <= 15:
        raw += 12
    elif cr <= 25:
        raw += 8
    elif cr <= 35:
        raw += 1
    else:
        raw -= 12

    if -2 <= r5 <= 5:
        raw += 9
    elif r5 <= 10:
        raw += 5
    elif r5 <= 15:
        raw += 0
    else:
        raw -= 8

    if d1 is not None and d1 < 0:
        later = d5 if d5 is not None else d3
        if later is not None and later > d1:
            raw += min(14.0, max(0.0, later - d1) * 0.75)

    if d1 is not None and d1 > 0 and d5 is not None and d5 >= d1:
        raw += min(8.0, (d5 - d1) * 0.4 + 3)

    if 3 <= pdays <= 15:
        raw += 5
    elif pdays <= 25:
        raw += 2
    elif pdays >= 35:
        raw -= 3

    raw -= n0(overheat) * 0.28

    # 飽和抑制。raw 110前後でようやく100点に届く。
    scaled = raw * 1.10 - 21.0
    return round(clamp(scaled), 1)


def score_short_low_risk(current_ret, high_dd, ma5_gap, ret5,
                         entry_score, strength_score, overheat, stretch_flag) -> tuple[float, int]:
    """
    「短期で取りに行けるが、今からの下落リスクはなるべく小さい」を0～100点化。

    重要: 信用倍率・買い残・出来高・機関空売りなどの需給情報は一切使わない。
    価格反応だけで、以下を合成する。
      - 今からスコア 30点: 伸び切りすぎていない入りやすさ
      - 強さスコア   20点: 決算後の実際の上昇継続力
      - 高値維持     20点: 決算後高値から崩れていないか
      - 低過熱       15点: 過熱していないほど加点
      - MA5位置      10点: MA5付近で追いかけ過ぎていないか
      - 直近5日       5点: 急落でも急騰でもない穏やかな足元

    🛡️短期低リスクの合格ゲートは厳しめ:
      score >= 85 / 決算前比 >= +2% / 高値から -4%以内 /
      MA5乖離 -1.5～+5% / 過熱度 <=45 / 直近5日 -3～+10% / 伸び切りでない。
    """
    cr = n0(current_ret, -99)
    hd = n0(high_dd, -99)
    m5 = n0(ma5_gap, 99)
    r5 = n0(ret5, 99)
    entry = clamp(n0(entry_score), 0, 100)
    strength = clamp(n0(strength_score), 0, 100)
    heat = clamp(n0(overheat, 100), 0, 100)

    score = entry * 0.30 + strength * 0.20

    # 高値維持 0～20
    if hd >= -1:
        score += 20
    elif hd >= -2:
        score += 18
    elif hd >= -3:
        score += 15
    elif hd >= -4:
        score += 12
    elif hd >= -6:
        score += 6

    # 過熱回避 0～15
    score += 15.0 * (1.0 - heat / 100.0)

    # MA5との距離 0～10。少しの押しは許すが、上に離れすぎるほど減点。
    if 0 <= m5 <= 3:
        score += 10
    elif (-1 <= m5 < 0) or (3 < m5 <= 5):
        score += 8
    elif (-2 <= m5 < -1) or (5 < m5 <= 7):
        score += 5

    # 直近5日 0～5。短期急騰を追わず、崩れも避ける。
    if -1 <= r5 <= 5:
        score += 5
    elif -3 <= r5 <= 8:
        score += 3
    elif -5 <= r5 <= 12:
        score += 1

    score = round(clamp(score), 1)
    flag = int(
        score >= 85
        and cr >= 2
        and hd >= -4
        and -1.5 <= m5 <= 5
        and heat <= 45
        and -3 <= r5 <= 10
        and int(stretch_flag or 0) == 0
    )
    return score, flag



def score_range_low_risk(short_low_risk_score, short_low_risk_flag,
                         post_max_up, max_up_day, ret60, max_dd120,
                         current_ret, high_dd, overheat, stretch_flag) -> tuple[float, int]:
    """
    🚀「値幅×低リスク」を0～100点化。

    前提は🛡️短期低リスク合格。信用・出来高・機関空売りは使わない。
    「大きく動ける」だけでなく、「その割に下へ崩れにくい」を評価する。

      - 決算後最大上昇幅 25点 : 今回の決算後に実際に出せた上方向の値幅
      - 最大1日上昇率   10点 : 瞬発力。ただし20%超の一発急騰は3点減点
      - 60日上昇実績     5点 : 中期にも上方向へ動けるか
      - 短期低リスク質  20点 : 現在位置の入りやすさ・強さ・非過熱
      - 120日最大DD     20点 : 下方向の深さ。ここを強く重視
      - 高値維持        10点 : 決算後高値から崩れていないか
      - 低過熱          10点 : 追いかけ過ぎを避ける

    合格ゲート:
      🛡️合格 / score>=70 / 決算後最大上昇>=8% / 最大1日上昇>=7% /
      120日最大DD>=-20% / 高値から-3%以内 / 過熱<=35 / 伸び切りでない。

    注意: 「最大1日上昇率」は値幅能力の補助指標であり、20%超の一発依存はむしろ減点。
    """
    low = clamp(n0(short_low_risk_score), 0, 100)
    post = max(0.0, n0(post_max_up))
    day = max(0.0, n0(max_up_day))
    r60 = max(0.0, n0(ret60))
    dd = n0(max_dd120, -99)
    hd = n0(high_dd, -99)
    heat = clamp(n0(overheat, 100), 0, 100)

    score = 0.0

    # 上方向の値幅 0～40
    score += min(25.0, post * 1.60)
    score += min(10.0, day * 0.70)
    if day > 20:
        score -= 3.0  # 一日だけの極端なジャンプはリスクとして扱う
    score += min(5.0, r60 * 0.18)

    # 現在の低リスク品質 0～20
    score += low * 0.20

    # 120日最大DD耐性 0～20
    if dd >= -8:
        score += 20
    elif dd >= -12:
        score += 17
    elif dd >= -15:
        score += 14
    elif dd >= -20:
        score += 8
    elif dd >= -25:
        score += 3

    # 決算後高値維持 0～10
    if hd >= -0.5:
        score += 10
    elif hd >= -1:
        score += 9
    elif hd >= -2:
        score += 7
    elif hd >= -3:
        score += 5

    # 低過熱 0～10
    score += 10.0 * (1.0 - heat / 100.0)

    score = round(clamp(score), 1)
    flag = int(
        int(short_low_risk_flag or 0) == 1
        and score >= 70
        and post >= 8
        and day >= 7
        and dd >= -20
        and hd >= -3
        and heat <= 35
        and int(stretch_flag or 0) == 0
    )
    return score, flag


def detect_styles(state, entry_score, strength_score, overheat, d1, d3, d5,
                  current_ret, high_dd, ma5_gap, ma25_gap, ret5,
                  stretch_flag) -> list[str]:
    """タイプは排他的にしない。該当するタグをすべて返す。"""
    tags: list[str] = []

    # 今から見やすい位置。100点=上昇確率100%ではなく、条件フィット度の上限。
    if entry_score >= 75:
        tags.append("🎯 今から候補")

    # 決算初日は売られたが、その後プラス圏・高値圏へ戻った。
    if d1 is not None and d1 < 0 and current_ret >= 5 and high_dd >= -4:
        tags.append("🔄 再評価")

    # 既に上昇しているが、MA5の近くで直近5日が加速しすぎていない。
    if current_ret >= 8 and high_dd >= -3 and 0 <= ma5_gap <= 4 and -2 <= ret5 <= 6:
        tags.append("🟢 高値圏の押し目")

    # 決算初動が強く、その後も決算前比プラス・高値圏を維持。
    if state == "S 継続上昇":
        tags.append("🚀 継続上昇")

    # 強さスコアによる純粋なトレンド強者。
    if strength_score >= 70:
        tags.append("💪 強い")

    # 伸び切っていても削除しない。別タグとして残す。
    if stretch_flag == 1:
        tags.append("🔥 伸び切り")

    if not tags:
        tags.append("✅ 上昇維持")

    return tags


def make_summary(row: dict) -> str:
    d1, d3, d5 = row.get("D1"), row.get("D3"), row.get("D5")
    cr = n0(row.get("決算後騰落率"))
    hd = n0(row.get("高値から乖離率"), -99)
    m5 = n0(row.get("MA5乖離率"))
    m25 = n0(row.get("MA25乖離率"))
    r5 = n0(row.get("直近5日騰落率"))
    entry = n0(row.get("今からスコア"))
    strength = n0(row.get("強さスコア"))
    heat = n0(row.get("過熱度"))

    parts = []
    stable = n0(row.get("安定上昇スコア"))
    if stable >= 70:
        parts.append(f"安定上昇{stable:.0f}点")
    if d1 is not None and d1 < 0:
        if d5 is not None and d5 > 0:
            parts.append(f"初日{d1:+.1f}%からD5{d5:+.1f}%へ再評価")
        elif d3 is not None and d3 > d1:
            parts.append(f"初日{d1:+.1f}%からD3{d3:+.1f}%へ改善")
        else:
            parts.append(f"初日{d1:+.1f}%安から現在は切り返し")
    elif d1 is not None and d1 >= 3:
        if d5 is not None:
            parts.append(f"D1{d1:+.1f}%→D5{d5:+.1f}%の継続型")
        else:
            parts.append(f"D1{d1:+.1f}%の強い初動")

    parts.append(f"現在は決算前比{cr:+.1f}%")
    if hd >= -1:
        parts.append("決算後高値ほぼ維持")
    elif hd >= -3:
        parts.append(f"高値から{hd:.1f}%で高値圏")
    elif hd >= -5:
        parts.append(f"高値から{hd:.1f}%の軽い調整")
    else:
        parts.append(f"高値から{hd:.1f}%")

    if 0 <= m5 <= 2:
        parts.append(f"MA5乖離{m5:+.1f}%で入りやすい位置")
    elif m5 >= 8:
        parts.append(f"MA5乖離{m5:+.1f}%で短期過熱")
    else:
        parts.append(f"MA5乖離{m5:+.1f}%")

    if m25 >= 20:
        parts.append(f"MA25乖離{m25:+.1f}%で伸び切り注意")
    if r5 < -2:
        parts.append(f"直近5日{r5:+.1f}%で調整中")
    elif r5 > 12:
        parts.append(f"直近5日{r5:+.1f}%で加速過熱")

    if heat >= 65:
        parts.append(f"強さ{strength:.0f}点だが過熱{heat:.0f}点")
    elif entry >= 75:
        parts.append(f"今からスコア{entry:.0f}点")
    else:
        parts.append(f"今から{entry:.0f}点 / 強さ{strength:.0f}点")
    return "。".join(parts) + "。"


# ============================================================
# 分析
# ============================================================

def analyze(earnings, prices, screen, min_return, max_drawdown):
    if earnings.empty or prices.empty:
        return pd.DataFrame()

    screen_map = screen.set_index("コード").to_dict("index") if not screen.empty else {}
    price_groups = {
        code: g.sort_values("日付_dt").reset_index(drop=True)
        for code, g in prices.groupby("コード", sort=False)
    }
    rows = []

    for e in earnings.itertuples(index=False):
        code = normalize_code(getattr(e, "コード"))
        g = price_groups.get(code)
        if g is None or g.empty:
            continue

        closes = g["終値"].dropna()
        if closes.empty:
            continue

        current = float(closes.iloc[-1])
        ma5 = float(closes.tail(5).mean()) if len(closes) >= 5 else None
        ma25 = float(closes.tail(25).mean()) if len(closes) >= 25 else None

        d1_date = pd.Timestamp(getattr(e, "D1日付_dt")).normalize()
        post = g[g["日付_dt"] >= d1_date].copy()
        if post.empty:
            continue

        post_high = float(post["高値"].fillna(post["終値"]).max())
        post_low = float(post["安値"].fillna(post["終値"]).min())
        post_days = int(post["日付_dt"].nunique())
        base = float(getattr(e, "基準終値"))

        current_ret = pct(current, base)
        high_dd = pct(current, post_high)
        low_rebound = pct(current, post_low)
        ma5_gap = pct(current, ma5) if ma5 else None
        ma25_gap = pct(current, ma25) if ma25 else None

        def val(name):
            x = getattr(e, name)
            return None if pd.isna(x) else float(x)

        d1 = val("D1終値騰落率")
        d3 = val("D3終値騰落率")
        d5 = val("D5終値騰落率")

        # 営業日不足ならD3/D5は「未来がまだ無い」ので無効化。
        d3_valid = post_days >= 3
        d5_valid = post_days >= 5
        if not d3_valid:
            d3 = None
        if not d5_valid:
            d5 = None

        above5 = ma5 is None or current >= ma5
        above25 = ma25 is None or current >= ma25

        state = "C 除外"
        if d1 is not None and d1 < 0 and current_ret >= 5 and high_dd >= -5 and above5 and above25:
            state = "S 再評価型"
        elif d1 is not None and d1 >= 3 and current_ret >= 8 and high_dd >= -5 and above5 and above25:
            state = "S 継続上昇"
        elif current_ret >= 5 and high_dd >= -max_drawdown and above5 and above25:
            state = "A 強い"
        elif current_ret >= min_return and high_dd >= -max_drawdown and above5 and above25:
            state = "B 上昇維持"

        if state == "C 除外" or current_ret < min_return or high_dd < -max_drawdown:
            continue

        ret5 = pct(current, float(closes.iloc[-6])) if len(closes) >= 6 else None
        ret10 = pct(current, float(closes.iloc[-11])) if len(closes) >= 11 else None
        srow = screen_map.get(code, {})
        name = srow.get("銘柄名_s") or getattr(e, "銘柄名") or ""

        row = {
            "コード": code,
            "銘柄名": name,
            "発表日時": getattr(e, "発表日時"),
            "決算種別": getattr(e, "決算種別"),
            "決算前終値": round(base, 1),
            "現在値": round(current, 1),
            "決算後騰落率": round(current_ret, 2),
            "D1": round(d1, 2) if d1 is not None else None,
            "D3": round(d3, 2) if d3 is not None else None,
            "D5": round(d5, 2) if d5 is not None else None,
            "D3有効": 1 if d3_valid else 0,
            "D5有効": 1 if d5_valid else 0,
            "決算後最高値": round(post_high, 1),
            "高値から乖離率": round(high_dd, 2),
            "決算後最安値": round(post_low, 1),
            "安値から反発率": round(low_rebound, 2),
            "決算後営業日数": post_days,
            "MA5": round(ma5, 1) if ma5 is not None else None,
            "MA5乖離率": round(ma5_gap, 2) if ma5_gap is not None else None,
            "MA25": round(ma25, 1) if ma25 is not None else None,
            "MA25乖離率": round(ma25_gap, 2) if ma25_gap is not None else None,
            "直近5日騰落率": round(ret5, 2) if ret5 is not None else None,
            "直近10日騰落率": round(ret10, 2) if ret10 is not None else None,
            "決算後状態": state,
        }

        row["強さスコア"] = score_strength(
            row["決算後騰落率"], row["高値から乖離率"], row["MA5乖離率"],
            row["MA25乖離率"], row["直近5日騰落率"], row["直近10日騰落率"],
        )
        row["過熱度"] = score_overheat(
            row["決算後騰落率"], row["MA5乖離率"], row["MA25乖離率"],
            row["直近5日騰落率"], row["直近10日騰落率"],
        )
        row["今からスコア"] = score_entry(
            row["決算後騰落率"], row["高値から乖離率"], row["MA5乖離率"],
            row["MA25乖離率"], row["直近5日騰落率"], row["直近10日騰落率"],
            row["D1"], row["D3"], row["D5"], row["決算後営業日数"], row["過熱度"],
        )
        row["伸び切りフラグ"] = 1 if (
            row["過熱度"] >= 60
            or row["決算後騰落率"] >= 30
            or n0(row["MA5乖離率"]) >= 8
            or n0(row["MA25乖離率"]) >= 20
            or n0(row["直近5日騰落率"]) >= 15
        ) else 0
        row["タイプ一覧"] = detect_styles(
            state=row["決算後状態"],
            entry_score=row["今からスコア"], strength_score=row["強さスコア"],
            overheat=row["過熱度"], d1=row["D1"], d3=row["D3"], d5=row["D5"],
            current_ret=row["決算後騰落率"], high_dd=row["高値から乖離率"],
            ma5_gap=n0(row["MA5乖離率"]), ma25_gap=n0(row["MA25乖離率"]),
            ret5=n0(row["直近5日騰落率"]), stretch_flag=row["伸び切りフラグ"],
        )
        # CSVでも見やすいように「｜」区切り文字列も持つ。
        row["タイプ"] = "｜".join(row.pop("タイプ一覧"))
        row["サマリー"] = make_summary(row)
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # 初期表示は「今から」優先。同点なら低過熱→高値に近い→強さで並べる。
    return out.sort_values(
        ["今からスコア", "過熱度", "高値から乖離率", "強さスコア", "決算後騰落率"],
        ascending=[False, True, False, False, False],
    ).reset_index(drop=True)


# ============================================================
# HTMLダッシュボード
# ============================================================

def js_safe_records(df: pd.DataFrame) -> list[dict]:
    clean = df.copy().where(pd.notna(df), None)
    records = clean.to_dict("records")
    out = []
    for r in records:
        d = {}
        for k, v in r.items():
            if hasattr(v, "item"):
                try:
                    v = v.item()
                except Exception:
                    pass
            d[str(k)] = v
        out.append(d)
    return out


def generate_dashboard(df: pd.DataFrame, out_path: Path,
                       days: int, min_return: float, max_drawdown: float) -> None:
    records = js_safe_records(df)
    data_json = json.dumps(records, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    template = r'''<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>決算後上昇ダッシュボード</title>
<style>
:root{--bg:#f7f8fb;--card:#fff;--line:#e5e7eb;--text:#111827;--muted:#6b7280;--good:#047857;--hot:#b91c1c;--blue:#1d4ed8}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI","Noto Sans JP",sans-serif}
.wrap{max-width:1900px;margin:0 auto;padding:16px} h1{font-size:22px;margin:0 0 4px}.sub{color:var(--muted);font-size:12px;margin-bottom:14px}
.cards{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:10px;margin-bottom:12px}.card,.panel{background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,.03)}
.card{padding:12px}.card .label{font-size:12px;color:var(--muted)}.card .value{font-size:24px;font-weight:800;margin-top:2px}.panel{padding:12px;margin-bottom:12px}.panel h2{font-size:15px;margin:0 0 10px}
.controls{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.guide{font-size:12px;line-height:1.65;color:#374151}.guide b{color:#111827}.guide-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 18px}.tag-wrap{display:flex;gap:4px;flex-wrap:wrap}input,select,button{height:34px;border:1px solid var(--line);border-radius:8px;padding:0 10px;background:#fff;color:var(--text)}button{cursor:pointer;font-weight:700}button.active{background:#111827;color:#fff}.quick{display:flex;gap:6px;flex-wrap:wrap}
.summary-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.summary-item{border:1px solid var(--line);border-radius:10px;padding:10px;background:#fff}.summary-item .name{font-weight:800}.summary-item .meta{font-size:12px;color:var(--muted);margin:3px 0 6px}.summary-item .txt{font-size:12px;line-height:1.55}
.badge{display:inline-block;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:800;margin-right:4px}.badge-now{background:#dcfce7;color:#166534}.badge-hot{background:#fee2e2;color:#991b1b}.badge-rev{background:#ede9fe;color:#5b21b6}.badge-strong{background:#dbeafe;color:#1e40af}
.table-wrap{overflow:auto;max-height:72vh;border:1px solid var(--line);border-radius:10px}table{width:max-content;min-width:100%;border-collapse:separate;border-spacing:0;font-size:12px}th,td{padding:7px 8px;border-bottom:1px solid var(--line);border-right:1px solid #f1f5f9;white-space:nowrap;background:#fff}th{position:sticky;top:0;z-index:3;background:#f9fafb;text-align:left}tr:hover td{background:#f8fafc}td.summary-cell{white-space:normal;min-width:440px;max-width:620px;line-height:1.45}.num{text-align:right;font-variant-numeric:tabular-nums}.pos{color:#b91c1c;font-weight:700}.neg{color:#1d4ed8;font-weight:700}.score-high{color:#047857;font-weight:900}.score-mid{color:#b45309;font-weight:800}.heat-high{color:#b91c1c;font-weight:900}
@media(max-width:1050px){.cards{grid-template-columns:repeat(3,1fr)}.summary-grid{grid-template-columns:1fr}.guide-grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<h1>決算後上昇ダッシュボード</h1><div class="sub">生成: __GENERATED__ / 直近__DAYS__日 / 最低上昇率 __MINRET__% / 高値から最大 __MAXDD__% まで許容</div>
<div class="cards">
<div class="card"><div class="label">全候補</div><div id="cAll" class="value">-</div></div><div class="card"><div class="label">今から75点以上</div><div id="cNow" class="value">-</div></div><div class="card"><div class="label">再評価型</div><div id="cRev" class="value">-</div></div><div class="card"><div class="label">伸び切り強者</div><div id="cHot" class="value">-</div></div><div class="card"><div class="label">強さ70点以上</div><div id="cStrong" class="value">-</div></div><div class="card"><div class="label">表示中</div><div id="cVisible" class="value">-</div></div>
</div>
<div class="panel"><h2>見方（タイプと並び順）</h2>
<div class="guide guide-grid">
  <div><b>🎯 今から候補</b>：今からスコア75点以上。高値圏・MA5/25との距離・上がり過ぎ・直近5日・決算後の経過日数などから「今の位置」を評価。</div>
  <div><b>🔄 再評価</b>：D1がマイナスなのに、現在は決算前比+5%以上かつ決算後高値から-4%以内まで回復。</div>
  <div><b>🟢 高値圏の押し目</b>：決算後+8%以上、高値から-3%以内、MA5乖離0～+4%、直近5日-2～+6%。強いのに短期的には離れ過ぎていない。</div>
  <div><b>🚀 継続上昇</b>：D1から強く、その後も高値圏を維持しているS継続上昇型。</div>
  <div><b>💪 強い</b>：強さスコア70点以上。上昇幅・高値維持・MA上方・直近5/10日の勢いを評価し、伸び切りは減点しない。</div>
  <div><b>🔥 伸び切り</b>：過熱60点以上、決算後+30%以上、MA5+8%以上、MA25+20%以上、直近5日+15%以上のどれか。強いので削除せず警告タグとして残す。</div>
  <div><b>今から向き順</b>：今からスコア高い順。同点は「過熱が低い → 決算後高値に近い → 強さが高い」順。</div>
  <div><b>トレンド強さ順</b>：強さスコア高い順。今から買いやすいかではなく、現在どれだけ強いか。</div>
  <div><b>伸び切り順</b>：過熱度が高い順。上位ほど強い一方、追いかけリスクも大きい。</div>
  <div><b>高値に近い順</b>：決算後最高値からの乖離が0%に近い順。<br><span class="small">※スコア100点は上昇確率100%ではなく、このルールへの適合度が上限に達した意味。</span></div>
</div></div>
<div class="panel"><h2>絞り込み</h2><div class="controls">
<input id="q" type="text" placeholder="コード・銘柄名・タイプ・サマリー検索" style="width:280px"><select id="typeFilter"><option value="">全タイプ</option></select>
<label>今から ≥ <input id="minEntry" type="number" value="0" min="0" max="100" style="width:72px"></label><label>強さ ≥ <input id="minStrength" type="number" value="0" min="0" max="100" style="width:72px"></label><label>過熱 ≤ <input id="maxHeat" type="number" value="100" min="0" max="100" style="width:72px"></label>
<select id="sortBy"><option value="今からスコア">今から向き順</option><option value="強さスコア">トレンド強さ順</option><option value="安定上昇スコア">安定上昇順</option><option value="過熱度">伸び切り順</option><option value="決算後騰落率">決算後上昇率順</option><option value="高値から乖離率">決算後高値に近い順</option></select>
<div class="quick"><button data-mode="all" class="active">全部</button><button data-mode="now">🎯 今から候補</button><button data-mode="rev">🔄 再評価</button><button data-mode="pullback">🟢 高値圏の押し目</button><button data-mode="cont">🚀 継続上昇</button><button data-mode="hot">🔥 伸び切り</button><button data-mode="strong">💪 強い</button></div>
</div></div>
<div class="panel"><h2>上位サマリー</h2><div id="summaryGrid" class="summary-grid"></div></div>
<div class="panel"><h2>全候補</h2><div class="table-wrap"><table><thead><tr id="thead"></tr></thead><tbody id="tbody"></tbody></table></div></div>
</div>
<script>
const DATA=__DATA_JSON__;
const COLS=["コード","銘柄名","発表日時","決算種別","今からスコア","強さスコア","過熱度","タイプ","決算後騰落率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近5日騰落率","直近10日騰落率","決算後営業日数","伸び切りフラグ","GCフラグ","三役好転フラグ","サマリー"];
const NUMS=new Set(["今からスコア","強さスコア","過熱度","決算後騰落率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近5日騰落率","直近10日騰落率","決算後営業日数","伸び切りフラグ","GCフラグ","三役好転フラグ"]);
let mode="all";
function vnum(v){const n=Number(v);return Number.isFinite(n)?n:null}
function esc(s){return String(s??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;")}
function fmt(v,c){if(v===null||v===undefined||v==="")return "";if(NUMS.has(c)){const n=Number(v);if(!Number.isFinite(n))return esc(v);if(["GCフラグ","三役好転フラグ","決算後営業日数"].includes(c))return String(Math.round(n));return n.toFixed(1)}return esc(v)}
function cls(v,c){const n=Number(v);if(c==="今からスコア"){if(n>=75)return"score-high";if(n>=60)return"score-mid"}if(c==="強さスコア"){if(n>=80)return"score-high";if(n>=65)return"score-mid"}if(c==="過熱度"&&n>=65)return"heat-high";if(["決算後騰落率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近5日騰落率","直近10日騰落率"].includes(c)){if(n>0)return"pos";if(n<0)return"neg"}return""}
function splitTypes(r){return String(r["タイプ"]||"").split("｜").map(x=>x.trim()).filter(Boolean)}
function hasType(r,needle){return splitTypes(r).some(t=>t.includes(needle))}
function badgeOne(t){t=String(t||"");if(t.includes("今から")||t.includes("押し目"))return`<span class="badge badge-now">${esc(t)}</span>`;if(t.includes("伸び切り"))return`<span class="badge badge-hot">${esc(t)}</span>`;if(t.includes("再評価"))return`<span class="badge badge-rev">${esc(t)}</span>`;if(t.includes("継続")||t.includes("安定上昇")||t.includes("強い"))return`<span class="badge badge-strong">${esc(t)}</span>`;return`<span class="badge">${esc(t)}</span>`}
function badges(r){return `<span class="tag-wrap">${splitTypes(r).map(badgeOne).join("")}</span>`}
function populateTypes(){const s=document.getElementById("typeFilter");const types=[...new Set(DATA.flatMap(r=>splitTypes(r)))].sort();types.forEach(t=>{const o=document.createElement("option");o.value=t;o.textContent=t;s.appendChild(o)})}
function filterRow(r){const q=(document.getElementById("q").value||"").trim().toLowerCase();const tf=document.getElementById("typeFilter").value;const me=Number(document.getElementById("minEntry").value||0);const ms=Number(document.getElementById("minStrength").value||0);const mh=Number(document.getElementById("maxHeat").value||100);if(q&&!([r["コード"],r["銘柄名"],r["タイプ"],r["サマリー"]].join(" ").toLowerCase().includes(q)))return false;if(tf&&!splitTypes(r).includes(tf))return false;if((vnum(r["今からスコア"])??0)<me)return false;if((vnum(r["強さスコア"])??0)<ms)return false;if((vnum(r["過熱度"])??0)>mh)return false;if(mode==="now"&&!hasType(r,"今から候補"))return false;if(mode==="rev"&&!hasType(r,"再評価"))return false;if(mode==="pullback"&&!hasType(r,"高値圏の押し目"))return false;if(mode==="cont"&&!hasType(r,"継続上昇"))return false;if(mode==="stable"&&!hasType(r,"安定上昇"))return false;if(mode==="hot"&&!hasType(r,"伸び切り"))return false;if(mode==="strong"&&!hasType(r,"強い"))return false;return true}
function cmpNumDesc(a,b,k){const av=vnum(a[k]),bv=vnum(b[k]);if(av===null&&bv===null)return 0;if(av===null)return 1;if(bv===null)return-1;return bv-av}
function sortRows(rows){const k=document.getElementById("sortBy").value;return rows.sort((a,b)=>{if(k==="今からスコア"){return cmpNumDesc(a,b,"今からスコア") || ((vnum(a["過熱度"])??999)-(vnum(b["過熱度"])??999)) || cmpNumDesc(a,b,"高値から乖離率") || cmpNumDesc(a,b,"強さスコア")}return cmpNumDesc(a,b,k)})}
function renderSummary(rows){const h=document.getElementById("summaryGrid");h.innerHTML="";rows.slice(0,9).forEach(r=>{const d=document.createElement("div");d.className="summary-item";d.innerHTML=`<div class="name">${esc(r["コード"])} ${esc(r["銘柄名"])}</div><div class="meta">${badges(r)} 今から <b>${fmt(r["今からスコア"],"今からスコア")}</b> / 強さ <b>${fmt(r["強さスコア"],"強さスコア")}</b> / 過熱 <b>${fmt(r["過熱度"],"過熱度")}</b></div><div class="txt">${esc(r["サマリー"])}</div>`;h.appendChild(d)})}
function renderTable(rows){const head=document.getElementById("thead");if(!head.childElementCount){COLS.forEach(c=>{const th=document.createElement("th");th.textContent=c;head.appendChild(th)})}const b=document.getElementById("tbody");b.innerHTML="";const f=document.createDocumentFragment();rows.forEach(r=>{const tr=document.createElement("tr");COLS.forEach(c=>{const td=document.createElement("td");td.innerHTML=c==="タイプ"?badges(r):fmt(r[c],c);if(NUMS.has(c))td.classList.add("num");const cc=cls(r[c],c);if(cc)td.classList.add(cc);if(c==="サマリー")td.classList.add("summary-cell");tr.appendChild(td)});f.appendChild(tr)});b.appendChild(f)}
function updateCards(rows){document.getElementById("cAll").textContent=DATA.length;document.getElementById("cNow").textContent=DATA.filter(r=>hasType(r,"今から候補")).length;document.getElementById("cRev").textContent=DATA.filter(r=>hasType(r,"再評価")).length;document.getElementById("cHot").textContent=DATA.filter(r=>hasType(r,"伸び切り")).length;document.getElementById("cStrong").textContent=DATA.filter(r=>hasType(r,"強い")).length;document.getElementById("cVisible").textContent=rows.length}
function apply(){let rows=sortRows(DATA.filter(filterRow));updateCards(rows);renderSummary(rows);renderTable(rows)}
document.querySelectorAll("input,select").forEach(el=>{el.addEventListener("input",apply);el.addEventListener("change",apply)});document.querySelectorAll("button[data-mode]").forEach(btn=>btn.addEventListener("click",()=>{mode=btn.dataset.mode;document.querySelectorAll("button[data-mode]").forEach(b=>b.classList.remove("active"));btn.classList.add("active");if(mode==="now"||mode==="rev"||mode==="pullback")document.getElementById("sortBy").value="今からスコア";if(mode==="hot")document.getElementById("sortBy").value="過熱度";if(mode==="strong"||mode==="cont")document.getElementById("sortBy").value="強さスコア";apply()}));populateTypes();apply();
</script></body></html>'''

    text = (template
            .replace("__GENERATED__", html.escape(generated))
            .replace("__DAYS__", str(int(days)))
            .replace("__MINRET__", str(min_return))
            .replace("__MAXDD__", str(max_drawdown))
            .replace("__DATA_JSON__", data_json))
    out_path.write_text(text, encoding="utf-8")


# ============================================================
# メイン
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="決算後上昇 + 今から/強さ/過熱スコア + HTMLダッシュボード")
    ap.add_argument("--db", default=None, help="kani2.db のフルパス")
    ap.add_argument("--days", type=int, default=60, help="直近何日分の決算を見るか")
    ap.add_argument("--price-lookback", type=int, default=120, help="price_historyの参照日数")
    ap.add_argument("--min-return", type=float, default=2.0, help="決算前→現在の最低上昇率(%%)")
    ap.add_argument("--max-drawdown", type=float, default=7.0, help="決算後高値から許容する下落率(%%)")
    ap.add_argument("--out", default="決算後上昇スクリーニング.csv", help="全候補CSV")
    ap.add_argument("--html", default="決算後上昇ダッシュボード.html", help="HTMLダッシュボード")
    ap.add_argument("--top", type=int, default=50, help="コンソール表示件数")
    ap.add_argument("--open", action="store_true", help="生成後にHTMLをブラウザで開く")
    args = ap.parse_args()

    started = datetime.now()
    db = resolve_db_path(args.db)
    log(f"[DB] {db}")
    conn = connect_readonly(db)

    try:
        for t in ("earnings_reaction_labels", "price_history", "screener"):
            if not table_exists(conn, t):
                raise RuntimeError(f"必要テーブルがありません: {t}")

        log(f"[1/5] 直近{args.days}日の決算を取得")
        earnings = fetch_latest_earnings(conn, args.days)
        if earnings.empty:
            log("対象決算なし")
            return
        codes = sorted(earnings["コード"].unique().tolist())
        log(f"      対象={len(codes)}銘柄")

        log("[2/5] screener現在値を取得")
        screen = fetch_screener_current(conn, codes)

        lookback = max(args.price_lookback, args.days + 45)
        start_date = (date.today() - timedelta(days=lookback)).isoformat()
        log(f"[3/5] price_historyを対象コードだけ取得 ({start_date}～)")
        prices = fetch_price_history(conn, codes, start_date)
        prices = append_live_if_needed(prices, screen)
        log(f"      price_history={len(prices):,}行")

        log("[4/5] pandasでトレンド・スコア・サマリー計算")
        result = analyze(earnings, prices, screen, args.min_return, args.max_drawdown)

        # 候補だけchart_flagsを読む。無くても動く。
        if not result.empty:
            flags = fetch_chart_flags(conn, result["コード"].tolist())
            if not flags.empty:
                result = result.merge(flags, on="コード", how="left")
                for idx, r in result.iterrows():
                    extras = []
                    if r.get("GCフラグ") == 1:
                        extras.append("GC")
                    if r.get("三役好転フラグ") == 1:
                        extras.append("三役好転")
                    if extras:
                        result.at[idx, "サマリー"] = str(result.at[idx, "サマリー"]) + " テクニカル: " + "・".join(extras) + "。"

        # 出力先は常に固定フォルダ。--out / --html はファイル名だけ採用する。
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_out = OUTPUT_DIR / Path(args.out).name
        result.to_csv(csv_out, index=False, encoding="utf-8-sig")

        html_out = OUTPUT_DIR / Path(args.html).name
        log("[5/5] HTMLダッシュボード生成")
        generate_dashboard(result, html_out, args.days, args.min_return, args.max_drawdown)

        elapsed = (datetime.now() - started).total_seconds()
        log(f"\n[完了] {len(result)}銘柄 / {elapsed:.2f}秒")
        log(f"[CSV]  {csv_out}")
        log(f"[HTML] {html_out}")

        if result.empty:
            return

        now_n = int((result["今からスコア"] >= 75).sum())
        hot_n = int((result["伸び切りフラグ"] == 1).sum())
        strong_n = int((result["強さスコア"] >= 70).sum())
        rev_n = int(result["タイプ"].astype(str).str.contains("再評価", regex=False).sum())
        log(f"[内訳] 今から75+={now_n} / 再評価={rev_n} / 強さ70+={strong_n} / 伸び切り={hot_n}")

        show = [
            "コード", "銘柄名", "発表日時", "決算種別",
            "今からスコア", "強さスコア", "過熱度", "タイプ",
            "決算後騰落率", "D1", "D3", "D5", "高値から乖離率",
            "MA5乖離率", "MA25乖離率", "直近5日騰落率", "直近10日騰落率", "サマリー",
        ]
        show = [c for c in show if c in result.columns]
        with pd.option_context("display.max_rows", args.top, "display.max_columns", None,
                               "display.width", 280, "display.unicode.east_asian_width", True):
            print(result[show].head(args.top).to_string(index=False))

        if args.open:
            try:
                webbrowser.open(html_out.resolve().as_uri())
            except Exception as e:
                log(f"[WARN] ブラウザ自動起動失敗: {e}")
    finally:
        conn.close()



# ============================================================================
# v7 overrides: 下げ止まり / 信用需給 / ヘルプドロワー / 市場・株価 / コードコピー / 詳細ミニチャート
# ============================================================================
SCREENER_SUPPLY_COLUMNS = [
    "信用倍率", "売り残", "買い残", "需給OH", "需給安全フラグ", "踏み上げ期待スコア",
    "信用買い残_浮動株比率", "信用買い残増減率_5d", "信用買い残増減率_20d",
    "決算前20日買い残増加率", "信用需給負荷スコア", "空売り機関", "短期需給判定",
]
SUPPLY_NUMERIC_COLUMNS = {
    "信用倍率", "売り残", "買い残", "需給OH", "需給安全フラグ", "踏み上げ期待スコア",
    "信用買い残_浮動株比率", "信用買い残増減率_5d", "信用買い残増減率_20d",
    "決算前20日買い残増加率", "信用需給負荷スコア",
}


def fetch_screener_current(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    # 現在値・市場 + 自動スクリーニング側の信用・需給DB列を、存在するものだけ読む。
    if not codes or not table_exists(conn, "screener"):
        return pd.DataFrame(columns=["コード", "銘柄名_s", "市場", "現在値"])
    sc = columns(conn, "screener")
    name_expr = '"銘柄名"' if "銘柄名" in sc else "NULL"
    market_expr = '"市場"' if "市場" in sc else "NULL"
    px_expr = '"現在値"' if "現在値" in sc else "NULL"
    extras = [c for c in SCREENER_SUPPLY_COLUMNS if c in sc]
    select_cols = [
        'CAST("コード" AS TEXT) AS コード',
        f'{name_expr} AS 銘柄名_s',
        f'{market_expr} AS 市場',
        f'{px_expr} AS 現在値',
    ] + [f'"{c}"' for c in extras]
    parts=[]
    for i in range(0,len(codes),700):
        part=codes[i:i+700]
        ph=','.join('?' for _ in part)
        sql=f'SELECT {", ".join(select_cols)} FROM screener WHERE コード IN ({ph})'
        parts.append(pd.read_sql_query(sql,conn,params=part))
    df=pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["コード","銘柄名_s","市場","現在値"])
    df["コード"]=df["コード"].map(normalize_code)
    if "現在値" in df.columns:
        df["現在値"]=to_num(df["現在値"])
    for c in extras:
        if c in SUPPLY_NUMERIC_COLUMNS and c in df.columns:
            df[c]=to_num(df[c])
    return df.drop_duplicates("コード",keep="last")


def fetch_institution_shorts(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    # 自動スクリーニングと同じ institution_short_sales の各銘柄最新日を集計。
    if not codes or not table_exists(conn,"institution_short_sales"):
        return pd.DataFrame()
    have=columns(conn,"institution_short_sales")
    need={"code","calc_date","shares","shares_change","institution_name"}
    if not need.issubset(have):
        return pd.DataFrame()
    parts=[]
    for i in range(0,len(codes),500):
        part=codes[i:i+500]
        ph=','.join('?' for _ in part)
        sql=f'''
            SELECT CAST(s.code AS TEXT) AS コード,
                   s.calc_date AS 空売り更新日,
                   SUM(COALESCE(s.shares,0)) AS 機関空売り合計株数,
                   SUM(COALESCE(s.shares_change,0)) AS 本日の増減合計株数,
                   GROUP_CONCAT(
                       s.institution_name || '(' ||
                       CASE WHEN COALESCE(s.shares_change,0)>0
                            THEN '+' || CAST(s.shares_change AS TEXT)
                            ELSE CAST(COALESCE(s.shares_change,0) AS TEXT) END || '株)',
                       ' / '
                   ) AS 主要機関の動き
            FROM institution_short_sales s
            JOIN (
                SELECT code,MAX(calc_date) AS max_date
                FROM institution_short_sales
                WHERE code IN ({ph})
                GROUP BY code
            ) m ON m.code=s.code AND m.max_date=s.calc_date
            GROUP BY s.code,s.calc_date
        '''
        parts.append(pd.read_sql_query(sql,conn,params=part))
    df=pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()
    if not df.empty:
        df["コード"]=df["コード"].map(normalize_code)
        for c in ("機関空売り合計株数","本日の増減合計株数"):
            if c in df.columns: df[c]=to_num(df[c])
        df=df.drop_duplicates("コード",keep="last")
    return df



def score_stable_uptrend(closes: pd.Series) -> tuple[float, dict]:
    """
    5074型の「ゆったり持ちやすい上昇」を評価。
    最大120営業日を使い、
      - 60/120営業日の上昇
      - MA25/MA75の上向き
      - MA25より上にいる日数割合
      - 20営業日ごとの安値切り上げ
      - 最大ドローダウンの小ささ
      - 一日急騰への依存が小さい
    を合成する。

    単なる決算後継続上昇とは別物。
    「速さ」より「崩れにくく長く上がった質」を見る。
    """
    s = pd.to_numeric(closes, errors="coerce").dropna().tail(120)
    if len(s) < 40:
        return 0.0, {}

    cur = float(s.iloc[-1])
    ret60 = (cur / float(s.iloc[-61]) - 1) * 100 if len(s) >= 61 else None
    ret120 = (cur / float(s.iloc[0]) - 1) * 100 if len(s) >= 100 else None

    ma25 = s.rolling(25).mean()
    ma75 = s.rolling(75).mean()

    # MA25の傾き: 約20営業日前との比較
    ma25_slope = None
    valid25 = ma25.dropna()
    if len(valid25) >= 21:
        ma25_slope = (float(valid25.iloc[-1]) / float(valid25.iloc[-21]) - 1) * 100

    ma75_slope = None
    valid75 = ma75.dropna()
    if len(valid75) >= 21:
        ma75_slope = (float(valid75.iloc[-1]) / float(valid75.iloc[-21]) - 1) * 100

    # 直近60営業日のうちMA25より上にいた割合
    z = pd.DataFrame({"c": s, "m": ma25}).dropna().tail(60)
    above25_ratio = float((z["c"] >= z["m"]).mean() * 100) if len(z) else None

    # 最大ドローダウン
    peak = s.cummax()
    dd = (s / peak - 1) * 100
    max_dd = float(dd.min())

    # 20営業日ブロックの安値切り上げ率
    lows = []
    vals = s.to_numpy()
    for i in range(max(0, len(vals)-100), len(vals), 20):
        block = vals[i:i+20]
        if len(block) >= 10:
            lows.append(float(min(block)))
    higher_low_ratio = None
    if len(lows) >= 3:
        higher_low_ratio = sum(lows[i] >= lows[i-1] for i in range(1, len(lows))) / (len(lows)-1) * 100

    # 「一日だけの爆上げ」依存を減点
    daily = s.pct_change().dropna() * 100
    max_up_day = float(daily.max()) if len(daily) else 0.0

    score = 0.0

    # 長期上昇 0-25
    if ret60 is not None:
        score += min(15.0, max(0.0, ret60) * 0.50)
    if ret120 is not None:
        score += min(10.0, max(0.0, ret120) * 0.20)

    # 移動平均の上向き 0-20
    if ma25_slope is not None and ma25_slope > 0:
        score += min(12.0, 4.0 + ma25_slope * 0.8)
    if ma75_slope is not None and ma75_slope > 0:
        score += min(8.0, 3.0 + ma75_slope * 0.7)

    # MA25上の滞在率 0-20
    if above25_ratio is not None:
        score += max(0.0, min(20.0, (above25_ratio - 45.0) * 0.45))

    # 安値切り上げ 0-15
    if higher_low_ratio is not None:
        score += max(0.0, min(15.0, (higher_low_ratio - 40.0) * 0.25))

    # ドローダウン耐性 0-20
    if max_dd >= -8:
        score += 20
    elif max_dd >= -12:
        score += 16
    elif max_dd >= -18:
        score += 11
    elif max_dd >= -25:
        score += 5

    # 一発急騰依存を減点
    if max_up_day >= 20:
        score -= 15
    elif max_up_day >= 15:
        score -= 10
    elif max_up_day >= 10:
        score -= 5

    metrics = {
        "安定60日騰落率": round(ret60, 2) if ret60 is not None else None,
        "安定120日騰落率": round(ret120, 2) if ret120 is not None else None,
        "MA25傾き20日": round(ma25_slope, 2) if ma25_slope is not None else None,
        "MA75傾き20日": round(ma75_slope, 2) if ma75_slope is not None else None,
        "MA25上滞在率": round(above25_ratio, 1) if above25_ratio is not None else None,
        "最大DD120日": round(max_dd, 2),
        "安値切上げ率": round(higher_low_ratio, 1) if higher_low_ratio is not None else None,
        "最大1日上昇率": round(max_up_day, 2),
    }
    return round(clamp(score), 1), metrics


def score_streak_acceleration(g: pd.DataFrame, closes: pd.Series,
                              ma5: float | None, ma25: float | None,
                              high_dd: float | None) -> tuple[float, dict]:
    """4317型の短期連騰・高値更新加速を0～100点で評価。"""
    s = pd.to_numeric(closes, errors="coerce").dropna()
    if len(s) < 8:
        return 0.0, {"連騰加速スコア":0.0,"直近5日上昇日数":None,"MA5傾き5日":None,"直近5日高値更新回数":None}

    a = s.tail(6).to_numpy(dtype=float)
    up_days5 = int(((a[1:] - a[:-1]) > 0).sum())
    ret3 = (float(s.iloc[-1]) / float(s.iloc[-4]) - 1.0) * 100.0
    ret5 = (float(s.iloc[-1]) / float(s.iloc[-6]) - 1.0) * 100.0

    ma5_slope5 = None
    if len(s) >= 10:
        old_ma5 = float(s.iloc[-10:-5].mean())
        new_ma5 = float(s.tail(5).mean())
        if old_ma5 > 0:
            ma5_slope5 = (new_ma5 / old_ma5 - 1.0) * 100.0

    high_updates5 = 0
    if "高値" in g.columns:
        hs = pd.to_numeric(g["高値"], errors="coerce").dropna().tail(6).to_numpy(dtype=float)
        if len(hs) >= 2:
            high_updates5 = int((hs[1:] > hs[:-1]).sum())

    score = 0.0

    if up_days5 == 5: score += 30
    elif up_days5 == 4: score += 25
    elif up_days5 == 3: score += 13

    if ret5 >= 12: score += 16
    elif ret5 >= 8: score += 13
    elif ret5 >= 5: score += 9
    elif ret5 >= 3: score += 5

    if ret3 >= 7: score += 9
    elif ret3 >= 4: score += 7
    elif ret3 >= 2: score += 4

    if ma5 is not None and ma25 is not None and ma5 > ma25:
        score += 8

    if ma5_slope5 is not None:
        if ma5_slope5 >= 5: score += 12
        elif ma5_slope5 >= 3: score += 10
        elif ma5_slope5 >= 1.5: score += 7
        elif ma5_slope5 > 0: score += 3

    if high_updates5 >= 4: score += 15
    elif high_updates5 == 3: score += 12
    elif high_updates5 == 2: score += 7

    hd = n0(high_dd, -99)
    if hd >= -1: score += 10
    elif hd >= -2: score += 8
    elif hd >= -4: score += 5

    score = round(clamp(score), 1)
    return score, {
        "連騰加速スコア": score,
        "直近5日上昇日数": up_days5,
        "MA5傾き5日": round(ma5_slope5, 2) if ma5_slope5 is not None else None,
        "直近5日高値更新回数": high_updates5,
    }


def calc_bottoming_gate(g: pd.DataFrame, post: pd.DataFrame,
                        high_dd, ret5, ma5_gap) -> tuple[int, float | None, int]:
    """
    下げ止まりを採点してよいかのゲート。

    必須:
      - 現在が高値から -2.5%以上押している
        OR
      - 直近10営業日で -3%以上の押しを経験し、かつMA5を1%以上割った日がある
      - 直近5日が +6%超の加速局面ではない
      - MA5乖離が +5%超ではない

    これで「連騰中なのに下げ止まり」が付きにくくする。
    """
    if post is None or post.empty:
        return 0, None, 0

    recent = post.tail(10).copy()
    rc = pd.to_numeric(recent["終値"], errors="coerce").dropna()
    pullback = None
    if len(rc) >= 2:
        peak = rc.cummax()
        dd = (rc / peak - 1.0) * 100.0
        pullback = float(dd.min())

    ma5_break = 0
    try:
        full_close = pd.to_numeric(g["終値"], errors="coerce")
        full_ma5 = full_close.rolling(5).mean()
        idx = recent.index.intersection(g.index)
        if len(idx):
            cond = (full_close.loc[idx] < full_ma5.loc[idx] * 0.99)
            ma5_break = 1 if cond.fillna(False).any() else 0
    except Exception:
        ma5_break = 0

    hd = n0(high_dd, -99)
    r5 = n0(ret5, 99)
    m5 = n0(ma5_gap, 99)

    had_real_pullback = (hd <= -2.5) or (
        pullback is not None and pullback <= -3.0 and ma5_break == 1
    )
    gate = int(had_real_pullback and r5 <= 6.0 and m5 <= 5.0)
    return gate, (round(pullback, 2) if pullback is not None else None), ma5_break

def score_bottoming(current_ret,peak_ret,high_dd,ma5_gap,ma25_gap,
                    ret1,ret3,ret5,higher_low_flag,post_days)->float:
    # 確定底ではなく「下落速度が鈍り、安値切上げが出ている」度合い。
    if int(post_days or 0)<5 or peak_ret is None or peak_ret<5:
        return 0.0

    cr=n0(current_ret,-99); hd=n0(high_dd,-99)
    m5=n0(ma5_gap,-99); m25=n0(ma25_gap,-99)
    r1=n0(ret1,-99); r3=n0(ret3,-99); r5=n0(ret5,-99)

    s=0.0
    if -8<=hd<=-2.5: s+=22
    elif (-12<=hd<-8) or (-2.5<hd<=-1.5): s+=10

    if -2.0<=r3<=2.0: s+=18
    elif -3.5<=r3<=3.5: s+=9

    if -1<=r1<=2.0: s+=12
    elif 2.0<r1<=4.0: s+=6

    if higher_low_flag==1: s+=24

    if -3<=m25<=5: s+=12
    elif -5<=m25<=8: s+=6

    if -6<=r5<=2.0: s+=10
    elif -6<=r5<=4.0: s+=5

    if cr>=0: s+=4
    if -2<=m5<=2.5: s+=5

    # 旧版は100点が多すぎたため圧縮
    return round(clamp(s * 0.92),1)


def detect_styles(state, entry_score, strength_score, overheat, d1, d3, d5,
                  current_ret, high_dd, ma5_gap, ma25_gap, ret5,
                  stretch_flag, bottoming_flag=0, rebound_flag=0, stable_score=0, accel_score=0) -> list[str]:
    tags=[]
    if entry_score>=85: tags.append("🎯 今から候補")
    if d1 is not None and d1<0 and current_ret>=5 and high_dd>=-4: tags.append("🔄 再評価")
    if current_ret>=8 and high_dd>=-3 and 0<=ma5_gap<=4 and -2<=ret5<=6: tags.append("🟢 高値圏の押し目")
    if state=="S 継続上昇": tags.append("🚀 継続上昇")
    if stable_score>=70: tags.append("🪜 安定上昇")
    if 70<=accel_score<85: tags.append("⚡ 連騰気配")
    if accel_score>=85: tags.append("🔥 連騰加速")
    if bottoming_flag==1: tags.append("🛬 下げ止まり気配")
    if rebound_flag==1: tags.append("↗️ 反発初動")
    if strength_score>=70: tags.append("💪 強い")
    if stretch_flag==1: tags.append("🔥 伸び切り")
    if not tags:
        tags.append("👀 監視" if state=="C 監視" else "✅ 上昇維持")
    return tags


def make_summary(row:dict)->str:
    d1,d3,d5=row.get("D1"),row.get("D3"),row.get("D5")
    cr=n0(row.get("決算後騰落率")); hd=n0(row.get("高値から乖離率"),-99)
    m5=n0(row.get("MA5乖離率")); m25=n0(row.get("MA25乖離率")); r5=n0(row.get("直近5日騰落率"))
    entry=n0(row.get("今からスコア")); strength=n0(row.get("強さスコア")); heat=n0(row.get("過熱度"))
    parts=[]
    if d1 is not None and d1<0:
        if d5 is not None and d5>0: parts.append(f"初日{d1:+.1f}%からD5{d5:+.1f}%へ再評価")
        elif d3 is not None and d3>d1: parts.append(f"初日{d1:+.1f}%からD3{d3:+.1f}%へ改善")
        else: parts.append(f"初日{d1:+.1f}%安から現在は切り返し")
    elif d1 is not None and d1>=3:
        if d5 is not None: parts.append(f"D1{d1:+.1f}%→D5{d5:+.1f}%の継続型")
        else: parts.append(f"D1{d1:+.1f}%の強い初動")
    parts.append(f"現在は決算前比{cr:+.1f}%")
    if hd>=-1: parts.append("決算後高値ほぼ維持")
    elif hd>=-3: parts.append(f"高値から{hd:.1f}%で高値圏")
    elif hd>=-5: parts.append(f"高値から{hd:.1f}%の軽い調整")
    else: parts.append(f"高値から{hd:.1f}%")
    if row.get("下げ止まりフラグ")==1: parts.append(f"下げ止まりスコア{n0(row.get('下げ止まりスコア')):.0f}点（安値切上げ・短期鈍化）")
    if row.get("反発初動フラグ")==1: parts.append("直近で反発初動の形")
    if 0<=m5<=2: parts.append(f"MA5乖離{m5:+.1f}%で入りやすい位置")
    elif m5>=8: parts.append(f"MA5乖離{m5:+.1f}%で短期過熱")
    else: parts.append(f"MA5乖離{m5:+.1f}%")
    if m25>=20: parts.append(f"MA25乖離{m25:+.1f}%で伸び切り注意")
    if r5<-2: parts.append(f"直近5日{r5:+.1f}%で調整中")
    elif r5>12: parts.append(f"直近5日{r5:+.1f}%で加速過熱")
    if heat>=65: parts.append(f"強さ{strength:.0f}点だが過熱{heat:.0f}点")
    elif entry>=85: parts.append(f"今からスコア{entry:.0f}点")
    else: parts.append(f"今から{entry:.0f}点 / 強さ{strength:.0f}点")
    if n0(row.get("連騰加速スコア"))>=85:
        parts.append(f"連騰加速{n0(row.get('連騰加速スコア')):.0f}点・直近5日で{int(n0(row.get('直近5日上昇日数')))}日上昇")
    elif n0(row.get("連騰加速スコア"))>=70:
        parts.append(f"連騰気配{n0(row.get('連騰加速スコア')):.0f}点")
    return "。".join(parts)+"。"


def analyze(earnings,prices,screen,min_return,max_drawdown,
            bottom_min_return=0.0,bottom_max_drawdown=12.0):
    """
    v22 フル母集団版。

    earnings_reaction_labels から取得した発表済み決算は、
    候補条件に該当しなくても捨てずに全件返す。

    母集団区分:
      候補      = 従来の上昇継続 / 下げ止まり / 反発初動のどれか
      監視      = 解析可能だが従来候補条件には未到達
      D1待ち    = 発表済みだがD1未確定、またはD1価格未収録
      解析不可  = price_history / 基準終値などが不足

    これにより閾値を後からダッシュボードで緩めて再発見できる。
    """
    if earnings.empty:
        return pd.DataFrame()

    screen_map=screen.set_index("コード").to_dict("index") if not screen.empty else {}
    price_groups={}
    if prices is not None and not prices.empty:
        price_groups={
            code:g.sort_values("日付_dt").reset_index(drop=True)
            for code,g in prices.groupby("コード",sort=False)
        }

    rows=[]

    def _safe_float(v):
        try:
            x=float(v)
            return x if pd.notna(x) else None
        except Exception:
            return None

    for e in earnings.itertuples(index=False):
        code=normalize_code(getattr(e,"コード"))
        srow=screen_map.get(code,{})
        name=srow.get("銘柄名_s") or getattr(e,"銘柄名") or ""
        market=srow.get("市場") or ""
        announced=pd.Timestamp(getattr(e,"発表日時_dt"))
        d1_raw=getattr(e,"D1日付_dt",pd.NaT)
        d1_date=None if pd.isna(d1_raw) else pd.Timestamp(d1_raw).normalize()

        g=price_groups.get(code)
        base=_safe_float(getattr(e,"基準終値"))
        base_source=str(getattr(e,"基準終値ソース","") or "")
        if base is not None and base>0 and not base_source:
            base_source="入力済み"

        # 基準終値が無ければprice_historyから補完を試す。
        if (base is None or base<=0) and g is not None and not g.empty:
            try:
                if d1_date is not None:
                    pre_base=g[g["日付_dt"] < d1_date]
                else:
                    precision=str(getattr(e,"発表時刻精度","") or "")
                    if precision=="時刻":
                        tm=announced.time()
                        cutoff=datetime.strptime("15:30","%H:%M").time()
                        pre_base=(
                            g[g["日付_dt"] < announced.normalize()]
                            if tm < cutoff
                            else g[g["日付_dt"] <= announced.normalize()]
                        )
                    else:
                        pre_base=g[g["日付_dt"] < announced.normalize()]
                pre_close=pd.to_numeric(pre_base["終値"],errors="coerce").dropna()
                if not pre_close.empty and float(pre_close.iloc[-1])>0:
                    base=float(pre_close.iloc[-1])
                    base_source="price_history補完"
            except Exception:
                pass

        # 共通の空行。解析不能でも母集団から落とさない。
        row={
            "コード":code,
            "銘柄名":name,
            "市場":market,
            "母集団ソース":str(getattr(e,"母集団ソース","") or ""),
            "母集団根拠":str(getattr(e,"母集団根拠","") or ""),
            "決算タイトル":str(getattr(e,"決算タイトル","") or ""),
            "発表時刻精度":str(getattr(e,"発表時刻精度","") or ""),
            "反応ラベル有無":int(getattr(e,"反応ラベル有無",0) or 0),
            "D1ソース":str(getattr(e,"D1ソース","") or ""),
            "D1復元精度":str(getattr(e,"D1復元精度","") or ""),
            "D1状態":str(getattr(e,"D1状態","") or ""),
            "D1待ち理由":str(getattr(e,"D1待ち理由","") or ""),
            "現在値ソース":"",
            "母集団区分":"解析不可",
            "解析状態":"未解析",
            "従来候補フラグ":0,
            "発表日時":getattr(e,"発表日時"),
            "決算種別":getattr(e,"決算種別"),
            "基準終値ソース":base_source or "不足",
            "決算前終値":round(base,1) if base is not None and base>0 else None,
            "現在値":None,
            "決算後騰落率":None,
            "決算後最大上昇率":None,
            "D1":None,"D3":None,"D5":None,
            "D3有効":0,"D5有効":0,
            "決算後最高値":None,"高値から乖離率":None,
            "決算後最安値":None,"安値から反発率":None,
            "決算後営業日数":0,
            "チャートデータ":[],"チャート日付":[],"チャートOHLC":[],
            "チャート区分":[],"チャート決算境界":0,
            "MA5":None,"MA5乖離率":None,
            "MA25":None,"MA25乖離率":None,
            "直近1日騰落率":None,"直近3日騰落率":None,
            "直近5日騰落率":None,"直近10日騰落率":None,
            "直近3日安値切上げ":0,
            "下げ止まりゲート":0,"直近10日最大押し率":None,
            "直近10日MA5割れ":0,"下げ止まりスコア":None,
            "下げ止まりフラグ":0,"反発初動フラグ":0,
            "採用枠":"解析不可","決算後状態":"X 解析不可",
            "安定上昇スコア":None,"連騰加速スコア":None,
            "強さスコア":None,"過熱度":None,"今からスコア":None,
            "短期低リスクスコア":None,"短期低リスクフラグ":0,"今ならまだ間に合う順位":None,
            "値幅×低リスクスコア":None,"値幅×低リスクフラグ":0,"値幅×低リスク順位":None,
            "伸び切りフラグ":0,
        }

        for c in SCREENER_SUPPLY_COLUMNS:
            if c in srow:
                row[c]=srow.get(c)

        # price_historyなし: 現在値だけはscreenerから残す。
        if g is None or g.empty:
            spx=_safe_float(srow.get("現在値"))
            if spx is not None and spx>0:
                row["現在値"]=round(spx,1)
                row["現在値ソース"]="screener.現在値"
                if base is not None and base>0:
                    row["決算後騰落率"]=round(pct(spx,base),2)
            row["解析状態"]="price_history不足"
            row["タイプ"]="⚠️ 価格履歴不足"
            row["サマリー"]="price_historyが無いため母集団には残し、決算後トレンド採点は保留。"
            rows.append(row)
            continue

        closes=pd.to_numeric(g["終値"],errors="coerce").dropna()
        if closes.empty:
            row["解析状態"]="終値データ不足"
            row["タイプ"]="⚠️ 価格履歴不足"
            row["サマリー"]="終値データが無いため母集団には残し、決算後トレンド採点は保留。"
            rows.append(row)
            continue

        # currentはscreener.現在値を最優先
        spx=_safe_float(srow.get("現在値"))
        current=spx if spx is not None and spx>0 else float(closes.iloc[-1])
        row["現在値"]=round(current,1)
        row["現在値ソース"]="screener.現在値" if spx is not None and spx>0 else "price_history"

        closes=closes.copy()
        closes.iloc[-1]=current
        ma5=float(closes.tail(5).mean()) if len(closes)>=5 else None
        ma25=float(closes.tail(25).mean()) if len(closes)>=25 else None
        ret1=pct(current,float(closes.iloc[-2])) if len(closes)>=2 else None
        ret3=pct(current,float(closes.iloc[-4])) if len(closes)>=4 else None
        ret5=pct(current,float(closes.iloc[-6])) if len(closes)>=6 else None
        ret10=pct(current,float(closes.iloc[-11])) if len(closes)>=11 else None

        row["MA5"]=round(ma5,1) if ma5 is not None else None
        row["MA25"]=round(ma25,1) if ma25 is not None else None
        row["MA5乖離率"]=round(pct(current,ma5),2) if ma5 else None
        row["MA25乖離率"]=round(pct(current,ma25),2) if ma25 else None
        row["直近1日騰落率"]=round(ret1,2) if ret1 is not None else None
        row["直近3日騰落率"]=round(ret3,2) if ret3 is not None else None
        row["直近5日騰落率"]=round(ret5,2) if ret5 is not None else None
        row["直近10日騰落率"]=round(ret10,2) if ret10 is not None else None

        # 基準終値も補完できない場合は採点不能だが全件保持。
        if base is None or base<=0:
            stable_score,stable_metrics=score_stable_uptrend(closes)
            row["安定上昇スコア"]=stable_score
            row.update(stable_metrics)
            row["解析状態"]="基準終値不足"
            row["タイプ"]="⚠️ 基準終値不足"
            row["サマリー"]="基準終値を取得・補完できないため母集団には残し、決算後騰落率の採点は保留。"
            rows.append(row)
            continue

        row["決算前終値"]=round(base,1)
        row["決算後騰落率"]=round(pct(current,base),2)

        # D1未確定: 「真のD1待ち」と「既に来たはずなのに価格不足」を分離。
        if d1_date is None:
            stable_score,stable_metrics=score_stable_uptrend(closes)
            row["安定上昇スコア"]=stable_score
            row.update(stable_metrics)

            # D1待ちはチャートを決算前5営業日中心で表示。
            pre=g[g["日付_dt"] <= announced.normalize()].tail(5).copy()
            chart_vals=[]; chart_dates=[]; chart_ohlc=[]; chart_phase=[]
            _cols=[c for c in ["日付_dt","始値","高値","安値","終値"] if c in g.columns]
            for rr in pre[_cols].dropna(subset=["終値"]).itertuples(index=False):
                cc=pct(float(rr.終値),base)
                if cc is None: continue
                oo=pct(float(getattr(rr,"始値",rr.終値)),base)
                hh=pct(float(getattr(rr,"高値",rr.終値)),base)
                ll=pct(float(getattr(rr,"安値",rr.終値)),base)
                chart_vals.append(round(cc,2))
                chart_dates.append(pd.Timestamp(rr.日付_dt).strftime("%m/%d"))
                chart_ohlc.append({
                    "o":round(oo if oo is not None else cc,2),
                    "h":round(hh if hh is not None else cc,2),
                    "l":round(ll if ll is not None else cc,2),
                    "c":round(cc,2)
                })
                chart_phase.append("pre")
            row["チャートデータ"]=chart_vals
            row["チャート日付"]=chart_dates
            row["チャートOHLC"]=chart_ohlc
            row["チャート区分"]=chart_phase
            row["チャート決算境界"]=max(0,len(chart_vals)-1)

            d1_state=str(getattr(e,"D1状態","") or "")
            wait_reason=str(getattr(e,"D1待ち理由","") or "")
            if d1_state=="価格不足":
                row["母集団区分"]="解析不可"
                row["解析状態"]="D1価格不足"
                row["採用枠"]="解析不可"
                row["決算後状態"]="X D1価格不足"
                row["タイプ"]="⚠️ D1価格不足"
                row["サマリー"]=(
                    f"D1反応日は到来済みとみられるが価格履歴不足。"
                    f"{wait_reason or 'price_historyを確認'}。母集団には残す。"
                )
            else:
                row["母集団区分"]="D1待ち"
                row["解析状態"]="真のD1待ち"
                row["採用枠"]="D1待ち"
                row["決算後状態"]="N D1待ち"
                row["タイプ"]="🆕 D1待ち"
                row["サマリー"]=(
                    f"次の反応営業日待ち。現在は基準終値比{row['決算後騰落率']:+.1f}%"
                    f"。{wait_reason or 'D1到来後に正式採点'}。"
                )
            rows.append(row)
            continue

        post=g[g["日付_dt"]>=d1_date].copy()
        if post.empty:
            row["母集団区分"]="D1待ち"
            row["解析状態"]="D1価格未収録"
            row["採用枠"]="D1待ち"
            row["決算後状態"]="N D1待ち"
            row["タイプ"]="⏳ D1価格待ち"
            row["サマリー"]="D1日付は確定済みだがprice_historyにD1以降が未収録。母集団には残して次回更新待ち。"
            rows.append(row)
            continue

        # --------------------------
        # ここから従来の正式採点
        # --------------------------
        post_high=float(post["高値"].fillna(post["終値"]).max())
        post_low=float(post["安値"].fillna(post["終値"]).min())
        post_days=int(post["日付_dt"].nunique())
        peak_ret=pct(post_high,base)
        current_ret=pct(current,base)
        high_dd=pct(current,post_high)
        low_rebound=pct(current,post_low)
        ma5_gap=pct(current,ma5) if ma5 else None
        ma25_gap=pct(current,ma25) if ma25 else None

        row["決算後騰落率"]=round(current_ret,2)
        row["決算後最大上昇率"]=round(peak_ret,2) if peak_ret is not None else None
        row["決算後最高値"]=round(post_high,1)
        row["高値から乖離率"]=round(high_dd,2)
        row["決算後最安値"]=round(post_low,1)
        row["安値から反発率"]=round(low_rebound,2)
        row["決算後営業日数"]=post_days

        # chart: 決算前5営業日 + D1～現在
        pre5=g[g["日付_dt"]<d1_date].tail(5).copy()
        chart_vals=[]; chart_dates=[]; chart_ohlc=[]; chart_phase=[]
        def _append_chart_row(_r0,_phase):
            _close=float(_r0.終値)
            _c=pct(_close,base)
            if _c is None: return
            _o=pct(float(getattr(_r0,"始値",_close)),base)
            _h=pct(float(getattr(_r0,"高値",_close)),base)
            _l=pct(float(getattr(_r0,"安値",_close)),base)
            chart_vals.append(round(float(_c),2))
            chart_ohlc.append({
                "o":round(float(_o if _o is not None else _c),2),
                "h":round(float(_h if _h is not None else _c),2),
                "l":round(float(_l if _l is not None else _c),2),
                "c":round(float(_c),2)
            })
            chart_phase.append(_phase)
            try: chart_dates.append(pd.Timestamp(_r0.日付_dt).strftime("%m/%d"))
            except Exception: chart_dates.append("")

        _cols=[c for c in ["日付_dt","始値","高値","安値","終値"] if c in g.columns]
        for rr in pre5[_cols].dropna(subset=["終値"]).itertuples(index=False):
            _append_chart_row(rr,"pre")
        chart_event_index=max(0,len(chart_vals)-1)
        for rr in post[_cols].dropna(subset=["終値"]).itertuples(index=False):
            _append_chart_row(rr,"post")

        row["チャートデータ"]=chart_vals
        row["チャート日付"]=chart_dates
        row["チャートOHLC"]=chart_ohlc
        row["チャート区分"]=chart_phase
        row["チャート決算境界"]=chart_event_index

        def val(name):
            x=getattr(e,name)
            return None if pd.isna(x) else float(x)

        d1=val("D1終値騰落率")
        d3=val("D3終値騰落率")
        d5=val("D5終値騰落率")
        d3_valid=post_days>=3
        d5_valid=post_days>=5
        if not d3_valid: d3=None
        if not d5_valid: d5=None
        row["D1"]=round(d1,2) if d1 is not None else None
        row["D3"]=round(d3,2) if d3 is not None else None
        row["D5"]=round(d5,2) if d5 is not None else None
        row["D3有効"]=1 if d3_valid else 0
        row["D5有効"]=1 if d5_valid else 0

        higher_low_flag=0
        if len(g)>=6:
            last3=g.tail(3); prev3=g.iloc[-6:-3]
            last3_low=float(last3["安値"].fillna(last3["終値"]).min())
            prev3_low=float(prev3["安値"].fillna(prev3["終値"]).min())
            if prev3_low>0 and last3_low>=prev3_low*0.99:
                higher_low_flag=1
        row["直近3日安値切上げ"]=higher_low_flag

        above5=ma5 is None or current>=ma5
        above25=ma25 is None or current>=ma25
        trend_ok=(
            current_ret is not None and current_ret>=min_return
            and high_dd is not None and high_dd>=-max_drawdown
            and above5 and above25
        )

        stable_score,stable_metrics=score_stable_uptrend(closes)
        accel_score,accel_metrics=score_streak_acceleration(g,closes,ma5,ma25,high_dd)
        bottom_gate,recent_pullback,ma5_break_recent=calc_bottoming_gate(g,post,high_dd,ret5,ma5_gap)
        bottom_score=(
            score_bottoming(current_ret,peak_ret,high_dd,ma5_gap,ma25_gap,
                            ret1,ret3,ret5,higher_low_flag,post_days)
            if bottom_gate else 0.0
        )

        bottoming_flag=1 if (
            bottom_gate==1 and bottom_score>=70
            and current_ret is not None and current_ret>=bottom_min_return
            and high_dd is not None and high_dd>=-bottom_max_drawdown and high_dd<=-1.5
            and ma25 is not None and current>=ma25*0.97
            and n0(ret5,99)<=5.0
        ) else 0

        rebound_flag=1 if (
            bottom_gate==1 and bottom_score>=65
            and ret1 is not None and ret1>0
            and ret3 is not None and ret3>0
            and n0(ret5,99)<=8.0
            and (ma5 is None or current>=ma5*0.995)
        ) else 0

        row["下げ止まりゲート"]=bottom_gate
        row["直近10日最大押し率"]=recent_pullback
        row["直近10日MA5割れ"]=ma5_break_recent
        row["下げ止まりスコア"]=bottom_score
        row["下げ止まりフラグ"]=bottoming_flag
        row["反発初動フラグ"]=rebound_flag
        row["安定上昇スコア"]=stable_score
        row.update(stable_metrics)
        row.update(accel_metrics)

        candidate_flag=bool(trend_ok or bottoming_flag or rebound_flag)
        if trend_ok and (bottoming_flag or rebound_flag):
            adopt="両方"
        elif trend_ok:
            adopt="上昇継続"
        elif rebound_flag:
            adopt="反発初動"
        elif bottoming_flag:
            adopt="下げ止まり"
        else:
            adopt="監視"

        state="C 監視"
        if (bottoming_flag or rebound_flag) and not trend_ok:
            state="P 下げ止まり候補"
        if d1 is not None and d1<0 and current_ret>=5 and high_dd>=-5 and above5 and above25:
            state="S 再評価型"
        elif d1 is not None and d1>=3 and current_ret>=8 and high_dd>=-5 and above5 and above25:
            state="S 継続上昇"
        elif trend_ok and current_ret>=5:
            state="A 強い"
        elif trend_ok:
            state="B 上昇維持"

        row["採用枠"]=adopt
        row["決算後状態"]=state
        row["母集団区分"]="候補" if candidate_flag else "監視"
        row["解析状態"]="解析済み"
        row["従来候補フラグ"]=1 if candidate_flag else 0

        row["強さスコア"]=score_strength(
            row["決算後騰落率"],row["高値から乖離率"],
            row["MA5乖離率"],row["MA25乖離率"],
            row["直近5日騰落率"],row["直近10日騰落率"],
            row["D1"],row["D3"],row["D5"]
        )
        row["過熱度"]=score_overheat(
            row["決算後騰落率"],row["MA5乖離率"],
            row["MA25乖離率"],row["直近5日騰落率"],
            row["直近10日騰落率"]
        )
        row["今からスコア"]=score_entry(
            row["決算後騰落率"],row["高値から乖離率"],
            row["MA5乖離率"],row["MA25乖離率"],
            row["直近5日騰落率"],row["直近10日騰落率"],
            row["D1"],row["D3"],row["D5"],
            row["決算後営業日数"],row["過熱度"]
        )
        row["伸び切りフラグ"]=1 if (
            row["過熱度"]>=60
            or row["決算後騰落率"]>=30
            or n0(row["MA5乖離率"])>=8
            or n0(row["MA25乖離率"])>=20
            or n0(row["直近5日騰落率"])>=15
        ) else 0

        row["短期低リスクスコア"],row["短期低リスクフラグ"]=score_short_low_risk(
            current_ret=row["決算後騰落率"],
            high_dd=row["高値から乖離率"],
            ma5_gap=row["MA5乖離率"],
            ret5=row["直近5日騰落率"],
            entry_score=row["今からスコア"],
            strength_score=row["強さスコア"],
            overheat=row["過熱度"],
            stretch_flag=row["伸び切りフラグ"],
        )
        row["値幅×低リスクスコア"],row["値幅×低リスクフラグ"]=score_range_low_risk(
            short_low_risk_score=row["短期低リスクスコア"],
            short_low_risk_flag=row["短期低リスクフラグ"],
            post_max_up=row["決算後最大上昇率"],
            max_up_day=row.get("最大1日上昇率"),
            ret60=row.get("安定60日騰落率"),
            max_dd120=row.get("最大DD120日"),
            current_ret=row["決算後騰落率"],
            high_dd=row["高値から乖離率"],
            overheat=row["過熱度"],
            stretch_flag=row["伸び切りフラグ"],
        )

        tags=detect_styles(
            state=row["決算後状態"],
            entry_score=row["今からスコア"],
            strength_score=row["強さスコア"],
            overheat=row["過熱度"],
            d1=row["D1"],d3=row["D3"],d5=row["D5"],
            current_ret=row["決算後騰落率"],
            high_dd=row["高値から乖離率"],
            ma5_gap=n0(row["MA5乖離率"]),
            ma25_gap=n0(row["MA25乖離率"]),
            ret5=n0(row["直近5日騰落率"]),
            stretch_flag=row["伸び切りフラグ"],
            bottoming_flag=bottoming_flag,
            rebound_flag=rebound_flag,
            stable_score=row["安定上昇スコア"],
            accel_score=row.get("連騰加速スコア",0)
        )
        if row.get("短期低リスクフラグ")==1:
            tags.insert(0,"🛡️ 短期低リスク")
        if row.get("値幅×低リスクフラグ")==1:
            tags.insert(0,"🚀 値幅×低リスク")
        row["タイプ"]="｜".join(tags)
        row["サマリー"]=make_summary(row)
        if row.get("短期低リスクフラグ")==1:
            row["サマリー"]+=f" 短期低リスク{n0(row.get('短期低リスクスコア')):.1f}点（信用・出来高は不使用）。"
        if row.get("値幅×低リスクフラグ")==1:
            row["サマリー"]+=f" 値幅×低リスク{n0(row.get('値幅×低リスクスコア')):.1f}点（決算後最大+{n0(row.get('決算後最大上昇率')):.1f}% / 120日最大DD{n0(row.get('最大DD120日')):.1f}%）。"
        if not candidate_flag:
            row["サマリー"]="【監視】従来候補条件には未到達。"+row["サマリー"]
        rows.append(row)

    out=pd.DataFrame(rows)
    if out.empty:
        return out

    # 🛡️短期低リスク条件を通過した銘柄だけに「今ならまだ間に合う順位」を付与。
    # 同点は 今からスコア→高値維持→低過熱→強さ の順で決める。
    out["今ならまだ間に合う順位"]=pd.Series(pd.NA,index=out.index,dtype="Int64")
    _lr_mask=pd.to_numeric(out.get("短期低リスクフラグ",0),errors="coerce").fillna(0).astype(int).eq(1)
    if _lr_mask.any():
        _ranked=out.loc[_lr_mask].copy()
        _ranked["_lr_score"]=pd.to_numeric(_ranked["短期低リスクスコア"],errors="coerce").fillna(-999)
        _ranked["_lr_entry"]=pd.to_numeric(_ranked["今からスコア"],errors="coerce").fillna(-999)
        _ranked["_lr_high"]=pd.to_numeric(_ranked["高値から乖離率"],errors="coerce").fillna(-999)
        _ranked["_lr_heat"]=pd.to_numeric(_ranked["過熱度"],errors="coerce").fillna(999)
        _ranked["_lr_strength"]=pd.to_numeric(_ranked["強さスコア"],errors="coerce").fillna(-999)
        _ranked=_ranked.sort_values(
            ["_lr_score","_lr_entry","_lr_high","_lr_heat","_lr_strength"],
            ascending=[False,False,False,True,False],
            kind="mergesort"
        )
        for _rank,_idx in enumerate(_ranked.index,start=1):
            out.at[_idx,"今ならまだ間に合う順位"]=_rank

    # 🚀値幅×低リスク合格銘柄に専用順位を付与。
    # 同点は 値幅×低リスクスコア→短期低リスク→120日DDの浅さ→高値維持 の順。
    out["値幅×低リスク順位"]=pd.Series(pd.NA,index=out.index,dtype="Int64")
    _vr_mask=pd.to_numeric(out.get("値幅×低リスクフラグ",0),errors="coerce").fillna(0).astype(int).eq(1)
    if _vr_mask.any():
        _ranked=out.loc[_vr_mask].copy()
        _ranked["_vr_score"]=pd.to_numeric(_ranked["値幅×低リスクスコア"],errors="coerce").fillna(-999)
        _ranked["_vr_low"]=pd.to_numeric(_ranked["短期低リスクスコア"],errors="coerce").fillna(-999)
        _ranked["_vr_dd"]=pd.to_numeric(_ranked["最大DD120日"],errors="coerce").fillna(-999)
        _ranked["_vr_high"]=pd.to_numeric(_ranked["高値から乖離率"],errors="coerce").fillna(-999)
        _ranked["_vr_post"]=pd.to_numeric(_ranked["決算後最大上昇率"],errors="coerce").fillna(-999)
        _ranked=_ranked.sort_values(
            ["_vr_score","_vr_low","_vr_dd","_vr_high","_vr_post"],
            ascending=[False,False,False,False,False],
            kind="mergesort"
        )
        for _rank,_idx in enumerate(_ranked.index,start=1):
            out.at[_idx,"値幅×低リスク順位"]=_rank

    # 母集団区分を優先しつつ、その中で今から順。
    order={"候補":0,"監視":1,"D1待ち":2,"解析不可":3}
    out["_母集団順"]=out["母集団区分"].map(order).fillna(9)
    out["_今から並び"]=pd.to_numeric(out["今からスコア"],errors="coerce").fillna(-999)
    out["_過熱並び"]=pd.to_numeric(out["過熱度"],errors="coerce").fillna(999)
    out["_高値並び"]=pd.to_numeric(out["高値から乖離率"],errors="coerce").fillna(-999)
    out["_強さ並び"]=pd.to_numeric(out["強さスコア"],errors="coerce").fillna(-999)
    out=out.sort_values(
        ["_母集団順","_今から並び","_過熱並び","_高値並び","_強さ並び"],
        ascending=[True,False,True,False,False]
    ).drop(columns=["_母集団順","_今から並び","_過熱並び","_高値並び","_強さ並び"])
    return out.reset_index(drop=True)

def enrich_supply(result:pd.DataFrame)->pd.DataFrame:
    # 信用・機関空売りは価格タイプとは別軸。今から/強さスコアには混ぜない。
    if result.empty: return result
    out=result.copy(); all_tags=[]; notes=[]
    for _,r in out.iterrows():
        tags=[]; memo=[]
        def fv(v):
            try: return None if pd.isna(v) else float(v)
            except Exception: return None
        ratio=fv(r.get("信用倍率")); fr=fv(r.get("信用買い残_浮動株比率")); inc20=fv(r.get("信用買い残増減率_20d")); oh=fv(r.get("需給OH")); load=fv(r.get("信用需給負荷スコア")); safe=fv(r.get("需給安全フラグ")); squeeze=fv(r.get("踏み上げ期待スコア")); inst_change=fv(r.get("本日の増減合計株数")); agency=str(r.get("空売り機関") or "")
        if fr is not None and fr<1 and (oh is None or oh<=3): tags.append("🟦 信用軽い")
        if safe is not None and safe>=1: tags.append("🟩 需給安全")
        if squeeze is not None and squeeze>=75: tags.append("🩳 踏み上げ余地")
        if (fr is not None and fr>=5) or (ratio is not None and ratio>=10) or (load is not None and load>=70): tags.append("⚠️ 信用買い重い")
        if inc20 is not None and inc20>=30: tags.append("📈 買い残増加")
        elif inc20 is not None and inc20<=-20: tags.append("📉 買い残減少")
        if agency=="なし": tags.append("✅ 機関空売りなし")
        if inst_change is not None and inst_change>0: tags.append("🏦 機関売り増")
        elif inst_change is not None and inst_change<0: tags.append("🔁 機関買戻し")
        if fr is not None: memo.append(f"買残/浮動株{fr:.1f}%")
        if ratio is not None and ratio<999: memo.append(f"信用倍率{ratio:.1f}倍")
        if inc20 is not None: memo.append(f"買残20日{inc20:+.1f}%")
        if load is not None: memo.append(f"信用負荷{load:.0f}点")
        if inst_change is not None and inst_change!=0: memo.append(f"機関空売り増減{inst_change:+,.0f}株")
        all_tags.append("｜".join(tags) if tags else "-"); notes.append(" / ".join(memo) if memo else "データなし")
    out["需給タグ"]=all_tags; out["信用需給メモ"]=notes
    out["サマリー"]=out.apply(lambda r:str(r.get("サマリー") or "")+(f" 需給: {r.get('信用需給メモ')}。" if r.get("信用需給メモ") not in (None,"","データなし") else ""),axis=1)
    return out



# ============================================================
# v25: 全母集団CSVと軽量ダッシュボードを分離
# ============================================================

def build_practical_dashboard_pool(
    full_df: pd.DataFrame,
    gate_return: float = 2.0,
    gate_d1: float = 3.0,
    gate_strength: float = 50.0,
    gate_stable: float = 60.0,
    gate_accel: float = 60.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    全母集団は一切捨てずCSVへ保存し、
    HTMLにはD1以降を評価できる掲載対象銘柄だけ渡し、監視/D1待ちは除外する。

    通過条件はOR。
      - 従来候補
      - 決算後騰落率 >= gate_return
      - D1 >= gate_d1
      - 強さ >= gate_strength
      - 安定上昇 >= gate_stable
      - 連騰加速 >= gate_accel
      - 再評価タグ
      - 真のD1待ち

    監視・D1待ち・解析不可・D1価格不足などはHTMLへ入れないが、
    全母集団CSV / 足切り除外CSVには必ず残す。
    """
    if full_df is None or full_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    df = full_df.copy()

    def n(col):
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index)
        return pd.to_numeric(df[col], errors="coerce")

    candidate = n("従来候補フラグ").fillna(0).eq(1)
    analyzed = (
        df["解析状態"].astype(str).eq("解析済み")
        if "解析状態" in df.columns
        else pd.Series(False, index=df.index)
    )
    universe_class = (
        df["母集団区分"].astype(str)
        if "母集団区分" in df.columns
        else pd.Series("", index=df.index)
    )
    d1_state = (
        df["D1状態"].astype(str)
        if "D1状態" in df.columns
        else pd.Series("", index=df.index)
    )
    types = (
        df["タイプ"].astype(str)
        if "タイプ" in df.columns
        else pd.Series("", index=df.index)
    )

    true_d1_wait = universe_class.eq("D1待ち") & (
        d1_state.eq("真のD1待ち")
        | d1_state.eq("")
        | d1_state.eq("ラベル未作成")
    )

    cond_return = analyzed & n("決算後騰落率").ge(float(gate_return))
    cond_d1 = analyzed & n("D1").ge(float(gate_d1))
    cond_strength = analyzed & n("強さスコア").ge(float(gate_strength))
    cond_stable = analyzed & n("安定上昇スコア").ge(float(gate_stable))
    cond_accel = analyzed & n("連騰加速スコア").ge(float(gate_accel))
    cond_reval = analyzed & types.str.contains("再評価", na=False)

    # v29: 「監視」と「D1待ち」はUI不要。
    # HTMLに載せるのは、すでにD1以降を評価できる掲載候補だけ。
    # 監視/D1待ちは全母集団CSV・足切り除外CSVには残す。
    watch = universe_class.eq("監視")
    d1_wait = universe_class.eq("D1待ち") | d1_state.eq("真のD1待ち")

    keep = (
        candidate
        | cond_return
        | cond_d1
        | cond_strength
        | cond_stable
        | cond_accel
        | cond_reval
    ) & (~watch) & (~d1_wait)

    pass_reasons = []
    cut_reasons = []

    for i in df.index:
        hit = []
        if bool(candidate.loc[i]):
            hit.append("従来候補")
        if bool(cond_return.loc[i]):
            hit.append(f"決算後>={gate_return:g}%")
        if bool(cond_d1.loc[i]):
            hit.append(f"D1>={gate_d1:g}%")
        if bool(cond_strength.loc[i]):
            hit.append(f"強さ>={gate_strength:g}")
        if bool(cond_stable.loc[i]):
            hit.append(f"安定>={gate_stable:g}")
        if bool(cond_accel.loc[i]):
            hit.append(f"連騰>={gate_accel:g}")
        if bool(cond_reval.loc[i]):
            hit.append("再評価")
        pass_reasons.append("｜".join(hit))

        if bool(keep.loc[i]):
            cut_reasons.append("")
            continue

        st = str(df.at[i, "解析状態"]) if "解析状態" in df.columns else ""
        uc = str(df.at[i, "母集団区分"]) if "母集団区分" in df.columns else ""

        if uc == "監視":
            cut_reasons.append("監視カテゴリのためHTML除外")
        elif uc == "D1待ち" or str(df.at[i, "D1状態"] if "D1状態" in df.columns else "") == "真のD1待ち":
            cut_reasons.append("D1待ちのためHTML除外")
        elif uc == "解析不可" or st not in ("解析済み", ""):
            cut_reasons.append(f"解析対象外:{st or uc or 'データ不足'}")
        else:
            vals = []
            for label, col in [
                ("決算後", "決算後騰落率"),
                ("D1", "D1"),
                ("強さ", "強さスコア"),
                ("安定", "安定上昇スコア"),
                ("連騰", "連騰加速スコア"),
            ]:
                v = n(col).loc[i]
                if pd.notna(v):
                    vals.append(f"{label}{float(v):.1f}")
            cut_reasons.append(
                "実戦監視OR条件未達"
                + (f" ({', '.join(vals)})" if vals else "")
            )

    df["実戦監視フラグ"] = keep.astype(int)
    df["足切り通過理由"] = pass_reasons
    df["足切り理由"] = cut_reasons
    df["ダッシュボード区分"] = np.where(
        keep,
        "掲載",
        np.where(d1_wait, "D1待ち除外", "足切り")
    )

    practical = df[df["実戦監視フラグ"].eq(1)].copy()
    excluded = df[df["実戦監視フラグ"].eq(0)].copy()

    if not practical.empty:
        _entry_src = practical["今からスコア"] if "今からスコア" in practical.columns else pd.Series(np.nan,index=practical.index)
        _strength_src = practical["強さスコア"] if "強さスコア" in practical.columns else pd.Series(np.nan,index=practical.index)
        practical["_entry_order"] = pd.to_numeric(_entry_src, errors="coerce").fillna(-999)
        practical["_strength_order"] = pd.to_numeric(_strength_src, errors="coerce").fillna(-999)
        practical = practical.sort_values(
            ["_entry_order", "_strength_order"],
            ascending=[False, False]
        ).drop(columns=["_entry_order", "_strength_order"])

    return (
        df.reset_index(drop=True),
        practical.reset_index(drop=True),
        excluded.reset_index(drop=True),
    )


def csv_safe_view(df: pd.DataFrame) -> pd.DataFrame:
    """HTML描画専用の配列列をCSVから外す。"""
    return df.drop(
        columns=[
            "チャートデータ", "チャート日付", "チャートOHLC",
            "チャート区分", "チャート決算境界"
        ],
        errors="ignore"
    ).copy()


def generate_dashboard(
    df:pd.DataFrame,
    out_path:Path,
    days:int,
    min_return:float,
    max_drawdown:float,
    bottom_min_return:float,
    bottom_max_drawdown:float,
    full_count:int|None=None,
    excluded_count:int|None=None,
)->None:
    # v26: UIで見ない診断列はHTML DATAにも埋め込まない。
    # 反応ラベル有無 / D1ソース はカード・D1価格復元絞込で内部利用するため保持。
    dashboard_df=df.drop(
        columns=[
            "母集団ソース","母集団根拠","足切り通過理由",
            "D1復元精度","D1状態","D1待ち理由","解析状態"
        ],
        errors="ignore"
    )
    records=js_safe_records(dashboard_df)
    data_json=json.dumps(records,ensure_ascii=False,separators=(",", ":")).replace("</","<\\/")
    generated=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_count=int(len(df) if full_count is None else full_count)
    excluded_count=int(max(0,full_count-len(df)) if excluded_count is None else excluded_count)
    template=r'''<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>決算後上昇ダッシュボード</title>
<style>
:root{--bg:#f7f8fb;--card:#fff;--line:#e5e7eb;--text:#111827;--muted:#6b7280}.wrap{max-width:1950px;margin:0 auto;padding:16px}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI","Noto Sans JP",sans-serif}.title-row{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.title-row h1{font-size:22px;margin:0 0 4px}.sub{font-size:12px;color:var(--muted);margin-bottom:14px}.help-open{background:#111827;color:#fff;border:0;border-radius:9px;height:36px;padding:0 13px;font-weight:800;cursor:pointer}.cards{display:grid;grid-template-columns:repeat(8,minmax(115px,1fr));gap:10px;margin-bottom:12px}.card,.panel{background:#fff;border:1px solid var(--line);border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,.03)}.card{padding:12px}.card .label{font-size:12px;color:var(--muted)}.card .value{font-size:23px;font-weight:800}.panel{padding:12px;margin-bottom:12px}.panel h2{font-size:15px;margin:0 0 10px}.controls,.quick,.tag-wrap{display:flex;gap:7px;flex-wrap:wrap;align-items:center}input,select,button{height:34px;border:1px solid var(--line);border-radius:8px;padding:0 10px;background:#fff;color:#111827}button{cursor:pointer;font-weight:700}button.active{background:#111827;color:#fff}.type-filter-wrap{display:flex;gap:6px;align-items:center;flex-wrap:wrap}.type-multi{position:relative}.type-multi summary{height:34px;display:flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:8px;background:#fff;padding:0 10px;cursor:pointer;list-style:none;font-size:12px;font-weight:700}.type-multi summary::-webkit-details-marker{display:none}.type-check-list{position:absolute;top:38px;left:0;z-index:30;width:290px;max-height:350px;overflow:auto;background:#fff;border:1px solid var(--line);border-radius:10px;padding:8px;box-shadow:0 12px 30px rgba(0,0,0,.14)}.type-check-row{display:flex;align-items:center;gap:8px;padding:6px;border-radius:6px;font-size:12px;cursor:pointer}.type-check-row:hover{background:#f8fafc}.type-check-row input{height:auto}.type-exclude-row{color:#991b1b}.type-actions{display:flex;gap:6px;padding:4px 4px 8px;border-bottom:1px solid var(--line);margin-bottom:4px}.type-actions button{height:27px;font-size:11px;padding:0 8px}.type-count{display:inline-flex;min-width:18px;height:18px;align-items:center;justify-content:center;border-radius:999px;background:#e5e7eb;font-size:10px}.summary-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;align-items:stretch}.summary-item{border:1px solid var(--line);border-radius:10px;padding:10px;background:#fff;min-width:0}.summary-item .name{font-weight:800}.summary-item .meta{font-size:12px;color:var(--muted);margin:3px 0 6px}.summary-item .txt{font-size:12px;line-height:1.55}.badge{display:inline-block;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:800}.badge-now{background:#dcfce7;color:#166534}.badge-hot{background:#fee2e2;color:#991b1b}.badge-accel{background:#ffedd5;color:#9a3412}.badge-rev{background:#ede9fe;color:#5b21b6}.badge-strong{background:#dbeafe;color:#1e40af}.badge-bottom{background:#fef3c7;color:#92400e}.badge-supply{background:#e0f2fe;color:#075985}.badge-lowrisk{background:#ecfdf5;color:#065f46;border:1px solid #a7f3d0}.code-copy{height:auto!important;min-height:0!important;padding:1px 5px!important;border:0!important;background:transparent!important;color:#1d4ed8!important;font-weight:900!important;text-decoration:underline!important;text-underline-offset:2px;cursor:pointer;border-radius:4px!important}.code-copy:hover{background:#eff6ff!important}.bookmark-btn{height:26px!important;min-width:28px!important;padding:0 6px!important;border:1px solid #d1d5db!important;background:#fff!important;color:#9ca3af!important;border-radius:7px!important;font-size:16px!important;line-height:1!important;cursor:pointer}.bookmark-btn:hover{background:#fffbeb!important;color:#d97706!important;border-color:#f59e0b!important}.bookmark-btn.on{background:#fff7ed!important;color:#f59e0b!important;border-color:#f59e0b!important}.bookmark-cell{text-align:center!important}.bookmarked-row td{background:#fffdf5}.bookmarked-row:hover td{background:#fff9e8}.summary-item.bookmarked{border-color:#f59e0b;box-shadow:0 0 0 1px rgba(245,158,11,.08)}.summary-head{display:flex;align-items:center;gap:6px;justify-content:space-between}.summary-head-left{min-width:0}.bookmark-note{font-size:11px;color:#92400e;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:6px 8px}.filter-preset-wrap{display:flex;gap:6px;align-items:center;flex-wrap:wrap;padding:5px 7px;border:1px solid #dbe3ee;border-radius:9px;background:#f8fafc}.filter-preset-wrap .preset-label{font-size:11px;font-weight:800;color:#475569}.filter-preset-wrap select{min-width:180px;max-width:260px}.preset-save{background:#111827!important;color:#fff!important;border-color:#111827!important}.preset-apply{background:#ecfdf5!important;color:#166534!important;border-color:#a7f3d0!important}.preset-delete{background:#fff1f2!important;color:#9f1239!important;border-color:#fecdd3!important}.copy-toast{position:fixed;left:50%;bottom:28px;transform:translate(-50%,18px);background:#111827;color:#fff;padding:9px 14px;border-radius:999px;font-size:12px;font-weight:800;opacity:0;pointer-events:none;transition:.18s;z-index:80;box-shadow:0 8px 24px rgba(0,0,0,.18)}.copy-toast.show{opacity:1;transform:translate(-50%,0)}.spark-wrap{width:340px;height:58px;display:flex;align-items:center;justify-content:center}.spark-svg{width:334px;height:54px;display:block;overflow:visible}.spark-grid{stroke:#e5e7eb;stroke-width:.6}.spark-base{stroke:#94a3b8;stroke-width:1;stroke-dasharray:2 2}.spark-event{stroke:#7c3aed;stroke-width:1.15;stroke-dasharray:2 1.5}.spark-event-label{font-size:6.5px;fill:#6d28d9;font-weight:800;font-family:system-ui,-apple-system,"Segoe UI","Noto Sans JP",sans-serif}.spark-pre-bg{fill:#f8fafc}.spark-line{fill:none;stroke:#1e293b;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;opacity:1}.spark-wick{stroke-width:.55;opacity:.25}.spark-body{stroke-width:.45;opacity:.28}.candle-up{stroke:#b91c1c;fill:#fee2e2}.candle-down{stroke:#1d4ed8;fill:#dbeafe}.candle-flat{stroke:#64748b;fill:#f1f5f9}.spark-point{fill:#fff;stroke:#334155;stroke-width:.8}.spark-point.key{fill:#334155}.spark-last{fill:#111827;stroke:#fff;stroke-width:1}.spark-label{font-size:6.5px;fill:#64748b;font-family:system-ui,-apple-system,"Segoe UI","Noto Sans JP",sans-serif}.spark-value{font-size:7px;fill:#111827;font-weight:800;font-family:system-ui,-apple-system,"Segoe UI","Noto Sans JP",sans-serif}.pager{display:flex;gap:7px;align-items:center;justify-content:flex-end;flex-wrap:wrap;margin:0 0 9px}.pager .page-info{font-size:12px;color:var(--muted);min-width:210px;text-align:center}.pager select,.pager button{height:30px}.table-wrap{overflow:auto;max-height:72vh;border:1px solid var(--line);border-radius:10px}table{width:max-content;min-width:100%;border-collapse:separate;border-spacing:0;font-size:12px}th,td{padding:7px 8px;border-bottom:1px solid var(--line);border-right:1px solid #f1f5f9;white-space:nowrap;background:#fff}th{position:sticky;top:0;z-index:3;background:#f9fafb;text-align:left}tr:hover td{background:#f8fafc}.num{text-align:right;font-variant-numeric:tabular-nums}.pos{color:#b91c1c;font-weight:700}.neg{color:#1d4ed8;font-weight:700}.score-high{color:#047857;font-weight:900}.score-mid{color:#b45309;font-weight:800}.heat-high{color:#b91c1c;font-weight:900}td.summary-cell{white-space:normal;min-width:440px;max-width:620px;line-height:1.45}td.long-cell{white-space:normal;max-width:430px}.help-overlay{position:fixed;inset:0;background:rgba(15,23,42,.28);opacity:0;pointer-events:none;transition:.18s;z-index:50}.help-overlay.open{opacity:1;pointer-events:auto}.help-drawer{position:fixed;top:0;right:0;width:min(720px,94vw);height:100vh;background:#fff;box-shadow:-12px 0 36px rgba(0,0,0,.18);transform:translateX(102%);transition:.22s;z-index:51;display:flex;flex-direction:column}.help-drawer.open{transform:translateX(0)}.help-head{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;border-bottom:1px solid var(--line)}.help-head h2{margin:0;font-size:18px}.help-close{border:0;background:#f3f4f6;width:34px;padding:0;font-size:18px}.help-tabs{display:flex;gap:6px;flex-wrap:wrap;padding:10px 14px;border-bottom:1px solid var(--line)}.help-tab.active{background:#111827;color:#fff}.help-body{padding:16px;overflow:auto;font-size:13px;line-height:1.7}.help-pane{display:none}.help-pane.active{display:block}.help-grid{display:grid;gap:10px}.help-item{border:1px solid var(--line);border-radius:10px;padding:10px}.help-item b{display:block}.help-note{background:#f8fafc;border-left:4px solid #94a3b8;padding:9px 10px;margin:10px 0}.small{font-size:11px;color:var(--muted)}@media(max-width:1200px){.cards{grid-template-columns:repeat(4,1fr)}}@media(max-width:800px){.cards{grid-template-columns:repeat(2,1fr)}.summary-grid{grid-template-columns:1fr}}

@media(max-width:1250px){.summary-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:760px){.summary-grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="title-row"><div><h1>決算後上昇ダッシュボード</h1><div class="sub">生成: __GENERATED__ / 直近__DAYS__日 / HTMLは掲載対象のみ / 全母集団はCSV保存 / 上昇継続枠: +__MINRET__%以上・高値から-__MAXDD__%以内 / 下げ止まり枠: 現在__BOTTOMMIN__%以上・高値から-__BOTTOMMAX__%以内</div></div><button id="helpOpen" class="help-open">？ 見方・基準</button></div>
<div class="cards"><div class="card"><div class="label">全母集団</div><div class="value">__FULL_COUNT__</div></div><div class="card"><div class="label">掲載銘柄</div><div id="cAll" class="value">-</div></div><div class="card"><div class="label">足切り除外</div><div class="value">__EXCLUDED_COUNT__</div></div><div class="card"><div class="label">従来候補</div><div id="cCandidate" class="value">-</div></div><div class="card"><div class="label">D1価格復元</div><div id="cD1Rebuilt" class="value">-</div></div><div class="card"><div class="label">当日発表</div><div id="cTodayRelease" class="value">-</div></div><div class="card"><div class="label">ラベル未作成</div><div id="cNoLabel" class="value">-</div></div><div class="card"><div class="label">解析不可</div><div id="cUnusable" class="value">-</div></div><div class="card"><div class="label">今から85+</div><div id="cNow" class="value">-</div></div><div class="card"><div class="label">再評価</div><div id="cRev" class="value">-</div></div><div class="card"><div class="label">下げ止まり</div><div id="cBottom" class="value">-</div></div><div class="card"><div class="label">反発初動</div><div id="cRebound" class="value">-</div></div><div class="card"><div class="label">伸び切り</div><div id="cHot" class="value">-</div></div><div class="card"><div class="label">強さ70+</div><div id="cStrong" class="value">-</div></div><div class="card"><div class="label">🛡️短期低リスク</div><div id="cLowRisk" class="value">-</div></div><div class="card"><div class="label">🚀値幅×低リスク</div><div id="cRangeLowRisk" class="value">-</div></div><div class="card"><div class="label">ブックマーク</div><div id="cBookmarks" class="value">-</div></div><div class="card"><div class="label">表示中</div><div id="cVisible" class="value">-</div></div></div>
<div class="panel"><h2>絞り込み</h2><div class="controls"><input id="q" type="text" placeholder="コード・銘柄名・タイプ・需給・サマリー検索" style="width:300px"><div class="type-filter-wrap"><details class="type-multi"><summary>含むタイプ <span id="includeCount" class="type-count">0</span></summary><div id="typeIncludeList" class="type-check-list"></div></details><details class="type-multi"><summary>除外タイプ <span id="excludeCount" class="type-count">0</span></summary><div id="typeExcludeList" class="type-check-list"></div></details></div><div class="type-filter-wrap supply-filter-wrap">
<details class="type-multi"><summary>含む需給タグ <span id="supplyIncludeCount" class="type-count">0</span></summary><div id="supplyIncludeList" class="type-check-list"></div></details>
<details class="type-multi"><summary>除外需給タグ <span id="supplyExcludeCount" class="type-count">0</span></summary><div id="supplyExcludeList" class="type-check-list"></div></details>
</div><label>今から ≥ <input id="minEntry" type="number" value="0" min="0" max="100" style="width:72px"></label><label>強さ ≥ <input id="minStrength" type="number" value="0" min="0" max="100" style="width:72px"></label><label>過熱 ≤ <input id="maxHeat" type="number" value="100" min="0" max="100" style="width:72px"></label><label>全期間GU率 ≥ <input id="minGuAll" type="number" value="0" min="0" max="100" step="1" style="width:72px"></label><label>直近10日GU率 ≥ <input id="minGu10" type="number" value="0" min="0" max="100" step="10" style="width:72px"></label><label>GU加速差 ≥ <input id="minGuAccel" type="number" value="-100" min="-100" max="100" step="5" style="width:72px"></label><label>全期間高値圏引け率 ≥ <input id="minCloseAll" type="number" value="0" min="0" max="100" step="1" style="width:72px"></label><label>直近10日高値圏引け率 ≥ <input id="minClose10" type="number" value="0" min="0" max="100" step="10" style="width:72px"></label><select id="sortBy"><option value="今からスコア">今から向き順</option><option value="今ならまだ間に合う順位">今ならまだ間に合う順位</option><option value="値幅×低リスク順位">値幅×低リスク順位</option><option value="強さスコア">トレンド強さ順</option><option value="安定上昇スコア">安定上昇順</option><option value="連騰加速スコア">連騰加速順</option><option value="下げ止まりスコア">下げ止まり順</option><option value="過熱度">伸び切り順</option><option value="決算後騰落率">決算後上昇率順</option><option value="高値から乖離率">決算後高値に近い順</option><option value="信用需給負荷スコア">信用負荷が軽い順</option><option value="全期間GU率">全期間GU率順</option><option value="直近10日GU率">直近10日GU率順</option><option value="GU加速差">GU加速差順</option><option value="全期間高値圏引け率">全期間高値圏引け率順</option><option value="直近10日高値圏引け率">直近10日高値圏引け率順</option></select>
<div class="filter-preset-wrap">
<span class="preset-label">絞込条件</span>
<select id="filterPresetSelect"><option value="">保存した条件...</option></select>
<button type="button" id="filterPresetSave" class="preset-save">💾 保存</button>
<button type="button" id="filterPresetApply" class="preset-apply">▶ 適用</button>
<button type="button" id="filterPresetDelete" class="preset-delete">🗑 削除</button>
</div>
<div class="quick"><button data-mode="all" class="active">全部</button><button data-mode="candidate">✅ 従来候補</button><button data-mode="reconstructed">♻️ D1価格復元</button><button data-mode="bookmark">★ ブックマーク</button><button data-mode="lowrisk">🛡️ 短期低リスク</button><button data-mode="rangelowrisk">🚀 値幅×低リスク</button><button data-mode="now">🎯 今から</button><button data-mode="rev">🔄 再評価</button><button data-mode="pullback">🟢 押し目</button><button data-mode="bottom">🛬 下げ止まり</button><button data-mode="rebound">↗️ 反発初動</button><button data-mode="cont">🚀 継続</button><button data-mode="accel">🔥 連騰加速</button><button data-mode="stable">🪜 安定上昇</button><button data-mode="hot">🔥 伸び切り</button><button data-mode="strong">💪 強い</button></div></div></div>
<div class="panel"><h2>上位サマリー</h2><div id="summaryGrid" class="summary-grid"></div></div><div class="panel"><h2>ダッシュボード掲載銘柄</h2><div class="pager"><label class="small">1ページ <select id="pageSize"><option value="25">25件</option><option value="50" selected>50件</option><option value="100">100件</option><option value="200">200件</option></select></label><button type="button" id="pageFirst">≪</button><button type="button" id="pagePrev">‹ 前</button><span id="pageInfo" class="page-info">-</span><button type="button" id="pageNext">次 ›</button><button type="button" id="pageLast">≫</button><button type="button" id="downloadPageCsv" style="background:#0f766e;color:#fff;border-color:#0f766e">今のページの銘柄をCSVダウンロード</button></div><div class="table-wrap"><table><thead><tr id="thead"></tr></thead><tbody id="tbody"></tbody></table></div></div></div>
<div id="copyToast" class="copy-toast">コードをコピーしました</div><div id="helpOverlay" class="help-overlay"></div><aside id="helpDrawer" class="help-drawer" aria-hidden="true"><div class="help-head"><h2>ダッシュボードの見方</h2><button id="helpClose" class="help-close">×</button></div><div class="help-tabs"><button class="help-tab active" data-help-tab="types">タイプ</button><button class="help-tab" data-help-tab="sort">並び順</button><button class="help-tab" data-help-tab="args">引数</button><button class="help-tab" data-help-tab="supply">信用・需給</button></div><div class="help-body">
<div class="help-pane active" data-help-pane="types"><div class="help-note"><b>🚀 値幅×低リスク / 専用順位</b>まず🛡️短期低リスクに合格した銘柄だけを対象に、決算後最大上昇幅25点＋最大1日上昇率10点＋60日上昇5点＋短期低リスク品質20点＋120日最大DD20点＋高値維持10点＋低過熱10点で再採点します。合格は70点以上、決算後最大+8%以上、最大1日+7%以上、120日最大DD-20%以内、高値から-3%以内、過熱35以下。一日+20%超は「一発急騰依存」として3点減点します。信用・出来高・機関空売りは使いません。<br>🚀ボタンを押すと合格銘柄だけを専用順位で表示します。</div><div class="help-note"><b>🛡️ 短期低リスク / 今ならまだ間に合う順位</b>信用・出来高・機関空売りは順位に入れません。今からスコア30%＋強さ20%＋高値維持20点＋低過熱15点＋MA5位置10点＋直近5日5点で採点し、85点以上かつ「決算前比+2%以上・高値から-4%以内・MA5乖離-1.5～+5%・過熱45以下・直近5日-3～+10%・伸び切りでない」を満たす銘柄だけに順位を付けます。<br>🛡️ボタンを押すと合格銘柄だけを順位の若い順に表示します。</div><div class="help-note"><b>今のページCSV</b>現在の絞り込み・並び順・ページ番号をそのまま反映し、画面に表示中の25/50/100/200件だけをCSV保存します。出力形式は既存スクリーナーの「今のページの銘柄をCSVダウンロード」と同じ <b>"コード","銘柄名",TKY,,,,,,</b>（ヘッダーなし・UTF-8 BOM付き）です。★ブックマーク表示中なら、そのページに見えているブックマーク銘柄だけが対象です。</div><div class="help-note"><b>v30 高速UI</b>一覧は初期50件だけ描画します。絞り込み自体は全掲載銘柄を対象にしますが、毎回数百行×全列×ミニチャートを再生成しません。25/50/100/200件に変更可能。検索・数値入力は140ms待ってから反映し、ミニチャートSVGも一度作れば再利用します。<br><b>v25 軽量ダッシュボード</b>全母集団は <b>決算後上昇_全母集団.csv</b> に保存し、HTMLへ埋め込むのはD1以降を評価できる掲載対象銘柄だけです。「監視」と「D1待ち」はHTMLへ入れません。除外銘柄も <b>決算後上昇_足切り除外.csv</b> に足切り理由付きで残ります。HTMLの「全母集団」「足切り除外」は件数だけで、除外銘柄本体はJavaScript DATAへ入れません。<br><b>D1自動復元・0時跨ぎ修正</b>v24から、reaction_labels未作成でもprice_historyからD1/D3/D5を復元します。15:30より前の時刻付き発表は発表日をD1、15:30以降は次営業日をD1。日付しか無い履歴は保守的に次営業日をD1とします。また市場開始前は翌日の日付で現在値を追加しないため、0時台に「前日終値を今日の終値として複製」することがありません。D1復元の監査情報はCSVに保存し、HTML一覧では省略します。<br><b>真の発表済み母集団</b>v23から、TDnet決算短信・earnings_events・TDnet XBRL・quarterly_actual_history・reaction_labelsを統合します。reaction_labelsはD1/D3/D5の後付け情報として扱うため、今日決算を出してまだラベルが無い銘柄もD1待ちで残ります。発表済み判定の根拠は全母集団CSVに保存し、HTML一覧では省略します。<br><b>母集団フル保持</b>v22から、直近期間の発表済み決算は「上昇継続・下げ止まり・反発初動」に該当しなくてもPython側で捨てません。「監視」と「D1待ち」はCSVにだけ残し、HTMLからは除外します。価格や基準終値不足の「解析不可」もCSV側に残します。「✅ 従来候補」を押すと旧来の候補集合だけ再現できます。母集団ソースは複数DBを統合しています。<br><b>絞り込み条件ブックマーク</b>タイプの含む/除外、需給タグの含む/除外、各数値条件、並び順、クイックモード、検索文字をまとめてlocalStorageへ名前付き保存できます。「💾 保存」→名前を付け、あとで保存条件を選んで「▶ 適用」。不要になったら「🗑 削除」。HTMLを再生成しても同じブラウザなら残ります。<br><b>銘柄ブックマーク</b>一覧または上位サマリーの☆を押すと★になり、ブラウザのlocalStorageへコード番号を保存します。HTMLを再生成しても同じブラウザなら残ります。「★ ブックマーク」ボタンで保存銘柄だけ表示できます。<br><b>複数タイプ絞り込み</b>「含むタイプ」は複数選択するとOR条件で、どれか1つに当てはまれば表示します。「除外タイプ」はNOT条件で、1つでも該当すれば除外します。含む・除外は同時に使えます。<br><b>価格基準</b>今から・強さ・過熱・下げ止まり・反発初動・再評価・押し目・継続・伸び切りなど、価格に関係する全スコア/タイプは実行時点の <b>screener.現在値</b> を最優先して再計算します。今日のprice_historyが既にあっても終値を最新現在値へ更新してから計算します。<br><b>コード番号</b>青いコード番号をクリックすると、そのコードをクリップボードへコピーします。<br><b>ミニチャート</b>終値ラインを太く主役にし、ローソク足は薄い補助表示にしています。決算前5営業日→決算→D1から現在までを連続表示。最後の決算前終値を0%基準にし、左の薄い領域が決算前、紫の縦線が決算境界です。各営業日はOHLC極小ローソク＋終値ライン。これで決算前から買われていたか、決算後に初めて評価されたかを見分けられます。</div><div class="help-grid"><div class="help-item"><b>🎯 今から候補</b>今からスコア85点以上。高値圏・MA5/25との距離・上がり過ぎ・直近5日・経過日数を評価。</div><div class="help-item"><b>🔄 再評価</b>D1マイナス→現在+5%以上、高値から-4%以内まで回復。</div><div class="help-item"><b>🟢 高値圏の押し目</b>決算後+8%以上、高値-3%以内、MA5乖離0～+4%、直近5日-2～+6%。</div><div class="help-item"><b>🚀 継続上昇</b>D1から強く、その後も高値圏。</div><div class="help-item"><b>⚡ 連騰気配</b>連騰加速スコア70～84点。短期の上向きは明確だが、強烈な加速までは未到達。</div><div class="help-item"><b>🔥 連騰加速</b>4317型。直近5営業日の上昇日数、3/5日騰落率、MA5&gt;MA25、MA5の上向き、高値更新回数、高値圏維持を評価。70～84点は「⚡ 連騰気配」、85点以上で「🔥 連騰加速」。継続上昇・強い・伸び切りなどと同時に付きます。</div><div class="help-item"><b>🪜 安定上昇</b>5074型。決算後だけでなく最大120営業日を見て、60/120日上昇、MA25/75の上向き、MA25上の滞在率、安値切り上げ、最大ドローダウン、一日急騰への依存を評価。70点以上で付与。🚀継続上昇と同時に付くこともあります。</div><div class="help-item"><b>🛬 下げ止まり気配</b>実際に-2.5%以上の押し、または直近10日で-3%以上の押し＋MA5割れを経験した銘柄だけ採点。さらに直近5日の加速を制限し、70点以上で付与。連騰中の誤検出を減らした。</div><div class="help-item"><b>↗️ 反発初動</b>下げ止まり気配に加え直近1日・3日がプラス、MA5近辺まで戻したもの。</div><div class="help-item"><b>💪 強い</b>強さ70点以上。単純な上昇率ではなく、D1→D3→D5→現在の継続、高値維持、MA5/25、直近5/10日を重視。単発急騰より右肩上がりを上位にします。</div><div class="help-item"><b>🔥 伸び切り</b>過熱60点以上、決算後+30%以上、MA5+8%以上、MA25+20%以上、直近5日+15%以上のどれか。</div></div></div>
<div class="help-pane" data-help-pane="sort"><div class="help-note"><b>高値圏引け率</b>完全な「終値=高値」ではなく、終値が当日値幅の上位20%以内かつ終値≥始値の日を「強い高値圏引け」とします。全期間と直近10日の2軸で表示・絞り込みできます。</div><div class="help-note"><b>GU率</b>GUは「当日始値 &gt; 前営業日終値」。全期間GU率はprice_history保存全履歴、直近10日GU率は最新10営業日。GU加速差=直近10日GU率−全期間GU率。+20pt以上なら最近GUが明確に増えた、と見やすい。</div><div class="help-grid"><div class="help-item"><b>今から向き順</b>今からスコア高い順。同点は低過熱→高値に近い→強さ。</div><div class="help-item"><b>トレンド強さ順</b>右肩上がりの継続性を重視。D1→D3→D5→現在、高値維持、MA5/25、直近5/10日を総合評価。単なる決算後上昇率順とは別。</div><div class="help-item"><b>安定上昇順</b>急騰の大きさより「長く崩れず上がった質」を優先。5074のような階段状の上昇を上位にします。</div><div class="help-item"><b>連騰加速順</b>直近の連騰・高値更新が加速している順。4317のような保ち合い抜け後の連続上昇を上位にします。</div><div class="help-item"><b>下げ止まり順</b>安値切上げ・短期鈍化・MA25近辺・調整幅の総合点。</div><div class="help-item"><b>伸び切り順</b>過熱度の高い順。</div><div class="help-item"><b>決算後上昇率順</b>決算前→現在の上昇率。</div><div class="help-item"><b>決算後高値に近い順</b>高値乖離0%に近い順。</div><div class="help-item"><b>信用負荷が軽い順</b>信用需給負荷スコアを低い順。空欄は最後。</div><div class="help-note">今から100点=上昇確率100%ではなく、ルール適合度の上限。</div></div></div>
<div class="help-pane" data-help-pane="args"><div class="help-note"><b>標準おすすめ</b>--days 60 --min-return 5 --max-drawdown 4</div><div class="help-grid"><div class="help-item"><b>--days 60</b>直近60暦日の決算。再評価・下げ止まり・遅れて始まる上昇まで拾うため、候補数を優先して60日を標準にしています。</div><div class="help-item"><b>--min-return 5</b>上昇継続枠は決算前より現在+5%以上。1～2%のノイズより、実際に評価された動きを優先。広げるなら2～3。</div><div class="help-item"><b>--max-drawdown 4</b>上昇継続枠は決算後高値から-4%以内。初動だけ上がって崩れたものを除き、高値圏維持を重視。緩めるなら7～8。</div><div class="help-item"><b>下げ止まり枠</b>通常枠とは別に既定で現在0%以上・高値から-12%以内。--bottom-min-return / --bottom-max-drawdownで変更可能。</div></div></div>
<div class="help-pane" data-help-pane="supply"><div class="help-note"><b>需給タグ複数絞り込み</b>「含む需給タグ」は複数選択するとOR条件で、どれか1つに当てはまれば表示します。「除外需給タグ」はNOT条件で、1つでも該当すれば除外します。含む・除外は同時使用できます。例: 「🟦 信用軽い OR 🟩 需給安全」を含み、「⚠️ 信用買い重い」を除外。<br>信用・需給は価格タイプとは別軸。今から/強さスコアには混ぜず、判断材料として表示。</div><div class="help-grid"><div class="help-item"><b>信用倍率</b>買い残÷売り残。極端に高いと買い長。</div><div class="help-item"><b>買い残/浮動株%</b>1%未満は軽め、5%以上は買い過多を意識、10%以上はかなり重い目安。</div><div class="help-item"><b>需給OH</b>買い残を平均出来高で消化する目安日数。3日以下は安全条件の一つ。</div><div class="help-item"><b>信用需給負荷スコア</b>浮動株比率・需給OH・20日買い残増加率の統合。高いほど重い。</div><div class="help-item"><b>踏み上げ期待スコア</b>75点以上を踏み上げ余地タグ。</div><div class="help-item"><b>機関空売り</b>各銘柄の最新日を集計。増減プラス=空売り増、マイナス=買戻し方向。</div></div></div></div></aside>
<script>
const DATA=__DATA_JSON__;const COLS=["★","コード","銘柄名","市場","現在値","今ならまだ間に合う順位","値幅×低リスク順位","ミニチャート","発表日時","決算種別","採用枠","従来候補フラグ","今からスコア","強さスコア","安定上昇スコア","連騰加速スコア","下げ止まりスコア","過熱度","タイプ","需給タグ","全期間GU率","直近10日GU率","GU加速差","全期間高値圏引け率","直近10日高値圏引け率","決算後騰落率","決算後最大上昇率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近1日騰落率","直近3日騰落率","直近5日騰落率","直近10日騰落率","決算後営業日数","信用倍率","信用買い残_浮動株比率","信用買い残増減率_20d","需給OH","信用需給負荷スコア","踏み上げ期待スコア","売り残","買い残","機関空売り合計株数","本日の増減合計株数","主要機関の動き","GCフラグ","三役好転フラグ","サマリー"];const NUMS=new Set(["今ならまだ間に合う順位","値幅×低リスク順位","値幅×低リスクスコア","短期低リスクスコア","直近10日高値圏引け率","全期間高値圏引け率","GU加速差","直近10日GU率","全期間GU率","現在値","今からスコア","強さスコア","安定上昇スコア","連騰加速スコア","下げ止まりスコア","過熱度","決算後騰落率","決算後最大上昇率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近1日騰落率","直近3日騰落率","直近5日騰落率","直近10日騰落率","決算後営業日数","信用倍率","信用買い残_浮動株比率","信用買い残増減率_20d","需給OH","信用需給負荷スコア","踏み上げ期待スコア","売り残","買い残","機関空売り合計株数","本日の増減合計株数","GCフラグ","三役好転フラグ"]);let mode="all";
const BOOKMARK_KEY="earnings_after_dashboard_bookmarks_v1";
function loadBookmarks(){
  try{
    const raw=localStorage.getItem(BOOKMARK_KEY);
    const arr=raw?JSON.parse(raw):[];
    return new Set(Array.isArray(arr)?arr.map(String):[]);
  }catch(e){return new Set()}
}
let BOOKMARKS=loadBookmarks();
function saveBookmarks(){
  try{localStorage.setItem(BOOKMARK_KEY,JSON.stringify([...BOOKMARKS]))}catch(e){}
}
function isBookmarked(code){return BOOKMARKS.has(String(code??""))}
function bookmarkButton(code){
  const s=String(code??"");
  const on=isBookmarked(s);
  return `<button type="button" class="bookmark-btn ${on?"on":""}" data-bookmark-code="${esc(s)}" title="${on?"ブックマーク解除":"ブックマーク追加"}">${on?"★":"☆"}</button>`;
}
function toggleBookmark(code){
  const s=String(code??"");
  if(!s)return;
  if(BOOKMARKS.has(s))BOOKMARKS.delete(s);else BOOKMARKS.add(s);
  saveBookmarks();
  apply(false);
}

const FILTER_PRESET_KEY="earnings_after_dashboard_filter_presets_v1";

function loadFilterPresets(){
  try{
    const raw=localStorage.getItem(FILTER_PRESET_KEY);
    const obj=raw?JSON.parse(raw):{};
    return obj && typeof obj==="object" && !Array.isArray(obj) ? obj : {};
  }catch(e){return {}}
}
let FILTER_PRESETS=loadFilterPresets();

function saveFilterPresets(){
  try{localStorage.setItem(FILTER_PRESET_KEY,JSON.stringify(FILTER_PRESETS))}catch(e){}
}

function captureFilterState(){
  return {
    q: document.getElementById("q").value || "",
    typeInclude: selectedTypes("include"),
    typeExclude: selectedTypes("exclude"),
    supplyInclude: selectedSupply("include"),
    supplyExclude: selectedSupply("exclude"),
    minEntry: document.getElementById("minEntry").value,
    minStrength: document.getElementById("minStrength").value,
    maxHeat: document.getElementById("maxHeat").value,
    minGuAll: document.getElementById("minGuAll").value,
    minGu10: document.getElementById("minGu10").value,
    minGuAccel: document.getElementById("minGuAccel").value,
    minCloseAll: document.getElementById("minCloseAll").value,
    minClose10: document.getElementById("minClose10").value,
    sortBy: document.getElementById("sortBy").value,
    mode: mode
  };
}

function setChecks(selector, values){
  const wanted=new Set((values||[]).map(String));
  document.querySelectorAll(selector).forEach(cb=>{cb.checked=wanted.has(String(cb.value))});
}

function setModeButton(nextMode){
  mode=nextMode || "all";
  let found=false;
  document.querySelectorAll("button[data-mode]").forEach(b=>{
    const on=b.dataset.mode===mode;
    b.classList.toggle("active",on);
    if(on)found=true;
  });
  if(!found){
    mode="all";
    document.querySelectorAll("button[data-mode]").forEach(b=>b.classList.toggle("active",b.dataset.mode==="all"));
  }
}

function applyFilterState(s){
  if(!s || typeof s!=="object")return;

  document.getElementById("q").value=s.q ?? "";
  setChecks('input[data-type-kind="include"]',s.typeInclude);
  setChecks('input[data-type-kind="exclude"]',s.typeExclude);
  setChecks('input[data-supply-kind="include"]',s.supplyInclude);
  setChecks('input[data-supply-kind="exclude"]',s.supplyExclude);

  const values={
    minEntry:s.minEntry ?? 0,
    minStrength:s.minStrength ?? 0,
    maxHeat:s.maxHeat ?? 100,
    minGuAll:s.minGuAll ?? 0,
    minGu10:s.minGu10 ?? 0,
    minGuAccel:s.minGuAccel ?? -100,
    minCloseAll:s.minCloseAll ?? 0,
    minClose10:s.minClose10 ?? 0
  };
  Object.entries(values).forEach(([id,v])=>{document.getElementById(id).value=v});

  if(s.sortBy && [...document.getElementById("sortBy").options].some(o=>o.value===s.sortBy)){
    document.getElementById("sortBy").value=s.sortBy;
  }
  setModeButton(s.mode || "all");
  updateTypeCounts();
  updateSupplyCounts();
  apply();
}

function refreshFilterPresetSelect(selectedName=""){
  const sel=document.getElementById("filterPresetSelect");
  const prev=selectedName || sel.value || "";
  sel.innerHTML='<option value="">保存した条件...</option>';
  Object.keys(FILTER_PRESETS).sort((a,b)=>a.localeCompare(b,"ja")).forEach(name=>{
    const o=document.createElement("option");
    o.value=name;
    o.textContent=name;
    sel.appendChild(o);
  });
  if(prev && Object.prototype.hasOwnProperty.call(FILTER_PRESETS,prev))sel.value=prev;
}

function saveCurrentFilterPreset(){
  const sel=document.getElementById("filterPresetSelect");
  const suggested=sel.value || `条件${Object.keys(FILTER_PRESETS).length+1}`;
  const name=(window.prompt("この絞り込み条件の名前",suggested)||"").trim();
  if(!name)return;
  if(Object.prototype.hasOwnProperty.call(FILTER_PRESETS,name) && !window.confirm(`「${name}」を上書きしますか？`))return;
  FILTER_PRESETS[name]=captureFilterState();
  saveFilterPresets();
  refreshFilterPresetSelect(name);
}

function applySelectedFilterPreset(){
  const name=document.getElementById("filterPresetSelect").value;
  if(!name || !FILTER_PRESETS[name])return;
  applyFilterState(FILTER_PRESETS[name]);
}

function deleteSelectedFilterPreset(){
  const sel=document.getElementById("filterPresetSelect");
  const name=sel.value;
  if(!name || !FILTER_PRESETS[name])return;
  if(!window.confirm(`保存条件「${name}」を削除しますか？`))return;
  delete FILTER_PRESETS[name];
  saveFilterPresets();
  refreshFilterPresetSelect();
}

function vnum(v){if(v===null||v===undefined||v==="")return null;const n=Number(v);return Number.isFinite(n)?n:null}function esc(s){return String(s??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;")}function fmt(v,c){if(v===null||v===undefined||v===""||String(v)==="nan")return"";if(NUMS.has(c)){const n=Number(v);if(!Number.isFinite(n))return esc(v);if(["今ならまだ間に合う順位","値幅×低リスク順位","決算後営業日数","GCフラグ","三役好転フラグ"].includes(c))return String(Math.round(n));if(["売り残","買い残","機関空売り合計株数","本日の増減合計株数"].includes(c))return Math.round(n).toLocaleString();return n.toFixed(1)}return esc(v)}function cls(v,c){const n=Number(v);if(c==="今ならまだ間に合う順位"||c==="値幅×低リスク順位"){if(n>0&&n<=10)return"score-high";if(n<=30)return"score-mid"}if(c==="今からスコア"){if(n>=85)return"score-high";if(n>=70)return"score-mid"}if(c==="強さスコア"||c==="安定上昇スコア"||c==="連騰加速スコア"||c==="下げ止まりスコア"){if(n>=70)return"score-high";if(n>=60)return"score-mid"}if(c==="過熱度"&&n>=60)return"heat-high";if(["決算後騰落率","決算後最大上昇率","D1","D3","D5","高値から乖離率","MA5乖離率","MA25乖離率","直近1日騰落率","直近3日騰落率","直近5日騰落率","直近10日騰落率","信用買い残増減率_20d","本日の増減合計株数","GU加速差"].includes(c)){if(n>0)return"pos";if(n<0)return"neg"}return""}
function splitBy(v){return String(v||"").split("｜").map(x=>x.trim()).filter(x=>x&&x!=="-")}function splitTypes(r){return splitBy(r["タイプ"])}function splitSupply(r){return splitBy(r["需給タグ"])}function hasType(r,n){return splitTypes(r).some(t=>t.includes(n))}function badgeOne(t){if(t.includes("値幅×低リスク"))return`<span class="badge badge-accel">${esc(t)}</span>`;if(t.includes("短期低リスク"))return`<span class="badge badge-lowrisk">${esc(t)}</span>`;if(t.includes("D1待ち")||t.includes("D1価格待ち"))return`<span class="badge badge-now">${esc(t)}</span>`;if(t.includes("監視")||t.includes("不足"))return`<span class="badge">${esc(t)}</span>`;if(t.includes("下げ止まり")||t.includes("反発初動"))return`<span class="badge badge-bottom">${esc(t)}</span>`;if(t.includes("今から")||t.includes("押し目"))return`<span class="badge badge-now">${esc(t)}</span>`;if(t.includes("連騰加速"))return`<span class="badge badge-accel">${esc(t)}</span>`;if(t.includes("伸び切り"))return`<span class="badge badge-hot">${esc(t)}</span>`;if(t.includes("再評価"))return`<span class="badge badge-rev">${esc(t)}</span>`;if(t.includes("継続")||t.includes("強い"))return`<span class="badge badge-strong">${esc(t)}</span>`;return`<span class="badge">${esc(t)}</span>`}function badges(r){return`<span class="tag-wrap">${splitTypes(r).map(badgeOne).join("")}</span>`}function supplyBadges(r){return`<span class="tag-wrap">${splitSupply(r).map(t=>`<span class="badge badge-supply">${esc(t)}</span>`).join("")}</span>`}
function selectedTypes(kind){return [...document.querySelectorAll(`input[data-type-kind="${kind}"]:checked`)].map(x=>x.value)}
function updateTypeCounts(){document.getElementById("includeCount").textContent=selectedTypes("include").length;document.getElementById("excludeCount").textContent=selectedTypes("exclude").length}
function selectedSupply(kind){return [...document.querySelectorAll(`input[data-supply-kind="${kind}"]:checked`)].map(x=>x.value)}
function updateSupplyCounts(){document.getElementById("supplyIncludeCount").textContent=selectedSupply("include").length;document.getElementById("supplyExcludeCount").textContent=selectedSupply("exclude").length}
function populateFilters(){
  const types=[...new Set(DATA.flatMap(splitTypes))].sort();
  const supplies=[...new Set(DATA.flatMap(splitSupply))].sort();
  const makeType=(kind,exclude=false)=>`<div class="type-actions"><button type="button" data-type-action="all-${kind}">全選択</button><button type="button" data-type-action="clear-${kind}">解除</button></div>`+types.map(t=>`<label class="type-check-row ${exclude?"type-exclude-row":""}"><input type="checkbox" data-type-kind="${kind}" value="${esc(t)}"><span>${esc(t)}</span></label>`).join("");
  const makeSupply=(kind,exclude=false)=>`<div class="type-actions"><button type="button" data-supply-action="all-${kind}">全選択</button><button type="button" data-supply-action="clear-${kind}">解除</button></div>`+supplies.map(t=>`<label class="type-check-row ${exclude?"type-exclude-row":""}"><input type="checkbox" data-supply-kind="${kind}" value="${esc(t)}"><span>${esc(t)}</span></label>`).join("");
  document.getElementById("typeIncludeList").innerHTML=makeType("include",false);
  document.getElementById("typeExcludeList").innerHTML=makeType("exclude",true);
  document.getElementById("supplyIncludeList").innerHTML=makeSupply("include",false);
  document.getElementById("supplyExcludeList").innerHTML=makeSupply("exclude",true);
  updateTypeCounts();
  updateSupplyCounts();
}
function filterRow(r){const q=(document.getElementById("q").value||"").trim().toLowerCase(),inc=selectedTypes("include"),exc=selectedTypes("exclude"),sinc=selectedSupply("include"),sexc=selectedSupply("exclude"),me=Number(document.getElementById("minEntry").value||0),ms=Number(document.getElementById("minStrength").value||0),mh=Number(document.getElementById("maxHeat").value||100),mga=Number(document.getElementById("minGuAll").value||0),mg10=Number(document.getElementById("minGu10").value||0),mga2=Number(document.getElementById("minGuAccel").value||-100),mca=Number(document.getElementById("minCloseAll").value||0),mc10=Number(document.getElementById("minClose10").value||0);if(q&&!([r["コード"],r["銘柄名"],r["市場"],r["決算タイトル"],r["採用枠"],r["タイプ"],r["需給タグ"],r["信用需給メモ"],r["サマリー"],r["主要機関の動き"]].join(" ").toLowerCase().includes(q)))return false;if(inc.length&&!inc.some(t=>splitTypes(r).includes(t)))return false;if(exc.length&&exc.some(t=>splitTypes(r).includes(t)))return false;if(sinc.length&&!sinc.some(t=>splitSupply(r).includes(t)))return false;if(sexc.length&&sexc.some(t=>splitSupply(r).includes(t)))return false;if((vnum(r["今からスコア"])??0)<me)return false;if((vnum(r["強さスコア"])??0)<ms)return false;if((vnum(r["過熱度"])??0)>mh)return false;if((vnum(r["全期間GU率"])??-1)<mga)return false;if((vnum(r["直近10日GU率"])??-1)<mg10)return false;if((vnum(r["GU加速差"])??-999)<mga2)return false;if((vnum(r["全期間高値圏引け率"])??-1)<mca)return false;if((vnum(r["直近10日高値圏引け率"])??-1)<mc10)return false;if(mode==="candidate"&&Number(r["従来候補フラグ"]||0)!==1)return false;if(mode==="reconstructed"&&String(r["D1ソース"]||"")!=="price_history復元")return false;if(mode==="unusable"&&String(r["母集団区分"]||"")!=="解析不可")return false;if(mode==="bookmark"&&!isBookmarked(r["コード"]))return false;if(mode==="lowrisk"&&Number(r["短期低リスクフラグ"]||0)!==1)return false;if(mode==="rangelowrisk"&&Number(r["値幅×低リスクフラグ"]||0)!==1)return false;if(mode==="now"&&!hasType(r,"今から候補"))return false;if(mode==="rev"&&!hasType(r,"再評価"))return false;if(mode==="pullback"&&!hasType(r,"高値圏の押し目"))return false;if(mode==="bottom"&&!hasType(r,"下げ止まり"))return false;if(mode==="rebound"&&!hasType(r,"反発初動"))return false;if(mode==="cont"&&!hasType(r,"継続上昇"))return false;if(mode==="accel"&&!hasType(r,"連騰加速"))return false;if(mode==="stable"&&!hasType(r,"安定上昇"))return false;if(mode==="hot"&&!hasType(r,"伸び切り"))return false;if(mode==="strong"&&!hasType(r,"強い"))return false;return true}
function cmpDesc(a,b,k){const av=vnum(a[k]),bv=vnum(b[k]);if(av===null&&bv===null)return 0;if(av===null)return 1;if(bv===null)return-1;return bv-av}function cmpAsc(a,b,k){const av=vnum(a[k]),bv=vnum(b[k]);if(av===null&&bv===null)return 0;if(av===null)return 1;if(bv===null)return-1;return av-bv}function sortRows(rows){const k=document.getElementById("sortBy").value;return rows.sort((a,b)=>{if(k==="今ならまだ間に合う順位"||k==="値幅×低リスク順位")return cmpAsc(a,b,k);if(k==="今からスコア")return cmpDesc(a,b,"今からスコア")||cmpAsc(a,b,"過熱度")||cmpDesc(a,b,"高値から乖離率")||cmpDesc(a,b,"強さスコア");if(k==="信用需給負荷スコア")return cmpAsc(a,b,k);return cmpDesc(a,b,k)})}
let copyToastTimer=null;
async function copyCode(code){
  const s=String(code??"").trim();
  if(!s)return;
  let ok=false;
  try{
    if(navigator.clipboard && window.isSecureContext){
      await navigator.clipboard.writeText(s);
      ok=true;
    }
  }catch(e){}
  if(!ok){
    try{
      const ta=document.createElement("textarea");
      ta.value=s;
      ta.setAttribute("readonly","");
      ta.style.position="fixed";
      ta.style.opacity="0";
      ta.style.pointerEvents="none";
      document.body.appendChild(ta);
      ta.select();
      ok=document.execCommand("copy");
      document.body.removeChild(ta);
    }catch(e){}
  }
  const toast=document.getElementById("copyToast");
  if(toast){
    toast.textContent=ok ? `${s} をコピーしました` : `コピーできませんでした: ${s}`;
    toast.classList.add("show");
    if(copyToastTimer)clearTimeout(copyToastTimer);
    copyToastTimer=setTimeout(()=>toast.classList.remove("show"),1400);
  }
}
function codeButton(code){
  const s=String(code??"");
  return `<button type="button" class="code-copy" data-copy-code="${esc(s)}" title="クリックでコードをコピー">${esc(s)}</button>`;
}

function sparkline(vals,dates,ohlc,eventIndex,phase){
  if(!Array.isArray(vals)||vals.length<2)return "";
  const arr=vals.map(Number).filter(Number.isFinite);
  const bars=Array.isArray(ohlc)?ohlc:[];
  const ds=Array.isArray(dates)?dates:[];
  const phases=Array.isArray(phase)?phase:[];
  if(arr.length<2)return "";

  const w=334,h=54,pl=5,pr=28,pt=4,pb=11;
  const all=[0];
  bars.forEach(b=>["o","h","l","c"].forEach(k=>{
    const n=Number(b&&b[k]);
    if(Number.isFinite(n))all.push(n);
  }));
  if(all.length===1)all.push(...arr);

  let mn=Math.min(...all),mx=Math.max(...all);
  if(mx===mn){mx+=1;mn-=1}
  const pad=Math.max((mx-mn)*.10,.25);
  mn-=pad;mx+=pad;

  const range=mx-mn,n=arr.length;
  const x=i=>pl+(w-pl-pr)*(i/Math.max(1,n-1));
  const y=v=>pt+(h-pt-pb)*((mx-v)/range);
  const dx=(w-pl-pr)/Math.max(1,n-1);
  const bodyW=Math.max(1.15,Math.min(3.8,dx*.44));

  const grids=[.25,.5,.75].map(t=>{
    const gy=(pt+(h-pt-pb)*t).toFixed(1);
    return `<line class="spark-grid" x1="${pl}" y1="${gy}" x2="${w-pr}" y2="${gy}"></line>`;
  }).join("");

  const zeroY=y(0);
  const base=(zeroY>=pt&&zeroY<=h-pb)
    ? `<line class="spark-base" x1="${pl}" y1="${zeroY.toFixed(1)}" x2="${w-pr}" y2="${zeroY.toFixed(1)}"></line>`
    : "";

  let ev=Number(eventIndex);
  if(!Number.isFinite(ev))ev=Math.max(0,phases.lastIndexOf("pre"));
  ev=Math.max(0,Math.min(n-2,Math.round(ev)));

  // 決算前領域を薄く塗り、pre→postの切れ目を紫線で表示
  const boundaryX=(x(ev)+x(ev+1))/2;
  const preBg=`<rect class="spark-pre-bg" x="${pl}" y="${pt}" width="${Math.max(0,boundaryX-pl).toFixed(1)}" height="${h-pt-pb}"></rect>`;
  const eventLine=`<line class="spark-event" x1="${boundaryX.toFixed(1)}" y1="${pt}" x2="${boundaryX.toFixed(1)}" y2="${h-pb}"></line>`;
  const eventLabel=`<text class="spark-event-label" x="${boundaryX.toFixed(1)}" y="7" text-anchor="middle">決算</text>`;

  const candles=[],hits=[];
  for(let i=0;i<n;i++){
    const b=bars[i]||{o:i?arr[i-1]:arr[i],h:arr[i],l:arr[i],c:arr[i]};
    const o=Number(b.o),hi=Number(b.h),lo=Number(b.l),c=Number(b.c);
    if(![o,hi,lo,c].every(Number.isFinite))continue;

    const xx=x(i);
    const cls=c>o+.001?"candle-up":(c<o-.001?"candle-down":"candle-flat");
    const top=Math.min(y(o),y(c));
    const bh=Math.max(1,Math.abs(y(o)-y(c)));

    candles.push(
      `<line class="spark-wick ${cls}" x1="${xx.toFixed(1)}" y1="${y(hi).toFixed(1)}" x2="${xx.toFixed(1)}" y2="${y(lo).toFixed(1)}"></line>`+
      `<rect class="spark-body ${cls}" x="${(xx-bodyW/2).toFixed(1)}" y="${top.toFixed(1)}" width="${bodyW.toFixed(1)}" height="${bh.toFixed(1)}" rx=".3"></rect>`
    );

    const label=ds[i]||"";
    const ph=phases[i]==="pre"?"決算前":"決算後";
    const sg=v=>`${v>=0?"+":""}${v.toFixed(2)}%`;
    hits.push(
      `<rect x="${(xx-Math.max(3,dx/2)).toFixed(1)}" y="${pt}" width="${Math.max(6,dx).toFixed(1)}" height="${h-pt-pb}" fill="transparent">`+
      `<title>${esc(label)} ${ph}  始 ${sg(o)} / 高 ${sg(hi)} / 安 ${sg(lo)} / 終 ${sg(c)}</title></rect>`
    );
  }

  const pts=arr.map((v,i)=>`${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const line=`<polyline class="spark-line" points="${pts}"></polyline>`;

  // D1/D3/D5 は決算境界から相対位置で計算
  const d1=ev+1,d3=ev+3,d5=ev+5,lastIdx=n-1;
  const keyIdx=new Set([ev,d1,d3,d5,lastIdx].filter(i=>i>=0&&i<n));
  const points=[...keyIdx].sort((a,b)=>a-b).map(i=>
    `<circle class="${i===lastIdx?"spark-last":"spark-point key"}" cx="${x(i).toFixed(1)}" cy="${y(arr[i]).toFixed(1)}" r="${i===lastIdx?2.3:1.45}"></circle>`
  ).join("");

  const labels=[];
  [
    [Math.max(0,ev-4),"前5"],
    [ev,"前1"],
    [d1,"D1"],
    [d3,"D3"],
    [d5,"D5"],
    [lastIdx,"今"]
  ].forEach(([i,t])=>{
    if(i>=0&&i<n){
      labels.push(`<text class="spark-label" x="${x(i).toFixed(1)}" y="${h-2}" text-anchor="middle">${t}</text>`);
    }
  });

  const last=arr[lastIdx],hi=Math.max(...all),lo=Math.min(...all),sign=last>=0?"+":"";
  const lastText=`<text class="spark-value" x="${w-1}" y="${Math.max(8,Math.min(h-pb-1,y(last)+2)).toFixed(1)}" text-anchor="end">${sign}${last.toFixed(1)}%</text>`;
  const hiText=`<text class="spark-label" x="${pl}" y="7">${hi>=0?"+":""}${hi.toFixed(1)}</text>`;
  const loText=`<text class="spark-label" x="${pl}" y="${h-pb-1}">${lo>=0?"+":""}${lo.toFixed(1)}</text>`;

  // front-run: 前5起点→決算前基準日の変化
  const firstPre=0;
  const preMove=arr[ev]-arr[firstPre];
  const preSign=preMove>=0?"+":"";
  const title=`決算前5日 ${preSign}${preMove.toFixed(1)}% / 決算前基準=0% / 現在 ${sign}${last.toFixed(1)}% / 範囲 ${lo>=0?"+":""}${lo.toFixed(1)}%～${hi>=0?"+":""}${hi.toFixed(1)}%`;

  return `<div class="spark-wrap" title="${esc(title)}"><svg class="spark-svg" viewBox="0 0 ${w} ${h}" aria-label="${esc(title)}">${preBg}${grids}${base}${eventLine}${candles.join("")}${line}${points}${hits.join("")}${eventLabel}${labels.join("")}${hiText}${loText}${lastText}</svg></div>`;
}

const SPARK_CACHE=new Map();
function sparklineCached(r){
  const key=String(r["コード"]??"");
  if(SPARK_CACHE.has(key))return SPARK_CACHE.get(key);
  const h=sparkline(
    r["チャートデータ"],r["チャート日付"],r["チャートOHLC"],
    r["チャート決算境界"],r["チャート区分"]
  );
  SPARK_CACHE.set(key,h);
  return h;
}
function renderSummary(rows){const h=document.getElementById("summaryGrid");h.innerHTML="";rows.slice(0,9).forEach(r=>{const d=document.createElement("div");d.className="summary-item"+(isBookmarked(r["コード"])?" bookmarked":"");d.innerHTML=`<div class="summary-head"><div class="summary-head-left"><div class="name">${codeButton(r["コード"])} ${esc(r["銘柄名"])} <span class="small">${esc(r["市場"]||"")} / ${fmt(r["現在値"],"現在値")}円</span></div></div>${bookmarkButton(r["コード"])}</div><div class="meta">${vnum(r["値幅×低リスク順位"])!==null?`🚀順位 <b>#${fmt(r["値幅×低リスク順位"],"値幅×低リスク順位")}</b> / 値幅低リスク <b>${fmt(r["値幅×低リスクスコア"],"値幅×低リスクスコア")}</b> / `:""}${vnum(r["今ならまだ間に合う順位"])!==null?`🛡️順位 <b>#${fmt(r["今ならまだ間に合う順位"],"今ならまだ間に合う順位")}</b> / 低リスク <b>${fmt(r["短期低リスクスコア"],"短期低リスクスコア")}</b> / `:""}${badges(r)} ${supplyBadges(r)} 今から <b>${fmt(r["今からスコア"],"今からスコア")}</b> / 強さ <b>${fmt(r["強さスコア"],"強さスコア")}</b> / 底 <b>${fmt(r["下げ止まりスコア"],"下げ止まりスコア")}</b></div><div class="txt">${esc(r["サマリー"])}</div>`;h.appendChild(d)})}let CURRENT_PAGE=1;
let PAGE_SIZE=50;
let LAST_ROWS=[];

const TODAY_KEY=new Date().toLocaleDateString("sv-SE");
const STATIC_COUNTS={
  all:DATA.length,
  candidate:DATA.filter(r=>Number(r["従来候補フラグ"]||0)===1).length,
  d1rebuilt:DATA.filter(r=>String(r["D1ソース"]||"")==="price_history復元").length,
  today:DATA.filter(r=>String(r["発表日時"]||"").slice(0,10)===TODAY_KEY).length,
  nolabel:DATA.filter(r=>Number(r["反応ラベル有無"]||0)!==1).length,
  unusable:DATA.filter(r=>String(r["母集団区分"]||"")==="解析不可").length,
  now:DATA.filter(r=>hasType(r,"今から候補")).length,
  rev:DATA.filter(r=>hasType(r,"再評価")).length,
  bottom:DATA.filter(r=>hasType(r,"下げ止まり")).length,
  rebound:DATA.filter(r=>hasType(r,"反発初動")).length,
  hot:DATA.filter(r=>hasType(r,"伸び切り")).length,
  strong:DATA.filter(r=>hasType(r,"強い")).length,
  lowrisk:DATA.filter(r=>Number(r["短期低リスクフラグ"]||0)===1).length,
  rangelowrisk:DATA.filter(r=>Number(r["値幅×低リスクフラグ"]||0)===1).length
};

function setTextIf(id,v){
  const el=document.getElementById(id);
  if(el)el.textContent=v;
}

function updateCards(rows){
  setTextIf("cAll",STATIC_COUNTS.all);
  setTextIf("cCandidate",STATIC_COUNTS.candidate);
  setTextIf("cD1Rebuilt",STATIC_COUNTS.d1rebuilt);
  setTextIf("cTodayRelease",STATIC_COUNTS.today);
  setTextIf("cNoLabel",STATIC_COUNTS.nolabel);
  setTextIf("cUnusable",STATIC_COUNTS.unusable);
  setTextIf("cNow",STATIC_COUNTS.now);
  setTextIf("cRev",STATIC_COUNTS.rev);
  setTextIf("cBottom",STATIC_COUNTS.bottom);
  setTextIf("cRebound",STATIC_COUNTS.rebound);
  setTextIf("cHot",STATIC_COUNTS.hot);
  setTextIf("cStrong",STATIC_COUNTS.strong);
  setTextIf("cLowRisk",STATIC_COUNTS.lowrisk);
  setTextIf("cRangeLowRisk",STATIC_COUNTS.rangelowrisk);
  setTextIf("cBookmarks",DATA.filter(r=>isBookmarked(r["コード"])).length);
  setTextIf("cVisible",rows.length);
}

function updatePager(total){
  const totalPages=Math.max(1,Math.ceil(total/PAGE_SIZE));
  CURRENT_PAGE=Math.min(Math.max(1,CURRENT_PAGE),totalPages);
  const start=total===0?0:(CURRENT_PAGE-1)*PAGE_SIZE+1;
  const end=Math.min(total,CURRENT_PAGE*PAGE_SIZE);
  setTextIf("pageInfo",`${total.toLocaleString()}件中 ${start.toLocaleString()}–${end.toLocaleString()}件 / ${CURRENT_PAGE}/${totalPages}ページ`);
  ["pageFirst","pagePrev"].forEach(id=>{const el=document.getElementById(id);if(el)el.disabled=CURRENT_PAGE<=1});
  ["pageNext","pageLast"].forEach(id=>{const el=document.getElementById(id);if(el)el.disabled=CURRENT_PAGE>=totalPages});
  return totalPages;
}

function renderTable(rows){
  LAST_ROWS=rows;
  const head=document.getElementById("thead");
  if(!head.childElementCount){
    COLS.forEach(c=>{
      const th=document.createElement("th");
      th.textContent=c;
      head.appendChild(th);
    });
  }
  const totalPages=updatePager(rows.length);
  CURRENT_PAGE=Math.min(Math.max(1,CURRENT_PAGE),totalPages);
  const start=(CURRENT_PAGE-1)*PAGE_SIZE;
  const pageRows=rows.slice(start,start+PAGE_SIZE);

  const b=document.getElementById("tbody");
  b.innerHTML="";
  const f=document.createDocumentFragment();

  pageRows.forEach(r=>{
    const tr=document.createElement("tr");
    if(isBookmarked(r["コード"]))tr.classList.add("bookmarked-row");
    COLS.forEach(c=>{
      const td=document.createElement("td");
      if(c==="★"){
        td.innerHTML=bookmarkButton(r["コード"]);
        td.classList.add("bookmark-cell");
      }else if(c==="コード"){
        td.innerHTML=codeButton(r[c]);
      }else if(c==="ミニチャート"){
        td.innerHTML=sparklineCached(r);
      }else if(c==="タイプ"){
        td.innerHTML=badges(r);
      }else if(c==="需給タグ"){
        td.innerHTML=supplyBadges(r);
      }else{
        td.innerHTML=fmt(r[c],c);
      }
      if(NUMS.has(c))td.classList.add("num");
      const cc=cls(r[c],c);
      if(cc)td.classList.add(cc);
      if(c==="サマリー")td.classList.add("summary-cell");
      if(c==="主要機関の動き")td.classList.add("long-cell");
      tr.appendChild(td);
    });
    f.appendChild(tr);
  });
  b.appendChild(f);
}

function getCurrentPageRows(){
  const start=Math.max(0,(CURRENT_PAGE-1)*PAGE_SIZE);
  return LAST_ROWS.slice(start,start+PAGE_SIZE);
}

function downloadCurrentPageCsv(){
  const pageRows=getCurrentPageRows();
  if(!pageRows.length){
    alert("今のページに銘柄がありません");
    return;
  }
  const csvQuote=(v)=>`"${String(v??"").replace(/"/g,'""')}"`;
  const lines=pageRows.map(r=>{
    const code=String(r["コード"]??"").padStart(4,"0");
    const name=String(r["銘柄名"]??"");
    if(!code || code==="0000" || !name)return "";
    return `${csvQuote(code)},${csvQuote(name)},TKY,,,,,,`;
  }).filter(Boolean);
  if(!lines.length){
    alert("CSVに出力できる銘柄がありません");
    return;
  }
  const csv=lines.join("\r\n");
  const blob=new Blob(["\uFEFF",csv],{type:"text/csv;charset=utf-8"});
  const pad=n=>String(n).padStart(2,"0");
  const dt=new Date();
  const filename=`screen_current_page_only_${dt.getFullYear()}${pad(dt.getMonth()+1)}${pad(dt.getDate())}_${pad(dt.getHours())}${pad(dt.getMinutes())}${pad(dt.getSeconds())}.csv`;
  const url=URL.createObjectURL(blob);
  const a=document.createElement("a");
  a.href=url;
  a.download=filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=>URL.revokeObjectURL(url),1000);
}

function renderCurrentPage(){
  renderTable(LAST_ROWS);
}

function apply(resetPage=true){
  if(resetPage)CURRENT_PAGE=1;
  const rows=sortRows(DATA.filter(filterRow));
  LAST_ROWS=rows;
  updateCards(rows);
  renderSummary(rows);
  renderTable(rows);
}

function goPage(where){
  const totalPages=Math.max(1,Math.ceil(LAST_ROWS.length/PAGE_SIZE));
  if(where==="first")CURRENT_PAGE=1;
  else if(where==="prev")CURRENT_PAGE=Math.max(1,CURRENT_PAGE-1);
  else if(where==="next")CURRENT_PAGE=Math.min(totalPages,CURRENT_PAGE+1);
  else if(where==="last")CURRENT_PAGE=totalPages;
  renderCurrentPage();
  const wrap=document.querySelector(".table-wrap");
  if(wrap)wrap.scrollTop=0;
}

function debounce(fn,wait=140){
  let timer=null;
  return (...args)=>{
    clearTimeout(timer);
    timer=setTimeout(()=>fn(...args),wait);
  };
}
const debouncedApply=debounce(()=>apply(true),140);

document.addEventListener("change",e=>{if(e.target.matches('input[data-type-kind]')){updateTypeCounts();apply(true)}});
document.addEventListener("change",e=>{if(e.target.matches('input[data-supply-kind]')){updateSupplyCounts();apply(true)}});
document.addEventListener("click",e=>{const a=e.target.closest("[data-type-action]");if(!a)return;e.preventDefault();const [cmd,kind]=(a.dataset.typeAction||"").split("-");document.querySelectorAll(`input[data-type-kind="${kind}"]`).forEach(cb=>cb.checked=(cmd==="all"));updateTypeCounts();apply(true)});
document.addEventListener("click",e=>{const a=e.target.closest("[data-supply-action]");if(!a)return;e.preventDefault();const [cmd,kind]=(a.dataset.supplyAction||"").split("-");document.querySelectorAll(`input[data-supply-kind="${kind}"]`).forEach(cb=>cb.checked=(cmd==="all"));updateSupplyCounts();apply(true)});
document.addEventListener("click",e=>{
  const b=e.target.closest("[data-bookmark-code]");
  if(!b)return;
  e.preventDefault();
  e.stopPropagation();
  toggleBookmark(b.dataset.bookmarkCode);
});
document.addEventListener("click",e=>{const btn=e.target.closest("[data-copy-code]");if(btn){e.preventDefault();e.stopPropagation();copyCode(btn.dataset.copyCode)}});
const qInput=document.getElementById("q");
if(qInput){
  qInput.addEventListener("input",debouncedApply);
  qInput.addEventListener("change",()=>apply(true));
}
document.querySelectorAll("#minEntry,#minStrength,#maxHeat,#minGuAll,#minGu10,#minGuAccel,#minCloseAll,#minClose10").forEach(el=>{
  el.addEventListener("input",debouncedApply);
  el.addEventListener("change",()=>apply(true));
});
const sortEl=document.getElementById("sortBy");
if(sortEl)sortEl.addEventListener("change",()=>apply(true));
document.getElementById("filterPresetSave").addEventListener("click",saveCurrentFilterPreset);
document.getElementById("filterPresetApply").addEventListener("click",applySelectedFilterPreset);
document.getElementById("filterPresetDelete").addEventListener("click",deleteSelectedFilterPreset);
document.getElementById("filterPresetSelect").addEventListener("dblclick",applySelectedFilterPreset);
document.querySelectorAll("button[data-mode]").forEach(btn=>btn.addEventListener("click",()=>{mode=btn.dataset.mode;document.querySelectorAll("button[data-mode]").forEach(b=>b.classList.remove("active"));btn.classList.add("active");if(mode==="lowrisk")document.getElementById("sortBy").value="今ならまだ間に合う順位";if(mode==="rangelowrisk")document.getElementById("sortBy").value="値幅×低リスク順位";if(["now","rev","pullback"].includes(mode))document.getElementById("sortBy").value="今からスコア";if(["bottom","rebound"].includes(mode))document.getElementById("sortBy").value="下げ止まりスコア";if(mode==="hot")document.getElementById("sortBy").value="過熱度";if(["strong","cont"].includes(mode))document.getElementById("sortBy").value="強さスコア";apply(true)}));

document.getElementById("pageFirst").addEventListener("click",()=>goPage("first"));
document.getElementById("pagePrev").addEventListener("click",()=>goPage("prev"));
document.getElementById("pageNext").addEventListener("click",()=>goPage("next"));
document.getElementById("pageLast").addEventListener("click",()=>goPage("last"));
document.getElementById("downloadPageCsv").addEventListener("click",downloadCurrentPageCsv);
document.getElementById("pageSize").addEventListener("change",e=>{
  PAGE_SIZE=Math.max(25,Number(e.target.value)||50);
  CURRENT_PAGE=1;
  renderCurrentPage();
});
const drawer=document.getElementById("helpDrawer"),overlay=document.getElementById("helpOverlay");function openHelp(){drawer.classList.add("open");overlay.classList.add("open");drawer.setAttribute("aria-hidden","false")}function closeHelp(){drawer.classList.remove("open");overlay.classList.remove("open");drawer.setAttribute("aria-hidden","true")}document.getElementById("helpOpen").addEventListener("click",openHelp);document.getElementById("helpClose").addEventListener("click",closeHelp);overlay.addEventListener("click",closeHelp);document.addEventListener("keydown",e=>{if(e.key==="Escape")closeHelp()});document.querySelectorAll("button[data-help-tab]").forEach(btn=>btn.addEventListener("click",()=>{document.querySelectorAll("button[data-help-tab]").forEach(b=>b.classList.remove("active"));document.querySelectorAll("[data-help-pane]").forEach(p=>p.classList.remove("active"));btn.classList.add("active");document.querySelector(`[data-help-pane="${btn.dataset.helpTab}"]`).classList.add("active")}));populateFilters();refreshFilterPresetSelect();apply(true);
</script></body></html>'''
    text=(
        template
        .replace("__GENERATED__",html.escape(generated))
        .replace("__DAYS__",str(int(days)))
        .replace("__MINRET__",str(min_return))
        .replace("__MAXDD__",str(max_drawdown))
        .replace("__BOTTOMMIN__",str(bottom_min_return))
        .replace("__BOTTOMMAX__",str(bottom_max_drawdown))
        .replace("__FULL_COUNT__",str(full_count))
        .replace("__EXCLUDED_COUNT__",str(excluded_count))
        .replace("__DATA_JSON__",data_json)
    )
    out_path.write_text(text,encoding="utf-8")


def main():
    ap=argparse.ArgumentParser(description="決算後上昇 + 下げ止まり + 信用需給 + HTMLダッシュボード")
    ap.add_argument("--db",default=None,help="kani2.db のフルパス")
    ap.add_argument("--days",type=int,default=60,help="直近何日分の決算を見るか（既定60日）")
    ap.add_argument("--price-lookback",type=int,default=220,help="安定上昇判定用。price_historyの参照暦日数（既定220日）")
    ap.add_argument("--min-return",type=float,default=5.0,help="上昇継続枠: 決算前→現在の最低上昇率(%%)")
    ap.add_argument("--max-drawdown",type=float,default=4.0,help="上昇継続枠: 決算後高値から許容する下落率(%%)")
    ap.add_argument("--bottom-min-return",type=float,default=0.0,help="下げ止まり枠: 決算前→現在の最低騰落率(%%)")
    ap.add_argument("--bottom-max-drawdown",type=float,default=12.0,help="下げ止まり枠: 決算後高値から許容する下落率(%%)")
    ap.add_argument("--out",default="決算後上昇スクリーニング.csv",help="ダッシュボード掲載銘柄CSVファイル名")
    ap.add_argument("--full-out",default="決算後上昇_全母集団.csv",help="足切り前の全母集団CSV")
    ap.add_argument("--excluded-out",default="決算後上昇_足切り除外.csv",help="HTMLから外した銘柄CSV")
    ap.add_argument("--gate-return",type=float,default=2.0,help="実戦監視OR条件: 決算後騰落率の最低値(%%)")
    ap.add_argument("--gate-d1",type=float,default=3.0,help="実戦監視OR条件: D1最低値(%%)")
    ap.add_argument("--gate-strength",type=float,default=50.0,help="実戦監視OR条件: 強さスコア")
    ap.add_argument("--gate-stable",type=float,default=60.0,help="実戦監視OR条件: 安定上昇スコア")
    ap.add_argument("--gate-accel",type=float,default=60.0,help="実戦監視OR条件: 連騰加速スコア")
    ap.add_argument("--html",default="決算後上昇ダッシュボード.html",help="HTMLダッシュボードファイル名")
    ap.add_argument("--top",type=int,default=50,help="コンソール表示件数")
    ap.add_argument("--open",action="store_true",help="生成後にHTMLをブラウザで開く")
    args=ap.parse_args(); started=datetime.now(); db=resolve_db_path(args.db); log(f"[DB] {db}"); conn=connect_readonly(db)
    try:
        for tbl in ("price_history","screener"):
            if not table_exists(conn,tbl):
                raise RuntimeError(f"必要テーブルがありません: {tbl}")
        universe_sources=(
            "tdnet_documents","earnings_events","tdnet_xbrl_metrics",
            "quarterly_actual_history","earnings_reaction_labels"
        )
        available_sources=[t for t in universe_sources if table_exists(conn,t)]
        if not available_sources:
            raise RuntimeError("発表済み決算を判定できる母集団ソースが1つもありません")
        log("[母集団ソース] "+", ".join(available_sources))
        log(f"[1/7] 直近{args.days}日の真の発表済み決算を複数ソースから統合"); earnings=fetch_latest_earnings(conn,args.days)
        if earnings.empty: log("対象決算なし"); return
        codes=sorted(earnings["コード"].unique().tolist()); log(f"      対象={len(codes)}銘柄")
        if "母集団ソース" in earnings.columns:
            sb=earnings["母集団ソース"].astype(str).value_counts()
            log("      主ソース="+ " / ".join(f"{k}:{int(v)}" for k,v in sb.items()))
        if "反応ラベル有無" in earnings.columns:
            no_label=int(
                (pd.to_numeric(earnings["反応ラベル有無"],errors="coerce").fillna(0)!=1).sum()
            )
            log(f"      反応ラベル未作成={no_label}銘柄")

        log("[2/7] screener現在値・信用需給を取得"); screen=fetch_screener_current(conn,codes)
        lookback=max(args.price_lookback,args.days+45); start_date=(date.today()-timedelta(days=lookback)).isoformat()
        log(f"[3/7] price_historyを対象コードだけ取得 ({start_date}～)")
        prices_raw=fetch_price_history(conn,codes,start_date)
        prices=append_live_if_needed(prices_raw,screen)
        log(
            f"      price_history={len(prices):,}行 / "
            f"現在値反映日={prices.attrs.get('live_session_date')} / "
            f"mode={prices.attrs.get('live_session_mode')} / "
            f"追加行={prices.attrs.get('live_rows_added',0)}"
        )
        log("[3b/7] reaction_labels未作成分のD1/D3/D5をprice_historyから復元")
        earnings=reconstruct_reaction_from_prices(earnings,prices)
        rebuilt=int((earnings.get("D1ソース","").astype(str)=="price_history復元").sum())
        true_wait=int((earnings.get("D1状態","").astype(str)=="真のD1待ち").sum())
        d1_price_lack=int((earnings.get("D1状態","").astype(str)=="価格不足").sum())
        log(f"      D1価格復元={rebuilt} / 真のD1待ち={true_wait} / D1価格不足={d1_price_lack}")
        log("[4/8] pandasで全母集団を分類・採点（この時点では全件保持）")
        full_result=analyze(
            earnings,prices,screen,
            args.min_return,args.max_drawdown,
            args.bottom_min_return,args.bottom_max_drawdown
        )
        if full_result.empty:
            log("解析結果なし")
            return

        log("[5/8] HTML掲載分を選定（監視・D1待ちは除外）")
        full_result,result,excluded=build_practical_dashboard_pool(
            full_result,
            gate_return=args.gate_return,
            gate_d1=args.gate_d1,
            gate_strength=args.gate_strength,
            gate_stable=args.gate_stable,
            gate_accel=args.gate_accel,
        )
        log(
            f"      全母集団={len(full_result):,} / "
            f"HTML掲載={len(result):,} / "
            f"足切り除外={len(excluded):,}"
        )
        log(
            "      OR条件: "
            f"従来候補 / 決算後>={args.gate_return:g}% / "
            f"D1>={args.gate_d1:g}% / 強さ>={args.gate_strength:g} / "
            f"安定>={args.gate_stable:g} / 連騰>={args.gate_accel:g} / "
            "再評価"
        )

        OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
        csv_out=OUTPUT_DIR/Path(args.out).name
        full_out=OUTPUT_DIR/Path(args.full_out).name
        excluded_out=OUTPUT_DIR/Path(args.excluded_out).name
        html_out=OUTPUT_DIR/Path(args.html).name

        # 取りこぼし検証用。HTMLには埋め込まない。
        csv_safe_view(full_result).to_csv(full_out,index=False,encoding="utf-8-sig")
        csv_safe_view(excluded).to_csv(excluded_out,index=False,encoding="utf-8-sig")
        log(f"      [全母集団CSV] {full_out}")
        log(f"      [足切りCSV]   {excluded_out}")

        # 重い付加集計は、実際にHTMLへ載せる銘柄だけ。
        if not result.empty:
            log("[6/8] HTML掲載銘柄だけGU・高値圏・信用需給を追加")
            gu=fetch_gu_stats(conn,result["コード"].tolist())
            if not gu.empty:
                result=result.merge(gu,on="コード",how="left")

            sc=fetch_strong_close_stats(conn,result["コード"].tolist())
            if not sc.empty:
                result=result.merge(sc,on="コード",how="left")

            flags=fetch_chart_flags(conn,result["コード"].tolist())
            if not flags.empty:
                result=result.merge(flags,on="コード",how="left")

            inst=fetch_institution_shorts(conn,result["コード"].tolist())
            if not inst.empty:
                result=result.merge(inst,on="コード",how="left")

            result=enrich_supply(result)

            for idx,r in result.iterrows():
                ex=[]
                if r.get("GCフラグ")==1:
                    ex.append("GC")
                if r.get("三役好転フラグ")==1:
                    ex.append("三役好転")
                if ex:
                    result.at[idx,"サマリー"]=str(result.at[idx,"サマリー"])+" テクニカル: "+"・".join(ex)+"。"

        csv_safe_view(result).to_csv(csv_out,index=False,encoding="utf-8-sig")

        log("[7/8] 軽量HTMLダッシュボード生成")
        generate_dashboard(
            result,html_out,
            args.days,args.min_return,args.max_drawdown,
            args.bottom_min_return,args.bottom_max_drawdown,
            full_count=len(full_result),
            excluded_count=len(excluded),
        )
        log("[8/8] 完了")
        elapsed=(datetime.now()-started).total_seconds()
        log(f"[完了] 全母集団={len(full_result):,} / HTML掲載={len(result):,} / {elapsed:.2f}秒")
        log(f"[全母集団CSV] {full_out}")
        log(f"[掲載銘柄CSV] {csv_out}")
        log(f"[足切りCSV]   {excluded_out}")
        log(f"[HTML]         {html_out}")
        if not result.empty:
            counts={
                "全母集団":len(full_result),
                "HTML掲載":len(result),
                "足切り除外":len(excluded),
                "従来候補":int(pd.to_numeric(result.get("従来候補フラグ",0),errors="coerce").fillna(0).sum()),
                "監視":int((result.get("母集団区分","").astype(str)=="監視").sum()),
                "D1待ち":int((result.get("母集団区分","").astype(str)=="D1待ち").sum()),
                "D1価格復元":int((result.get("D1ソース","").astype(str)=="price_history復元").sum()),
                "解析不可":int((result.get("母集団区分","").astype(str)=="解析不可").sum()),
                "今から":int(result["タイプ"].astype(str).str.contains("今から候補").sum()),
                "短期低リスク":int(pd.to_numeric(result.get("短期低リスクフラグ",0),errors="coerce").fillna(0).sum()),
                "値幅×低リスク":int(pd.to_numeric(result.get("値幅×低リスクフラグ",0),errors="coerce").fillna(0).sum()),
                "再評価":int(result["タイプ"].astype(str).str.contains("再評価").sum())
            }
            log("[内訳] "+" / ".join(f"{k}={int(v)}" for k,v in counts.items()))
            show=[c for c in ["コード","銘柄名","市場","母集団区分","解析状態","足切り通過理由","D1ソース","D1復元精度","採用枠","現在値","値幅×低リスク順位","値幅×低リスクスコア","今ならまだ間に合う順位","短期低リスクスコア","今からスコア","強さスコア","連騰加速スコア","下げ止まりスコア","GU加速差","過熱度","タイプ","需給タグ","決算後騰落率","高値から乖離率","信用倍率","信用買い残_浮動株比率","信用需給負荷スコア","サマリー"] if c in result.columns]
            with pd.option_context("display.max_rows",args.top,"display.max_columns",None,"display.width",280,"display.unicode.east_asian_width",True): print(result[show].head(args.top).to_string(index=False))
        if args.open:
            try: webbrowser.open(html_out.resolve().as_uri())
            except Exception as e: log(f"[WARN] ブラウザ自動起動失敗: {e}")
    finally: conn.close()


if __name__ == "__main__":
    main()
