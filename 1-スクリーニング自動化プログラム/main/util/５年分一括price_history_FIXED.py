# -*- coding: utf-8 -*-
"""
５年分一括price_history_FIXED.py
- yahooquery → yfinance の順で取得
- period ラダー → 全滅なら start/end フォールバック
- シンボルは override があればそれを使用、無ければ "<コード>.T" 一択（英字剥がし等なし）
- screener.市場 が 指数/ETF/ＥＴＦ/上場投資信託/インデックス の場合は対象外
- preflight で Yahoo 上の存在確認（存在しない場合は NEEDS_OVERRIDE ログ）
- --codes を渡したら **そのコードだけ** 実行（screener に無いコードも含める）
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Any

import pandas as pd
import yfinance as yf

DB_PATH_DEFAULT = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\db\kani2.db"
ETF_INDEX_KEYWORDS = ("指数", "ETF", "ＥＴＦ", "上場投資信託", "インデックス")

# ----------------- 基本ユーティリティ -----------------
def log(msg: str) -> None:
    print(msg, flush=True)

def fetch_override(conn: sqlite3.Connection, screen_code: str) -> Optional[str]:
    cur = conn.execute("SELECT 問い合わせシンボル FROM yahoo_symbol_override WHERE コード = ?", (screen_code,))
    row = cur.fetchone()
    return row[0] if row else None


def _scalar(x: Any) -> Any:
    if isinstance(x, pd.Series):
        nz = x.dropna()
        return nz.iloc[0] if len(nz) else (x.iloc[0] if len(x) else None)
    return x

def fetch_market(conn: sqlite3.Connection, code: str) -> str:
    cur = conn.execute("SELECT 市場 FROM screener WHERE コード = ? LIMIT 1", (code,))
    row = cur.fetchone()
    return (row[0] or "") if row else ""

def is_index_or_etf(market: str) -> bool:
    m = (market or "")
    u = m.upper()
    if any(k in m for k in ETF_INDEX_KEYWORDS):
        return True
    if "ETF" in u:
        return True
    return False

# ---- preflight ----
def preflight_symbol(symbol: str) -> Tuple[bool, dict]:
    try:
        from yahooquery import Ticker as YQ
    except Exception as e:
        log(f"  - yahooquery import failed (preflight): {e}")
        return True, {}
    try:
        yq = YQ(symbol, validate=True)
        price = yq.price
        qtype = yq.quote_type
        ok = False
        meta = {}
        if isinstance(price, dict):
            d = price.get(symbol) if symbol in price else None
            if d is None and len(price)==1:
                d = next(iter(price.values()))
            if isinstance(d, dict):
                meta.update({k: d.get(k) for k in ("exchange", "quoteType", "shortName", "regularMarketTime")})
                ok |= bool(d.get("exchange")) and bool(d.get("quoteType"))
        if not ok and isinstance(qtype, dict):
            d = qtype.get(symbol) if symbol in qtype else None
            if d is None and len(qtype)==1:
                d = next(iter(qtype.values()))
            if isinstance(d, dict):
                meta.update({k: d.get(k) for k in ("exchange", "quoteType", "shortName")})
                ok |= bool(d.get("quoteType"))
        return ok, meta
    except Exception as e:
        log(f"  - preflight error for {symbol}: {e}")
        return False, {}

# ----------------- 正規化 -----------------
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    rename_map = {
        "Open": "始値", "High": "高値", "Low": "安値",
        "Close": "終値", "Adj Close": "終値", "AdjClose": "終値",
        "Volume": "出来高",
        "open": "始値", "high": "高値", "low": "安値", "close": "終値", "adjclose": "終値",
        "volume": "出来高",
    }

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([c for c in col if c]).strip() for col in df.columns.values]

    if isinstance(df.index, pd.MultiIndex):
        if "date" in (df.index.names or []):
            try:
                df = df.reset_index()
                df = df.set_index("date")
            except Exception:
                df = df.reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)

    if "date" in df.columns:
        try:
            df = df.set_index("date")
        except Exception:
            pass

    tmp = df.rename(columns=rename_map).copy()
    if tmp.columns.duplicated().any():
        tmp = tmp.loc[:, ~tmp.columns.duplicated(keep="first")]

    need = ["始値", "高値", "安値", "終値", "出来高"]
    keep = [c for c in need if c in tmp.columns]
    if "終値" not in keep:
        return pd.DataFrame()

    out = tmp[keep].copy().dropna(subset=["終値"])

    if "出来高" in out.columns:
        try:
            out["出来高"] = out["出来高"].fillna(0).astype("int64")
        except Exception:
            pass

    try:
        out.index = pd.to_datetime(out.index).tz_localize(None)
    except Exception:
        pass
    return out


# ----------------- 取引所サフィックス推定 -----------------
def market_to_suffixes(market: str) -> list[str]:
    """
    市場名から優先的に試す Yahoo サフィックス候補を返す。
    例: 東証 -> .T, 札証 -> .S, 福証 -> .F, 名証 -> .N
    （最終的には preflight_symbol で存在確認して自動判定する）
    """
    m = (market or "").strip()
    cand: list[str] = []
    if not m:
        return cand
    # 東証（プライム/スタンダード/グロース含む）
    if ("東" in m) or ("ﾌﾟﾗｲﾑ" in m.upper()) or ("プライム" in m) or ("スタンダード" in m) or ("グロース" in m):
        cand.append(".T")
    # 札証（アンビシャス含む）
    if ("札" in m) or ("札幌" in m):
        cand.append(".S")
    # 福証（Q-Board 含む）
    if ("福" in m) or ("福岡" in m) or ("Q-Board" in m) or ("Ｑ－Ｂｏａｒｄ" in m):
        cand.append(".F")
    # 名証
    if ("名" in m) or ("名古屋" in m):
        cand.append(".N")
    # 重複除去
    seen, out = set(), []
    for s in cand:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def resolve_symbol(screen_code: str, market: str, override_symbol: Optional[str]) -> tuple[str, str]:
    """
    最終的に使用する Yahoo シンボルを決定する。
    戻り値: (symbol, resolution_note)
      resolution_note は "override" / "guess:<sfx>" / "default:.T" のいずれか。
    """
    # 1) override 最優先
    if override_symbol:
        return override_symbol, "override"

    # 2) 市場から候補を作り、preflight で存在確認して採用
    tried: list[str] = []
    # 市場に基づく候補
    cand = market_to_suffixes(market)

    # 一般的に試す候補（東/札/福/名）。市場由来と重複しないように後置
    base = [".T", ".S", ".F", ".N"]
    for sfx in base:
        if sfx not in cand:
            cand.append(sfx)

    # preflight を使って存在確認
    for sfx in cand:
        sym = f"{screen_code}{sfx}"
        tried.append(sym)
        ok, meta = preflight_symbol(sym)
        if ok:
            return sym, f"guess:{sfx}"

    # 3) どれもダメなら .T をデフォルトに（従来挙動と同じ）
    return (screen_code if screen_code.upper().endswith(".T") else f"{screen_code}.T"), "default:.T"
# ----------------- 取得（yfinance / yahooquery） -----------------
def yf_download_period(symbol: str, period_str: str) -> pd.DataFrame:
    df = yf.download(
        symbol,
        period=period_str,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return normalize_df(df)

def yf_download_range(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    df = yf.download(
        symbol,
        start=start_dt,
        end=end_dt,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return normalize_df(df)

def yq_history_period(symbol: str, period_str: str) -> pd.DataFrame:
    try:
        from yahooquery import Ticker as YQ
    except Exception as e:
        log(f"  - yahooquery import failed: {e}")
        return pd.DataFrame()
    try:
        yq = YQ(symbol, validate=True)
        df = yq.history(period=period_str, interval="1d")
        if isinstance(df, pd.DataFrame) and not df.empty:
            return normalize_df(df)
        return pd.DataFrame()
    except Exception as e:
        log(f"  - yahooquery error for {symbol} period={period_str}: {e}")
        return pd.DataFrame()

def yq_history_range(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    try:
        from yahooquery import Ticker as YQ
    except Exception as e:
        log(f"  - yahooquery import failed: {e}")
        return pd.DataFrame()
    try:
        yq = YQ(symbol, validate=True)
        df = yq.history(start=start_dt, end=end_dt, interval="1d")
        if isinstance(df, pd.DataFrame) and not df.empty:
            return normalize_df(df)
        return pd.DataFrame()
    except Exception as e:
        log(f"  - yahooquery error for {symbol} range={start_dt.date()}~{end_dt.date()}: {e}")
        return pd.DataFrame()

def backoff_periods(years: int) -> list[str]:
    ladder = [f"{y}y" for y in range(years, 0, -1)] + ["6mo", "3mo", "1mo"]
    seen, out = set(), []
    for p in ladder:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def date_range_for_years(years: int) -> tuple[datetime, datetime]:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=365*max(1, years))
    return start_dt, end_dt

def try_with_backoff(symbol: str, years: int, prefer_yf: bool) -> Tuple[pd.DataFrame, Optional[str], str]:
    periods = backoff_periods(years)
    order = ['yf', 'yq'] if prefer_yf else ['yq', 'yf']

    for api in order:
        for p in periods:
            if api == 'yq':
                df = yq_history_period(symbol, p)
            else:
                df = yf_download_period(symbol, p)
            if df is not None and not df.empty:
                log(f"    -> hit: api={api} mode=period period={p} rows={len(df)}")
                return df, p, api
            else:
                log(f"    miss: api={api} mode=period period={p}")

    start_dt, end_dt = date_range_for_years(years)
    for api in order:
        if api == 'yq':
            df = yq_history_range(symbol, start_dt, end_dt)
        else:
            df = yf_download_range(symbol, start_dt, end_dt)
        if df is not None and not df.empty:
            log(f"    -> hit: api={api} mode=range {start_dt.date()}~{end_dt.date()} rows={len(df)}")
            return df, 'range', api
        else:
            log(f"    miss: api={api} mode=range {start_dt.date()}~{end_dt.date()}")

    return pd.DataFrame(), None, ''

# ----------------- DB I/O -----------------
def build_symbol(screen_code: str, override_symbol: Optional[str]) -> str:
    # 互換のため残すが、resolve_symbol() に委譲
    sym, _note = resolve_symbol(screen_code, '', override_symbol)
    return sym

def df_to_rows(screen_code: str, df: pd.DataFrame):
    rows = []
    for day, row in df.iterrows():
        o = _scalar(row.get("始値", None))
        h = _scalar(row.get("高値", None))
        l = _scalar(row.get("安値", None))
        c = _scalar(row.get("終値", None))
        v = _scalar(row.get("出来高", 0))
        rows.append((
            screen_code,
            pd.Timestamp(day).strftime("%Y-%m-%d"),
            float(o) if pd.notna(o) else None,
            float(h) if pd.notna(h) else None,
            float(l) if pd.notna(l) else None,
            float(c) if pd.notna(c) else None,
            int(v) if pd.notna(v) else 0,
        ))
    return rows

def upsert_rows(conn: sqlite3.Connection, rows):
    if not rows:
        return 0
    cur = conn.executemany(
        """
        INSERT INTO price_history(コード, 日付, 始値, 高値, 安値, 終値, 出来高)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(コード, 日付) DO UPDATE SET
          始値=excluded.始値, 高値=excluded.高値, 安値=excluded.安値, 終値=excluded.終値, 出来高=excluded.出来高
        """,
        rows,
    )
    return cur.rowcount or 0


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """
    指定テーブルが存在するかを返すユーティリティ。
    """
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (name,),
    )
    return cur.fetchone() is not None


def ensure_tables(conn: sqlite3.Connection) -> None:
    """
    price_history / screener / yahoo_symbol_override が存在しない場合は
    最小限のスキーマを自動作成する。
    すでに存在する場合は何もしない（既存スキーマは壊さない）。
    """
    # screener
    if not table_exists(conn, "screener"):
        conn.execute(
            """
            CREATE TABLE screener (
                コード TEXT PRIMARY KEY,
                名称 TEXT,
                市場 TEXT
            );
            """
        )
        log("[INIT] screener テーブルを新規作成しました（まだ銘柄は入っていません）。")

    # price_history
    if not table_exists(conn, "price_history"):
        conn.execute(
            """
            CREATE TABLE price_history (
                コード TEXT NOT NULL,
                日付 TEXT NOT NULL,
                始値 REAL,
                高値 REAL,
                安値 REAL,
                終値 REAL,
                出来高 INTEGER,
                PRIMARY KEY(コード, 日付)
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_price_history_code ON price_history(コード);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(日付);"
        )
        log("[INIT] price_history テーブルを新規作成しました。")

    # yahoo_symbol_override
    if not table_exists(conn, "yahoo_symbol_override"):
        conn.execute(
            """
            CREATE TABLE yahoo_symbol_override (
                コード TEXT PRIMARY KEY,
                問い合わせシンボル TEXT
            );
            """
        )
        log("[INIT] yahoo_symbol_override テーブルを新規作成しました。")

    conn.commit()

def ensure_list(x: Optional[str]) -> List[str]:
    """
    "--codes" にカンマ区切り/空白区切りのどちらでも渡せるようにする。
    例: "--codes 444A,445A" / "--codes 444A 445A"
    """
    if not x:
        return []
    import re
    return [t for t in re.split(r"[\\s,]+", x) if t]

def detect_targets(conn: sqlite3.Connection, codes: Optional[List[str]], get_all: bool = False) -> List[str]:
    """
    Priority:
      1) If --codes is provided: use exactly those codes (exclude index/ETF only if market known).
      2) If --all is provided: use all screener codes (excluding index/ETF).
      3) Default: only codes not yet in price_history (excluding index/ETF).
    """
    if codes:
        placeholders = ",".join(["?"] * len(codes))
        q = f"""SELECT コード, IFNULL(市場,'') FROM screener WHERE コード IN ({placeholders})"""
        market_map = {row[0]: row[1] for row in conn.execute(q, codes).fetchall()}
        out = []
        for c in codes:
            market = market_map.get(c, "")
            if market and is_index_or_etf(market):
                log(f"[SKIP 指数/ETF] {c} (市場={market})")
                continue
            out.append(c)
        return out

    if get_all:
        q = "SELECT コード, IFNULL(市場,'') FROM screener"
        out = []
        for code, market in conn.execute(q).fetchall():
            if is_index_or_etf(market):
                continue
            out.append(code)
        return out

    q = """
    WITH ph AS (SELECT コード, COUNT(*) AS cnt FROM price_history GROUP BY コード)
    SELECT s.コード, IFNULL(s.市場,'')
    FROM screener s
    LEFT JOIN ph ON ph.コード = s.コード
    WHERE IFNULL(ph.cnt,0) = 0
    """
    out = []
    for code, market in conn.execute(q).fetchall():
        if is_index_or_etf(market):
            continue
        out.append(code)
    return out

    q = """
    WITH ph AS (SELECT コード, COUNT(*) AS cnt FROM price_history GROUP BY コード)
    SELECT s.コード, IFNULL(s.市場,'')
    FROM screener s
    LEFT JOIN ph ON ph.コード = s.コード
    WHERE IFNULL(ph.cnt,0) = 0
    """
    out = []
    for code, market in conn.execute(q).fetchall():
        if is_index_or_etf(market):
            continue
        out.append(code)
    return out

# ----------------- メイン -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--codes", type=str, help="カンマ or 空白区切りで画面コードを指定（例: \"444A 445A\"）")
    ap.add_argument("--all", action="store_true", help="screenerの全銘柄を対象（指数/ETFは除外）")
    ap.add_argument("--force", action="store_true", help="対象コードの既存行を削除してから取り直し")
    ap.add_argument("--prefer-yf", action="store_true", help="yfinance を先に試す（デフォルトは yahooquery 先行）")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # DB を間違って消してしまった場合でも動くように、
    # 必要なテーブルが無ければここで最低限のスキーマを自動作成する
    ensure_tables(conn)

    codes_arg = ensure_list(args.codes)
    codes = detect_targets(conn, codes_arg, get_all=args.all)
    log(f"=== Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  targets={len(codes)}")
    if not codes:
        if codes_arg:
            log("  ※ --codes で指定されたコードが解釈できなかったか、対象が 0 件でした。")
            log("     （screener に市場が入っていなくても、指定コード自体はそのまま扱われます）")
        elif args.all:
            log("  ※ --all が指定されていますが、screener テーブルに銘柄が 1 件もありません。")
            log("     先に screener を構築するか、'--codes 444A 445A' のように個別にコードを指定してください。")
        else:
            log("  ※ 対象銘柄が 0 件です。")
            log("     - 先に screener を構築するか、")
            log("     - あるいは '--codes 444A 445A' のように個別にコードを指定してください。")

    ok: List[str] = []
    fail: List[str] = []

    for i, screen_code in enumerate(codes, 1):
        market = fetch_market(conn, screen_code)
        log(f"[{i}/{len(codes)}] {screen_code}  市場='{market}'")
        if market and is_index_or_etf(market):
            log(f"  -> skip by market rule")
            continue

        if args.force:
            conn.execute("DELETE FROM price_history WHERE コード = ?", (screen_code,))
            conn.commit()

        ov = fetch_override(conn, screen_code)
        symbol, _how = resolve_symbol(screen_code, market, ov)
        log(f"  symbol: {symbol} ({_how})")

        # preflight
        exists, meta = preflight_symbol(symbol)
        if not exists:
            log(f"[NEEDS_OVERRIDE] {screen_code}: Yahoo上に該当シンボルなし (tried={symbol}) meta={meta}")
            fail.append(screen_code)
            continue
        else:
            if meta:
                log(f"  preflight ok: {meta}")

        try:
            df, used_period, api = try_with_backoff(symbol, args.years, prefer_yf=args.prefer_yf)
        except Exception as e:
            log(f"  - {symbol}: ERROR {e}")
            df, used_period, api = pd.DataFrame(), None, ""

        if df is None or df.empty:
            log(f"[NG] {screen_code}: 取得失敗/空  (tried={symbol})")
            fail.append(screen_code)
            continue

        rows = df_to_rows(screen_code, df)
        n = upsert_rows(conn, rows)
        conn.commit()

        period_note = f" period={used_period}" if used_period and used_period != 'range' else " range"
        api_note = f" api={api}" if api else ""
        log(f"[OK] {screen_code} via {symbol}:{period_note}{api_note} {n} rows upserted.")
        ok.append(screen_code)

    log("\n=== 結果 ===")
    log(f"成功 : {ok}")
    log(f"失敗 : {fail}")
    log("==================================================")
    log(f"[INFO] Done at {datetime.now()}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
