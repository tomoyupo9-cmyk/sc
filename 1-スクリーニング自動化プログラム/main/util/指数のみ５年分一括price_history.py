# -*- coding: utf-8 -*-
"""
指数のみ５年分一括price_history.py  [方式A対応版]
------------------------------------------------------------
指定した指数コードの「過去5年分・日足」を yfinance 経由で
SQLite の price_history テーブルへ一括投入します。

方式A:
  - 問い合わせ前に sqlite の yahoo_symbol_override(コード, 問い合わせシンボル) を参照
  - 取得は 問い合わせシンボル で実施
  - price_history には元の コード 名義で保存（表示や参照は従来どおり）

対象（必要に応じて編集してください）:
  ^N225, ^TOPIX, ^GSPC, ^DJI, ^IXIC, ^TRJ, ^MOTHERS, ^GRT250, ^JPXNK400, ^VIX

使い方（例）:
  python "指数のみ５年分一括price_history.py"

※ DB パスは下の DB_PATH を環境に合わせて変更してください。
------------------------------------------------------------
"""

import sqlite3
from datetime import datetime
from typing import List, Optional

import yfinance as yf
import pandas as pd

# ====== 設定 ======
DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

INDEX_CODES: List[str] = [
    "^N225",      # 日経平均
    "^TOPIX",     # TOPIX（Yahoo未対応のため通常はETFにオーバーライド）
    "^GSPC",      # S&P500
    "^DJI",       # ダウ平均
    "^IXIC",      # NASDAQ総合
    "^TRJ",       # 東証REIT指数（Yahoo未対応→ETFにオーバーライド）
    "^MOTHERS",   # 旧マザーズ（→東証グロース250 ETFにオーバーライド）
    "^GRT250",    # 東証グロース250（→ETFにオーバーライド）
    "^JPXNK400",  # JPX日経400（→1591.T などにオーバーライド）
    "^VIX",       # CBOE VIX
]

START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")


def ensure_tables(conn: sqlite3.Connection) -> None:
    """price_history が無ければ作成"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            コード   TEXT NOT NULL,
            日付     TEXT NOT NULL,
            始値     REAL,
            高値     REAL,
            安値     REAL,
            終値     REAL,
            出来高   INTEGER,
            PRIMARY KEY (コード, 日付)
        )
    """)
    conn.commit()


def get_override_symbol(conn: sqlite3.Connection, code: str) -> Optional[str]:
    """
    yahoo_symbol_override(コード TEXT PRIMARY KEY, 問い合わせシンボル TEXT) から
    対応する問い合わせ先シンボルを取得。無ければ None を返す。
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 問い合わせシンボル FROM yahoo_symbol_override WHERE コード = ? LIMIT 1",
            (code,),
        )
        row = cur.fetchone()
        if not row:
            return None
        sym = (row[0] or "").strip()
        return sym or None
    except sqlite3.OperationalError:
        # テーブル未作成などでもスクリプトは止めない（従来どおりの挙動）
        return None


def upsert_history(conn: sqlite3.Connection, code: str, df: pd.DataFrame) -> int:
    """DataFrame（yfinanceのhistory）を price_history に UPSERT（保存は元の code 名義）"""
    if df is None or df.empty:
        return 0

    rows = []
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d")
        o = None if pd.isna(row.get("Open")) else float(row["Open"])
        h = None if pd.isna(row.get("High")) else float(row["High"])
        l = None if pd.isna(row.get("Low")) else float(row["Low"])
        c = None if pd.isna(row.get("Close")) else float(row["Close"])
        v = 0 if pd.isna(row.get("Volume")) else int(row["Volume"])
        rows.append((code, d, o, h, l, c, v))

    cur = conn.cursor()
    cur.executemany(
        """INSERT OR REPLACE INTO price_history
           (コード, 日付, 始値, 高値, 安値, 終値, 出来高)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


def main():
    print(f"[INFO] Start at {datetime.now()}")
    print(f"[INFO] DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_tables(conn)

        ok, fail = [], []
        for code in INDEX_CODES:
            override = get_override_symbol(conn, code)
            qsym = override if override else code

            # ログ（オーバーライドの有無を明示）
            if override:
                print(f"\n=== {code} (query: {qsym}) ===")
            else:
                print(f"\n=== {code} ===")

            try:
                tkr = yf.Ticker(qsym)
                hist = tkr.history(start=START_DATE, end=END_DATE, interval="1d")

                if hist is None or hist.empty:
                    # 失敗時のヒントを出す
                    if override:
                        print(f"[WARN] {code}: override '{qsym}' でもデータ無し（未対応/休止/取得失敗の可能性）")
                    else:
                        print(f"[WARN] {code}: no data (possibly delisted/unsupported)")
                    fail.append(code)
                    continue

                # 取得は qsym でも、保存は元の code 名義
                n = upsert_history(conn, code, hist)
                note = f" via {qsym}" if override else ""
                print(f"[OK] {code}: {n} rows upserted.{note}")
                ok.append(code)

            except Exception as e:
                print(f"[ERR] {code}: {e}")
                fail.append(code)

        print("\n=== 結果 ===")
        print(f"成功: {ok}")
        print(f"失敗: {fail}")
        print("DB登録完了。")
        print("=" * 50)
    finally:
        conn.close()
        print(f"[INFO] Done at {datetime.now()}")


if __name__ == "__main__":
    main()
