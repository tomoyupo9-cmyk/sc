# -*- coding: utf-8 -*-
"""
株探: テーマ / 信用残 / 決算発表予定日 producer

役割:
- stock_theme_kabutan : 現在確認できるテーマだけを保持
- stock_theme_history : テーマの初回/最終確認日を保持
- stock_credit_margin : (コード, 基準日) の時系列として保持
- screener.決算発表予定日 : 株探の予定日を反映

dailyは決算イベント銘柄だけ、weeklyは全株式を更新する。
自動スクリーニング側はDBを読むだけにする。
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DEFAULT_DB_PATH = os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone() is not None


def _pk_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [r[1] for r in sorted((x for x in rows if int(x[5] or 0) > 0), key=lambda x: int(x[5]))]


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS theme_master (
            theme_id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT UNIQUE NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_theme_kabutan (
            コード TEXT NOT NULL,
            theme_id INTEGER NOT NULL,
            取得日 TEXT,
            PRIMARY KEY (コード, theme_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_theme_history (
            コード TEXT NOT NULL,
            theme_id INTEGER NOT NULL,
            初回確認日 TEXT NOT NULL,
            最終確認日 TEXT NOT NULL,
            PRIMARY KEY (コード, theme_id)
        )
    """)

    # 信用残は現行の時系列schemaだけを許容する。
    # 旧「コード単独PRIMARY KEY」schemaからの自動rename/migrationは廃止済み。
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_credit_margin (
            コード TEXT NOT NULL,
            基準日 TEXT NOT NULL,
            売り残 INTEGER,
            買い残 INTEGER,
            倍率 REAL,
            取得日 TEXT,
            PRIMARY KEY (コード, 基準日)
        )
    """)

    wanted_pk = ["コード", "基準日"]
    actual_pk = _pk_columns(conn, "stock_credit_margin")
    if actual_pk != wanted_pk:
        raise RuntimeError(
            "stock_credit_margin schema mismatch: "
            f"primary key={actual_pk!r}, expected={wanted_pk!r}. "
            "Automatic legacy migration is disabled."
        )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_credit_code_date "
        "ON stock_credit_margin(コード, 基準日 DESC)"
    )

    try:
        conn.execute("ALTER TABLE screener ADD COLUMN 決算発表予定日 TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()


STOCK_CODE_RE = re.compile(r"^(?:\d{4}|\d{3}[A-Z])$")


def _normalize_stock_code(raw) -> str | None:
    s = str(raw or "").strip().upper()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        s = s.zfill(4)
    return s if STOCK_CODE_RE.fullmatch(s) else None


def get_all_stock_codes(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT コード FROM screener WHERE コード IS NOT NULL").fetchall()
    out = []
    for (raw,) in rows:
        s = _normalize_stock_code(raw)
        if s is not None:
            out.append(s)
    return list(dict.fromkeys(out))


def safe_float(s):
    if s is None:
        return None
    t = str(s).replace(",", "").strip()
    if t in {"", "-", "--", "―", "－", "N/A"}:
        return None
    try:
        return float(t)
    except (TypeError, ValueError):
        return None


def _theme_ids(conn: sqlite3.Connection, names: set[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for name in sorted(names):
        conn.execute("INSERT OR IGNORE INTO theme_master(theme_name) VALUES (?)", (name,))
        row = conn.execute("SELECT theme_id FROM theme_master WHERE theme_name=?", (name,)).fetchone()
        if row:
            out[name] = int(row[0])
    return out


def _sync_current_themes(conn: sqlite3.Connection, code: str, names: set[str], today: str) -> int:
    ids = _theme_ids(conn, names)
    wanted = set(ids.values())

    # 株探の「テーマ」欄を正常に読めた時だけ authoritative snapshot として同期する。
    if wanted:
        q = ",".join("?" for _ in wanted)
        conn.execute(
            f"DELETE FROM stock_theme_kabutan WHERE コード=? AND theme_id NOT IN ({q})",
            [code, *sorted(wanted)],
        )
    else:
        conn.execute("DELETE FROM stock_theme_kabutan WHERE コード=?", (code,))

    for theme_id in sorted(wanted):
        conn.execute("""
            INSERT INTO stock_theme_kabutan(コード,theme_id,取得日)
            VALUES(?,?,?)
            ON CONFLICT(コード,theme_id) DO UPDATE SET 取得日=excluded.取得日
        """, (code, theme_id, today))
        conn.execute("""
            INSERT INTO stock_theme_history(コード,theme_id,初回確認日,最終確認日)
            VALUES(?,?,?,?)
            ON CONFLICT(コード,theme_id) DO UPDATE SET 最終確認日=excluded.最終確認日
        """, (code, theme_id, today, today))
    return len(wanted)


def _save_credit_history(conn: sqlite3.Connection, code: str, soup: BeautifulSoup, today: str) -> int:
    credit_h2 = soup.find("h2", string=re.compile(r"信用取引"))
    if not credit_h2:
        return 0
    credit_table = credit_h2.find_next_sibling("table")
    if not credit_table:
        return 0
    tbody = credit_table.find("tbody")
    if not tbody:
        return 0

    saved = 0
    for tr in tbody.find_all("tr"):
        time_tag = tr.find("time")
        margin_date = (time_tag.get("datetime") if time_tag else None) or ""
        margin_date = str(margin_date).strip().replace("/", "-")[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", margin_date):
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        sell_f = safe_float(tds[0].get_text(" ", strip=True))
        buy_f = safe_float(tds[1].get_text(" ", strip=True))
        ratio = safe_float(tds[2].get_text(" ", strip=True))
        sell_bal = int(round(sell_f * 1000)) if sell_f is not None else None
        buy_bal = int(round(buy_f * 1000)) if buy_f is not None else None
        conn.execute("""
            INSERT INTO stock_credit_margin(コード,基準日,売り残,買い残,倍率,取得日)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(コード,基準日) DO UPDATE SET
              売り残=excluded.売り残,
              買い残=excluded.買い残,
              倍率=excluded.倍率,
              取得日=excluded.取得日
        """, (code, margin_date, sell_bal, buy_bal, ratio, today))
        saved += 1
    return saved


def _update_earnings_schedule(conn: sqlite3.Connection, code: str, soup: BeautifulSoup) -> str | None:
    kessan_date = None
    kessan_div = soup.find("div", id="kessan_happyoubi")
    if kessan_div:
        time_tag = kessan_div.find("time")
        if time_tag:
            kessan_date = time_tag.get_text(" ", strip=True)

    if kessan_date:
        conn.execute("UPDATE screener SET 決算発表予定日=? WHERE コード=?", (kessan_date, code))
        return kessan_date

    # 予定日が取得できない時に「過去決算日」を予定日列へ入れない。
    # 過去実績は finance_notes / earnings履歴側の責務で、ここへ混ぜると
    # 決算前20日などの判定が過去日を次回予定と誤認する。
    conn.execute("UPDATE screener SET 決算発表予定日=NULL WHERE コード=?", (code,))
    return None


def scrape_kabutan_all_themes(
    db_path: str,
    *,
    sleep_sec: float = 1.5,
    timeout: float = 10.0,
    target_codes: list[str] | None = None,
) -> int:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60.0)
    ensure_schema(conn)
    if target_codes is None:
        codes = get_all_stock_codes(conn)
    else:
        codes = list(dict.fromkeys(
            code for raw in target_codes
            if (code := _normalize_stock_code(raw)) is not None
        ))
    if not codes:
        print("[themes-shinyo][ERROR] 処理対象の銘柄コードがありません。")
        conn.close()
        return 2

    today = date.today().isoformat()
    scope = "全件" if target_codes is None else "差分"
    print(f"[themes-shinyo] {scope} {len(codes)} 銘柄を開始")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36"
    })

    ok = fail = theme_total = credit_rows = 0
    try:
        for i, code in enumerate(codes, 1):
            try:
                res = session.get(f"https://kabutan.jp/stock/?code={code}", timeout=timeout)
                res.raise_for_status()
                res.encoding = "utf-8"
                soup = BeautifulSoup(res.text, "html.parser")

                theme_th = soup.find("th", string="テーマ")
                themes: set[str] = set()
                theme_td = theme_th.find_next_sibling("td") if theme_th else None
                # 見出しだけ取れて値セルが欠落したHTMLはparse failure扱い。
                # 正常な値セルまで確認できた時だけ、0件を含めauthoritative snapshotとする。
                theme_snapshot_valid = theme_td is not None
                if theme_td:
                    for a in theme_td.find_all("a", href=lambda h: h and h.startswith("/themes/?theme=")):
                        name = a.get_text(" ", strip=True)
                        if name:
                            themes.add(name)

                conn.execute("SAVEPOINT one_code")
                try:
                    tcount = _sync_current_themes(conn, code, themes, today) if theme_snapshot_valid else 0
                    ccount = _save_credit_history(conn, code, soup, today)
                    sched = _update_earnings_schedule(conn, code, soup)
                    conn.execute("RELEASE SAVEPOINT one_code")
                    conn.commit()
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT one_code")
                    conn.execute("RELEASE SAVEPOINT one_code")
                    raise

                ok += 1
                theme_total += tcount
                credit_rows += ccount
                print(f"[{i}/{len(codes)}] {code}: theme={tcount} credit_rows={ccount} earnings={sched or '-'}")
            except Exception as e:
                fail += 1
                print(f"[{i}/{len(codes)}] {code}: ERROR {type(e).__name__}: {e}")
            if sleep_sec > 0:
                time.sleep(sleep_sec)
    finally:
        session.close()
        conn.close()

    print(f"[themes-shinyo] 完了 ok={ok} fail={fail} themes={theme_total} credit_rows={credit_rows}")
    return 0 if fail == 0 else 2


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="株探テーマ・信用残・決算予定日収集")
    ap.add_argument("--db", default=DEFAULT_DB_PATH)
    ap.add_argument("--sleep", type=float, default=1.5)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--codes", nargs="*", default=None, help="差分対象コード。省略時は全株式")
    args = ap.parse_args(argv)
    return scrape_kabutan_all_themes(
        args.db,
        sleep_sec=max(0.0, args.sleep),
        timeout=max(1.0, args.timeout),
        target_codes=args.codes,
    )


if __name__ == "__main__":
    raise SystemExit(main())
