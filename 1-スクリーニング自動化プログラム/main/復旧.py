#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自動スクリーニング.py が使っている SQLite テーブル / カラムを
Python ファイルから機械的に抽出して、
- 無いテーブルは最低限のスキーマで CREATE TABLE
- 既にあるテーブルでも、足りないカラムは ALTER TABLE ... ADD COLUMN
で補完するワンショットスクリプト。

【使い方】
  1. このファイルを 自動スクリーニング.py と同じフォルダに置く
  2. python schema_guard_autoscreener.py などで実行する

DB_PATH は 自動スクリーニング.py 内の DB_PATH 定義を正規表現で抜き出すので、
そちらを変更すれば自動的に追従します。
"""

import ast
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Set


# === 設定 ===
BASE_DIR = Path(__file__).resolve().parent
SRC_PATH = BASE_DIR / "自動スクリーニング.py"


def load_source(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Python ソースが見つかりません: {path}")
    return path.read_text(encoding="utf-8", errors="ignore")


def extract_db_path(src: str) -> Path:
    """
    自動スクリーニング.py 内の DB_PATH 定義からパス文字列を抜き出す。
    例: DB_PATH = r"H:\\...\\kani2.db"
    """
    m = re.search(r"DB_PATH\s*=\s*r?['\"](.+?)['\"]", src)
    if not m:
        raise RuntimeError("自動スクリーニング.py 内に DB_PATH 定義が見つかりません。")
    raw = m.group(1)
    return Path(raw)


class StringCollector(ast.NodeVisitor):
    """ソース内の文字列リテラルを全部集める"""

    def __init__(self) -> None:
        self.strings: List[str] = []

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self.strings.append(node.value)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        # f-string の中の静的部分だけ連結しておく
        buf = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                buf.append(v.value)
        if buf:
            self.strings.append("".join(buf))


def collect_sql_like_strings(src: str) -> List[str]:
    tree = ast.parse(src)
    sc = StringCollector()
    sc.visit(tree)
    sql_like = [
        s
        for s in sc.strings
        if re.search(r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE TABLE|FROM|PRAGMA table_info)\b", s, re.I)
    ]
    return sql_like


# ---- テーブル / カラム抽出ロジック ----

def extract_table_columns(table: str, sql_list: List[str]) -> Set[str]:
    cols: Set[str] = set()
    t = table
    # INSERT INTO table (...)
    pattern_insert = re.compile(
        rf"INSERT\s+(?:OR\s+\w+\s+)?INTO\s+{re.escape(t)}\s*\((.*?)\)",
        re.I | re.S,
    )
    # UPDATE table SET ...
    pattern_update = re.compile(
        rf"UPDATE\s+{re.escape(t)}\s+SET\s+(.+?)(?:WHERE\b|$)",
        re.I | re.S,
    )
    # SELECT ... FROM table
    pattern_select = re.compile(
        rf"SELECT\s+(.*?)\s+FROM\s+{re.escape(t)}\b",
        re.I | re.S,
    )
    # ON CONFLICT(col, ...)
    pattern_conflict = re.compile(
        r"ON\s+CONFLICT\s*\((.*?)\)",
        re.I | re.S,
    )

    for s in sql_list:
        if t not in s:
            continue

        # INSERT
        for m in pattern_insert.finditer(s):
            part = m.group(1)
            for token in part.split(","):
                name = token.strip()
                # 「col TYPE」みたいなパターンを崩す
                name = re.sub(r"\s+.*", "", name)
                if name:
                    cols.add(name)

        # UPDATE
        for m in pattern_update.finditer(s):
            part = m.group(1)
            for assign in part.split(","):
                if "=" in assign:
                    name = assign.split("=", 1)[0].strip()
                    if name:
                        cols.add(name)

        # SELECT
        for m in pattern_select.finditer(s):
            part = m.group(1)
            # 念のため FROM でさらに分割
            part = re.split(r"\bFROM\b", part)[0]
            for token in part.split(","):
                token = token.strip()
                if not token:
                    continue
                # AS 以降は alias なので除去
                token = token.split(" AS ")[0].split(" as ")[0]
                # t.col の場合は末尾だけ
                if "." in token:
                    token = token.split(".")[-1]
                token = token.strip("() ")
                if not token or token == "*":
                    continue
                if token.startswith("'") or token.startswith('"'):
                    continue
                cols.add(token)

        # ON CONFLICT(...)
        for m in pattern_conflict.finditer(s):
            part = m.group(1)
            for token in part.split(","):
                name = token.strip()
                if name:
                    cols.add(name)

    return cols


SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "CASE", "WHEN", "END", "AS", "ON", "IN",
    "NOT", "NULL", "IS", "DISTINCT", "DATE", "WITH", "JOIN", "LEFT", "OUTER",
    "INNER", "GROUP", "BY", "ORDER", "LIMIT", "UNION", "ALL", "THEN", "ELSE",
}


def clean_col_name(name: str) -> str | None:
    if not name:
        return None
    name = name.strip()
    # かっこやスペース, クォートを含むものは式とみなして捨てる
    if any(ch in name for ch in " ()'\""):
        return None
    name = name.strip(");")
    if name.upper() in SQL_KEYWORDS:
        return None
    # 数値リテラルっぽいもの
    if all(ch in "0123456789.+-" for ch in name):
        return None
    return name


# 「明らかに CTE エイリアス」と分かる名前を除外
ALIAS_LIKE = {
    "order", "where", "base", "cur", "c", "e", "hist", "last_row", "lastdate",
    "avg20", "conf", "pl_quarter", "prev", "today_rows", "tr", "v", "z",
    "sqlite_master", "atr", "p", "p0", "p1", "ph", "next",
}


def collect_table_columns(sql_like: List[str]) -> Dict[str, Set[str]]:
    tables: Dict[str, Set[str]] = {}
    # まず FROM / JOIN / INSERT / UPDATE / PRAGMA から「テーブル候補名」を拾う
    for s in sql_like:
        # PRAGMA table_info(table)
        for m in re.finditer(r"PRAGMA\s+table_info\((\w+)\)", s, re.I):
            tables.setdefault(m.group(1), set())
        # FROM / JOIN
        for m in re.finditer(r"\bFROM\s+(\w+)", s, re.I):
            tables.setdefault(m.group(1), set())
        for m in re.finditer(r"\bJOIN\s+(\w+)", s, re.I):
            tables.setdefault(m.group(1), set())
        # INSERT INTO
        for m in re.finditer(r"INSERT\s+(?:OR\s+\w+\s+)?INTO\s+(\w+)", s, re.I):
            tables.setdefault(m.group(1), set())
        # UPDATE
        for m in re.finditer(r"UPDATE\s+(\w+)", s, re.I):
            tables.setdefault(m.group(1), set())

    # エイリアスっぽいのを削る
    for alias in list(tables.keys()):
        if alias.lower() in ALIAS_LIKE:
            tables.pop(alias, None)

    # 各テーブルについてカラム候補を抽出＆クレンジング
    result: Dict[str, Set[str]] = {}
    for t in tables.keys():
        raw_cols = extract_table_columns(t, sql_like)
        clean_cols: Set[str] = set()
        for c in raw_cols:
            c2 = clean_col_name(c)
            if c2:
                clean_cols.add(c2)
        result[t] = clean_cols

    return result


# === スキーマ補完 ===

def ensure_tables_and_columns(db_path: Path, table_cols: Dict[str, Set[str]]) -> None:
    print(f"[INFO] DB: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    cur = conn.cursor()

    # 既存テーブル一覧
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {r[0] for r in cur.fetchall()}

    for table, cols in sorted(table_cols.items()):
        if not cols:
            # カラム情報が空の場合は、とりあえず 'コード' だけ持つテーブルとして扱う
            cols = {"コード"}

        print(f"--- [{table}] ---")
        # テーブルが無い場合は新規作成（id + TEXT カラム）
        if table not in existing_tables:
            print(f"  * create table (新規)")
            col_defs = ['"id" INTEGER PRIMARY KEY AUTOINCREMENT']
            for c in sorted(cols):
                if c == "id":
                    continue
                col_defs.append(f'"{c}" TEXT')
            sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})'
            print(f"    SQL: {sql}")
            cur.execute(sql)
        else:
            print(f"  * table exists")

        # 既存カラムを取得
        cur.execute(f'PRAGMA table_info("{table}")')
        existing_cols = {r[1] for r in cur.fetchall()}

        # 足りないカラムを追加
        for c in sorted(cols):
            if c not in existing_cols:
                print(f"  + add column: {c}")
                sql = f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT'
                print(f"    SQL: {sql}")
                cur.execute(sql)

    conn.commit()
    conn.close()
    print("[INFO] schema guard finished.")


def main() -> None:
    print("[STEP] 自動スクリーニング.py を解析中...")
    src = load_source(SRC_PATH)

    db_path = extract_db_path(src)
    # 相対パスなら、自動スクリーニング.py の場所からの相対として解決
    if not db_path.is_absolute():
        db_path = (SRC_PATH.parent / db_path).resolve()

    sql_like = collect_sql_like_strings(src)
    table_cols = collect_table_columns(sql_like)

    print("[INFO] 発見したテーブルとカラム候補:")
    for t, cols in sorted(table_cols.items()):
        print(f"  - {t}: {sorted(cols)}")

    ensure_tables_and_columns(db_path, table_cols)


if __name__ == "__main__":
    main()
