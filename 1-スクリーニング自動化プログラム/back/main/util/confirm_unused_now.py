#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, re, sqlite3
from pathlib import Path

EXPR_TOKEN = "(expr)"  # for expression-based index columns

def read_csv_column(path: Path, header: str):
    vals = []
    with path.open('r', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        if header not in rdr.fieldnames:
            raise ValueError(f"{path.name}: header '{header}' not found. fields={rdr.fieldnames}")
        for row in rdr:
            v = (row.get(header) or '').strip()
            if v:
                vals.append(v)
    return vals

def parse_sql_pairs(sql_text: str):
    pairs = set()
    pat = re.compile(r'([A-Za-z0-9_]+)\s*\.\s*["`\[]?([\w一-龥ぁ-んァ-ン・ー％]+)["`\]]?')
    for ln in sql_text.splitlines():
        for m in pat.finditer(ln):
            t = m.group(1).strip()
            c = m.group(2).strip()
            if t and c:
                pairs.add((t,c))
    return pairs

def get_db_schema(conn):
    cur = conn.cursor()
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view')")
    tables, views = set(), set()
    for name, typ in cur.fetchall():
        if name.startswith('sqlite_'):
            continue
        (tables if typ=='table' else views).add(name)

    columns_by_table = {}
    for t in sorted(tables | views):
        try:
            cur.execute(f'PRAGMA table_info("{t}")')
            columns_by_table[t] = [r[1] for r in cur.fetchall()]
        except sqlite3.DatabaseError:
            columns_by_table[t] = []

    indexes_by_table = {}
    index_cols = {}
    for t in sorted(tables):
        try:
            cur.execute(f'PRAGMA index_list("{t}")')
            idx_rows = cur.fetchall()
            idx_names = [r[1] for r in idx_rows]
            indexes_by_table[t] = idx_names
            for idx in idx_names:
                cols = []
                try:
                    # index_xinfo gives 'name' None for expressions -> map to EXPR_TOKEN
                    cur.execute(f'PRAGMA index_xinfo("{idx}")')
                    for row in cur.fetchall():
                        # row: seqno, cid, name, desc, coll, key, ... (version dependent)
                        name = row[2] if len(row) > 2 else None
                        key = row[5] if len(row) > 5 else 1  # key=1 → part of index key
                        if key == 1:
                            cols.append(name if name is not None else EXPR_TOKEN)
                except sqlite3.DatabaseError:
                    try:
                        cur.execute(f'PRAGMA index_info("{idx}")')
                        tmp = [r[2] if r[2] is not None else EXPR_TOKEN for r in cur.fetchall()]
                        cols.extend(tmp)
                    except sqlite3.DatabaseError:
                        pass
                index_cols[idx] = cols
        except sqlite3.DatabaseError:
            indexes_by_table[t] = []
    return tables, views, columns_by_table, indexes_by_table, index_cols

def write_csv(path: Path, header, rows):
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True)
    ap.add_argument('--rt-dir', required=True)
    ap.add_argument('--out-dir', required=True)
    args = ap.parse_args()

    db_path = Path(args.db)
    rt_dir  = Path(args.rt_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    used_tables_path  = rt_dir / 'runtime_used_tables.csv'
    used_columns_path = rt_dir / 'runtime_used_columns.csv'
    sql_log_path      = rt_dir / 'runtime_sql_log.txt'

    used_tables = set(read_csv_column(used_tables_path, 'table')) if used_tables_path.exists() else set()
    used_columns_bare = set(read_csv_column(used_columns_path, 'column')) if used_columns_path.exists() else set()
    used_pairs = set()
    if sql_log_path.exists():
        used_pairs = parse_sql_pairs(sql_log_path.read_text(encoding='utf-8', errors='ignore'))

    conn = sqlite3.connect(str(db_path))
    try:
        tables, views, columns_by_table, indexes_by_table, index_cols = get_db_schema(conn)
    finally:
        conn.close()

    all_tab_like = tables | views
    now_unused_tables = sorted([t for t in all_tab_like if t not in used_tables])

    now_unused_columns = []
    for t, cols in columns_by_table.items():
        for c in cols:
            if (t,c) in used_pairs:
                continue
            if c in used_columns_bare:
                continue
            now_unused_columns.append((t,c))

    unused_columns_set = set(now_unused_columns)
    unused_tables_set = set(now_unused_tables)

    now_unused_indexes = []
    for t, idxs in indexes_by_table.items():
        for idx in idxs:
            cols = index_cols.get(idx, [])
            # 表示用：NoneをEXPR_TOKENに置換
            cols_for_print = [ (c if c is not None else EXPR_TOKEN) for c in cols ]
            if t in unused_tables_set:
                now_unused_indexes.append((t, idx, ','.join(cols_for_print)))
            else:
                # すべての列が未使用 または EXPRのみ（列依存性なし）
                if not cols_for_print:
                    # 列情報が取れなければテーブル使用に追随（= 使用扱い）。未使用にはしない。
                    continue
                all_unused = True
                has_real_col = False
                for c in cols_for_print:
                    if c == EXPR_TOKEN:
                        # 式インデックスは列依存判定ができないので「実列あり」の条件から外す
                        continue
                    has_real_col = True
                    if (t, c) not in unused_columns_set:
                        all_unused = False
                        break
                if has_real_col and all_unused:
                    now_unused_indexes.append((t, idx, ','.join(cols_for_print)))

    write_csv(out_dir / 'now_unused_tables.csv',  ['table'], [(t,) for t in now_unused_tables])
    write_csv(out_dir / 'now_unused_columns.csv', ['table','column'], now_unused_columns)
    write_csv(out_dir / 'now_unused_indexes.csv', ['table','index','columns'], now_unused_indexes)

    summary = [
        f"db={db_path}",
        f"rt_dir={rt_dir}",
        f"tables_in_db={len(all_tab_like)}",
        f"columns_in_db={sum(len(v) for v in columns_by_table.values())}",
        f"indexes_in_db={sum(len(v) for v in indexes_by_table.values())}",
        "---",
        f"used_tables={len(used_tables)}",
        f"used_columns_bare={len(used_columns_bare)}",
        f"used_pairs(table.column)={len(used_pairs)}",
        "---",
        f"now_unused_tables={len(now_unused_tables)}",
        f"now_unused_columns={len(now_unused_columns)}",
        f"now_unused_indexes={len(now_unused_indexes)}",
    ]
    (out_dir / 'now_summary.txt').write_text("\n".join(summary), encoding='utf-8')
    print("[OK] wrote outputs to", out_dir)

if __name__ == '__main__':
    main()
