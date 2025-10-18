#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (shortened header, see chat for details)
#python 全スクリプト利用DBチェック.py   --run "H:\desctop\株攻略\2-トレンドツール\fetch_all.py"   --run "H:\desctop\株攻略\1-スクリーニング自動化プログラム\自動スクリーニング.py"   --run "H:\desctop\株攻略\1-スクリーニング自動化プログラム\株探ファンダ.py"   --out-dir ".\rt_probe_out"   --verbose
import argparse, csv, os, re, sys, runpy, datetime
from pathlib import Path
from typing import List

SQL_TABLE_PATTERNS = [
    r'\bFROM\s+([A-Za-z0-9_\.]+)',
    r'\bJOIN\s+([A-Za-z0-9_\.]+)',
    r'\bINSERT\s+INTO\s+([A-Za-z0-9_\.]+)',
    r'\bUPDATE\s+([A-Za-z0-9_\.]+)',
    r'\bDELETE\s+FROM\s+([A-Za-z0-9_\.]+)',
    r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_\.]+)',
    r'\bALTER\s+TABLE\s+([A-Za-z0-9_\.]+)'
]
SQL_COLUMN_PATTERNS = [
    r'\bSELECT\s+(.*?)\s+\bFROM\b',
    r'\bWHERE\s+(.*?)\s*(?:GROUP BY|ORDER BY|LIMIT|$)',
    r'\bGROUP BY\s+(.*?)\s*(?:HAVING|ORDER BY|LIMIT|$)',
    r'\bORDER BY\s+(.*?)\s*(?:LIMIT|$)',
    r'\bINSERT\s+INTO\s+[A-Za-z0-9_\.]+\s*\((.*?)\)\s*VALUES',
    r'\bUPDATE\s+[A-Za-z0-9_\.]+\s+SET\s+(.*?)\s*(?:WHERE|RETURNING|$)',
]
TABLE_COLUMN_PAIR = re.compile(r'\b([A-Za-z0-9_]+)\s*\.\s*([A-Za-z0-9_一-龥ぁ-んァ-ン・ー％]+)')
CONTROL_STMT = re.compile(r'^(PRAGMA|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b', re.I)

def split_columns_chunk(chunk: str):
    parts = [p.strip() for p in re.split(r',(?![^()]*\))', chunk) if p.strip()]
    cols = []
    for p in parts:
        p = re.sub(r'\s+AS\s+.+$', '', p, flags=re.I).strip()
        p = re.sub(r'[\+\-\*/<>=!]+', ' ', p)
        p = re.sub(r'\b(CASE|WHEN|THEN|ELSE|END|COALESCE|IFNULL|NULLIF)\b.*', '', p, flags=re.I)
        p = re.sub(r'\b(COUNT|SUM|MAX|MIN|AVG|ROUND|CAST|DATE|DATETIME|STRFTIME)\s*\(.*?\)', '', p, flags=re.I)
        p = p.strip().split('.')[-1].strip('"`()[] ')
        if p and re.match(r'^[\w一-龥ぁ-んァ-ン・ー％]+$', p):
            cols.append(p)
    return [c for c in cols if c]

class RuntimeCollector:
    def __init__(self, verbose=False):
        self.seen_sql = []
        self._seen_keys = set()
        self.tables = set()
        self.columns = set()
        self.verbose = verbose
    def add_sql(self, sql: str):
        s = (sql or '').strip()
        if not s or CONTROL_STMT.match(s): return
        key = re.sub(r'\s+', ' ', s)
        if key in self._seen_keys: return
        self._seen_keys.add(key)
        self.seen_sql.append(s)
        for pat in SQL_TABLE_PATTERNS:
            for m in re.finditer(pat, s, re.I|re.S):
                name = (m.group(1) or '').strip().strip('";`[]')
                if name: self.tables.add(name)
        for m in TABLE_COLUMN_PAIR.finditer(s):
            self.columns.add(m.group(2))
        for pat in SQL_COLUMN_PATTERNS:
            for m in re.finditer(pat, s, re.I|re.S):
                chunk = m.group(1) or ''
                for c in split_columns_chunk(chunk):
                    self.columns.add(c)
        if self.verbose:
            print('[SQL]', s[:200].replace('\\n',' '))

def apply_connect_patch(col: RuntimeCollector):
    import sqlite3
    _orig_connect = sqlite3.connect
    def _patched_connect(*args, **kwargs):
        conn = _orig_connect(*args, **kwargs)
        try: conn.set_trace_callback(lambda stmt: col.add_sql(stmt))
        except Exception: pass
        return conn
    sqlite3.connect = _patched_connect

def write_outputs(col: RuntimeCollector, out_dir: Path, targets: List[str]):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'runtime_sql_log.txt').write_text('\n\n'.join(col.seen_sql), encoding='utf-8')
    with (out_dir / 'runtime_used_tables.csv').open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['table'])
        for t in sorted(col.tables): w.writerow([t])
    with (out_dir / 'runtime_used_columns.csv').open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['column'])
        for c in sorted(col.columns): w.writerow([c])
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    meta = [
        f'generated_at={now}',
        f'targets={targets}',
        f'sql_count={len(col.seen_sql)}',
        f'tables_count={len(col.tables)}',
        f'columns_count={len(col.columns)}',
    ]
    (out_dir / 'runtime_meta.txt').write_text('\n'.join(meta), encoding='utf-8')

def _augment_sys_path_for_script(script_path: Path, verbose: bool):
    import sys
    added = []
    def add(p: Path):
        s = str(p)
        if s not in sys.path and p.exists():
            sys.path.insert(0, s); added.append(s)
    add(script_path.parent)
    if script_path.parent.parent: add(script_path.parent.parent)
    add(script_path.parent / 'src')
    if script_path.parent.parent: add(script_path.parent.parent / 'src')
    if verbose:
        for s in added: print('[sys.path+]', s)
    return added

def run_with_probe(paths: List[str], out_dir: Path, verbose=False, passthrough_args=None):
    col = RuntimeCollector(verbose=verbose)
    apply_connect_patch(col)

    argv_saved = list(sys.argv)
    cwd_saved = Path.cwd()
    excs = []

    try:
        for path in paths:
            sp = Path(path)
            if verbose: print(f'[RUN] {sp}')
            added_paths = _augment_sys_path_for_script(sp, verbose)
            os.chdir(sp.parent)
            try:
                sys.argv = [str(sp)] + (passthrough_args or [])
                runpy.run_path(str(sp), run_name='__main__')
            except Exception as e:
                excs.append((sp, e))
            finally:
                os.chdir(cwd_saved)
                for s in added_paths:
                    try: sys.path.remove(s)
                    except ValueError: pass
                sys.argv = argv_saved
    finally:
        write_outputs(col, out_dir, paths)

    if excs:
        sp, e = excs[0]
        raise RuntimeError(f'{len(excs)} script(s) raised. first: {sp.name}: {e.__class__.__name__}: {e}') from e

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run', action='append', help='実行する .py（複数可）')
    ap.add_argument('--out-dir', default='./rt_probe_out')
    ap.add_argument('--verbose', action='store_true')
    ap.add_argument('passthrough', nargs='*', help='-- 以降は対象へ渡す')
    args = ap.parse_args()

    if not args.run:
        print('[ERROR] --run で少なくとも1ファイル指定してください', file=sys.stderr)
        sys.exit(2)

    out_dir = Path(args.out_dir)
    passthrough_args = args.passthrough if args.passthrough else []

    try:
        run_with_probe(args.run, out_dir, verbose=args.verbose, passthrough_args=passthrough_args)
        print(f'[OK] runtime probe v6 done. outputs -> {out_dir}')
    except Exception as e:
        print(f'[WARN] target raised exception: {e}', file=sys.stderr)
        print(f'[INFO] outputs were still written to: {out_dir}', file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
