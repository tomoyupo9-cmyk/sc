from __future__ import annotations
import ast, importlib.util, os, sqlite3, sys
from pathlib import Path

HERE=Path(__file__).resolve().parent
DB=Path(os.environ.get('KABU_DB_PATH', r'H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db'))
OUT=Path(os.environ.get('KABU_OUTPUT_DIR', r'H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data'))
CODES=Path(os.environ.get('KABU_CODES_PATH', r'H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt'))
MODEL_DIR=Path(os.environ.get('KABU_MODEL_DIR', str(HERE/'model')))

REQUIRED_FILES=['system_jobs.py','fetch_all.py','株探ファンダ.py','fetch_all_kabutan_themes_shinyo.py','空売り無しリスト出しスクリプト.py','浮動.py','shinden_logic.py','eod_finalize.py','yahoo_financials_daily.py','モデル学習_catboost.py','charts60_make.py','自動スクリーニング.py','template.html']
REQUIRED_TABLES=['screener','price_history','finance_notes']
OPTIONAL_TABLES=['earnings_events','tdnet_documents','forecast_achievement_history','stock_credit_margin','institution_short_sales','institution_short_snapshot']
MODULES=['pandas','numpy','requests','bs4','aiohttp','yfinance','joblib','catboost','playwright']

def ok(label,msg=''): print(f'[OK]   {label}'+(f' : {msg}' if msg else ''))
def warn(label,msg=''): print(f'[WARN] {label}'+(f' : {msg}' if msg else ''))
def fail(label,msg=''): print(f'[FAIL] {label}'+(f' : {msg}' if msg else ''))

def _top_level_functions(path: Path) -> set[str]:
    tree=ast.parse(path.read_text(encoding='utf-8-sig'),filename=str(path))
    return {
        node.name for node in tree.body
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))
    }

def main()->int:
    failures=0
    print('=== SYSTEM REWORK v5 PRE-FLIGHT (READ ONLY) ===')
    print('Python:',sys.executable,sys.version.split()[0])
    for mod in MODULES:
        if importlib.util.find_spec(mod) is None:
            # playwright/catboost are needed by specific jobs, so missing is a real deployment issue.
            fail('module '+mod,'not installed'); failures+=1
        else: ok('module '+mod)
    for name in REQUIRED_FILES:
        p=HERE/name
        if p.exists(): ok('file '+name)
        else: fail('file '+name,'missing'); failures+=1
    # 構文検査だけでは、呼び出し先の関数定義が丸ごと消えた事故を検出できない。
    # 株探ファンダの中核取得関数とweeklyオーケストレーションを明示的に検査する。
    try:
        funda_path=HERE/'株探ファンダ.py'
        funcs=_top_level_functions(funda_path)
        required_funcs={'fetch_full_year_financials','fetch_quarterly_financials','process_single_code'}
        missing=sorted(required_funcs-funcs)
        if missing:
            fail('株探ファンダ runtime contract','missing: '+', '.join(missing)); failures+=1
        else: ok('株探ファンダ runtime contract')
    except Exception as e:
        fail('株探ファンダ static check',str(e)); failures+=1
    try:
        jobs_path=HERE/'system_jobs.py'
        funcs=_top_level_functions(jobs_path)
        required_funcs={'run_daily','run_weekly','run_event_funda_refresh','run_event_theme_refresh'}
        missing=sorted(required_funcs-funcs)
        if missing:
            fail('system_jobs runtime contract','missing: '+', '.join(missing)); failures+=1
        else: ok('system_jobs daily/weekly contract')
    except Exception as e:
        fail('system_jobs static check',str(e)); failures+=1
    if DB.exists(): ok('DB',str(DB))
    else:
        fail('DB',f'missing: {DB}'); failures+=1
        return 2
    try:
        uri=DB.resolve().as_uri()+'?mode=ro'
        conn=sqlite3.connect(uri,uri=True,timeout=5)
        tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for t in REQUIRED_TABLES:
            if t in tables: ok('table '+t)
            else: fail('table '+t,'missing'); failures+=1
        for t in OPTIONAL_TABLES:
            if t in tables: ok('table '+t)
            else: warn('table '+t,'not present yet / producer may create or migrate it')
        try:
            row=conn.execute('SELECT COUNT(*) FROM screener').fetchone(); ok('screener rows',str(row[0]))
        except Exception as e: fail('screener read',str(e)); failures+=1
        try:
            row=conn.execute('SELECT MAX(日付) FROM price_history').fetchone(); ok('price_history latest',str(row[0]))
        except Exception as e: fail('price_history read',str(e)); failures+=1
        conn.close()
    except Exception as e:
        fail('DB read-only open',str(e)); failures+=1
    if CODES.exists(): ok('codes file',str(CODES))
    else: warn('codes file',f'missing: {CODES}')
    if OUT.exists():
        ok('output dir',str(OUT))
        if os.access(OUT,os.W_OK): ok('output writable')
        else: fail('output writable','no write permission'); failures+=1
    else: warn('output dir',f'will need creation: {OUT}')
    if MODEL_DIR.exists(): ok('model dir',str(MODEL_DIR))
    else: warn('model dir',f'not present yet: {MODEL_DIR}')
    print('---')
    if failures:
        print(f'PRE-FLIGHT FAILED: {failures} required issue(s)')
        return 2
    print('PRE-FLIGHT PASS')
    return 0

if __name__=='__main__': raise SystemExit(main())
