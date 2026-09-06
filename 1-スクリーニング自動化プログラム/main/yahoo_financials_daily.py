# -*- coding: utf-8 -*-
"""Yahoo/yahooquery財務補完producer。

通常はrefresh-days以内のcacheを再利用し、欠損/schema更新/最新決算後は即再取得する。
--force-refresh は障害復旧・手動監査用の明示的な全件再取得として残す。
"""
from __future__ import annotations
import argparse
import importlib.util
import os
from pathlib import Path

HERE=Path(__file__).resolve().parent
SCANNER_PATH=Path(os.environ.get('KABU_SCANNER_PATH', str(HERE/'自動スクリーニング.py')))

def _load_scanner():
    if not SCANNER_PATH.exists(): raise FileNotFoundError(f'scanner not found: {SCANNER_PATH}')
    os.environ.setdefault('KABU_EXTERNAL_JOBS_REQUIRED','0')
    spec=importlib.util.spec_from_file_location('_kabu_scanner_yfin',str(SCANNER_PATH))
    if spec is None or spec.loader is None: raise ImportError(str(SCANNER_PATH))
    m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

def main(argv=None)->int:
    ap=argparse.ArgumentParser(description='Yahoo財務補完（週次全件更新）')
    ap.add_argument('--force-refresh',action='store_true',help='cacheを無視して全銘柄を再取得（手動復旧/監査用）')
    ap.add_argument('--refresh-days',type=int,default=7,help='通常cacheの最大日数。weekly smartは28推奨')
    args=ap.parse_args(argv)
    try:
        m=_load_scanner(); conn=m._get_db_conn()
        try:
            m.ensure_runlog_schema(conn)
            summary=m.batch_update_all_financials(
                conn,
                chunk_size=200,
                force_refresh=bool(args.force_refresh),
                refresh_days=max(1,int(args.refresh_days)),
                verbose=True,
            )
            print(f'[yahoo-financials] complete: {summary}')
            return 0
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as e:
        print(f'[yahoo-financials][ERROR] {type(e).__name__}: {e}')
        return 2

if __name__=='__main__':
    raise SystemExit(main())
