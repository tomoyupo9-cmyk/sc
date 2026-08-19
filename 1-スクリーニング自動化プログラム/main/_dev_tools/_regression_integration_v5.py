import ast, importlib.util, os, sqlite3, tempfile, pathlib, subprocess, sys, time
from datetime import datetime, timedelta
HERE=pathlib.Path(__file__).resolve().parent
SC=HERE/'自動スクリーニング.py'; SJ=HERE/'system_jobs.py'; EOD=HERE/'eod_finalize.py'; TPL=HERE/'template.html'
checks=[]
def ck(name,cond,detail=''):
    checks.append((name,bool(cond),detail)); print(('PASS' if cond else 'FAIL'),name,detail)

for p in HERE.glob('*.py'):
    subprocess.run([sys.executable,'-m','py_compile',str(p)],check=True)
ck('all_py_compile',True)
s=SC.read_text(encoding='utf-8'); t=ast.parse(s); main=next(n for n in t.body if isinstance(n,ast.FunctionDef) and n.name=='main'); ms=ast.get_source_segment(s,main) or ''
for n in ['_run_fetch_all','run_fundamental_daily','run_karauri_script','run_shinden_daily','phase_yahoo_bulk_refresh','refresh_full_history_for_insufficient','update_market_cap_all','update_seasonal_progress_v3']:
    ck('scanner_main_no_'+n,n not in ms)
ck('scanner_no_19h','eod_19_full_batch' not in ms and '_now.hour == 19' not in ms)
ck('template_quality_ui','quality-health' in TPL.read_text(encoding='utf-8'))
ck('yahoo_fin_daily_exists',(HERE/'yahoo_financials_daily.py').exists())
ck('system_jobs_has_yahoo_fin','daily_yahoo_financials' in SJ.read_text(encoding='utf-8'))
ck('system_jobs_has_eod','live_eod_finalize' in SJ.read_text(encoding='utf-8'))
ck('system_jobs_has_event_funda','run_event_funda_refresh' in SJ.read_text(encoding='utf-8'))

os.environ['KABU_EXTERNAL_JOBS_REQUIRED']='0'
spec=importlib.util.spec_from_file_location('m',SC); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); ck('scanner_import',True)
# theme DB-only
fd,db=tempfile.mkstemp(suffix='.db'); os.close(fd); c=sqlite3.connect(db)
c.execute('create table theme_master(theme_id integer primary key,theme_name text)'); c.execute('create table stock_theme_kabutan(コード text,theme_id integer,取得日 text,primary key(コード,theme_id))'); c.execute("insert into theme_master values(1,'半導体')"); c.execute("insert into stock_theme_kabutan values('7203',1,'2026-08-16')"); c.commit(); m._update_kabutan_theme_ranking=lambda conn: (_ for _ in ()).throw(RuntimeError('network called'))
try: ck('theme_db_only',m.phase_refresh_theme_snapshot(c)=='2026-08-16')
except Exception as e: ck('theme_db_only',False,str(e))
c.close(); pathlib.Path(db).unlink()
# karauri authoritative semantics
fd,db=tempfile.mkstemp(suffix='.db'); os.close(fd); c=sqlite3.connect(db); c.execute('create table screener(コード text,空売り機関 text)'); c.executemany('insert into screener values(?,?)',[('7203','なし'),('6758','なし'),('9984',None)]); c.execute('create table institution_short_snapshot(code text,snapshot_date text,crawl_success integer,has_short integer,detail_count integer,checked_at text,error text,primary key(code,snapshot_date))'); td=m._today_jst(); c.executemany('insert into institution_short_snapshot values(?,?,?,?,?,?,?)',[('7203',td,1,0,0,'x',None),('6758',td,1,1,1,'x',None),('9984',td,0,None,0,'x','err')]); c.commit(); comp=m.phase_mark_karauri_nashi(c); got=dict(c.execute('select コード,空売り機関 from screener').fetchall()); ck('karauri_failure_not_none',comp is False and got['7203']=='なし' and got['6758'] is None and got['9984'] is None,str(got)); c.close(); pathlib.Path(db).unlink()
# external state freshness, including EOD requirement
root=pathlib.Path(tempfile.mkdtemp()); m.SYSTEM_JOB_STATE_DB=root/'state.db'; c=sqlite3.connect(m.SYSTEM_JOB_STATE_DB); c.execute('create table system_job_state(job_name text primary key,last_started_at text,last_finished_at text,last_success_at text,status text,return_code integer,message text)'); now=datetime.now().isoformat(timespec='seconds'); c.executemany('insert into system_job_state values(?,?,?,?,?,?,?)',[('live_materials',now,now,now,'success',0,'ok'),('live_eod_finalize',now,now,now,'success',0,'ok'),('live_shinden_full',now,now,now,'success',0,'ok')]); c.commit(); c.close(); oldmode=m._auto_run_mode; m._auto_run_mode=lambda:'EOD'; ck('eod_external_ready',m._external_live_materials_ready(45)[0]); c=sqlite3.connect(m.SYSTEM_JOB_STATE_DB); c.execute("update system_job_state set status='partial' where job_name='live_eod_finalize'"); c.commit(); c.close(); ck('eod_partial_rejected',not m._external_live_materials_ready(45)[0]); m._auto_run_mode=oldmode
# shared lock
m.SYSTEM_WRITER_LOCK=root/'runtime'/'db_writer.lock'; a=m._acquire_shared_writer_lock(); b=m._acquire_shared_writer_lock(); ck('shared_lock_exclusive',bool(a) and b is None); m._release_shared_writer_lock(a)
# fake EOD flow
spec=importlib.util.spec_from_file_location('eodm',EOD); e=importlib.util.module_from_spec(spec); spec.loader.exec_module(e)
class C:
    def execute(self,q,*args):
        if 'SELECT コード FROM screener' in q: return type('R',(),{'fetchall':lambda self:[('7203',)]})()
        return type('R',(),{'fetchall':lambda self:[]})()
    def close(self): pass
calls=[]
def f(name,ret=None):
    def z(*a,**k): calls.append(name); return ret
    return z
fake=type('M',(),{})(); fake._now_jst=lambda:datetime(2026,8,17,15,50); fake._is_jp_business_day=lambda d:True; fake._get_db_conn=lambda:C(); fake.ensure_runlog_schema=f('runlog'); fake._cleanup_screener_logical_duplicates=f('cleanup'); fake.phase_sync_finance_comments=f('syncfin'); fake.phase_refresh_theme_snapshot=f('theme'); fake.canonical_code_for_db=lambda x:str(x); fake._timed_daily_once=lambda name,func,*a,**k:(calls.append(name),func(*a,**k))[1]; fake.phase_yahoo_bulk_refresh=f('yahoo'); fake.refresh_full_history_for_insufficient=f('hist'); fake._require_current_price_history_snapshot=f('require'); fake.compute_right_up_persistent=f('rup'); fake.compute_right_up_early_triggers=f('rue'); fake.phase_update_margin_metrics=f('margin'); fake.update_market_cap_all=f('mcap'); fake.update_seasonal_progress=f('season'); fake.phase_derive_update=f('derive'); fake.phase_update_market_metrics=f('market'); fake.phase_signal_detection=f('signal'); fake.phase_snapshot_shodou_baseline=f('base'); fake.phase_update_shodou_multipliers=f('mult'); fake.apply_shodou_score=f('shodou'); fake.phase_update_since_dates=f('since'); fake.phase_validate_prev_business_day=f('validate',True); fake.update_operating_income_and_ratio=f('op'); fake.phase_sync_latest_prices=f('syncprice'); e._load_scanner=lambda:fake; rc=e.main(); ck('eod_flow',rc==0 and 'yahoo' in calls and 'syncprice' in calls,str(calls))

# P3-43 chart path + PTS freshness contracts
src_sc=SC.read_text(encoding='utf-8')
src_f=(HERE/'株探ファンダ.py').read_text(encoding='utf-8')
src_post=(HERE/'決算後上昇スクリーニング.py').read_text(encoding='utf-8')
ck('chart_defaults_output_dir','charts_dir=None' in src_sc and 'charts_dir = charts_dir or os.path.join(OUTPUT_DIR, "charts60")' in src_sc)
ck('funda_pts_timestamp','PTS取得日時' in src_f and 'UPDATE screener SET PTS株価=?, PTS時刻=?, PTS取得日時=?' in src_f)
ck('funda_dead_batch_writer_removed','def batch_record_to_sqlite' not in src_f)
ck('post_earnings_env_paths','KABU_DB_PATH' in src_post and 'KABU_OUTPUT_DIR' in src_post)
_old_now=m._now_jst; _old_prev=m.prev_business_day_jp
m._now_jst=lambda: datetime.fromisoformat('2026-08-17T08:30:00+09:00')
m.prev_business_day_jp=lambda d: d-timedelta(days=1)
r=[{'PTS株価':100.0,'PTS時刻':'23:00','PTS取得日時':'2026-08-16T21:00:00'}]
m._mask_stale_pts_for_run(r,'PREOPEN'); ck('pts_prevnight_preopen_allowed',r[0]['PTS株価']==100.0,str(r))
r=[{'PTS株価':100.0,'PTS時刻':'23:00','PTS取得日時':'2026-08-16T21:00:00'}]
m._mask_stale_pts_for_run(r,'MIDDAY'); ck('pts_prevnight_midday_masked',r[0]['PTS株価'] is None,str(r))
r=[{'PTS株価':101.0,'PTS時刻':'16:00','PTS取得日時':'2026-08-17T16:05:00'}]
m._mask_stale_pts_for_run(r,'EOD'); ck('pts_same_day_eod_allowed',r[0]['PTS株価']==101.0,str(r))
m._now_jst=_old_now; m.prev_business_day_jp=_old_prev

# PREOPEN accepts either morning or live full snapshot from today (08:00 race-safe)
_old_ext=m.EXTERNAL_JOBS_REQUIRED; _old_mode=m._auto_run_mode; _old_state=m._external_job_state; _old_today=m._today_jst
m.EXTERNAL_JOBS_REQUIRED=True; m._auto_run_mode=lambda:'PREOPEN'; m._today_jst=lambda:datetime.now().date().isoformat()
_nowiso=datetime.now().isoformat(timespec='seconds')
m._external_job_state=lambda j: ({'status':'success','last_finished_at':_nowiso} if j=='live_shinden_full' else {})
class ShC:
    def execute(self,*a,**k): return type('R',(),{'fetchone':lambda self:(datetime.now().date().isoformat(),)})()
ck('preopen_live_shinden_fallback',m._shinden_snapshot_is_current(ShC()))
m.EXTERNAL_JOBS_REQUIRED=_old_ext; m._auto_run_mode=_old_mode; m._external_job_state=_old_state; m._today_jst=_old_today

# Crash/reboot stale lock recovery
spec=importlib.util.spec_from_file_location('sjm',SJ); sjm=importlib.util.module_from_spec(spec); spec.loader.exec_module(sjm)
_lroot=pathlib.Path(tempfile.mkdtemp())
sjm.LOCK_PATH=_lroot/'shared.lock'
sjm.LOCK_PATH.write_text(__import__('json').dumps({'pid':99999999,'boot':sjm._boot_token()}),encoding='utf-8')
with sjm.writer_lock() as acq:
    ck('system_jobs_dead_pid_lock_recovered',acq is True)
# scanner shared lock
m.SYSTEM_WRITER_LOCK=_lroot/'scanner.lock'
m.SYSTEM_WRITER_LOCK.write_text(__import__('json').dumps({'pid':99999999,'boot':m._boot_token()}),encoding='utf-8')
_tok=m._acquire_shared_writer_lock(); ck('scanner_dead_pid_lock_recovered',bool(_tok)); m._release_shared_writer_lock(_tok)
# scanner daily phase lock
_old_out=m.OUTPUT_DIR; m.OUTPUT_DIR=str(_lroot/'out'); pathlib.Path(m.OUTPUT_DIR).mkdir(parents=True,exist_ok=True)
_lp=pathlib.Path(m.OUTPUT_DIR)/'last_deadlock_test.txt.lock'; _lp.write_text(f'pid=99999999 boot={m._boot_token()} started=2026-08-16 08:00:00\n',encoding='utf-8')
_claim=m._claim_daily_phase_lock('deadlock_test'); ck('daily_phase_dead_pid_lock_recovered',bool(_claim)); m._release_daily_phase_lock(_claim); m.OUTPUT_DIR=_old_out

# Live owner must never be stolen just because mtime is old
sjm.LOCK_PATH=_lroot/'alive-old.lock'; sjm.LOCK_PATH.write_text(__import__('json').dumps({'pid':os.getpid(),'boot':sjm._boot_token()}),encoding='utf-8'); os.utime(sjm.LOCK_PATH,(time.time()-10*3600,time.time()-10*3600))
with sjm.writer_lock(stale_hours=6) as acq:
    ck('system_jobs_alive_old_lock_preserved',acq is False)
m.SYSTEM_WRITER_LOCK=_lroot/'scanner-alive-old.lock'; m.SYSTEM_WRITER_LOCK.write_text(__import__('json').dumps({'pid':os.getpid(),'boot':m._boot_token()}),encoding='utf-8'); os.utime(m.SYSTEM_WRITER_LOCK,(time.time()-10*3600,time.time()-10*3600)); ck('scanner_alive_old_lock_preserved',m._acquire_shared_writer_lock(stale_hours=6) is None)
_old_out=m.OUTPUT_DIR; m.OUTPUT_DIR=str(_lroot/'out2'); pathlib.Path(m.OUTPUT_DIR).mkdir(parents=True,exist_ok=True); _lp=pathlib.Path(m.OUTPUT_DIR)/'last_alive_test.txt.lock'; _lp.write_text(f'pid={os.getpid()} boot={m._boot_token()} started=2026-08-16 00:00:00\n',encoding='utf-8'); os.utime(_lp,(time.time()-20*3600,time.time()-20*3600)); ck('daily_phase_alive_old_lock_preserved',m._claim_daily_phase_lock('alive_test',stale_seconds=12*3600) is None); m.OUTPUT_DIR=_old_out

fails=[x for x in checks if not x[1]]; print(f'ALL_INTEGRATION_V5 {len(checks)-len(fails)}/{len(checks)} PASS');
if fails:
    print('FAILURES',fails); raise SystemExit(1)
