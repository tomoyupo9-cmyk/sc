import os, tempfile, sqlite3, importlib.util, asyncio, re
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

BASE=Path('/mnt/data/system_rework_work')

def load(name,file):
    spec=importlib.util.spec_from_file_location(name,BASE/file)
    m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

passed=[]
def ok(name, cond=True):
    assert cond, name; passed.append(name)

# 1 credit migration
th=load('th','fetch_all_kabutan_themes_shinyo.py')
c=sqlite3.connect(':memory:')
c.execute('create table screener(コード text primary key)'); c.execute("insert into screener values('7203')")
c.execute('create table stock_credit_margin(コード text primary key, 基準日 text, 売り残 integer, 買い残 integer, 倍率 real, 取得日 text)')
c.execute("insert into stock_credit_margin values('7203','2026-08-15',1,2,2.0,'2026-08-15')")
c.commit()
th.ensure_schema(c)
pk=[r[1] for r in c.execute('pragma table_info(stock_credit_margin)') if r[5]]
ok('credit_pk_migration',pk==['コード','基準日'])
ok('credit_legacy_preserved',c.execute('select count(*) from stock_credit_margin').fetchone()[0]==1)

# 2 schedule no historical fallback
from bs4 import BeautifulSoup
c.execute('alter table screener add column 決算発表予定日 text') if '決算発表予定日' not in [r[1] for r in c.execute('pragma table_info(screener)')] else None
c.execute("update screener set 決算発表予定日='2099/01/01'")
th._update_earnings_schedule(c,'7203',BeautifulSoup('<html/>','html.parser'))
ok('earnings_schedule_missing_is_null',c.execute('select 決算発表予定日 from screener').fetchone()[0] is None)

# 3 system job state + orchestration
with tempfile.TemporaryDirectory() as td:
    os.environ['KABU_DB_PATH']=str(Path(td)/'state.db')
    os.environ['KABU_JOB_STATE_DB']=str(Path(td)/'jobs.db')
    sj=load('sj','system_jobs.py')
    sj.state_finish('x','partial',2,'x'); ok('partial_not_fresh',not sj.is_fresh('x',999))
    ok('cutoff_before_18',sj._required_daily_cutoff(datetime(2026,8,16,15))==datetime(2026,8,15,18))
    ok('cutoff_after_18',sj._required_daily_cutoff(datetime(2026,8,16,20))==datetime(2026,8,16,18))
    # lock exclusion
    with sj.writer_lock() as a:
        with sj.writer_lock() as b:
            ok('writer_lock_exclusive',a is True and b is False)
    # tdnet enrichment partial acceptable as daily core
    sj.state_start=lambda *a,**k:None; sj.state_finish=lambda *a,**k:None
    calls=[]
    def rs(job,script,args=None,timeout_sec=0):
        calls.append(job)
        return ('partial',2) if job=='daily_fetch_all' else ('success',0)
    sj.run_script=rs
    st,rc=sj.run_daily_core(); ok('daily_core_accepts_tdnet_enrichment_partial',(st,rc)==('success',0))
    # optional doesn't cause task retry
    sj._job_success_since=lambda *a,**k:True; sj.is_fresh=lambda *a,**k:False
    def rs2(job,script,args=None,timeout_sec=0):
        if job=='morning_karauri': return ('failed',1)
        return ('success',0)
    sj.run_script=rs2
    st,rc=sj.run_morning(); ok('optional_failure_no_task_retry',st=='partial' and rc==0)

# 4 shinden --full zero -> nonzero
sh=load('sh','shinden_logic.py')
with tempfile.TemporaryDirectory() as td:
    db=Path(td)/'a.db'; sqlite3.connect(db).close()
    sh.apply_shinden_pattern_metrics=lambda *a,**k:0
    rc=sh.main(['--db',str(db),'--full','--quiet'])
    ok('shinden_full_zero_fails',rc==3)

# 5 fetch_all exit contract
fa=load('fa','fetch_all.py')
def configure_fa(mode):
    cc=sqlite3.connect(':memory:')
    fa.get_db_conn=lambda:cc
    for n in ['ensure_earnings_schema','ensure_offerings_schema','ensure_tob_schema','ensure_tdnet_documents_schema','ensure_forecast_history_schema','ensure_forecast_achievement_schema','ensure_tdnet_xbrl_metrics_schema']:
        setattr(fa,n,lambda conn:None)
    fa._latest_teishutsu_ts=lambda conn:None
    fa.fetch_earnings_tdnet_only=lambda **k:[]; fa.tdnet_items_to_earnings_rows=lambda x:[]
    fa.fetch_tdnet_by_keywords=lambda **k:[]; fa.fetch_tdnet_tob=lambda **k:[]
    fa.upsert_offerings_events=lambda *a:None; fa.upsert_tob_events=lambda *a:None; fa.upsert_earnings_rows=lambda *a:None
    fa.upsert_tdnet_document_texts=lambda *a,**k:0
    fa.update_daily_forecast_history=lambda *a,**k:{'initial_saved':0,'revision_saved':0,'achievement_updated':0}
    if mode=='partial': fa.upsert_tdnet_document_texts=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('doc'))
    if mode=='failed': fa.fetch_earnings_tdnet_only=lambda **k:(_ for _ in ()).throw(RuntimeError('earn'))
for mode,exp in [('success',0),('partial',2),('failed',1)]:
    configure_fa(mode); ok('fetch_all_'+mode,fa.main()==exp)

# 6 short snapshot states
ks=load('ks','空売り無しリスト出しスクリプト.py')
with tempfile.TemporaryDirectory() as td:
    db=Path(td)/'k.db'; ks.init_db(db)
    res=[({'code':'7203','success':1,'has_short':0,'detail_count':0,'error':None},[]),
         ({'code':'6758','success':0,'has_short':None,'detail_count':0,'error':'x'},[])]
    suc,fail,none=ks.save_results(db,res)
    ok('karauri_no_short_vs_failure',(suc,fail,none)==(1,1,1))

# 7 fair value single owner in funda
text=(BASE/'株探ファンダ.py').read_text(encoding='utf-8')
ok('funda_no_fairvalue_writer','UPDATE screener SET 適正株価' not in text and 'class FairValueEngine' not in text)

# 8 XML contract
ns={'t':'http://schemas.microsoft.com/windows/2004/02/mit/task'}
def root(name): return ET.parse(BASE/'task_xml'/name).getroot()
live=root('LIVE_MATERIALS.xml'); scr=root('株集計_新構成_自動スクリーニング改修後.xml')
live_start=live.find('.//t:CalendarTrigger/t:StartBoundary',ns).text
scr_start=scr.find('.//t:CalendarTrigger/t:StartBoundary',ns).text
ok('scheduler_stagger_5min','T08:00:00' in live_start and 'T08:05:00' in scr_start)
for name in ['DAILY_MAINTENANCE.xml','MORNING_CATCHUP.xml']:
    r=root(name); ok('restart_'+name, r.find('.//t:RestartOnFailure',ns) is not None)
for p in (BASE/'task_xml').glob('*.xml'):
    r=ET.parse(p).getroot(); w=r.find('.//t:WakeToRun',ns); ok('no_wake_'+p.name,w is not None and w.text=='false')


# 9 model EOD marker gate
mdl=load('mdl','モデル学習_catboost.py')
with tempfile.TemporaryDirectory() as td:
    ok('model_eod_marker_absent', not mdl._eod_marker_ready(td))
    mp=Path(td)/'last_yahoo_bulk_refresh.txt'
    mp.write_text(datetime.now().date().isoformat()+'|build',encoding='utf-8')
    ok('model_eod_marker_present', mdl._eod_marker_ready(td))
text_sj=(BASE/'system_jobs.py').read_text(encoding='utf-8')
ok('system_jobs_model_requires_eod_marker','--require-eod-marker' in text_sj)

print(f'ALL_SYSTEM_REWORK_TESTS_PASS {len(passed)}/{len(passed)}')
for x in passed: print('PASS',x)
