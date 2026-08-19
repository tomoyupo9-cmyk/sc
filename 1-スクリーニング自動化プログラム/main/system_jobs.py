# -*- coding: utf-8 -*-
"""
株スクリーニング基盤のTask Scheduler向けオーケストレータ。

Task Schedulerは時刻だけを担当し、依存順・再実行・writer競合回避はここで管理する。

modes:
  daily          夜の差分producer更新
  weekly         週1回の株探ファンダ全銘柄補修
  morning        朝/ログオン時catch-up + TDnet + シンデンfull
  live-materials TDnet増分 + シンデンfull（場中10分周期向け・EOD重処理なし）
  eod-finalize   引け後EOD確定を独立実行。同日成功後はno-op
  model          新EOD足がある時だけCatBoost再学習

2026-08-17 P3-49:
  EOD子プロセスの孤児化事故を受け、Task Schedulerの2時間制限より前に内部timeoutし、
  Windowsではtaskkill /T /Fでプロセスツリーを回収する。回収不能時はwriter lockを残す。

2026-08-19 P4-LIVE-EOD-SPLIT:
  live-materialsからeod_finalize.pyを分離。場中/材料更新を重いEOD処理で塞がない。
  eod-finalize独立modeは15:30以降のみ実行し、同日成功後は次回triggerをno-opにする。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_DB_PATH = os.environ.get(
    "KABU_DB_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db",
)
DEFAULT_CODES_PATH = os.environ.get(
    "KABU_CODES_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt",
)
DEFAULT_OUTPUT_DIR = os.environ.get(
    "KABU_OUTPUT_DIR",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data",
)
DEFAULT_MODEL_DIR = os.environ.get("KABU_MODEL_DIR", str(HERE / "model"))
LOG_DIR = Path(os.environ.get("KABU_JOB_LOG_DIR", str(HERE / "runtime_logs")))
LOCK_PATH = Path(os.environ.get("KABU_JOB_LOCK", str(HERE / "runtime" / "db_writer.lock")))
STATE_DB_PATH = Path(os.environ.get("KABU_JOB_STATE_DB", str(HERE / "runtime" / "system_jobs_state.db")))

SUCCESS_STATUSES = {"success"}
STOCK_CODE_RE = re.compile(r"^(?:\d{4}|\d{3}[A-Z])$")

# P3-49 (2026-08-17): Task Scheduler側の実行時間制限（2時間）より先に
# オーケストレータ自身がEOD子プロセスを回収する。親Taskが先に強制終了されると
# eod_finalize.pyだけが孤児化し、DB writer lockを保持したまま残るため。
try:
    EOD_FINALIZE_TIMEOUT_SEC = int(os.environ.get("KABU_EOD_FINALIZE_TIMEOUT_SEC", "2700"))
except (TypeError, ValueError):
    EOD_FINALIZE_TIMEOUT_SEC = 2700
EOD_FINALIZE_TIMEOUT_SEC = max(300, min(EOD_FINALIZE_TIMEOUT_SEC, 2 * 3600 - 60))


def _normalize_stock_code(value) -> str | None:
    """株探へ渡せる東証銘柄コードだけを返す。指数・0000・欠損値は除外。"""
    code = str(value or "").strip().upper()
    if code.endswith(".0"):
        code = code[:-2]
    if not STOCK_CODE_RE.fullmatch(code):
        return None
    if code.isdigit() and int(code) < 1000:
        return None
    return code


def now_s() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _conn() -> sqlite3.Connection:
    # ジョブ状態は市場データkani2.dbから分離する。
    # model/read-only処理の状態記録だけでkani2へwriter競合を起こさない。
    STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(STATE_DB_PATH), timeout=30.0)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA busy_timeout=30000;")
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_job_state (
            job_name TEXT PRIMARY KEY,
            last_started_at TEXT,
            last_finished_at TEXT,
            last_success_at TEXT,
            status TEXT,
            return_code INTEGER,
            message TEXT
        )
    """)
    c.commit()
    return c


def state_get(job_name: str) -> dict:
    c = _conn()
    try:
        row = c.execute("""
            SELECT last_started_at,last_finished_at,last_success_at,status,return_code,message
            FROM system_job_state WHERE job_name=?
        """, (job_name,)).fetchone()
    finally:
        c.close()
    if not row:
        return {}
    return dict(zip(
        ["last_started_at", "last_finished_at", "last_success_at", "status", "return_code", "message"],
        row,
    ))


def state_start(job_name: str, message: str = "") -> None:
    c = _conn()
    try:
        c.execute("""
            INSERT INTO system_job_state(job_name,last_started_at,status,message)
            VALUES(?,?,?,?)
            ON CONFLICT(job_name) DO UPDATE SET
              last_started_at=excluded.last_started_at,
              status=excluded.status,
              message=excluded.message
        """, (job_name, now_s(), "running", message))
        c.commit()
    finally:
        c.close()


def state_finish(job_name: str, status: str, rc: int, message: str = "") -> None:
    finished = now_s()
    c = _conn()
    try:
        last_success = finished if status in SUCCESS_STATUSES else None
        c.execute("""
            INSERT INTO system_job_state(
              job_name,last_finished_at,last_success_at,status,return_code,message
            ) VALUES(?,?,?,?,?,?)
            ON CONFLICT(job_name) DO UPDATE SET
              last_finished_at=excluded.last_finished_at,
              last_success_at=CASE
                WHEN excluded.last_success_at IS NOT NULL THEN excluded.last_success_at
                ELSE system_job_state.last_success_at
              END,
              status=excluded.status,
              return_code=excluded.return_code,
              message=excluded.message
        """, (job_name, finished, last_success, status, int(rc), message[:1000]))
        c.commit()
    finally:
        c.close()


def is_fresh(job_name: str, max_age_hours: float) -> bool:
    st = state_get(job_name)
    if st.get("status") not in SUCCESS_STATUSES:
        return False
    ts = st.get("last_finished_at") or st.get("last_success_at")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(str(ts))
    except Exception:
        return False
    return datetime.now() - dt <= timedelta(hours=max(0.0, max_age_hours))


def _boot_token() -> str:
    """再起動を跨いだ残骸lockを即判定するためのboot識別子。"""
    try:
        if os.name == "nt":
            import ctypes
            uptime_ms = int(ctypes.windll.kernel32.GetTickCount64())
            boot_epoch_min = int((time.time() - uptime_ms / 1000.0) // 60)
            return f"win:{boot_epoch_min}"
        p = Path("/proc/sys/kernel/random/boot_id")
        if p.exists():
            return "linux:" + p.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return "unknown"


def _pid_is_alive(pid) -> bool:
    try:
        pid = int(pid)
    except Exception:
        return False
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        if os.name == "nt":
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if h:
                ctypes.windll.kernel32.CloseHandle(h)
                return True
            return False
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _lock_owner_is_stale(path: Path) -> bool:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    old_boot = str(data.get("boot") or "")
    cur_boot = _boot_token()
    if old_boot and old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
        return True
    pids = [data.get("pid"), data.get("child_pid")]
    known = [pid for pid in pids if pid is not None]
    return bool(known) and not any(_pid_is_alive(pid) for pid in known)


def _lock_owner_is_alive(path: Path) -> bool:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    old_boot = str(data.get("boot") or "")
    cur_boot = _boot_token()
    if old_boot and old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
        return False
    pids = [data.get("pid"), data.get("child_pid")]
    return any(pid is not None and _pid_is_alive(pid) for pid in pids)


def _set_writer_lock_child_pid(child_pid=None):
    """親Taskが強制終了しても、生存childがDBを書いている間はlockを保持する。"""
    try:
        data = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
        if int(data.get("pid") or -1) != os.getpid():
            return
        if child_pid is None:
            data.pop("child_pid", None); data.pop("child_started_at", None)
        else:
            data["child_pid"] = int(child_pid); data["child_started_at"] = now_s()
        tmp = LOCK_PATH.with_suffix(LOCK_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, LOCK_PATH)
    except Exception:
        pass


@contextmanager
def writer_lock(stale_hours: float = 6.0):
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    owned = False
    try:
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            owned = True
        except FileExistsError:
            try:
                age = time.time() - LOCK_PATH.stat().st_mtime
            except Exception:
                age = 0
            if _lock_owner_is_stale(LOCK_PATH) or (not _lock_owner_is_alive(LOCK_PATH) and age > stale_hours * 3600):
                try:
                    LOCK_PATH.unlink()
                except Exception:
                    pass
                fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                owned = True
            else:
                yield False
                return
        payload = json.dumps({"pid": os.getpid(), "boot": _boot_token(), "acquired_at": now_s()}, ensure_ascii=False)
        os.write(fd, payload.encode("utf-8"))
        os.close(fd)
        fd = None
        yield True
    finally:
        if fd is not None:
            try: os.close(fd)
            except Exception: pass
        if owned:
            try:
                if LOCK_PATH.exists():
                    # P3-49: child回収に失敗した時だけはlockを残す。
                    # 生存中の孤児writerを「空き」と誤判定して別writerを重ねない。
                    try:
                        data = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
                    except Exception:
                        data = {}
                    child_pid = data.get("child_pid")
                    if child_pid is not None and _pid_is_alive(child_pid):
                        print(
                            f"[writer-lock] child pid={child_pid} is still alive; lock preserved",
                            flush=True,
                        )
                    else:
                        LOCK_PATH.unlink()
            except Exception:
                pass


def _log_path(job_name: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"{job_name}_{stamp}.log"


def _terminate_process_tree(cp: subprocess.Popen, grace_sec: float = 10.0) -> bool:
    """子ジョブとその配下を終了し、回収できた場合だけTrueを返す。

    P3-49 incident note:
    2026-08-17に親pythonwだけが終了し、eod_finalize.pyが3時間超生存した。
    WindowsのPopen.terminate()/kill()は子孫を保証しないため taskkill /T /F を使う。
    """
    if cp.poll() is not None:
        return True

    try:
        if os.name == "nt":
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            subprocess.run(
                ["taskkill", "/PID", str(cp.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=max(15.0, grace_sec + 5.0),
                creationflags=flags,
            )
            cp.wait(timeout=grace_sec)
        else:
            try:
                os.killpg(cp.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                cp.wait(timeout=grace_sec)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(cp.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                cp.wait(timeout=grace_sec)
    except Exception as e:
        print(f"[process-tree] primary termination failed: {type(e).__name__}: {e}", flush=True)

    if cp.poll() is None:
        try:
            cp.kill()
            cp.wait(timeout=grace_sec)
        except Exception as e:
            print(f"[process-tree] fallback kill failed: {type(e).__name__}: {e}", flush=True)
    return cp.poll() is not None


def run_script(job_name: str, script_name: str, args: list[str] | None = None, *, timeout_sec: int = 4 * 3600) -> tuple[str, int]:
    script = HERE / script_name
    if not script.exists():
        msg = f"script missing: {script}"
        state_start(job_name, msg)
        state_finish(job_name, "failed", 127, msg)
        return "failed", 127

    env = os.environ.copy()
    env["KABU_DB_PATH"] = DEFAULT_DB_PATH
    env.setdefault("KABU_CODES_PATH", DEFAULT_CODES_PATH)
    env.setdefault("KABU_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)
    env.setdefault("KABU_MODEL_DIR", DEFAULT_MODEL_DIR)

    cmd = [sys.executable, str(script), *(args or [])]
    state_start(job_name, " ".join(cmd))
    log_path = _log_path(job_name)
    print(f"[START] {job_name}: {script_name}", flush=True)
    print(f"[LOG]   {log_path}", flush=True)
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\n\n===== {now_s()} START {' '.join(cmd)} =====\n")
        log.flush()
        try:
            popen_kwargs = dict(
                cwd=str(HERE),
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            if os.name == "nt":
                popen_kwargs["creationflags"] = (
                    getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                )
            else:
                # timeout時に子孫までkillpgで回収できる独立process group。
                popen_kwargs["start_new_session"] = True
            cp = subprocess.Popen(cmd, **popen_kwargs)
            _set_writer_lock_child_pid(cp.pid)
            try:
                started = time.monotonic()
                next_heartbeat = 30.0
                while True:
                    polled = cp.poll()
                    if polled is not None:
                        rc = int(polled)
                        break
                    elapsed = time.monotonic() - started
                    if elapsed >= timeout_sec:
                        print(
                            f"[TIMEOUT] {job_name}: elapsed={int(elapsed)}s; terminating process tree",
                            flush=True,
                        )
                        terminated = _terminate_process_tree(cp)
                        rc = 124 if terminated else 126
                        break
                    if elapsed >= next_heartbeat:
                        print(f"[RUNNING] {job_name}: elapsed={int(elapsed)}s", flush=True)
                        next_heartbeat += 30.0
                    time.sleep(1.0)
            finally:
                if cp.poll() is not None:
                    _set_writer_lock_child_pid(None)
                else:
                    # writer_lock() finallyがchild_pidを見てlockを保持する。
                    print(
                        f"[process-tree] child pid={cp.pid} survived; writer lock child marker retained",
                        flush=True,
                    )
            status = "success" if rc == 0 else ("partial" if rc == 2 else "failed")
            msg = (f"timeout>{timeout_sec}s " if rc == 124 else f"rc={rc} ") + f"log={log_path}"
        except Exception as e:
            rc = 125
            status = "failed"
            msg = f"{type(e).__name__}: {e}"
        log.write(f"===== {now_s()} END status={status} rc={rc} =====\n")
    state_finish(job_name, status, rc, msg)
    print(f"[DONE]  {job_name}: status={status} rc={rc}", flush=True)
    return status, rc


def _aggregate_status(results: list[tuple[str, int]], *, optional: bool = False) -> tuple[str, int]:
    """子ジョブ結果を集約する。

    critical群では partial/failed をそのまま非成功にする。
    optional群では失敗があっても全体を partial に留める。
    """
    if any(st == "failed" for st, _ in results):
        return ("partial", 2) if optional else ("failed", 1)
    if any(st == "partial" for st, _ in results):
        return "partial", 2
    return "success", 0


def clear_legacy_pts_values() -> None:
    """廃止したPTSの旧値を残して当日値と誤認しないよう、一度NULL化する。

    SQLiteでは列削除が既存dashboardとの互換性を壊すため、列自体は残す。
    """
    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "screener" not in tables:
            return
        cols = {r[1] for r in c.execute("PRAGMA table_info(screener)").fetchall()}
        targets = [name for name in ("PTS株価", "PTS時刻", "PTS取得日時") if name in cols]
        if not targets:
            return
        where = " OR ".join(f'"{name}" IS NOT NULL' for name in targets)
        set_sql = ", ".join(f'"{name}"=NULL' for name in targets)
        cur = c.execute(f'UPDATE screener SET {set_sql} WHERE {where}')
        c.commit()
        if cur.rowcount:
            print(f"[PTS REMOVED] legacy values cleared: rows={cur.rowcount}", flush=True)
    finally:
        c.close()


def _required_daily_cutoff(now: datetime | None = None) -> datetime:
    """今回のPC稼働時点で必要な日次generationの下限。

    18:00より前なら前日18:00以降の成功でよい。
    18:00以降なら当日18:00以降の成功を要求する。
    これにより「前夜21時成功→翌15時ログオン」で重い日次をやり直さず、
    夜にPCを初めて起動した場合はその日の夜generationを作る。
    """
    n = now or datetime.now()
    base = n.replace(hour=18, minute=0, second=0, microsecond=0)
    return base if n >= base else base - timedelta(days=1)


def _job_success_since(job_name: str, cutoff: datetime) -> bool:
    st = state_get(job_name)
    if st.get("status") != "success":
        return False
    ts = st.get("last_success_at") or st.get("last_finished_at")
    if not ts:
        return False
    try:
        return datetime.fromisoformat(str(ts)) >= cutoff
    except Exception:
        return False


def run_daily_core(*, cutoff: datetime | None = None) -> tuple[str, int]:
    """ダッシュボード整合性に必要な日次producerだけを直列実行する。"""
    cutoff = cutoff or _required_daily_cutoff()
    state_start("daily_core", f"required_since={cutoff.isoformat(timespec='seconds')}")
    results: list[tuple[str, int]] = []
    clear_legacy_pts_values()

    # TDnetは上流。ここが失敗したら後続を成功generationとして確定しない。
    r = run_script("daily_fetch_all", "fetch_all.py")
    results.append(r)
    if r[0] == "failed":
        status, rc = _aggregate_status(results)
        state_finish("daily_core", status, rc, "fetch_all core failed")
        return status, rc

    # 株探ファンダは、fetch_allで判明した決算・業績修正銘柄と
    # 新規/欠損/前回失敗銘柄だけを差分更新する。全件補修はweeklyの責務。
    r = run_event_funda_refresh("daily_funda_delta", include_repairs=True)
    results.append(r)
    if r[0] == "failed":
        status, rc = _aggregate_status(results)
        state_finish("daily_core", status, rc, "kabutan funda delta failed")
        return status, rc

    # テーマ・信用・決算予定日も新しい決算銘柄だけ差分更新する。
    # Yahoo財務全件とテーマ/信用全件はweeklyへ分離。
    r = run_event_theme_refresh("daily_themes_shinyo_delta")
    results.append(r)

    # fetch_all=partial は「TDnetイベント本体成功 / シンデンenrichment不足」。
    # 日次coreとしては利用可能なので、後続coreが成功していればgenerationを成功扱いにする。
    hard_failed = any(st == "failed" for st, _ in results)
    if hard_failed:
        status, rc = _aggregate_status(results)
    else:
        status, rc = "success", 0
    partial_jobs = sum(1 for st, _ in results if st == "partial")
    note = f"usable_with_partial_jobs={partial_jobs}" if partial_jobs else "ok"
    state_finish("daily_core", status, rc, f"required_since={cutoff.isoformat(timespec='seconds')} {note}")
    return status, rc


def _parse_state_dt(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def _earnings_codes_since(dt: datetime | None) -> list[str]:
    """TDnet earnings_events から指定時刻より後に発表されたコードを返す。"""
    if dt is None:
        return []
    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "earnings_events" not in tables:
            return []
        cols={r[1] for r in c.execute("PRAGMA table_info(earnings_events)").fetchall()}
        code_col=next((x for x in ("コード","code","銘柄コード") if x in cols),None)
        time_col=next((x for x in ("提出時刻","発表日時","time","pubdate") if x in cols),None)
        if not code_col or not time_col:
            return []
        rows=c.execute(f'SELECT "{code_col}", "{time_col}" FROM earnings_events WHERE "{time_col}" IS NOT NULL').fetchall()
    finally:
        c.close()
    out=[]
    for code,ts in rows:
        edt=_parse_state_dt(ts)
        if edt is not None and edt > dt:
            cc=_normalize_stock_code(code)
            if cc and cc not in out: out.append(cc)
    return out


def _screener_universe_codes() -> set[str] | None:
    """現在のdashboard対象screenerに存在する通常銘柄コードを返す。

    P3-50 (2026-08-17): TDnetはTOKYO PRO Market等、株探通常ページを持たない
    開示も含む。イベント差分はdashboardの対象集合に限ることで、単一404が
    live-materials全体（EODを含む）をpartial停止させない。

    DBスキーマを読めない場合は安全側でNoneを返し、呼び出し元は従来の対象を維持する。
    """
    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "screener" not in tables:
            return None
        cols = {r[1] for r in c.execute("PRAGMA table_info(screener)").fetchall()}
        if "コード" not in cols:
            return None
        rows = c.execute('SELECT DISTINCT CAST("コード" AS TEXT) FROM screener WHERE "コード" IS NOT NULL').fetchall()
    except Exception as e:
        print(f"[event-filter] screener universe unavailable: {type(e).__name__}: {e}", flush=True)
        return None
    finally:
        c.close()
    return {cc for (raw,) in rows if (cc := _normalize_stock_code(raw))}


def _filter_event_codes_to_screener(codes: list[str], job_name: str) -> tuple[list[str], list[str]]:
    """TDnetイベントからdashboard対象外コードを除外する（P3-50）。"""
    universe = _screener_universe_codes()
    if universe is None:
        return codes, []
    selected = [code for code in codes if code in universe]
    skipped = [code for code in codes if code not in universe]
    if skipped:
        print(
            f"[event-filter] {job_name}: out-of-screener skip={len(skipped)} sample={skipped[:10]}",
            flush=True,
        )
    return selected, skipped


def _funda_repair_codes(limit: int = 200) -> list[str]:
    """新規・finance_notes欠損・前回取得失敗を日次の補修対象にする。"""
    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        out: list[str] = []

        if "screener" in tables:
            if "finance_notes" in tables:
                rows = c.execute("""
                    SELECT DISTINCT CAST(s.コード AS TEXT)
                    FROM screener s
                    LEFT JOIN finance_notes f ON CAST(f.コード AS TEXT)=CAST(s.コード AS TEXT)
                    WHERE s.コード IS NOT NULL AND f.コード IS NULL
                    ORDER BY CAST(s.コード AS TEXT)
                    LIMIT ?
                """, (int(limit),)).fetchall()
            else:
                rows = c.execute("""
                    SELECT DISTINCT CAST(コード AS TEXT)
                    FROM screener WHERE コード IS NOT NULL
                    ORDER BY CAST(コード AS TEXT) LIMIT ?
                """, (int(limit),)).fetchall()
            out.extend(str(r[0]).strip() for r in rows if r and r[0] is not None)

        remaining = max(0, int(limit) - len(out))
        if remaining and "earnings_cache" in tables:
            bad = sorted(BAD_LAST_STATUSES)
            placeholders = ",".join("?" for _ in bad)
            rows = c.execute(f"""
                SELECT DISTINCT CAST(コード AS TEXT)
                FROM earnings_cache
                WHERE last_status IN ({placeholders})
                ORDER BY updated_at ASC LIMIT ?
            """, [*bad, remaining]).fetchall()
            out.extend(str(r[0]).strip() for r in rows if r and r[0] is not None)
    finally:
        c.close()

    normalized: list[str] = []
    for code in out:
        cc = _normalize_stock_code(code)
        if cc and cc not in normalized:
            normalized.append(cc)
    return normalized[:max(0, int(limit))]


BAD_LAST_STATUSES = {
    "ERROR_429", "ERROR_500", "ERROR_502", "ERROR_503", "ERROR_504",
    "TIMEOUT", "CONN_ERROR", "HTTP_ERROR", "PARSE_ERROR", "UNKNOWN_ERROR",
}


def run_event_funda_refresh(job_name: str, *, include_repairs: bool = False) -> tuple[str, int]:
    """前回チェック後の新決算銘柄だけ株探ファンダを差分更新する。"""
    prev=state_get(job_name)
    # 前回成功終了時刻から2時間重複取得。失敗runのlast_started_atで基準を進めず、
    # refresh実行中に追加された決算も次回取りこぼさない。UPSERT/株探再取得なので重複は安全。
    _prev_success=_parse_state_dt(prev.get("last_success_at"))
    baseline=(_prev_success - timedelta(hours=2)) if _prev_success is not None else None
    if baseline is None:
        base=state_get("weekly_kabutan_funda_full")
        baseline=_parse_state_dt(base.get("last_started_at") or base.get("last_success_at") or base.get("last_finished_at"))
    if baseline is None:
        # 初回dailyでも全件取得には戻さず、直近2日だけを対象にする。
        # 過去の欠損はinclude_repairsとweeklyが段階的に補修する。
        baseline = datetime.now() - timedelta(hours=48)

    state_start(job_name,f"earnings since {baseline.isoformat(timespec='seconds')}")
    raw_event_codes = _earnings_codes_since(baseline)
    codes, skipped_event_codes = _filter_event_codes_to_screener(raw_event_codes, job_name)
    event_code_count = len(codes)
    repair_codes = _funda_repair_codes(limit=200) if include_repairs else []
    for code in repair_codes:
        if code not in codes:
            codes.append(code)
    if not codes:
        state_finish(job_name,"success",0,"no new earnings")
        return "success",0
    # Windows command line上限を避けるため最大100銘柄ずつ。通常は数銘柄。
    results=[]
    for i in range(0,len(codes),100):
        part=codes[i:i+100]
        results.append(run_script(f"{job_name}_chunk{i//100+1}","株探ファンダ.py",[",".join(part),"--force-refresh"],timeout_sec=3600))
    status,rc=_aggregate_status(results)
    state_finish(
        job_name, status, rc,
        f"codes={len(codes)} events={event_code_count}/{len(raw_event_codes)} "
        f"event_skipped={len(skipped_event_codes)} repairs={len(repair_codes)} sample={codes[:10]}"
    )
    return status,rc


def run_event_theme_refresh(job_name: str) -> tuple[str, int]:
    """前回成功後の決算銘柄だけテーマ・信用・決算予定日を差分更新する。"""
    prev = state_get(job_name)
    prev_success = _parse_state_dt(prev.get("last_success_at"))
    baseline = (prev_success - timedelta(hours=2)) if prev_success is not None else None
    if baseline is None:
        base = state_get("weekly_themes_shinyo_full")
        baseline = _parse_state_dt(
            base.get("last_started_at") or base.get("last_success_at") or base.get("last_finished_at")
        )
    if baseline is None:
        baseline = datetime.now() - timedelta(hours=48)

    state_start(job_name, f"earnings since {baseline.isoformat(timespec='seconds')}")
    raw_event_codes = _earnings_codes_since(baseline)
    codes, skipped_event_codes = _filter_event_codes_to_screener(raw_event_codes, job_name)
    if not codes:
        detail = "no dashboard-eligible earnings"
        if skipped_event_codes:
            detail += f" / skipped={len(skipped_event_codes)}"
        state_finish(job_name, "success", 0, detail)
        return "success", 0

    results: list[tuple[str, int]] = []
    for i in range(0, len(codes), 100):
        part = codes[i:i + 100]
        results.append(run_script(
            f"{job_name}_chunk{i // 100 + 1}",
            "fetch_all_kabutan_themes_shinyo.py",
            ["--codes", *part],
            timeout_sec=3600,
        ))
    status, rc = _aggregate_status(results)
    state_finish(
        job_name, status, rc,
        f"codes={len(codes)} events={len(codes)}/{len(raw_event_codes)} "
        f"event_skipped={len(skipped_event_codes)} sample={codes[:10]}",
    )
    return status, rc


def run_weekly() -> tuple[str, int]:
    """週1回、重い全銘柄producerを実行して日次差分の取りこぼしを補修する。"""
    state_start("weekly_maintenance", "full fundamentals + Yahoo + themes/credit repair")
    results: list[tuple[str, int]] = []
    results.append(run_script(
        "weekly_kabutan_funda_full", "株探ファンダ.py", ["--force-refresh"], timeout_sec=8 * 3600
    ))
    results.append(run_script(
        "weekly_yahoo_financials_full", "yahoo_financials_daily.py", ["--force-refresh"], timeout_sec=8 * 3600
    ))
    results.append(run_script(
        "weekly_themes_shinyo_full", "fetch_all_kabutan_themes_shinyo.py", timeout_sec=8 * 3600
    ))
    status, rc = _aggregate_status(results)
    state_finish("weekly_maintenance", status, rc, "full fundamentals + Yahoo + themes/credit")
    return status, rc


def run_daily_optional() -> tuple[str, int]:
    """多少遅れてもダッシュボード全体を止めない日次producer。"""
    state_start("daily_optional", "karauri + weekly float")
    results: list[tuple[str, int]] = []
    results.append(run_script("daily_karauri", "空売り無しリスト出しスクリプト.py"))

    # 浮動株は毎日Taskを増やさず、7日以上古い時だけ再確認。
    if not is_fresh("weekly_float_shares", 7 * 24 - 1):
        results.append(run_script("weekly_float_shares", "浮動.py", ["--refresh-days", "7"]))

    status, rc = _aggregate_status(results, optional=True)
    state_finish("daily_optional", status, rc, f"subjobs={len(results)}")
    return status, rc


def run_daily() -> tuple[str, int]:
    """夜の日次producer。core成功を必須、optional失敗はpartialとして記録する。"""
    cutoff = _required_daily_cutoff()
    state_start("daily_maintenance", f"required_since={cutoff.isoformat(timespec='seconds')}")

    core = run_daily_core(cutoff=cutoff)
    if core[0] != "success":
        state_finish("daily_maintenance", "failed", 1, "daily core incomplete")
        return "failed", 1

    optional = run_daily_optional()
    if optional[0] == "success":
        state_finish("daily_maintenance", "success", 0, "core+optional success")
        return "success", 0

    # optionalだけ失敗/partialでもcore generationは使える。
    state_finish("daily_maintenance", "partial", 2, "core success / optional incomplete")
    # optional失敗だけではTask Scheduler再試行を発生させない。
    return "partial", 0


def ensure_daily_core() -> tuple[str, int]:
    """寝坊/PC停止後でも、現在必要な日次core generationをその場で補完する。"""
    cutoff = _required_daily_cutoff()
    if _job_success_since("daily_core", cutoff):
        return "success", 0
    return run_daily_core(cutoff=cutoff)


def run_morning(max_age_hours: float = 18.0) -> tuple[str, int]:
    """08:00またはログオン時。coreを最優先でcatch-upし、optionalは後から補完。"""
    cutoff = _required_daily_cutoff()
    state_start("morning_catchup", f"required_since={cutoff.isoformat(timespec='seconds')}")
    results: list[tuple[str, int]] = []

    core = ensure_daily_core()
    results.append(core)
    if core[0] != "success":
        state_finish("morning_catchup", "failed", 1, "daily core catch-up failed")
        return "failed", 1

    # 夜generation後のTDnet差分を必ず一度catch-upする。
    tdnet = run_script("morning_fetch_all", "fetch_all.py")
    results.append(tdnet)
    if tdnet[0] == "failed":
        status, rc = _aggregate_status(results)
        state_finish("morning_catchup", status, rc, "morning TDnet core failed")
        return status, rc
    if tdnet[0] == "partial":
        state_finish("morning_catchup", "partial", 2, "TDnet core fresh / shinden enrichment incomplete")
        return "partial", 2

    funda_delta = run_event_funda_refresh("morning_funda_events")
    results.append(funda_delta)
    if funda_delta[0] != "success":
        status, rc = _aggregate_status(results)
        state_finish("morning_catchup", status, rc, "new earnings funda refresh incomplete")
        return status, rc

    theme_delta = run_event_theme_refresh("morning_themes_shinyo_events")
    results.append(theme_delta)
    if theme_delta[0] != "success":
        status, rc = _aggregate_status(results)
        state_finish("morning_catchup", status, rc, "new earnings theme/credit refresh incomplete")
        return status, rc

    sh = run_script("morning_shinden_full", "shinden_logic.py", ["--full"])
    results.append(sh)

    # optionalは朝の整合性criticalではない。古い時だけ補完するが失敗でcoreを無効化しない。
    optional_results: list[tuple[str, int]] = []
    if not is_fresh("daily_karauri", 36.0):
        optional_results.append(run_script("morning_karauri", "空売り無しリスト出しスクリプト.py"))
    if not is_fresh("weekly_float_shares", 7 * 24 - 1):
        optional_results.append(run_script("weekly_float_shares", "浮動.py", ["--refresh-days", "7"]))

    status, rc = _aggregate_status(results)
    optional_only_partial = False
    if status == "success" and optional_results:
        opt_status, _ = _aggregate_status(optional_results, optional=True)
        if opt_status != "success":
            status, rc = "partial", 2
            optional_only_partial = True
    state_finish("morning_catchup", status, rc, f"core_since={cutoff.isoformat(timespec='seconds')}")
    # core + TDnet + shinden が成功していれば、optional不足だけで朝Task全体を再試行しない。
    return status, (0 if optional_only_partial else rc)


def run_live_materials() -> tuple[str, int]:
    """日次core保証→TDnet増分→シンデンfull。EOD重処理は独立eod-finalizeへ分離。"""
    cutoff = _required_daily_cutoff()
    state_start("live_materials", f"core_since={cutoff.isoformat(timespec='seconds')} / TDnet -> shinden full")
    results: list[tuple[str, int]] = []

    core = ensure_daily_core()
    results.append(core)
    if core[0] != "success":
        state_finish("live_materials", "failed", 1, "daily core unavailable")
        return "failed", 1

    tdnet = run_script("live_fetch_all", "fetch_all.py")
    results.append(tdnet)
    if tdnet[0] == "failed":
        status, rc = _aggregate_status(results)
        state_finish("live_materials", status, rc, "TDnet core failed; downstream not refreshed")
        return status, rc

    if tdnet[0] == "success":
        funda_delta = run_event_funda_refresh("live_funda_events")
        results.append(funda_delta)
        if funda_delta[0] != "success":
            state_finish("live_materials", "partial", 2, "TDnet fresh / new earnings funda incomplete")
            return "partial", 2
        theme_delta = run_event_theme_refresh("live_themes_shinyo_events")
        results.append(theme_delta)
        if theme_delta[0] != "success":
            state_finish("live_materials", "partial", 2, "TDnet fresh / new earnings theme/credit incomplete")
            return "partial", 2

    # TDnet enrichment partialならシンデン根拠が不完全なのでfullをfresh確定しない。
    if tdnet[0] == "partial":
        state_finish("live_materials", "partial", 2, "TDnet core fresh / shinden enrichment incomplete")
        return "partial", 2

    results.append(run_script("live_shinden_full", "shinden_logic.py", ["--full"]))
    status, rc = _aggregate_status(results)
    state_finish("live_materials", status, rc, "daily core -> TDnet -> shinden (EOD separated)")
    return status, rc

def _job_success_today(job_name: str, now: datetime | None = None) -> bool:
    """system_job_state上で、そのジョブがローカル日付の本日すでに成功しているか。"""
    st = state_get(job_name)
    if st.get("status") != "success":
        return False
    ts = st.get("last_success_at") or st.get("last_finished_at")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(str(ts))
    except Exception:
        return False
    n = now or datetime.now()
    return dt.date() == n.date()


def run_eod_finalize(*, force: bool = False) -> tuple[str, int]:
    """引け後EOD確定専用。live-materialsから分離し、同日成功後の重複実行を抑止する。"""
    n = datetime.now()
    if not force and _job_success_today("eod_finalize", n):
        print(f"[EOD] already finalized today ({n.date().isoformat()}); skip", flush=True)
        return "success", 0

    # 誤操作で場中に重いEODを走らせない。Task Schedulerは15:35以降を想定。
    if (n.hour, n.minute) < (15, 30) and not force:
        msg = f"before close: now={n.strftime('%H:%M:%S')}"
        print(f"[EOD] {msg}; skip", flush=True)
        return "success", 0

    return run_script(
        "eod_finalize",
        "eod_finalize.py",
        timeout_sec=EOD_FINALIZE_TIMEOUT_SEC,
    )


def run_model() -> tuple[str, int]:
    # モデル側がprice_data_max_dateを比較するため、同日複数回Taskを置いても新足が無ければno-op。
    return run_script("model_eod", "モデル学習_catboost.py", ["--require-eod-marker"], timeout_sec=6 * 3600)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="株スクリーニング全体ジョブオーケストレータ")
    ap.add_argument("mode", choices=["daily", "weekly", "morning", "live-materials", "eod-finalize", "model"])
    ap.add_argument("--daily-max-age-hours", type=float, default=18.0)
    ap.add_argument("--force-eod", action="store_true", help="eod-finalizeの時刻/同日成功skipを無視")
    args = ap.parse_args(argv)

    if args.mode == "model":
        status, rc = run_model()
        return rc

    # DB writer系は別Task同士でも同時実行しない。
    with writer_lock() as acquired:
        if not acquired:
            # lock保有中はsystem_job_stateへも書かない（同じDBへのwriter競合を避ける）。
            # morning/dailyはTask Scheduler側の再試行対象、10分周期LIVEは次回へ任せる。
            # P3-52 (2026-08-18): LIVEがexit=0で無言skipすると、手動試験でも
            # 成功と誤認しやすい。ロック競合は意図した安全skipであることを明示する。
            print(
                f"[shared-writer-lock] active writer detected; mode={args.mode} safely skipped",
                flush=True,
            )
            return 4 if args.mode in {"daily", "weekly", "morning"} else 0
        if args.mode == "daily":
            _, rc = run_daily()
        elif args.mode == "weekly":
            _, rc = run_weekly()
        elif args.mode == "morning":
            _, rc = run_morning(args.daily_max_age_hours)
        elif args.mode == "live-materials":
            _, rc = run_live_materials()
        else:
            _, rc = run_eod_finalize(force=args.force_eod)
        return rc


if __name__ == "__main__":
    raise SystemExit(main())
