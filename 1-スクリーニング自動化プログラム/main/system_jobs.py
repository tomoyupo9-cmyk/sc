# === LIVE-EOD-SPLIT-RESTORE-V1 2026-08-31 ===
# 8/31最新版を土台に、過去に実装済みだったP3-49/P4-LIVE-EOD-SPLITを復元。
# - live-materialsからeod_finalize.pyを分離
# - eod-finalize独立mode追加（15:30以降、同日成功後はno-op）
# - EOD子プロセス内部timeout + Windows process tree回収
# - child生存時はwriter lockを誤って削除しない
# FLOAT-SHARES-STALL-GUARD / KABUTAN-EVENT-ELIGIBILITY等の8/31変更は維持。
# === /LIVE-EOD-SPLIT-RESTORE-V1 ===
# === FLOAT-SHARES-STALL-GUARD-V1 2026-08-31 ===
# 浮動.py の外部HTTPS待ち長期化からsystem_jobsを保護。
# 10分timeout（環境変数で変更可）。morningでは重要producer完了後に実行。
# 売買・スクリーニング判定ロジック変更なし。
# === /FLOAT-SHARES-STALL-GUARD-V1 ===
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
"""

# === KABUTAN-EVENT-ELIGIBILITY-V3 2026-08-26 ===
# TDnetイベントには、screener内でも株探通常ページ非対応（TOKYO PRO Market等）が混ざり得る。
# event-driven funda/theme refreshの前で除外し、単一404でlive-materials/EOD全体を止めない。
# screener外 / PRO市場 / 実測404既知コードを分離してlogへ残す。
# === /KABUTAN-EVENT-ELIGIBILITY-V3 ===
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

# 浮動株は補助producer。外部通信待ちで朝全体を塞がない上限。
FLOAT_SHARES_TIMEOUT_SEC = int(os.environ.get("KABU_FLOAT_SHARES_TIMEOUT_SEC", "600"))

# P3-49: Task Schedulerの長時間制限より先にEOD子プロセスを回収する。
# 親だけ落ちてeod_finalize.pyが孤児化し、writer lockを長時間占有する事故を防ぐ。
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
              last_finished_at=NULL,
              return_code=NULL,
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
                    # 生存中の孤児writerへ別writerを重ねない。
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
    """子ジョブとその配下を終了し、回収できた場合だけTrueを返す。"""
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
    # BRISK-DOWNSTREAM-UNIVERSE-V1: refresh BRiSK P/S/G universe before every producer child.
    from brisk_universe_source import prepare_brisk_codes_path as _prepare_brisk_codes_path
    env["KABU_CODES_PATH"] = str(_prepare_brisk_codes_path())
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


# 株探の通常 stock/finance 系ページへ渡さない既知コード。
# 市場表記だけでは判定できない実測404を保守的に吸収する。
# 追加は環境変数 KABU_KABUTAN_UNSUPPORTED_CODES=7170,.... でも可能。
_KABUTAN_UNSUPPORTED_DEFAULT = {"7170"}


def _kabutan_unsupported_codes() -> set[str]:
    out = set(_KABUTAN_UNSUPPORTED_DEFAULT)
    raw = os.environ.get("KABU_KABUTAN_UNSUPPORTED_CODES", "")
    for token in re.split(r"[,;\\s]+", raw):
        cc = _normalize_stock_code(token)
        if cc:
            out.add(cc)
    return out


def _filter_event_codes_for_kabutan(codes: list[str], job_name: str) -> tuple[list[str], list[str]]:
    """TDnet/repair候補から株探通常ページへ安全に渡せるコードだけ残す。

    - 現在screenerに存在しないコード: dashboard対象外なので除外
    - screener市場がTOKYO PRO Market/TPM系: 株探通常ページ非対応として除外
    - 実測404の既知コード: 市場表記欠損時も除外

    screener schemaを読めない場合はfail-openではなく「既知非対応だけ除外」に留め、
    既存の通常銘柄まで誤って落とさない。
    """
    normalized: list[str] = []
    for raw in codes or []:
        cc = _normalize_stock_code(raw)
        if cc and cc not in normalized:
            normalized.append(cc)
    if not normalized:
        return [], []

    known_bad = _kabutan_unsupported_codes()
    universe: set[str] | None = None
    market_by_code: dict[str, str] = {}

    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "screener" in tables:
            cols = {r[1] for r in c.execute("PRAGMA table_info(screener)").fetchall()}
            if "コード" in cols:
                market_col = next((x for x in ("市場", "市場区分", "market", "market_segment") if x in cols), None)
                if market_col:
                    rows = c.execute(
                        f'SELECT CAST("コード" AS TEXT), CAST("{market_col}" AS TEXT) '
                        'FROM screener WHERE "コード" IS NOT NULL'
                    ).fetchall()
                else:
                    rows = [
                        (r[0], "")
                        for r in c.execute(
                            'SELECT CAST("コード" AS TEXT) FROM screener WHERE "コード" IS NOT NULL'
                        ).fetchall()
                    ]
                universe = set()
                for raw_code, raw_market in rows:
                    cc = _normalize_stock_code(raw_code)
                    if not cc:
                        continue
                    universe.add(cc)
                    text = str(raw_market or "").strip()
                    # alias行が複数ある場合、空欄で既存の市場表記を上書きしない。
                    if text or cc not in market_by_code:
                        market_by_code[cc] = text
    except Exception as e:
        print(f"[event-filter][WARN] screener lookup unavailable: {type(e).__name__}: {e}", flush=True)
        universe = None
        market_by_code = {}
    finally:
        c.close()

    def _is_pro_market(text: str) -> bool:
        t = str(text or "").strip().upper().replace(" ", "").replace("　", "")
        return (
            "TOKYOPRO" in t
            or "PROMARKET" in t
            or t == "TPM"
            or "東証PRO" in t
            or "東証ＰＲＯ" in t
            or "プロマーケット" in t
        )

    selected: list[str] = []
    skipped: list[str] = []
    reasons: dict[str, str] = {}
    for cc in normalized:
        reason = None
        if cc in known_bad:
            reason = "known_kabutan_unsupported"
        elif universe is not None and cc not in universe:
            reason = "out_of_screener"
        elif _is_pro_market(market_by_code.get(cc, "")):
            reason = "tokyo_pro_market"

        if reason:
            skipped.append(cc)
            reasons[cc] = reason
        else:
            selected.append(cc)

    if skipped:
        sample = [f"{cc}:{reasons.get(cc)}" for cc in skipped[:10]]
        print(
            f"[event-filter] {job_name}: kabutan-unsupported skip={len(skipped)} sample={sample}",
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
    event_codes, skipped_event_codes = _filter_event_codes_for_kabutan(raw_event_codes, job_name)

    raw_repair_codes = _funda_repair_codes(limit=200) if include_repairs else []
    repair_codes, skipped_repair_codes = _filter_event_codes_for_kabutan(
        raw_repair_codes, f"{job_name}/repairs"
    ) if raw_repair_codes else ([], [])

    codes = list(event_codes)
    for code in repair_codes:
        if code not in codes:
            codes.append(code)
    if not codes:
        detail = "no dashboard-eligible Kabutan earnings/repairs"
        skipped_total = len(skipped_event_codes) + len(skipped_repair_codes)
        if skipped_total:
            detail += f" / skipped={skipped_total}"
        state_finish(job_name,"success",0,detail)
        return "success",0
    # Windows command line上限を避けるため最大100銘柄ずつ。通常は数銘柄。
    results=[]
    for i in range(0,len(codes),100):
        part=codes[i:i+100]
        results.append(run_script(f"{job_name}_chunk{i//100+1}","株探ファンダ.py",[",".join(part),"--force-refresh"],timeout_sec=3600))
    status,rc=_aggregate_status(results)
    state_finish(
        job_name, status, rc,
        f"codes={len(codes)} events={len(event_codes)}/{len(raw_event_codes)} "
        f"event_skipped={len(skipped_event_codes)} repairs={len(repair_codes)}/{len(raw_repair_codes)} "
        f"repair_skipped={len(skipped_repair_codes)} sample={codes[:10]}"
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
    codes, skipped_event_codes = _filter_event_codes_for_kabutan(raw_event_codes, job_name)
    if not codes:
        detail = "no dashboard-eligible Kabutan earnings"
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
        f"event_skipped={len(skipped_event_codes)} sample={codes[:10]}"
    )
    return status, rc


def run_weekly() -> tuple[str, int]:
    """週1回の補修。

    Yahoo財務だけは毎週の全件forceを廃止:
      - 財務更新日が28日以上古い
      - raw_fin_json欠損
      - financial fetch schema更新
      - 最新決算日 >= Yahoo財務更新日
    の銘柄だけ取得する。これで決算直後の鮮度は落とさず、3581銘柄全件取得を避ける。
    手動の完全再構築が必要な時だけ yahoo_financials_daily.py --force-refresh を使う。
    """
    state_start("weekly_maintenance", "full Kabutan + smart Yahoo(28d/earnings) + themes/credit repair")
    results: list[tuple[str, int]] = []
    results.append(run_script(
        "weekly_kabutan_funda_full", "株探ファンダ.py", ["--force-refresh"], timeout_sec=8 * 3600
    ))
    results.append(run_script(
        "weekly_yahoo_financials_full",
        "yahoo_financials_daily.py",
        ["--refresh-days", "28"],
        timeout_sec=8 * 3600,
    ))
    results.append(run_script(
        "weekly_themes_shinyo_full", "fetch_all_kabutan_themes_shinyo.py", timeout_sec=8 * 3600
    ))
    status, rc = _aggregate_status(results)
    state_finish(
        "weekly_maintenance",
        status,
        rc,
        "full Kabutan + smart Yahoo(28d/earnings) + themes/credit",
    )
    return status, rc


def run_daily_optional() -> tuple[str, int]:
    """多少遅れてもダッシュボード全体を止めない日次producer。"""
    state_start("daily_optional", "karauri + weekly float")
    results: list[tuple[str, int]] = []
    results.append(run_script("daily_karauri", "空売り無しリスト出しスクリプト.py"))

    # 浮動株は毎日Taskを増やさず、7日以上古い時だけ再確認。
    if not is_fresh("weekly_float_shares", 7 * 24 - 1):
        print(f"[daily][float][INFO] 浮動株更新開始 timeout={FLOAT_SHARES_TIMEOUT_SEC}s。失敗してもcoreは維持します。", flush=True)
        _float_r = run_script(
            "weekly_float_shares", "浮動.py", ["--refresh-days", "7"],
            timeout_sec=FLOAT_SHARES_TIMEOUT_SEC,
        )
        results.append(_float_r)
        if _float_r[0] != "success":
            print(f"[daily][float][WARN] status={_float_r[0]} rc={_float_r[1]}。補助producer失敗として後処理へ進みます。", flush=True)

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


def _karauri_today_snapshot_status(now: datetime | None = None) -> dict:
    """本体が要求する「当日 institution_short_snapshot」の準備状態を返す。

    freshness(最終ジョブ成功から何時間)ではなく、実データの当日snapshotを確認する。
    テーブル/schemaを確認できない場合も理由を返し、朝ログだけで原因を判別できるようにする。
    """
    n = now or datetime.now()
    today = n.strftime("%Y-%m-%d")
    result = {
        "date": today,
        "table_exists": False,
        "date_column": None,
        "success_column": None,
        "rows_today": 0,
        "success_today": 0,
        "ready": False,
        "reason": "",
    }
    c = sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "institution_short_snapshot" not in tables:
            result["reason"] = "institution_short_snapshot table missing"
            return result
        result["table_exists"] = True
        cols = {r[1] for r in c.execute("PRAGMA table_info(institution_short_snapshot)").fetchall()}
        date_col = next((x for x in ("snapshot_date", "取得日", "日付", "date") if x in cols), None)
        success_col = next((x for x in ("crawl_success", "取得成功", "success") if x in cols), None)
        result["date_column"] = date_col
        result["success_column"] = success_col
        if not date_col:
            result["reason"] = "snapshot date column missing"
            return result

        result["rows_today"] = int(c.execute(
            f'SELECT COUNT(*) FROM institution_short_snapshot WHERE "{date_col}"=?',
            (today,),
        ).fetchone()[0] or 0)

        if success_col:
            result["success_today"] = int(c.execute(
                f'SELECT COUNT(*) FROM institution_short_snapshot '
                f'WHERE "{date_col}"=? AND COALESCE("{success_col}",0)=1',
                (today,),
            ).fetchone()[0] or 0)
            result["ready"] = result["success_today"] > 0
            result["reason"] = (
                "today snapshot has successful rows"
                if result["ready"]
                else "today snapshot exists but has no successful rows"
                if result["rows_today"] > 0
                else "today snapshot is missing"
            )
        else:
            # 古いschemaでも当日行があれば「当日snapshotあり」と判定する。
            result["success_today"] = result["rows_today"]
            result["ready"] = result["rows_today"] > 0
            result["reason"] = (
                "today snapshot exists (no success column)"
                if result["ready"] else "today snapshot is missing"
            )
        return result
    except Exception as e:
        result["reason"] = f"snapshot check failed: {type(e).__name__}: {e}"
        return result
    finally:
        c.close()


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

    print(
        "[morning][shinden] 当日full snapshotを生成します。"
        " 本体で『current full snapshot unavailable』が出る場合、この処理の成否とshinden_logic.pyのwriterを確認してください。",
        flush=True,
    )
    sh = run_script("morning_shinden_full", "shinden_logic.py", ["--full"])
    results.append(sh)
    print(
        f"[morning][shinden] full producer status={sh[0]} rc={sh[1]}. "
        "successなら8:11本体は当日fullを利用できる想定です。",
        flush=True,
    )

    # 空売りは本体が「当日 institution_short_snapshot」を要求するため、
    # 旧36時間freshnessではなく実DBの当日snapshot有無で朝の補完要否を決める。
    # これにより「前夜daily_karauri成功→36h以内なので朝skip→本体は当日snapshot無し」
    # という契約不整合を防ぐ。
    optional_results: list[tuple[str, int]] = []
    _ks = _karauri_today_snapshot_status()
    print(
        "[morning][karauri] "
        f"date={_ks['date']} rows_today={_ks['rows_today']} "
        f"success_today={_ks['success_today']} ready={_ks['ready']} "
        f"reason={_ks['reason']}",
        flush=True,
    )
    if not _ks["ready"]:
        print(
            "[morning][karauri] 当日snapshotが未準備のため空売りproducerを実行します。"
            " これは『空売りが0件』という意味ではなく、『本体が使える当日データがまだ無い』ための補完です。",
            flush=True,
        )
        _kr = run_script("morning_karauri", "空売り無しリスト出しスクリプト.py")
        optional_results.append(_kr)
        _ks_after = _karauri_today_snapshot_status()
        print(
            "[morning][karauri][after] "
            f"producer_status={_kr[0]} rc={_kr[1]} "
            f"rows_today={_ks_after['rows_today']} success_today={_ks_after['success_today']} "
            f"ready={_ks_after['ready']} reason={_ks_after['reason']}",
            flush=True,
        )
        if _kr[0] == "success" and not _ks_after["ready"]:
            print(
                "[morning][karauri][WARN] producer自体はsuccessですが、当日snapshotに成功行がありません。"
                " 本体では機関空売りを『未取得/不明』として扱います。"
                " 次に確認すべき対象は 空売り無しリスト出しスクリプト.py のsnapshot writerです。",
                flush=True,
            )
    else:
        print(
            "[morning][karauri] 当日snapshotは既に準備済みです。再取得は省略します。",
            flush=True,
        )

    if not is_fresh("weekly_float_shares", 7 * 24 - 1):
        print(
            f"[morning][float][INFO] 浮動株更新開始 timeout={FLOAT_SHARES_TIMEOUT_SEC}s。"
            " 空売り・シンデン等の重要処理はこの時点で完了済みです。",
            flush=True,
        )
        _float_r = run_script(
            "weekly_float_shares", "浮動.py", ["--refresh-days", "7"],
            timeout_sec=FLOAT_SHARES_TIMEOUT_SEC,
        )
        optional_results.append(_float_r)
        if _float_r[0] != "success":
            print(
                f"[morning][float][WARN] status={_float_r[0]} rc={_float_r[1]}。"
                " 浮動株はoptionalなので、朝の重要処理を巻き添えにせず後処理へ進みます。",
                flush=True,
            )
    else:
        print("[morning][float] 7日以内の成功データあり。更新省略。", flush=True)

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

    print(
        "[live-materials][shinden] TDnet/enrichmentが揃ったため当日full snapshotを更新します。",
        flush=True,
    )
    _live_sh = run_script("live_shinden_full", "shinden_logic.py", ["--full"])
    results.append(_live_sh)
    print(
        f"[live-materials][shinden] full producer status={_live_sh[0]} rc={_live_sh[1]}",
        flush=True,
    )
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
            # morning/dailyはTask Scheduler側の再試行対象、10分周期LIVE/EODは次回へ任せる。
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
