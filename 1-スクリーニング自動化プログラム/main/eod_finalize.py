# -*- coding: utf-8 -*-
"""15:30以降のEOD確定処理を自動スクリーニング本体から分離して実行する。

2026-08-17 P3-48:
- 15:30開始runが ``phase_sync_finance_comments`` 内で3時間超CPUを使い続け、
  Yahoo EOD取得へ一度も到達しなかった実障害を修正。
- finance同期・logical alias整理・テーマsnapshot確認はEOD固有処理ではなく、
  live-materials前段producerまたはEOD成功後のscanner本体が担当するためここでは重複実行しない。
- 各工程のSTART/DONEと所要秒を必ずflushし、次回停止箇所をログだけで特定できるようにする。

2026-08-18 P3-51:
- 8/17当日足がすでに揃った状態で再実行されたyahoo_bulk_refreshが、全銘柄再取得へ入り
  長時間CPUを使用した。Yahoo取得前に既存snapshotの完全性を検証し、正常なら再取得を省く。
"""
from __future__ import annotations
import importlib.util
import os
import sys
import time
from datetime import time as dt_time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCANNER_PATH = Path(os.environ.get("KABU_SCANNER_PATH", str(HERE / "自動スクリーニング.py")))


def _run_step(name, func, *args, **kwargs):
    """EOD工程をfail-visibleに計測する。成功ログの無い工程が停止地点になる。"""
    started = time.perf_counter()
    print(f"[eod][START] {name}", flush=True)
    try:
        result = func(*args, **kwargs)
    except Exception as exc:
        elapsed = time.perf_counter() - started
        print(
            f"[eod][ERROR] {name}: elapsed={elapsed:.1f}s "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        raise
    elapsed = time.perf_counter() - started
    print(f"[eod][DONE]  {name}: elapsed={elapsed:.1f}s", flush=True)
    return result


def _load_scanner():
    if not SCANNER_PATH.exists():
        raise FileNotFoundError(f"scanner not found: {SCANNER_PATH}")
    # wrapper自身がproducer freshness gateで再帰的に止まらないようimport時だけ無効化。
    os.environ.setdefault("KABU_EXTERNAL_JOBS_REQUIRED", "0")
    spec = importlib.util.spec_from_file_location("_kabu_scanner_eod", str(SCANNER_PATH))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load scanner: {SCANNER_PATH}")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main() -> int:
    m = _load_scanner()
    now = m._now_jst()
    if not m._is_jp_business_day(now.date()):
        print(f"[eod] non-business day {now.date()} -> no-op")
        return 0
    if now.time() < dt_time(15, 30):
        print(f"[eod] before close {now:%H:%M:%S} -> no-op")
        return 0

    conn = m._get_db_conn()
    try:
        _run_step("ensure_runlog_schema", m.ensure_runlog_schema, conn)

        # P3-48: 下記3処理はEOD固有ではないため削除した。
        # - _cleanup_screener_logical_duplicates
        # - phase_sync_finance_comments
        # - phase_refresh_theme_snapshot
        # live-materials前段で差分producerを終え、EOD成功後のscanner本体が財務同期を行う。
        # EOD開始前に同じ大きなscreener更新を重ねない。
        codes = list(dict.fromkeys(
            m.canonical_code_for_db(r[0])
            for r in conn.execute("SELECT コード FROM screener").fetchall()
            if m.canonical_code_for_db(r[0])
        ))
        if not codes:
            raise RuntimeError("screener universe is empty")

        # P3-51: EODは10分周期で再試行される。既に当日price_historyが完全なら、
        # Yahoo全銘柄再取得を絶対に繰り返さない。max(日付)だけでは一部銘柄でも通るため、
        # scanner本体の完全性判定 _require_current_price_history_snapshot をprecheckに再利用する。
        snapshot_ready = False
        try:
            _run_step(
                "require_current_price_history_snapshot_precheck",
                m._require_current_price_history_snapshot,
                conn,
                "EOD precheck",
            )
            snapshot_ready = True
        except Exception as exc:
            print(
                f"[eod] current price snapshot not ready -> yahoo refresh required: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

        if snapshot_ready:
            print("[eod] yahoo_bulk_refresh skipped: current EOD snapshot already valid", flush=True)
        else:
            # Yahooの当日期待足がまだ無ければphase側が例外を返し、成功markerは立たない。
            _run_step(
                "yahoo_bulk_refresh",
                m._timed_daily_once,
                "yahoo_bulk_refresh", m.phase_yahoo_bulk_refresh, conn, codes, batch_size=200,
            )
        _run_step(
            "refresh_full_history_for_insufficient",
            m._timed_daily_once,
            "refresh_full_history_for_insufficient", m.refresh_full_history_for_insufficient,
            conn, codes, batch_size=200,
        )
        _run_step("require_current_price_history_snapshot", m._require_current_price_history_snapshot, conn, "EOD")
        _run_step(
            "compute_right_up_persistent_eod",
            m._timed_daily_once,
            "compute_right_up_persistent_eod", m.compute_right_up_persistent,
            conn, replace_log_day=True,
        )
        _run_step(
            "compute_right_up_early_triggers_eod",
            m._timed_daily_once,
            "compute_right_up_early_triggers_eod", m.compute_right_up_early_triggers,
            conn, replace_log_day=True,
        )
        _run_step("update_margin_metrics", m.phase_update_margin_metrics, conn)
        _run_step(
            "update_market_cap_all",
            m._timed_daily_once,
            "update_market_cap_all", m.update_market_cap_all,
            conn, batch_size=100, max_workers=4,
        )
        _run_step(
            "update_seasonal_progress_v3",
            m._timed_daily_once,
            "update_seasonal_progress_v3", m.update_seasonal_progress, conn,
        )
        _run_step("derive_update", m.phase_derive_update, conn)
        _run_step("update_market_metrics", m.phase_update_market_metrics, conn)
        _run_step("signal_detection", m.phase_signal_detection, conn)
        _run_step("snapshot_shodou_baseline", m.phase_snapshot_shodou_baseline, conn)
        _run_step("update_shodou_multipliers", m.phase_update_shodou_multipliers, conn)
        _run_step("apply_shodou_score", m.apply_shodou_score, conn)
        _run_step("update_since_dates", m.phase_update_since_dates, conn)
        try:
            _run_step(
                "validate_prev_business_day",
                m._timed_daily_once,
                "validate_prev_business_day", m.phase_validate_prev_business_day, conn,
            )
        except RuntimeError as e:
            if str(e).startswith("daily phase returned False: validate_prev_business_day"):
                print(f"[eod][validate-prev] pending: {e}")
            else:
                raise
        _run_step("update_operating_income_and_ratio", m.update_operating_income_and_ratio, conn)
        _run_step("sync_latest_prices", m.phase_sync_latest_prices, conn)
        print(f"[eod] finalized {len(codes)} securities for {now.date()}", flush=True)
        return 0
    except Exception as e:
        # Yahoo未確定を含む再試行可能なEOD失敗はpartial=2。10分後に再試行する。
        print(f"[eod][PENDING/ERROR] {type(e).__name__}: {e}", flush=True)
        return 2
    finally:
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    raise SystemExit(main())
