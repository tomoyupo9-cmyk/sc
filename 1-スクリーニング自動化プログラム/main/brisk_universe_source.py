# -*- coding: utf-8 -*-
"""
Shared BRiSK equity-universe path provider.

This module is intentionally small and independent from 自動スクリーニング.py.
It is used by system_jobs / preflight / direct producers so they all see the
same Tokyo P/S/G ordinary-equity universe.

Normal universe:
  prefix P/S/G
  security code = 4 digits or 3 digits + A-Z
Excluded:
  E/R/Pro/Y, 5-digit preferred-like codes, benchmarks/proxy ETFs
Fallback:
  legacy 株コード番号.txt if BRiSK is unavailable/invalid and REQUIRED=0
"""
from __future__ import annotations

import csv
import os
import re
import tempfile
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

JST = timezone(timedelta(hours=9))

HERE = Path(__file__).resolve().parent
BRISK_MASTER = Path(os.environ.get(
    "KABU_BRISK_MASTER",
    r"D:\kabu\main\0-市場データ基盤\BRiSK\output\brisk_master_all.csv"
))
LEGACY_CODES = Path(os.environ.get(
    "KABU_LEGACY_CODES_PATH",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt"
))
EFFECTIVE_CODES = Path(os.environ.get(
    "KABU_BRISK_EFFECTIVE_UNIVERSE",
    str(HERE / "runtime" / "brisk_equity_universe.csv")
))

_MARKET_MAP = {"P": "東P", "S": "東S", "G": "東G"}

def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}

def _canon(v) -> str:
    if v is None:
        return ""
    s = str(v).strip().upper()
    if not s or s in {"NAN", "NONE"}:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    if s.isdigit():
        return s.zfill(4)
    return s

def _atomic_write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8-sig", newline="",
            dir=str(path.parent), prefix=path.name + ".tmp.", delete=False
        ) as f:
            tmp_name = f.name
            w = csv.DictWriter(f, fieldnames=["コード", "銘柄名", "市場"])
            w.writeheader()
            w.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
        tmp_name = None
    finally:
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except Exception:
                pass

def prepare_brisk_codes_path() -> Path:
    enabled = _bool_env("KABU_BRISK_UNIVERSE_ENABLED", True)
    required = _bool_env("KABU_BRISK_UNIVERSE_REQUIRED", False)

    def fallback(reason: str) -> Path:
        if required:
            raise RuntimeError("BRiSK universe required but invalid: " + reason)
        if not LEGACY_CODES.is_file():
            raise FileNotFoundError(
                "BRiSK invalid and legacy codes missing: "
                f"brisk={BRISK_MASTER} legacy={LEGACY_CODES} reason={reason}"
            )
        print(f"[universe-source][WARN] BRiSK -> LEGACY fallback: {reason}", flush=True)
        return LEGACY_CODES

    if not enabled:
        return fallback("KABU_BRISK_UNIVERSE_ENABLED=0")

    try:
        if not BRISK_MASTER.is_file():
            raise FileNotFoundError(str(BRISK_MASTER))

        with BRISK_MASTER.open("r", encoding="utf-8-sig", newline="") as f:
            src = list(csv.DictReader(f))

        if not src:
            raise RuntimeError("BRiSK master empty")

        need = {"issue_code", "name", "prefix"}
        missing = need - set(src[0].keys())
        if missing:
            raise RuntimeError(f"missing columns={sorted(missing)}")

        rows = []
        seen = set()
        counts = Counter()
        trade_dates = []

        for r in src:
            code = _canon(r.get("issue_code"))
            name = str(r.get("name") or "").strip()
            pfx = str(r.get("prefix") or "").strip()

            td = str(r.get("trade_date") or "").strip()
            if td:
                trade_dates.append(td[:10])

            if pfx not in _MARKET_MAP:
                continue
            if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", code):
                continue
            if not name:
                continue
            if code in seen:
                raise RuntimeError(f"duplicate filtered code={code}")
            seen.add(code)
            counts[pfx] += 1
            rows.append({
                "コード": code,
                "銘柄名": name,
                "市場": _MARKET_MAP[pfx],
            })

        n = len(rows)
        lo = int(os.environ.get("KABU_BRISK_UNIVERSE_MIN", "3600"))
        hi = int(os.environ.get("KABU_BRISK_UNIVERSE_MAX", "3800"))
        if not (lo <= n <= hi):
            raise RuntimeError(f"equity count out of range: {n} not in [{lo},{hi}]")

        gates = {"P": (1400,1700), "S": (1400,1700), "G": (450,750)}
        for pfx, (a,b) in gates.items():
            v = int(counts.get(pfx, 0))
            if not (a <= v <= b):
                raise RuntimeError(f"{pfx} count out of range: {v} not in [{a},{b}]")

        max_td = max(trade_dates) if trade_dates else ""
        if max_td:
            try:
                d = datetime.strptime(max_td, "%Y-%m-%d").date()
                age = (datetime.now(JST).date() - d).days
                fresh_days = max(1, int(os.environ.get(
                    "KABU_BRISK_UNIVERSE_FRESH_DAYS", "10"
                )))
                if age < -1 or age > fresh_days:
                    raise RuntimeError(
                        f"trade_date stale/future: {max_td} age_days={age} limit={fresh_days}"
                    )
            except ValueError:
                raise RuntimeError(f"invalid trade_date={max_td}")

        rows.sort(key=lambda r: r["コード"])
        _atomic_write_csv(EFFECTIVE_CODES, rows)

        print(
            "[universe-source] source=BRISK "
            f"equities={n} P={counts.get('P',0)} "
            f"S={counts.get('S',0)} G={counts.get('G',0)} "
            f"trade_date={max_td or '-'} path={EFFECTIVE_CODES}",
            flush=True,
        )
        return EFFECTIVE_CODES

    except Exception as e:
        return fallback(str(e))

def resolve_codes_path() -> Path:
    # Child jobs launched by system_jobs receive the already-resolved path.
    explicit = str(os.environ.get("KABU_CODES_PATH") or "").strip()
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
    return prepare_brisk_codes_path()
