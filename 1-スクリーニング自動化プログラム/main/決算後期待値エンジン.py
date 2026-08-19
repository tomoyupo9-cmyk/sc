# -*- coding: utf-8 -*-
"""
決算後期待値エンジン v4（予想ギャップ精度検証）
========================
目的
----
決算後上昇スクリーニングの「今からスコア」を期待値と誤認せず、
過去の同じ決算後ステージ・似た価格パスから、セットアップ自体の統計的優位性を推定する。

重要な設計原則
--------------
- ユーザー個人の売買特性は使わない（Personal Judge / ENTRY NOW と分離）。
- 信用残・機関空売り・板・歩み値は使わない。
- 過去ケースは各snapshot時点までの情報だけで特徴量を作り、未来情報リークを避ける。
- 期待値の実現値は price_history の連続リターンで算出し、+5/-5の粗いラベルだけに依存しない。
- MIDDAYの現在値は過去の日足終値snapshotとの完全同条件ではないため、LIVE_PROXY と明示する。
- PRICE_EXCESSを価格パス基準モデルとし、PRICE_PLUS_ACH、PRICE_PLUS_ACH_GAPを同じ時系列holdoutで比較する。
- 予想達成履歴・会社予想ギャップは、改善幅とage別再現性が確認できた場合だけ本番採用する。

主な出力
--------
- 期待値スコア / 優位性ランク
- 類似ケース数 / 有効類似ケース数 / 信頼度
- 1/2/3/5/10営業日後の上昇確率・平均・中央値リターン
- 10営業日MFE/MAE 平均・中央値・比率
- 10営業日以内 +3/+5/+10/+15% 到達確率
- 高値到達日の平均・中央値、1/2/3/5日以内/10日以内割合

例
--
python 決算後期待値エンジン_v4_予想ギャップ検証.py
python 決算後期待値エンジン_v4_予想ギャップ検証.py --validate
python 決算後期待値エンジン_v4_予想ギャップ検証.py --input "...\\決算後上昇_全母集団.csv" --history-years 4
"""

from __future__ import annotations

import argparse
import math
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# paths / constants
# -----------------------------------------------------------------------------
DB_CANDIDATES = [
    Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"),
    Path(r"D:\kabu\main\1-スクリーニング自動化プログラム\main\db\kani2.db"),
]
OUTPUT_CANDIDATES = [
    Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data"),
    Path(r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data"),
]
DEFAULT_INPUT_NAME = "決算後上昇_全母集団.csv"
DEFAULT_OUTPUT_NAME = "決算後期待値.csv"
DEFAULT_TOP_NAME = "決算後期待値_上位.csv"
DEFAULT_AUDIT_NAME = "決算後期待値_精度監査.txt"
DEFAULT_VALIDATION_NAME = "決算後期待値_時系列検証.csv"
DEFAULT_VALIDATION_SUMMARY = "決算後期待値_時系列検証_summary.txt"

HORIZONS = (1, 2, 3, 5, 10)
TARGETS = (3, 5, 10, 15)
MAX_FORWARD = 10
MAX_AGE = 60

# v5: price-path baseline + forecast-achievement + company-forecast-gap experiments.
# v5 strengthens historical forecast-gap reconstruction with prior XBRL forecasts.
# --validate compares them on exactly the same chronological cases.
PRICE_FEATURE_WEIGHTS = {
    "current_ret": 1.45,
    "peak_ret": 0.80,
    "high_dd": 1.45,
    "low_rebound": 0.45,
    "d1": 1.05,
    "d3": 0.85,
    "d5": 0.85,
    "ma5_gap": 1.10,
    "ma25_gap": 0.80,
    "ret1": 0.55,
    "ret3": 0.75,
    "ret5": 0.90,
    "ret10": 0.70,
    "pre5_ret": 0.75,
    "pre20_ret": 0.95,
    "pre20_peak_dd": 0.75,
    "log_base_price": 0.40,
}
ACH_FEATURE_WEIGHTS = {
    "ach_hit_rate": 0.75,
    "ach_avg_pct": 0.65,
    "ach_min_pct": 0.45,
    "ach_revision_balance": 0.35,
}
PRICE_ACH_FEATURE_WEIGHTS = {**PRICE_FEATURE_WEIGHTS, **ACH_FEATURE_WEIGHTS}

# 会社予想の「今回 vs 発表前最新」のギャップ。
# OPを主役、EPSは補助。重みは固定し、v5ではチューニングせず時系列holdoutで効くかだけを見る。
GAP_FEATURE_WEIGHTS = {
    "forecast_op_gap_pct": 0.95,
    "forecast_eps_gap_pct": 0.35,
}
PRICE_ACH_GAP_FEATURE_WEIGHTS = {
    **PRICE_ACH_FEATURE_WEIGHTS,
    **GAP_FEATURE_WEIGHTS,
}

# prepare_bank / validation feature extraction needs the superset.
ALL_FEATURE_WEIGHTS = PRICE_ACH_GAP_FEATURE_WEIGHTS
FEATURE_WEIGHTS = ALL_FEATURE_WEIGHTS
MODEL_FEATURE_WEIGHTS = {
    "price": PRICE_FEATURE_WEIGHTS,
    "price-ach": PRICE_ACH_FEATURE_WEIGHTS,
    "price-ach-gap": PRICE_ACH_GAP_FEATURE_WEIGHTS,
}


CURRENT_COLUMN_MAP = {
    "current_ret": "決算後騰落率",
    "peak_ret": "決算後最大上昇率",
    "high_dd": "高値から乖離率",
    "low_rebound": "安値から反発率",
    "d1": "D1",
    "d3": "D3",
    "d5": "D5",
    "ma5_gap": "MA5乖離率",
    "ma25_gap": "MA25乖離率",
    "ret1": "直近1日騰落率",
    "ret3": "直近3日騰落率",
    "ret5": "直近5日騰落率",
    "ret10": "直近10日騰落率",
}


# -----------------------------------------------------------------------------
# generic helpers
# -----------------------------------------------------------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def canonical_code(v) -> str:
    if v is None:
        return ""
    s = str(v).strip().upper()
    if not s or s in {"NAN", "NONE"}:
        return ""
    if s in {"^TOPX", "998405", "998405.T", "TOPIX"}:
        return "^TOPX"
    for suf in (".T", ".N", ".S", ".F", "-T", "-N", "-S", "-F", ".JP", "-JP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    if re.fullmatch(r"[0-9]+\.0+", s):
        s = s.split(".", 1)[0]
    if s.isdigit():
        return s.zfill(4)
    return s


def num(v) -> Optional[float]:
    try:
        x = float(str(v).replace(",", ""))
        return x if math.isfinite(x) else None
    except Exception:
        return None


def pct(new, old) -> Optional[float]:
    a, b = num(new), num(old)
    if a is None or b in (None, 0):
        return None
    return (a / b - 1.0) * 100.0


def clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone() is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def resolve_db_path(cli: Optional[str]) -> Path:
    if cli:
        p = Path(cli)
        if not p.exists():
            raise FileNotFoundError(f"DBが見つかりません: {p}")
        return p
    for p in DB_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError("kani2.db を自動検出できません。--db で指定してください。")


def resolve_output_dir(cli: Optional[str]) -> Path:
    if cli:
        p = Path(cli)
        p.mkdir(parents=True, exist_ok=True)
        return p
    for p in OUTPUT_CANDIDATES:
        if p.exists():
            return p
    p = Path(__file__).resolve().parent / "output_data"
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_input_path(cli: Optional[str], output_dir: Path) -> Path:
    if cli:
        p = Path(cli)
        if not p.exists():
            raise FileNotFoundError(f"入力CSVが見つかりません: {p}")
        return p
    p = output_dir / DEFAULT_INPUT_NAME
    if p.exists():
        return p
    for d in OUTPUT_CANDIDATES:
        q = d / DEFAULT_INPUT_NAME
        if q.exists():
            return q
    raise FileNotFoundError(f"{DEFAULT_INPUT_NAME} を自動検出できません。--input で指定してください。")


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=60.0)
    conn.execute("PRAGMA query_only=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-120000;")
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn


def guess_kind(title: str = "", quarter_no=None, fallback: str = "") -> str:
    t = str(title or "").translate(str.maketrans("１２３４", "1234"))
    if re.search(r"(第\s*1\s*四半期|1Q)", t, re.I):
        return "1Q"
    if re.search(r"(第\s*2\s*四半期|2Q|中間|半期)", t, re.I):
        return "2Q/H1"
    if re.search(r"(第\s*3\s*四半期|3Q)", t, re.I):
        return "3Q"
    if re.search(r"(通期|本決算|決算短信)", t, re.I) and not re.search(r"第\s*[123]\s*四半期", t):
        if str(fallback or "").strip():
            return str(fallback).strip()
    try:
        q = int(quarter_no)
        return {1: "1Q", 2: "2Q/H1", 3: "3Q", 4: "FY"}.get(q, str(fallback or ""))
    except Exception:
        return str(fallback or "")


def weighted_mean(x: np.ndarray, w: np.ndarray) -> float:
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not mask.any():
        return float("nan")
    return float(np.sum(x[mask] * w[mask]) / np.sum(w[mask]))


def weighted_median(x: np.ndarray, w: np.ndarray) -> float:
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not mask.any():
        return float("nan")
    xx, ww = x[mask], w[mask]
    order = np.argsort(xx, kind="mergesort")
    xx, ww = xx[order], ww[order]
    c = np.cumsum(ww)
    return float(xx[np.searchsorted(c, c[-1] * 0.5, side="left")])


def weighted_rate(mask: np.ndarray, w: np.ndarray) -> float:
    valid = np.isfinite(w) & (w > 0)
    if not valid.any():
        return float("nan")
    return float(np.sum(w[valid] * mask[valid].astype(float)) / np.sum(w[valid]) * 100.0)


def winsor_weighted_mean(x: np.ndarray, w: np.ndarray, qlo: float = 0.05, qhi: float = 0.95) -> float:
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not mask.any():
        return float("nan")
    xx, ww = x[mask], w[mask]
    if len(xx) >= 20:
        lo, hi = np.quantile(xx, [qlo, qhi])
        xx = np.clip(xx, lo, hi)
    return float(np.sum(xx * ww) / np.sum(ww))


# -----------------------------------------------------------------------------
# current input
# -----------------------------------------------------------------------------
def load_current_candidates(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"コード": str}, low_memory=False)
    if "コード" not in df.columns:
        raise RuntimeError("入力CSVにコード列がありません")
    df["コード"] = df["コード"].map(canonical_code)
    if "決算後営業日数" not in df.columns:
        raise RuntimeError("入力CSVに決算後営業日数がありません。最新の決算後スクリーナー出力を使ってください。")
    return df


# -----------------------------------------------------------------------------
# forecast achievement history (strictly pre-event; no lookahead)
# -----------------------------------------------------------------------------
def load_achievement_history(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    if not table_exists(conn, "forecast_achievement_history"):
        return {}
    c = columns(conn, "forecast_achievement_history")
    need = {"コード", "fiscal_key", "initial_forecast_op", "actual_op"}
    if not need.issubset(c):
        return {}
    up = '"upward_revisions"' if "upward_revisions" in c else "0"
    down = '"downward_revisions"' if "downward_revisions" in c else "0"
    avail_col = next((x for x in ("actual_announcement_date", "announcement_date", "発表日時", "発表日") if x in c), None)
    avail_expr = f'"{avail_col}"' if avail_col else "NULL"
    q = (
        'SELECT CAST(コード AS TEXT) code, fiscal_key, initial_forecast_op, actual_op, '
        + up + ' upward_revisions, ' + down + ' downward_revisions, ' + avail_expr + ' availability_date '
        'FROM forecast_achievement_history'
    )
    rows = conn.execute(q).fetchall()
    out: dict[str, list[tuple]] = {}
    for code, fk, fc, act, ur, dr, avail_raw in rows:
        cc = canonical_code(code)
        m = re.match(r"^(\d{4})-(\d{1,2})", str(fk or "").strip())
        if not m:
            continue
        try:
            end_approx = pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=28).normalize()
        except Exception:
            continue
        availability = pd.to_datetime(avail_raw, errors="coerce") if avail_raw not in (None, "") else pd.NaT
        if pd.isna(availability):
            # No explicit disclosure timestamp: use a deliberately conservative lag to avoid lookahead.
            availability = end_approx + pd.Timedelta(days=120)
        else:
            availability = pd.Timestamp(availability).normalize()
        out.setdefault(cc, []).append((availability, num(fc), num(act), num(ur) or 0.0, num(dr) or 0.0))
    for code in out:
        out[code].sort(key=lambda x: x[0], reverse=True)
    return out


def achievement_features(rows: list[tuple] | None, cutoff) -> dict[str, float]:
    res = {
        "ach_hit_rate": float("nan"),
        "ach_avg_pct": float("nan"),
        "ach_min_pct": float("nan"),
        "ach_revision_balance": float("nan"),
        "ach_n": 0.0,
    }
    if not rows:
        return res
    try:
        cut = pd.Timestamp(cutoff).normalize()
    except Exception:
        return res
    usable = []
    for availability, fc, act, ur, dr in rows:
        if availability >= cut:
            continue
        if fc is None or act is None or not np.isfinite(fc) or not np.isfinite(act) or fc <= 0:
            continue
        ratio = float(act / fc * 100.0)
        usable.append((ratio, float(ur), float(dr)))
        if len(usable) >= 5:
            break
    if not usable:
        return res
    ratios = np.clip(np.array([x[0] for x in usable], dtype=float), -100.0, 250.0)
    res["ach_hit_rate"] = float(np.mean(ratios >= 100.0) * 100.0)
    res["ach_avg_pct"] = float(np.mean(ratios))
    res["ach_min_pct"] = float(np.min(ratios))
    res["ach_revision_balance"] = float(np.mean([u-d for _,u,d in usable]))
    res["ach_n"] = float(len(usable))
    return res


def enrich_aligned_achievement(aligned: list[dict], ach_map: dict[str, list[tuple]]) -> None:
    for ev in aligned:
        ev.update(achievement_features(ach_map.get(ev["code"]), ev["event_date"]))


# -----------------------------------------------------------------------------
# company forecast gap history (strictly event-time / pre-event; no lookahead)
# -----------------------------------------------------------------------------
def fiscal_key_from_title(title: str) -> Optional[str]:
    t = str(title or "").translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    m = re.search(r"(20\d{2})年\s*(\d{1,2})月期", t)
    if not m:
        return None
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"


def load_forecast_history(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    """
    forecast_history:
      (forecast_date, fiscal_key, forecast_op, forecast_eps, sequence)

    prior forecast is always selected strictly before the earnings event date.
    This deliberately refuses same-day prior rows because forecast_date can be date-only.
    """
    if not table_exists(conn, "forecast_history"):
        return {}
    c = columns(conn, "forecast_history")
    need = {"コード", "fiscal_key", "forecast_date", "forecast_op"}
    if not need.issubset(c):
        return {}
    eps_expr = '"forecast_eps"' if "forecast_eps" in c else "NULL"
    seq_expr = '"id"' if "id" in c else "rowid"
    q = f"""
        SELECT CAST(コード AS TEXT) code, fiscal_key, forecast_date,
               forecast_op, {eps_expr} forecast_eps, {seq_expr} seq
        FROM forecast_history
        WHERE forecast_op IS NOT NULL
        ORDER BY コード, forecast_date, seq
    """
    out: dict[str, list[tuple]] = {}
    for code, fk, fd, op, eps, seq in conn.execute(q).fetchall():
        cc = canonical_code(code)
        dt = pd.to_datetime(fd, errors="coerce")
        if not cc or pd.isna(dt):
            continue
        out.setdefault(cc, []).append(
            (pd.Timestamp(dt), str(fk or ""), num(op), num(eps), int(seq or 0))
        )
    return out


def load_xbrl_forecast_map(
    conn: sqlite3.Connection,
    start_date: str,
) -> tuple[
    dict[tuple[str, pd.Timestamp], tuple],
    dict[tuple[str, pd.Timestamp], list[tuple]],
    dict[tuple[str, str], list[tuple]],
]:
    """
    Current forecast at the earnings release is taken from tdnet_xbrl_metrics when available.

    exact map:
      (code, exact timestamp) -> (fiscal_key, op, eps, title, timestamp)

    day map:
      (code, normalized day) -> list[...] for conservative unique-row fallback.

    history map (v5):
      (code, fiscal_key) -> chronological list[(timestamp, op, eps, title)].
      This lets an older quarterly XBRL forecast serve as the strictly-prior
      company forecast when forecast_history is sparse.
    """
    if not table_exists(conn, "tdnet_xbrl_metrics"):
        return {}, {}, {}
    c = columns(conn, "tdnet_xbrl_metrics")
    need = {"コード", "提出時刻"}
    if not need.issubset(c):
        return {}, {}, {}
    fk_expr = '"forecast_fiscal_key"' if "forecast_fiscal_key" in c else "NULL"
    op_expr = '"forecast_op"' if "forecast_op" in c else "NULL"
    eps_expr = '"forecast_eps"' if "forecast_eps" in c else "NULL"
    title_expr = '"タイトル"' if "タイトル" in c else "''"
    q = f"""
        SELECT CAST(コード AS TEXT) code, 提出時刻, {title_expr} title,
               {fk_expr} fiscal_key, {op_expr} forecast_op, {eps_expr} forecast_eps
        FROM tdnet_xbrl_metrics
        WHERE date(提出時刻) >= date(?)
          AND ({op_expr} IS NOT NULL OR {eps_expr} IS NOT NULL)
        ORDER BY 提出時刻 ASC
    """
    exact: dict[tuple[str, pd.Timestamp], tuple] = {}
    by_day: dict[tuple[str, pd.Timestamp], list[tuple]] = {}
    history: dict[tuple[str, str], list[tuple]] = {}
    for code, pub, title, fk, op, eps in conn.execute(q, (start_date,)).fetchall():
        cc = canonical_code(code)
        dt = pd.to_datetime(pub, errors="coerce")
        if not cc or pd.isna(dt):
            continue
        ts = pd.Timestamp(dt)
        fiscal_key = str(fk or "") or (fiscal_key_from_title(str(title or "")) or "")
        item = (fiscal_key, num(op), num(eps), str(title or ""), ts)
        exact[(cc, ts)] = item
        by_day.setdefault((cc, ts.normalize()), []).append(item)
        if fiscal_key:
            history.setdefault((cc, fiscal_key), []).append(
                (ts, num(op), num(eps), str(title or ""))
            )
    return exact, by_day, history

def _latest_prior_forecast(
    rows: list[tuple] | None,
    fiscal_key: str,
    event_dt,
) -> tuple[Optional[float], Optional[float]]:
    if not rows or not fiscal_key:
        return None, None
    cut = pd.Timestamp(event_dt)
    day = cut.normalize()
    best = None
    for fd, fk, op, eps, seq in rows:
        if str(fk) != str(fiscal_key):
            continue
        # Strictly earlier calendar date. forecast_date is sometimes date-only.
        if pd.Timestamp(fd).normalize() >= day:
            continue
        if op is None and eps is None:
            continue
        if best is None or (pd.Timestamp(fd), seq) > (best[0], best[1]):
            best = (pd.Timestamp(fd), seq, op, eps)
    if best is None:
        return None, None
    return best[2], best[3]


def _latest_prior_xbrl_forecast(
    rows: list[tuple] | None,
    event_dt,
) -> tuple[Optional[float], Optional[float], Optional[pd.Timestamp]]:
    """
    XBRL has an exact submission timestamp, so the latest row strictly before
    the current earnings timestamp is safe even when it is on the same day.
    Equal timestamps are excluded because they belong to the current release.
    """
    if not rows:
        return None, None, None
    cut = pd.Timestamp(event_dt)
    best = None
    for ts, op, eps, _title in rows:
        tt = pd.Timestamp(ts)
        if tt >= cut:
            continue
        if op is None and eps is None:
            continue
        if best is None or tt > best[0]:
            best = (tt, op, eps)
    if best is None:
        return None, None, None
    return best[1], best[2], best[0]


def _prior_forecast_combined(
    history_rows: list[tuple] | None,
    xbrl_rows: list[tuple] | None,
    fiscal_key: str,
    event_dt,
) -> tuple[Optional[float], Optional[float], str]:
    """
    Pick the most recent strictly-prior company forecast from:
      1) forecast_history (date-only rows are restricted to prior calendar days)
      2) prior XBRL forecasts (exact timestamps, strictly before event timestamp)

    If both exist, prefer the source with the later observable timestamp.
    """
    cut = pd.Timestamp(event_dt)
    fh_op, fh_eps = _latest_prior_forecast(history_rows, fiscal_key, cut)
    fh_ts = None
    if history_rows and fiscal_key:
        day = cut.normalize()
        for fd, fk, op, eps, seq in history_rows:
            if str(fk) != str(fiscal_key):
                continue
            tt = pd.Timestamp(fd)
            if tt.normalize() >= day:
                continue
            if op is None and eps is None:
                continue
            if fh_ts is None or tt > fh_ts:
                fh_ts = tt

    xb_op, xb_eps, xb_ts = _latest_prior_xbrl_forecast(xbrl_rows, cut)

    if xb_ts is not None and (fh_ts is None or xb_ts > fh_ts):
        return xb_op, xb_eps, "prior_xbrl"
    if fh_ts is not None:
        return fh_op, fh_eps, "prior_forecast_history"
    if xb_ts is not None:
        return xb_op, xb_eps, "prior_xbrl"
    return None, None, ""


def forecast_gap_features(
    code: str,
    event_dt,
    title: str,
    forecast_history_map: dict[str, list[tuple]],
    xbrl_exact: dict[tuple[str, pd.Timestamp], tuple],
    xbrl_by_day: dict[tuple[str, pd.Timestamp], list[tuple]],
    xbrl_history: dict[tuple[str, str], list[tuple]],
) -> dict[str, object]:
    res: dict[str, object] = {
        "forecast_op_gap_pct": float("nan"),
        "forecast_eps_gap_pct": float("nan"),
        "forecast_gap_source": "",
        "forecast_gap_fiscal_key": "",
    }
    cc = canonical_code(code)
    dt = pd.to_datetime(event_dt, errors="coerce")
    if not cc or pd.isna(dt):
        return res
    ts = pd.Timestamp(dt)
    current = xbrl_exact.get((cc, ts))

    # If exact timestamp linkage is unavailable, accept only a unique XBRL forecast row on that day.
    if current is None:
        day_rows = xbrl_by_day.get((cc, ts.normalize()), [])
        usable = [x for x in day_rows if x[1] is not None or x[2] is not None]
        if len(usable) == 1:
            current = usable[0]

    if current is None:
        return res

    fk, cur_op, cur_eps, _xtitle, _xdt = current
    fk = str(fk or "") or (fiscal_key_from_title(title) or "")
    if not fk:
        return res
    prev_op, prev_eps, prior_source = _prior_forecast_combined(
        forecast_history_map.get(cc),
        xbrl_history.get((cc, fk)),
        fk,
        ts,
    )
    res["forecast_gap_fiscal_key"] = fk

    if cur_op is not None and prev_op is not None and np.isfinite(cur_op) and np.isfinite(prev_op) and cur_op > 0 and prev_op > 0:
        gap = (float(cur_op) / float(prev_op) - 1.0) * 100.0
        if -90.0 <= gap <= 500.0:
            res["forecast_op_gap_pct"] = float(gap)

    # EPS can be negative around turnarounds; only use same-sign positive values for a clean percentage gap.
    if cur_eps is not None and prev_eps is not None and np.isfinite(cur_eps) and np.isfinite(prev_eps) and cur_eps > 0 and prev_eps > 0:
        gap = (float(cur_eps) / float(prev_eps) - 1.0) * 100.0
        if -90.0 <= gap <= 500.0:
            res["forecast_eps_gap_pct"] = float(gap)

    if np.isfinite(res["forecast_op_gap_pct"]) or np.isfinite(res["forecast_eps_gap_pct"]):
        res["forecast_gap_source"] = "tdnet_xbrl_current+" + (prior_source or "prior_unknown")
    return res


def enrich_aligned_forecast_gap(
    aligned: list[dict],
    forecast_history_map: dict[str, list[tuple]],
    xbrl_exact: dict[tuple[str, pd.Timestamp], tuple],
    xbrl_by_day: dict[tuple[str, pd.Timestamp], list[tuple]],
    xbrl_history: dict[tuple[str, str], list[tuple]],
) -> None:
    for ev in aligned:
        ev.update(
            forecast_gap_features(
                ev["code"], ev["announced_dt"], ev.get("title", ""),
                forecast_history_map, xbrl_exact, xbrl_by_day, xbrl_history,
            )
        )


# -----------------------------------------------------------------------------
# historical event loading
# -----------------------------------------------------------------------------
def load_historical_events(conn: sqlite3.Connection, start_date: str, allow_date_only: bool = False) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def add(df: pd.DataFrame, source: str, priority: int, precision: str):
        if df is None or df.empty:
            return
        x = df.copy()
        x["source"] = source
        x["priority"] = priority
        x["precision"] = precision
        frames.append(x)

    if table_exists(conn, "tdnet_documents"):
        c = columns(conn, "tdnet_documents")
        if {"コード", "提出時刻", "タイトル"}.issubset(c):
            q = """
                SELECT CAST(コード AS TEXT) code, 提出時刻 announced, タイトル title,
                       NULL kind, NULL quarter_no
                FROM tdnet_documents
                WHERE date(提出時刻) >= date(?)
                  AND datetime(提出時刻) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add(pd.read_sql_query(q, conn, params=(start_date,)), "tdnet_documents", 50, "時刻")

    if table_exists(conn, "earnings_events"):
        c = columns(conn, "earnings_events")
        if {"コード", "タイトル"}.issubset(c) and ("発表日時" in c or "提出時刻" in c):
            dt_expr = (
                "COALESCE(NULLIF(発表日時,''), 提出時刻)"
                if {"発表日時", "提出時刻"}.issubset(c)
                else ("発表日時" if "発表日時" in c else "提出時刻")
            )
            kind_expr = "決算種別" if "決算種別" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) code, {dt_expr} announced, タイトル title,
                       {kind_expr} kind, NULL quarter_no
                FROM earnings_events
                WHERE date({dt_expr}) >= date(?)
                  AND datetime({dt_expr}) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add(pd.read_sql_query(q, conn, params=(start_date,)), "earnings_events", 45, "時刻")

    if table_exists(conn, "tdnet_xbrl_metrics"):
        c = columns(conn, "tdnet_xbrl_metrics")
        if {"コード", "提出時刻", "タイトル"}.issubset(c):
            kind_expr = "決算種別" if "決算種別" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) code, 提出時刻 announced, タイトル title,
                       {kind_expr} kind, NULL quarter_no
                FROM tdnet_xbrl_metrics
                WHERE date(提出時刻) >= date(?)
                  AND datetime(提出時刻) <= datetime('now','localtime')
                  AND タイトル LIKE '%決算短信%'
                  AND タイトル NOT LIKE '%訂正%'
            """
            add(pd.read_sql_query(q, conn, params=(start_date,)), "tdnet_xbrl_metrics", 40, "時刻")

    if table_exists(conn, "earnings_reaction_labels"):
        c = columns(conn, "earnings_reaction_labels")
        if {"コード", "発表日時"}.issubset(c):
            title_expr = "タイトル" if "タイトル" in c else "''"
            kind_expr = "決算種別" if "決算種別" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) code, 発表日時 announced, {title_expr} title,
                       {kind_expr} kind, NULL quarter_no
                FROM earnings_reaction_labels
                WHERE date(発表日時) >= date(?)
                  AND datetime(発表日時) <= datetime('now','localtime')
            """
            add(pd.read_sql_query(q, conn, params=(start_date,)), "earnings_reaction_labels", 20, "時刻")

    if allow_date_only and table_exists(conn, "quarterly_actual_history"):
        c = columns(conn, "quarterly_actual_history")
        if {"コード", "announcement_date"}.issubset(c):
            label_expr = "quarter_label" if "quarter_label" in c else "''"
            qno_expr = "quarter_no" if "quarter_no" in c else "NULL"
            q = f"""
                SELECT CAST(コード AS TEXT) code, announcement_date announced, {label_expr} title,
                       NULL kind, {qno_expr} quarter_no
                FROM quarterly_actual_history
                WHERE date(announcement_date) >= date(?)
                  AND date(announcement_date) <= date('now','localtime')
            """
            add(pd.read_sql_query(q, conn, params=(start_date,)), "quarterly_actual_history", 10, "日付")

    if not frames:
        return pd.DataFrame()

    ev = pd.concat(frames, ignore_index=True, sort=False)
    ev["code"] = ev["code"].map(canonical_code)
    ev = ev[ev["code"].astype(str).str.match(r"^(?:\d{4}|\d{3}[A-Z])$")].copy()
    ev["announced_dt"] = pd.to_datetime(
        ev["announced"].astype(str).str.replace("/", "-", regex=False).str.replace("T", " ", regex=False).str.replace("+09:00", "", regex=False),
        errors="coerce", format="mixed"
    )
    ev = ev.dropna(subset=["code", "announced_dt"])
    ev["event_date"] = ev["announced_dt"].dt.normalize()
    # 00:00:00 は実際のTDnet発表時刻としては不自然。reaction_labels等の古い日付placeholderは
    # 「日付精度」へ落とし、通常モードでは教師から除外する。
    midnight_placeholder = (
        ev["source"].astype(str).eq("earnings_reaction_labels")
        & ev["announced_dt"].dt.hour.eq(0)
        & ev["announced_dt"].dt.minute.eq(0)
        & ev["announced_dt"].dt.second.eq(0)
    )
    ev.loc[midnight_placeholder, "precision"] = "日付"
    if not allow_date_only:
        ev = ev[ev["precision"].astype(str).eq("時刻")].copy()
    ev["_precision_rank"] = ev["precision"].map({"日付": 0, "時刻": 1}).fillna(0).astype(int)
    ev["kind"] = [
        guess_kind(r.title, r.quarter_no, r.kind) for r in ev.itertuples(index=False)
    ]

    # Same code/day is one earnings event. Prefer precise/high-priority source and latest timestamp.
    ev = ev.sort_values(
        ["code", "event_date", "_precision_rank", "priority", "announced_dt"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    )
    ev = ev.drop_duplicates(["code", "event_date"], keep="last")
    ev["event_id"] = ev["code"].astype(str) + "@" + ev["event_date"].dt.strftime("%Y-%m-%d")
    return ev.drop(columns=["_precision_rank"], errors="ignore").reset_index(drop=True)


# -----------------------------------------------------------------------------
# price history loading / dedupe
# -----------------------------------------------------------------------------
def load_price_history(conn: sqlite3.Connection, start_date: str) -> pd.DataFrame:
    if not table_exists(conn, "price_history"):
        raise RuntimeError("price_history がありません")
    c = columns(conn, "price_history")
    if not {"コード", "日付", "終値"}.issubset(c):
        raise RuntimeError("price_history の必須列不足")
    op = '"始値"' if "始値" in c else '"終値"'
    hi = '"高値"' if "高値" in c else '"終値"'
    lo = '"安値"' if "安値" in c else '"終値"'
    q = f"""
        SELECT rowid _rowid, CAST(コード AS TEXT) raw_code, 日付,
               {op} AS 始値, {hi} AS 高値, {lo} AS 安値, 終値
        FROM price_history
        WHERE 日付 >= ? AND 終値 IS NOT NULL
        ORDER BY 日付, rowid
    """
    df = pd.read_sql_query(q, conn, params=(start_date,))
    if df.empty:
        return df
    df["コード"] = df["raw_code"].map(canonical_code)
    df["日付_dt"] = pd.to_datetime(df["日付"], errors="coerce").dt.normalize()
    for col in ("始値", "高値", "安値", "終値"):
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
    df = df.dropna(subset=["コード", "日付_dt", "終値"])
    df = df[df["コード"].astype(str).str.match(r"^(?:\d{4}|\d{3}[A-Z])$")].copy()

    rawu = df["raw_code"].astype(str).str.strip().str.upper()
    df["_canon_match"] = rawu.eq(df["コード"].astype(str).str.upper()).astype(int)
    df["_quality"] = df[["始値", "高値", "安値", "終値"]].notna().sum(axis=1)
    df = (
        df.sort_values(
            ["コード", "日付_dt", "_quality", "_canon_match", "_rowid"],
            ascending=[True, True, True, True, True], kind="mergesort"
        )
        .drop_duplicates(["コード", "日付_dt"], keep="last")
        .sort_values(["コード", "日付_dt"], kind="mergesort")
        .reset_index(drop=True)
    )
    return df[["コード", "日付_dt", "始値", "高値", "安値", "終値"]]


@dataclass
class PricePack:
    dates: np.ndarray
    op: np.ndarray
    hi: np.ndarray
    lo: np.ndarray
    cl: np.ndarray


def build_price_groups(prices: pd.DataFrame) -> dict[str, PricePack]:
    out: dict[str, PricePack] = {}
    for code, g in prices.groupby("コード", sort=False):
        out[str(code)] = PricePack(
            dates=g["日付_dt"].to_numpy(dtype="datetime64[ns]", copy=False),
            op=g["始値"].to_numpy(dtype=float, copy=False),
            hi=g["高値"].to_numpy(dtype=float, copy=False),
            lo=g["安値"].to_numpy(dtype=float, copy=False),
            cl=g["終値"].to_numpy(dtype=float, copy=False),
        )
    return out


def _idx_exact(dates: np.ndarray, day: np.datetime64) -> Optional[int]:
    i = int(np.searchsorted(dates, day, side="left"))
    if i < len(dates) and dates[i] == day:
        return i
    return None


def align_events(events: pd.DataFrame, groups: dict[str, PricePack]) -> list[dict]:
    aligned = []
    cutoff = dt_time(15, 30)
    for e in events.itertuples(index=False):
        pack = groups.get(str(e.code))
        if pack is None or len(pack.dates) < 3:
            continue
        day = np.datetime64(pd.Timestamp(e.event_date).to_datetime64())
        exact = _idx_exact(pack.dates, day)
        t = pd.Timestamp(e.announced_dt).time()
        if str(e.precision) == "時刻" and t < cutoff and exact is not None:
            d1_idx = exact
        else:
            d1_idx = int(np.searchsorted(pack.dates, day, side="right"))
        if d1_idx <= 0 or d1_idx >= len(pack.dates):
            continue
        base_idx = d1_idx - 1
        base = pack.cl[base_idx]
        if not np.isfinite(base) or base <= 0:
            continue
        aligned.append({
            "event_id": str(e.event_id),
            "code": str(e.code),
            "event_date": pd.Timestamp(e.event_date),
            "announced_dt": pd.Timestamp(e.announced_dt),
            "kind": str(e.kind or ""),
            "title": str(getattr(e, "title", "") or ""),
            "source": str(e.source),
            "precision": str(e.precision),
            "d1_idx": int(d1_idx),
            "base_idx": int(base_idx),
            "base_price": float(base),
        })
    return aligned


# -----------------------------------------------------------------------------
# snapshot features / realized outcomes
# -----------------------------------------------------------------------------
def _ret(cl: np.ndarray, idx: int, n: int) -> float:
    if idx - n < 0 or cl[idx - n] <= 0 or not np.isfinite(cl[idx - n]) or not np.isfinite(cl[idx]):
        return float("nan")
    return (cl[idx] / cl[idx - n] - 1.0) * 100.0


def _pre_features(pack: PricePack, base_idx: int) -> dict[str, float]:
    cl = pack.cl
    hi = pack.hi
    out = {"pre5_ret": float("nan"), "pre20_ret": float("nan"), "pre20_peak_dd": float("nan")}
    if base_idx >= 5:
        out["pre5_ret"] = _ret(cl, base_idx, 5)
    if base_idx >= 20:
        out["pre20_ret"] = _ret(cl, base_idx, 20)
        ph = hi[base_idx - 19: base_idx + 1]
        ph = ph[np.isfinite(ph) & (ph > 0)]
        if len(ph):
            peak = float(np.max(ph))
            out["pre20_peak_dd"] = (float(cl[base_idx]) / peak - 1.0) * 100.0
    return out


def snapshot_row(ev: dict, pack: PricePack, age: int, require_future: bool = True) -> Optional[dict]:
    age = int(age)
    snap_idx = ev["d1_idx"] + age - 1
    if age < 1 or snap_idx >= len(pack.cl):
        return None
    if require_future and snap_idx + MAX_FORWARD >= len(pack.cl):
        return None
    base = ev["base_price"]
    current = pack.cl[snap_idx]
    if not np.isfinite(current) or current <= 0:
        return None

    ps = slice(ev["d1_idx"], snap_idx + 1)
    highs = pack.hi[ps]
    lows = pack.lo[ps]
    highs = np.where(np.isfinite(highs) & (highs > 0), highs, pack.cl[ps])
    lows = np.where(np.isfinite(lows) & (lows > 0), lows, pack.cl[ps])
    post_high = float(np.nanmax(highs))
    post_low = float(np.nanmin(lows))

    hist = pack.cl[:snap_idx + 1]
    ma5 = float(np.nanmean(hist[-5:])) if len(hist) >= 5 else float("nan")
    ma25 = float(np.nanmean(hist[-25:])) if len(hist) >= 25 else float("nan")

    def dclose(n: int) -> float:
        idx = ev["d1_idx"] + n - 1
        if age < n or idx >= len(pack.cl):
            return float("nan")
        return (pack.cl[idx] / base - 1.0) * 100.0

    pre = _pre_features(pack, ev["base_idx"])
    row = {
        "event_id": ev["event_id"],
        "code": ev["code"],
        "event_date": ev["event_date"],
        "kind": ev["kind"],
        "age": age,
        "base_price": base,
        "snapshot_price": float(current),
        "current_ret": (float(current) / base - 1.0) * 100.0,
        "peak_ret": (post_high / base - 1.0) * 100.0,
        "high_dd": (float(current) / post_high - 1.0) * 100.0 if post_high > 0 else float("nan"),
        "low_rebound": (float(current) / post_low - 1.0) * 100.0 if post_low > 0 else float("nan"),
        "d1": dclose(1),
        "d3": dclose(3),
        "d5": dclose(5),
        "ma5_gap": (float(current) / ma5 - 1.0) * 100.0 if np.isfinite(ma5) and ma5 > 0 else float("nan"),
        "ma25_gap": (float(current) / ma25 - 1.0) * 100.0 if np.isfinite(ma25) and ma25 > 0 else float("nan"),
        "ret1": _ret(pack.cl, snap_idx, 1),
        "ret3": _ret(pack.cl, snap_idx, 3),
        "ret5": _ret(pack.cl, snap_idx, 5),
        "ret10": _ret(pack.cl, snap_idx, 10),
        "pre5_ret": pre["pre5_ret"],
        "pre20_ret": pre["pre20_ret"],
        "pre20_peak_dd": pre["pre20_peak_dd"],
        "log_base_price": math.log(max(base, 1.0)),
    }

    # Future outcomes from tradable snapshot close proxy.
    for h in HORIZONS:
        if snap_idx + h < len(pack.cl):
            row[f"fwd{h}"] = (pack.cl[snap_idx + h] / current - 1.0) * 100.0
        else:
            row[f"fwd{h}"] = float("nan")

    end = min(len(pack.cl) - 1, snap_idx + MAX_FORWARD)
    if end <= snap_idx:
        row.update({"mfe10": float("nan"), "mae10": float("nan"), "high_day10": float("nan")})
        for t in TARGETS:
            row[f"hit{t}_10"] = float("nan")
        return row

    fh = pack.hi[snap_idx + 1:end + 1]
    fl = pack.lo[snap_idx + 1:end + 1]
    fc = pack.cl[snap_idx + 1:end + 1]
    fh = np.where(np.isfinite(fh) & (fh > 0), fh, fc)
    fl = np.where(np.isfinite(fl) & (fl > 0), fl, fc)
    maxh = float(np.nanmax(fh))
    minl = float(np.nanmin(fl))
    row["mfe10"] = (maxh / current - 1.0) * 100.0
    row["mae10"] = (minl / current - 1.0) * 100.0
    row["high_day10"] = float(int(np.nanargmax(fh)) + 1)
    for t in TARGETS:
        row[f"hit{t}_10"] = 1.0 if row["mfe10"] >= float(t) else 0.0
    return row


def build_bank_for_age(aligned: list[dict], groups: dict[str, PricePack], age: int) -> pd.DataFrame:
    rows = []
    for ev in aligned:
        pack = groups.get(ev["code"])
        if pack is None:
            continue
        r = snapshot_row(ev, pack, age, require_future=True)
        if r is not None:
            rows.append(r)
    return pd.DataFrame(rows)


@dataclass
class MultiAgeBankMatrix:
    """同じ過去決算をageごとに再走査しないための価格軌跡キャッシュ。"""
    meta: pd.DataFrame
    valid: np.ndarray
    values: dict[str, np.ndarray]
    base_price: np.ndarray
    pre5_ret: np.ndarray
    pre20_ret: np.ndarray
    pre20_peak_dd: np.ndarray
    log_base_price: np.ndarray
    d1_value: np.ndarray
    d3_value: np.ndarray
    d5_value: np.ndarray
    max_age: int

    def bank(self, age: int) -> pd.DataFrame:
        age = int(age)
        if age < 1 or age > self.max_age:
            return pd.DataFrame()
        j = age - 1
        mask = self.valid[:, j]
        if not mask.any():
            return pd.DataFrame()
        ids = np.flatnonzero(mask)
        cur = self.values["snapshot_price"][ids, j]
        out = pd.DataFrame({
            "event_id": self.meta.iloc[ids]["event_id"].to_numpy(),
            "code": self.meta.iloc[ids]["code"].to_numpy(),
            "event_date": self.meta.iloc[ids]["event_date"].to_numpy(),
            "kind": self.meta.iloc[ids]["kind"].to_numpy(),
            "age": np.full(len(ids), age, dtype=int),
            "base_price": self.base_price[ids],
            "snapshot_price": cur,
            "current_ret": self.values["current_ret"][ids, j],
            "peak_ret": self.values["peak_ret"][ids, j],
            "high_dd": self.values["high_dd"][ids, j],
            "low_rebound": self.values["low_rebound"][ids, j],
            "d1": np.where(age >= 1, self.d1_value[ids], np.nan),
            "d3": np.where(age >= 3, self.d3_value[ids], np.nan),
            "d5": np.where(age >= 5, self.d5_value[ids], np.nan),
            "ma5_gap": self.values["ma5_gap"][ids, j],
            "ma25_gap": self.values["ma25_gap"][ids, j],
            "ret1": self.values["ret1"][ids, j],
            "ret3": self.values["ret3"][ids, j],
            "ret5": self.values["ret5"][ids, j],
            "ret10": self.values["ret10"][ids, j],
            "pre5_ret": self.pre5_ret[ids],
            "pre20_ret": self.pre20_ret[ids],
            "pre20_peak_dd": self.pre20_peak_dd[ids],
            "log_base_price": self.log_base_price[ids],
        })
        for col in (
            "ach_hit_rate", "ach_avg_pct", "ach_min_pct", "ach_revision_balance", "ach_n",
            "forecast_op_gap_pct", "forecast_eps_gap_pct",
        ):
            if col in self.meta.columns:
                out[col] = pd.to_numeric(self.meta.iloc[ids][col], errors="coerce").to_numpy(dtype=float)
        if "forecast_gap_source" in self.meta.columns:
            out["forecast_gap_source"] = self.meta.iloc[ids]["forecast_gap_source"].astype(str).to_numpy()
        for h in HORIZONS:
            out[f"fwd{h}"] = self.values[f"fwd{h}"][ids, j]
        out["mfe10"] = self.values["mfe10"][ids, j]
        out["mae10"] = self.values["mae10"][ids, j]
        out["high_day10"] = self.values["high_day10"][ids, j]
        for t in TARGETS:
            mfe = out["mfe10"].to_numpy(dtype=float)
            out[f"hit{t}_10"] = np.where(np.isfinite(mfe), (mfe >= float(t)).astype(float), np.nan)
        return out


def build_multi_age_matrix(
    aligned: list[dict],
    groups: dict[str, PricePack],
    ages: list[int] | tuple[int, ...] | set[int],
) -> MultiAgeBankMatrix:
    """
    v2高速化:
      v1はageごとにaligned全件を再走査していた。
      v2は各決算イベントを1回だけ処理し、必要最大ageまでの軌跡をまとめて計算する。
      数式・基準価格・未来10営業日の定義はsnapshot_row()と同一。
    """
    ages = sorted({int(a) for a in ages if 1 <= int(a) <= MAX_AGE})
    max_age = max(ages) if ages else 1
    n_ev = len(aligned)
    meta = pd.DataFrame({
        "event_id": [ev["event_id"] for ev in aligned],
        "code": [ev["code"] for ev in aligned],
        "event_date": [ev["event_date"] for ev in aligned],
        "kind": [ev["kind"] for ev in aligned],
        "ach_hit_rate": [ev.get("ach_hit_rate", np.nan) for ev in aligned],
        "ach_avg_pct": [ev.get("ach_avg_pct", np.nan) for ev in aligned],
        "ach_min_pct": [ev.get("ach_min_pct", np.nan) for ev in aligned],
        "ach_revision_balance": [ev.get("ach_revision_balance", np.nan) for ev in aligned],
        "ach_n": [ev.get("ach_n", 0.0) for ev in aligned],
        "forecast_op_gap_pct": [ev.get("forecast_op_gap_pct", np.nan) for ev in aligned],
        "forecast_eps_gap_pct": [ev.get("forecast_eps_gap_pct", np.nan) for ev in aligned],
        "forecast_gap_source": [ev.get("forecast_gap_source", "") for ev in aligned],
    })
    valid = np.zeros((n_ev, max_age), dtype=bool)
    names = [
        "snapshot_price", "current_ret", "peak_ret", "high_dd", "low_rebound",
        "ma5_gap", "ma25_gap", "ret1", "ret3", "ret5", "ret10",
        *[f"fwd{h}" for h in HORIZONS], "mfe10", "mae10", "high_day10",
    ]
    values = {name: np.full((n_ev, max_age), np.nan, dtype=float) for name in names}
    base_price = np.full(n_ev, np.nan, dtype=float)
    pre5 = np.full(n_ev, np.nan, dtype=float)
    pre20 = np.full(n_ev, np.nan, dtype=float)
    pre20dd = np.full(n_ev, np.nan, dtype=float)
    log_base = np.full(n_ev, np.nan, dtype=float)
    d1v = np.full(n_ev, np.nan, dtype=float)
    d3v = np.full(n_ev, np.nan, dtype=float)
    d5v = np.full(n_ev, np.nan, dtype=float)

    for i, ev in enumerate(aligned):
        pack = groups.get(ev["code"])
        if pack is None:
            continue
        d1_idx = int(ev["d1_idx"])
        base_idx = int(ev["base_idx"])
        base = float(ev["base_price"])
        # require_future=True: snap_idx + MAX_FORWARD < len(cl)
        n = min(max_age, len(pack.cl) - MAX_FORWARD - d1_idx)
        if n <= 0 or not np.isfinite(base) or base <= 0:
            continue

        idx = d1_idx + np.arange(n, dtype=int)
        cur = pack.cl[idx].astype(float, copy=False)
        cur_ok = np.isfinite(cur) & (cur > 0)
        valid[i, :n] = cur_ok
        base_price[i] = base
        log_base[i] = math.log(max(base, 1.0))
        pre = _pre_features(pack, base_idx)
        pre5[i] = pre["pre5_ret"]
        pre20[i] = pre["pre20_ret"]
        pre20dd[i] = pre["pre20_peak_dd"]

        for dn, tgt in ((1, d1v), (3, d3v), (5, d5v)):
            di = d1_idx + dn - 1
            if di < len(pack.cl) and np.isfinite(pack.cl[di]):
                tgt[i] = (float(pack.cl[di]) / base - 1.0) * 100.0

        cl_post = pack.cl[d1_idx:d1_idx+n].astype(float, copy=False)
        hi_post = pack.hi[d1_idx:d1_idx+n].astype(float, copy=False)
        lo_post = pack.lo[d1_idx:d1_idx+n].astype(float, copy=False)
        hi_post = np.where(np.isfinite(hi_post) & (hi_post > 0), hi_post, cl_post)
        lo_post = np.where(np.isfinite(lo_post) & (lo_post > 0), lo_post, cl_post)
        peak = np.maximum.accumulate(hi_post)
        trough = np.minimum.accumulate(lo_post)

        values["snapshot_price"][i, :n] = cur
        values["current_ret"][i, :n] = (cur / base - 1.0) * 100.0
        values["peak_ret"][i, :n] = (peak / base - 1.0) * 100.0
        values["high_dd"][i, :n] = np.where(peak > 0, (cur / peak - 1.0) * 100.0, np.nan)
        values["low_rebound"][i, :n] = np.where(trough > 0, (cur / trough - 1.0) * 100.0, np.nan)

        # rolling MA: load_price_historyで終値欠損行は除外済み。snapshot_rowのnanmeanと同じ窓。
        cs = np.concatenate(([0.0], np.cumsum(pack.cl, dtype=float)))
        for w, name in ((5, "ma5_gap"), (25, "ma25_gap")):
            ok = idx >= (w - 1)
            if ok.any():
                ii = idx[ok]
                ma = (cs[ii + 1] - cs[ii + 1 - w]) / float(w)
                values[name][i, :n][ok] = np.where(ma > 0, (cur[ok] / ma - 1.0) * 100.0, np.nan)

        for rn, name in ((1, "ret1"), (3, "ret3"), (5, "ret5"), (10, "ret10")):
            ok = idx >= rn
            if ok.any():
                old = pack.cl[idx[ok] - rn]
                vv = np.where(np.isfinite(old) & (old > 0), (cur[ok] / old - 1.0) * 100.0, np.nan)
                values[name][i, :n][ok] = vv

        for h in HORIZONS:
            fut = pack.cl[idx + h]
            values[f"fwd{h}"][i, :n] = (fut / cur - 1.0) * 100.0

        # require_futureで全ageに10営業日先まで存在するので、n個の10日窓を一括作成できる。
        future_hi = pack.hi[d1_idx + 1:d1_idx + n + MAX_FORWARD].astype(float, copy=False)
        future_lo = pack.lo[d1_idx + 1:d1_idx + n + MAX_FORWARD].astype(float, copy=False)
        future_cl = pack.cl[d1_idx + 1:d1_idx + n + MAX_FORWARD].astype(float, copy=False)
        future_hi = np.where(np.isfinite(future_hi) & (future_hi > 0), future_hi, future_cl)
        future_lo = np.where(np.isfinite(future_lo) & (future_lo > 0), future_lo, future_cl)
        hw = np.lib.stride_tricks.sliding_window_view(future_hi, MAX_FORWARD)[:n]
        lw = np.lib.stride_tricks.sliding_window_view(future_lo, MAX_FORWARD)[:n]
        maxh = np.max(hw, axis=1)
        minl = np.min(lw, axis=1)
        values["mfe10"][i, :n] = (maxh / cur - 1.0) * 100.0
        values["mae10"][i, :n] = (minl / cur - 1.0) * 100.0
        values["high_day10"][i, :n] = np.argmax(hw, axis=1).astype(float) + 1.0

        if (i + 1) % 5000 == 0:
            log(f"      [trajectory] {i+1:,}/{n_ev:,}")

    return MultiAgeBankMatrix(
        meta=meta, valid=valid, values=values,
        base_price=base_price, pre5_ret=pre5, pre20_ret=pre20,
        pre20_peak_dd=pre20dd, log_base_price=log_base,
        d1_value=d1v, d3_value=d3v, d5_value=d5v, max_age=max_age,
    )


# -----------------------------------------------------------------------------
# current feature augmentation
# -----------------------------------------------------------------------------
def _current_pre_features(row: pd.Series, groups: dict[str, PricePack]) -> dict[str, float]:
    code = canonical_code(row.get("コード"))
    pack = groups.get(code)
    if pack is None:
        return {"pre5_ret": float("nan"), "pre20_ret": float("nan"), "pre20_peak_dd": float("nan")}
    dt = pd.to_datetime(row.get("発表日時"), errors="coerce")
    if pd.isna(dt):
        return {"pre5_ret": float("nan"), "pre20_ret": float("nan"), "pre20_peak_dd": float("nan")}
    day = np.datetime64(pd.Timestamp(dt).normalize().to_datetime64())
    exact = _idx_exact(pack.dates, day)
    precision = str(row.get("発表時刻精度", "") or "")
    cutoff = dt_time(15, 30)
    if precision == "時刻" and pd.Timestamp(dt).time() < cutoff and exact is not None:
        d1_idx = exact
    else:
        d1_idx = int(np.searchsorted(pack.dates, day, side="right"))
    base_idx = d1_idx - 1
    if base_idx < 0 or base_idx >= len(pack.cl):
        return {"pre5_ret": float("nan"), "pre20_ret": float("nan"), "pre20_peak_dd": float("nan")}
    return _pre_features(pack, base_idx)


def current_features(
    row: pd.Series,
    groups: dict[str, PricePack],
    ach_map: Optional[dict[str, list[tuple]]] = None,
    forecast_history_map: Optional[dict[str, list[tuple]]] = None,
    xbrl_exact: Optional[dict[tuple[str, pd.Timestamp], tuple]] = None,
    xbrl_by_day: Optional[dict[tuple[str, pd.Timestamp], list[tuple]]] = None,
    xbrl_history: Optional[dict[tuple[str, str], list[tuple]]] = None,
) -> dict[str, object]:
    out: dict[str, object] = {}
    for feat, col in CURRENT_COLUMN_MAP.items():
        v = num(row.get(col))
        out[feat] = float(v) if v is not None else float("nan")
    out.update(_current_pre_features(row, groups))
    bp = num(row.get("決算前終値"))
    out["log_base_price"] = math.log(max(bp, 1.0)) if bp is not None and bp > 0 else float("nan")
    code = canonical_code(row.get("コード"))
    cutoff = row.get("発表日時") if pd.notna(row.get("発表日時")) else datetime.now(ZoneInfo("Asia/Tokyo"))
    if ach_map is not None:
        out.update(achievement_features(ach_map.get(code), cutoff))
    if (
        forecast_history_map is not None
        and xbrl_exact is not None
        and xbrl_by_day is not None
        and xbrl_history is not None
    ):
        out.update(
            forecast_gap_features(
                code,
                cutoff,
                str(row.get("決算タイトル", row.get("タイトル", "")) or ""),
                forecast_history_map,
                xbrl_exact,
                xbrl_by_day,
                xbrl_history,
            )
        )
    return out


# -----------------------------------------------------------------------------
# similarity / expectation stats
# -----------------------------------------------------------------------------
def robust_scale(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return 1.0
    q25, q75 = np.quantile(x, [0.25, 0.75])
    iqr_scale = (q75 - q25) / 1.349
    med = np.median(x)
    mad = np.median(np.abs(x - med)) * 1.4826
    s = max(float(iqr_scale), float(mad), 0.25)
    return s




@dataclass
class PreparedBank:
    df: pd.DataFrame
    feat_arrays: dict[str, np.ndarray]
    scales: dict[str, float]
    kind_indices: dict[str, np.ndarray]
    kind_scales: dict[str, dict[str, float]]
    baseline_all: dict[str, float]
    kind_baselines: dict[str, dict[str, float]]


def _baseline_stats(pool: pd.DataFrame) -> dict[str, float]:
    if pool is None or pool.empty:
        return {"m5": np.nan, "m10": np.nan, "p5": np.nan, "hit5": np.nan, "ratio": np.nan}
    ones = np.ones(len(pool), dtype=float)
    def arr(col):
        return pd.to_numeric(pool[col], errors="coerce").to_numpy(dtype=float) if col in pool.columns else np.full(len(pool), np.nan)
    f5, f10, hit, mfe, mae = arr("fwd5"), arr("fwd10"), arr("hit5_10"), arr("mfe10"), arr("mae10")
    mfe_mean, mae_mean = winsor_weighted_mean(mfe, ones), winsor_weighted_mean(mae, ones)
    ratio = mfe_mean / max(abs(mae_mean), 0.25) if np.isfinite(mfe_mean) and np.isfinite(mae_mean) else np.nan
    return {
        "m5": winsor_weighted_mean(f5, ones),
        "m10": winsor_weighted_mean(f10, ones),
        "p5": weighted_rate(f5 > 0, np.where(np.isfinite(f5), ones, 0.0)),
        "hit5": weighted_rate(hit >= 0.5, np.where(np.isfinite(hit), ones, 0.0)),
        "ratio": ratio,
    }


def prepare_bank(bank: pd.DataFrame) -> PreparedBank:
    bank = bank.reset_index(drop=True)
    feat_arrays, scales = {}, {}
    for feat in ALL_FEATURE_WEIGHTS:
        arr = pd.to_numeric(bank[feat], errors="coerce").to_numpy(dtype=float) if feat in bank.columns else np.full(len(bank), np.nan)
        feat_arrays[feat] = arr
        scales[feat] = robust_scale(arr)
    kind_indices, kind_scales, kind_baselines = {}, {}, {}
    if "kind" in bank.columns:
        kinds = bank["kind"].astype(str).to_numpy()
        for k in pd.unique(kinds):
            kk = str(k)
            idx = np.flatnonzero(kinds == k)
            kind_indices[kk] = idx
            kind_scales[kk] = {feat: robust_scale(feat_arrays[feat][idx]) for feat in ALL_FEATURE_WEIGHTS}
            kind_baselines[kk] = _baseline_stats(bank.iloc[idx])
    return PreparedBank(bank, feat_arrays, scales, kind_indices, kind_scales, _baseline_stats(bank), kind_baselines)


def similarity_distances_prepared(
    prep: PreparedBank,
    feats: dict[str, float],
    indices: np.ndarray,
    scales: Optional[dict[str, float]] = None,
    feature_weights: Optional[dict[str, float]] = None,
) -> np.ndarray:
    n = len(indices)
    acc = np.zeros(n, dtype=float)
    total_w = 0.0
    weights = feature_weights or PRICE_FEATURE_WEIGHTS
    for feat, fw in weights.items():
        cv = feats.get(feat, float("nan"))
        if not np.isfinite(cv):
            continue
        arr = prep.feat_arrays[feat][indices]
        sc = (scales or prep.scales)[feat]
        valid = np.isfinite(arr)
        z = np.full(n, 2.25, dtype=float)
        z[valid] = np.abs(arr[valid] - cv) / sc
        z = np.minimum(z, 6.0)
        acc += fw * z * z
        total_w += fw
    if total_w <= 0:
        return np.full(n, np.inf)
    return np.sqrt(acc / total_w)


def _finish_excess_score(out: dict, baseline: dict[str, float], confidence: float, model_name: str, mfe_mean: float, mae_mean: float) -> dict:
    m5, m10 = out.get("5日後平均リターン", np.nan), out.get("10日後平均リターン", np.nan)
    p5, hit5 = out.get("5日後上昇確率", np.nan), out.get("10日以内+5%到達率", np.nan)
    b5, b10, bp5, bh5, bratio = baseline.get("m5",np.nan), baseline.get("m10",np.nan), baseline.get("p5",np.nan), baseline.get("hit5",np.nan), baseline.get("ratio",np.nan)
    e5 = m5-b5 if np.isfinite(m5) and np.isfinite(b5) else np.nan
    e10 = m10-b10 if np.isfinite(m10) and np.isfinite(b10) else np.nan
    ep5 = p5-bp5 if np.isfinite(p5) and np.isfinite(bp5) else np.nan
    eh5 = hit5-bh5 if np.isfinite(hit5) and np.isfinite(bh5) else np.nan
    ratio = mfe_mean/max(abs(mae_mean),0.25) if np.isfinite(mfe_mean) and np.isfinite(mae_mean) else np.nan
    eratio = ratio-bratio if np.isfinite(ratio) and np.isfinite(bratio) else np.nan
    out.update({
        "期待値モデル": model_name,
        "5日基準リターン": round(b5,2) if np.isfinite(b5) else np.nan,
        "5日超過期待値": round(e5,2) if np.isfinite(e5) else np.nan,
        "10日基準リターン": round(b10,2) if np.isfinite(b10) else np.nan,
        "10日超過期待値": round(e10,2) if np.isfinite(e10) else np.nan,
        "5日基準上昇率": round(bp5,1) if np.isfinite(bp5) else np.nan,
        "5日上昇確率超過": round(ep5,1) if np.isfinite(ep5) else np.nan,
        "10日+5%基準到達率": round(bh5,1) if np.isfinite(bh5) else np.nan,
        "10日+5%到達率超過": round(eh5,1) if np.isfinite(eh5) else np.nan,
    })
    raw=50.0
    if np.isfinite(e5): raw += clamp(e5*6.0,-24.0,24.0)
    if np.isfinite(e10): raw += clamp(e10*3.0,-14.0,14.0)
    if np.isfinite(ep5): raw += clamp(ep5*0.45,-12.0,12.0)
    if np.isfinite(eh5): raw += clamp(eh5*0.25,-8.0,8.0)
    if np.isfinite(eratio): raw += clamp(eratio*4.0,-6.0,8.0)
    raw=clamp(raw)
    score=clamp(50.0+(raw-50.0)*(confidence/100.0))
    out["期待値生スコア"]=round(raw,1)
    out["期待値スコア"]=round(score,1)
    out["優位性ランク"]=_rank_from_score(score,confidence)
    return out


def compute_expectation_prepared(
    prep: PreparedBank,
    feats: dict[str, float],
    kind: str,
    min_neighbors: int = 35,
    max_neighbors: int = 120,
    feature_weights: Optional[dict[str, float]] = None,
    model_name: str = "PRICE_EXCESS",
) -> dict:
    bank = prep.df
    if bank.empty:
        return {"期待値スコア": np.nan, "優位性ランク": "--", "類似ケース数": 0, "期待値信頼度": 0.0}
    kind = str(kind or "")
    same_idx = prep.kind_indices.get(kind, np.array([], dtype=int)) if kind else np.array([], dtype=int)
    if kind and len(same_idx) >= max(min_neighbors, 50):
        indices = same_idx
        condition = f"同決算種別:{kind}"
        distance_scales = prep.kind_scales.get(kind, prep.scales)
        baseline = prep.kind_baselines.get(kind, prep.baseline_all)
    else:
        indices = np.arange(len(bank), dtype=int)
        condition = "決算種別緩和"
        distance_scales = prep.scales
        baseline = prep.baseline_all
    d = similarity_distances_prepared(prep, feats, indices, distance_scales, feature_weights=feature_weights)
    finite = np.isfinite(d)
    indices, d = indices[finite], d[finite]
    if len(indices) == 0:
        return {"期待値スコア": np.nan, "優位性ランク": "--", "類似ケース数": 0, "期待値信頼度": 0.0}
    k = min(int(max_neighbors), len(indices))
    order = np.argpartition(d, k-1)[:k] if k < len(d) else np.arange(len(d))
    order = order[np.argsort(d[order], kind="mergesort")]
    sel = indices[order]
    nd = d[order]
    neigh = bank.iloc[sel].copy()
    tau = max(float(np.nanmedian(nd)), 0.75)
    w = np.exp(-0.5 * np.square(nd / tau))
    w = np.maximum(w, 1e-6)
    n_eff = float((w.sum() ** 2) / np.sum(w ** 2)) if len(w) else 0.0
    med_dist = float(np.nanmedian(nd)) if len(nd) else float("nan")
    dist_quality = math.exp(-max(med_dist - 0.5, 0.0) / 2.5) if np.isfinite(med_dist) else 0.0
    sample_quality = 1.0 - math.exp(-n_eff / 35.0)
    confidence = clamp(100.0 * sample_quality * dist_quality)
    if len(indices) < min_neighbors:
        confidence *= len(indices) / max(min_neighbors, 1)
    confidence = clamp(confidence)
    out = {
        "類似条件": condition, "類似ケース数": int(len(neigh)), "有効類似ケース数": round(n_eff,1),
        "類似距離中央値": round(med_dist,3) if np.isfinite(med_dist) else np.nan,
        "期待値信頼度": round(confidence,1), "信頼度ランク": "高" if confidence>=70 else ("中" if confidence>=45 else "低"),
    }
    for h in HORIZONS:
        arr = pd.to_numeric(neigh[f"fwd{h}"], errors="coerce").to_numpy(dtype=float)
        ww = np.where(np.isfinite(arr), w, 0.0)
        out[f"{h}日後上昇確率"] = round(weighted_rate(arr > 0, ww), 1)
        out[f"{h}日後平均リターン"] = round(winsor_weighted_mean(arr, w), 2)
        out[f"{h}日後中央値リターン"] = round(weighted_median(arr, w), 2)
        pos_mask = np.isfinite(arr) & (arr > 0)
        neg_mask = np.isfinite(arr) & (arr < 0)
        out[f"{h}日後平均利益"] = round(winsor_weighted_mean(arr[pos_mask], w[pos_mask]), 2) if np.any(pos_mask) else np.nan
        out[f"{h}日後平均損失"] = round(winsor_weighted_mean(arr[neg_mask], w[neg_mask]), 2) if np.any(neg_mask) else np.nan
    mfe = pd.to_numeric(neigh["mfe10"], errors="coerce").to_numpy(dtype=float)
    mae = pd.to_numeric(neigh["mae10"], errors="coerce").to_numpy(dtype=float)
    highday = pd.to_numeric(neigh["high_day10"], errors="coerce").to_numpy(dtype=float)
    mfe_mean, mae_mean = winsor_weighted_mean(mfe,w), winsor_weighted_mean(mae,w)
    mfe_med, mae_med = weighted_median(mfe,w), weighted_median(mae,w)
    out.update({
        "10日MFE平均": round(mfe_mean,2), "10日MFE中央値": round(mfe_med,2),
        "10日MAE平均": round(mae_mean,2), "10日MAE中央値": round(mae_med,2),
        "MFE_MAE比": round(mfe_mean/max(abs(mae_mean),0.25),2) if np.isfinite(mfe_mean) and np.isfinite(mae_mean) else np.nan,
    })
    for t in TARGETS:
        hit = pd.to_numeric(neigh[f"hit{t}_10"], errors="coerce").to_numpy(dtype=float)
        out[f"10日以内+{t}%到達率"] = round(weighted_rate(hit>=0.5, np.where(np.isfinite(hit),w,0.0)),1)
    out["高値到達日平均"] = round(weighted_mean(highday,w),2)
    out["高値到達日中央値"] = round(weighted_median(highday,w),1)
    wh = np.where(np.isfinite(highday),w,0.0)
    for dd in (1,2,3): out[f"高値{dd}日目割合"] = round(weighted_rate(highday==dd,wh),1)
    out["高値5日以内割合"] = round(weighted_rate(highday<=5,wh),1)
    out["高値10日以内割合"] = round(weighted_rate(highday<=10,wh),1)
    return _finish_excess_score(out, baseline, confidence, model_name, mfe_mean, mae_mean)


def similarity_distances(bank: pd.DataFrame, feats: dict[str, float], feature_weights: Optional[dict[str, float]] = None) -> np.ndarray:
    n = len(bank)
    acc = np.zeros(n, dtype=float)
    wsum = np.zeros(n, dtype=float)
    weights = feature_weights or PRICE_FEATURE_WEIGHTS
    for feat, fw in weights.items():
        cv = feats.get(feat, float("nan"))
        if not np.isfinite(cv) or feat not in bank.columns:
            continue
        arr = pd.to_numeric(bank[feat], errors="coerce").to_numpy(dtype=float)
        sc = robust_scale(arr)
        valid = np.isfinite(arr)
        z = np.full(n, 2.25, dtype=float)  # missing historical feature penalty
        z[valid] = np.abs(arr[valid] - cv) / sc
        z = np.minimum(z, 6.0)
        acc += fw * z * z
        wsum += fw
    if not np.any(wsum > 0):
        return np.full(n, np.inf)
    return np.sqrt(acc / np.maximum(wsum, 1e-9))


def _rank_from_score(score: float, confidence: float) -> str:
    if not np.isfinite(score):
        return "--"
    # S/A require a minimum confidence; low-sample apparent edge is not promoted.
    if score >= 75 and confidence >= 60:
        return "S"
    if score >= 65 and confidence >= 45:
        return "A"
    if score >= 55:
        return "B"
    if score >= 45:
        return "C"
    return "D"


def compute_expectation(
    bank: pd.DataFrame,
    feats: dict[str, float],
    kind: str,
    min_neighbors: int = 35,
    max_neighbors: int = 120,
    feature_weights: Optional[dict[str, float]] = None,
    model_name: str = "PRICE_EXCESS",
) -> dict:
    if bank is None or bank.empty:
        return {"期待値スコア": np.nan, "優位性ランク": "--", "類似ケース数": 0, "期待値信頼度": 0.0}

    kind = str(kind or "")
    same_kind = bank[bank["kind"].astype(str).eq(kind)] if kind else pd.DataFrame()
    if kind and len(same_kind) >= max(min_neighbors, 50):
        pool = same_kind.copy()
        condition = f"同決算種別:{kind}"
    else:
        pool = bank.copy()
        condition = "決算種別緩和"

    baseline = _baseline_stats(pool)
    d = similarity_distances(pool, feats, feature_weights=feature_weights)
    finite = np.isfinite(d)
    pool = pool.loc[finite].copy()
    d = d[finite]
    if len(pool) == 0:
        return {"期待値スコア": np.nan, "優位性ランク": "--", "類似ケース数": 0, "期待値信頼度": 0.0}

    k = min(int(max_neighbors), len(pool))
    order = np.argsort(d, kind="mergesort")[:k]
    neigh = pool.iloc[order].copy()
    nd = d[order]
    tau = max(float(np.nanmedian(nd)), 0.75)
    w = np.exp(-0.5 * np.square(nd / tau))
    w = np.maximum(w, 1e-6)
    n_eff = float((w.sum() ** 2) / np.sum(w ** 2)) if len(w) else 0.0
    med_dist = float(np.nanmedian(nd)) if len(nd) else float("nan")
    dist_quality = math.exp(-max(med_dist - 0.5, 0.0) / 2.5) if np.isfinite(med_dist) else 0.0
    sample_quality = 1.0 - math.exp(-n_eff / 35.0)
    confidence = clamp(100.0 * sample_quality * dist_quality)
    if len(pool) < min_neighbors:
        confidence *= len(pool) / max(min_neighbors, 1)
    confidence = clamp(confidence)

    out = {
        "類似条件": condition,
        "類似ケース数": int(len(neigh)),
        "有効類似ケース数": round(n_eff, 1),
        "類似距離中央値": round(med_dist, 3) if np.isfinite(med_dist) else np.nan,
        "期待値信頼度": round(confidence, 1),
        "信頼度ランク": "高" if confidence >= 70 else ("中" if confidence >= 45 else "低"),
    }

    for h in HORIZONS:
        arr = pd.to_numeric(neigh[f"fwd{h}"], errors="coerce").to_numpy(dtype=float)
        out[f"{h}日後上昇確率"] = round(weighted_rate(arr > 0, np.where(np.isfinite(arr), w, 0.0)), 1)
        out[f"{h}日後平均リターン"] = round(winsor_weighted_mean(arr, w), 2)
        out[f"{h}日後中央値リターン"] = round(weighted_median(arr, w), 2)
        pos_mask = np.isfinite(arr) & (arr > 0)
        neg_mask = np.isfinite(arr) & (arr < 0)
        out[f"{h}日後平均利益"] = round(winsor_weighted_mean(arr[pos_mask], w[pos_mask]), 2) if np.any(pos_mask) else np.nan
        out[f"{h}日後平均損失"] = round(winsor_weighted_mean(arr[neg_mask], w[neg_mask]), 2) if np.any(neg_mask) else np.nan

    mfe = pd.to_numeric(neigh["mfe10"], errors="coerce").to_numpy(dtype=float)
    mae = pd.to_numeric(neigh["mae10"], errors="coerce").to_numpy(dtype=float)
    highday = pd.to_numeric(neigh["high_day10"], errors="coerce").to_numpy(dtype=float)
    mfe_mean = winsor_weighted_mean(mfe, w)
    mae_mean = winsor_weighted_mean(mae, w)
    mfe_med = weighted_median(mfe, w)
    mae_med = weighted_median(mae, w)
    out["10日MFE平均"] = round(mfe_mean, 2)
    out["10日MFE中央値"] = round(mfe_med, 2)
    out["10日MAE平均"] = round(mae_mean, 2)
    out["10日MAE中央値"] = round(mae_med, 2)
    out["MFE_MAE比"] = round(mfe_mean / max(abs(mae_mean), 0.25), 2) if np.isfinite(mfe_mean) and np.isfinite(mae_mean) else np.nan

    for t in TARGETS:
        hit = pd.to_numeric(neigh[f"hit{t}_10"], errors="coerce").to_numpy(dtype=float)
        out[f"10日以内+{t}%到達率"] = round(weighted_rate(hit >= 0.5, np.where(np.isfinite(hit), w, 0.0)), 1)

    out["高値到達日平均"] = round(weighted_mean(highday, w), 2)
    out["高値到達日中央値"] = round(weighted_median(highday, w), 1)
    for dd in (1, 2, 3):
        out[f"高値{dd}日目割合"] = round(weighted_rate(highday == dd, np.where(np.isfinite(highday), w, 0.0)), 1)
    out["高値5日以内割合"] = round(weighted_rate(highday <= 5, np.where(np.isfinite(highday), w, 0.0)), 1)
    out["高値10日以内割合"] = round(weighted_rate(highday <= 10, np.where(np.isfinite(highday), w, 0.0)), 1)

    return _finish_excess_score(out, baseline, confidence, model_name, mfe_mean, mae_mean)


# -----------------------------------------------------------------------------
# validation (chronological / no lookahead)
# -----------------------------------------------------------------------------
def validation_band(score: float) -> str:
    if not np.isfinite(score): return "--"
    if score >= 75: return "S候補(75+)"
    if score >= 65: return "A候補(65-75)"
    if score >= 55: return "B(55-65)"
    if score >= 45: return "C(45-55)"
    return "D(<45)"


def _validation_metrics(df: pd.DataFrame) -> dict[str, float]:
    cols=["5日後平均リターン","5日超過期待値","5日後上昇確率","期待値スコア","実現5日リターン"]
    valid=df[cols].apply(pd.to_numeric,errors="coerce").dropna(subset=["5日後平均リターン","実現5日リターン"])
    if len(valid)<3: return {}
    pred,actual=valid["5日後平均リターン"],valid["実現5日リターン"]
    pred_ex=valid["5日超過期待値"]
    base_ret=pred-pred_ex
    actual_ex=actual-base_ret
    out={
        "n":float(len(valid)),
        "corr_raw":float(pred.corr(actual)),
        "rank_corr_raw":float(pred.rank().corr(actual.rank())),
        "corr_excess":float(pred_ex.corr(actual_ex)) if pred_ex.notna().sum()>=3 else np.nan,
        "rank_corr_excess":float(pred_ex.rank().corr(actual_ex.rank())) if pred_ex.notna().sum()>=3 else np.nan,
        "mae":float((pred-actual).abs().mean()),
    }
    pm=valid["5日後上昇確率"].notna()
    if pm.any():
        pp=valid.loc[pm,"5日後上昇確率"].clip(0,100)/100.0
        yy=(valid.loc[pm,"実現5日リターン"]>0).astype(float)
        out["brier"]=float(np.mean((pp-yy)**2))
        prevalence=float(yy.mean())
        out["naive_brier"]=prevalence*(1-prevalence)
        out["brier_gain"]=out["naive_brier"]-out["brier"]
        out["positive_rate"]=prevalence*100.0
    sv=valid.dropna(subset=["期待値スコア"]).sort_values("期待値スコア")
    if len(sv)>=20:
        q=max(1,int(len(sv)*0.20))
        bottom=float(sv.head(q)["実現5日リターン"].mean())
        top=float(sv.tail(q)["実現5日リターン"].mean())
        out.update({"bottom20":bottom,"top20":top,"top_minus_bottom":top-bottom})
    return out


def run_validation(aligned, groups, output_dir, min_neighbors, max_neighbors, limit=600, bank_cache=None) -> pd.DataFrame:
    rows=[]
    ages=(1,3,5,10)
    variants=[
        ("PRICE_EXCESS", PRICE_FEATURE_WEIGHTS),
        ("PRICE_PLUS_ACH", PRICE_ACH_FEATURE_WEIGHTS),
        ("PRICE_PLUS_ACH_GAP", PRICE_ACH_GAP_FEATURE_WEIGHTS),
    ]
    per_age_limit=max(1,int(math.ceil(max(int(limit),1)/len(ages))))
    for age in ages:
        if bank_cache is not None and age in bank_cache:
            log(f"[validate] age={age} ケースバンク再利用")
            bank=bank_cache[age].df.copy()
        else:
            bank=build_bank_for_age(aligned,groups,age)
        if len(bank)<120: continue
        bank=bank.sort_values("event_date").reset_index(drop=True)
        test=bank.iloc[int(len(bank)*0.80):].copy()
        if len(test)>per_age_limit: test=test.tail(per_age_limit)
        for j,(_,tr) in enumerate(test.iterrows(),1):
            train=bank[(bank["event_date"]<tr["event_date"]) & (bank["event_id"]!=tr["event_id"])]
            if len(train)<min_neighbors: continue
            feats={f:(float(num(tr.get(f))) if num(tr.get(f)) is not None else float("nan")) for f in ALL_FEATURE_WEIGHTS}
            for model_name,weights in variants:
                pred=compute_expectation(train,feats,str(tr.get("kind","")),min_neighbors,max_neighbors,feature_weights=weights,model_name=model_name)
                rows.append({
                    "model": model_name, "age": age,
                    "event_id": tr["event_id"], "event_date": tr["event_date"],
                    "code": tr["code"], "kind": tr["kind"],
                    "ach_n": tr.get("ach_n"),
                    "ach_hit_rate": tr.get("ach_hit_rate"),
                    "ach_avg_pct": tr.get("ach_avg_pct"),
                    "forecast_op_gap_pct": tr.get("forecast_op_gap_pct"),
                    "forecast_eps_gap_pct": tr.get("forecast_eps_gap_pct"),
                    "forecast_gap_source": tr.get("forecast_gap_source"),
                    **pred,
                    "実現5日リターン": tr.get("fwd5"),
                    "実現10日リターン": tr.get("fwd10"),
                    "実現10日MFE": tr.get("mfe10"),
                    "実現10日MAE": tr.get("mae10"),
                    "実現+5到達": tr.get("hit5_10"),
                })
            if j%100==0: log(f"[validate] age={age}: {j}/{len(test)}")
    df=pd.DataFrame(rows)
    if df.empty: return df
    df["スコア帯"]=pd.to_numeric(df["期待値スコア"],errors="coerce").map(validation_band)
    out=output_dir/DEFAULT_VALIDATION_NAME
    df.to_csv(out,index=False,encoding="utf-8-sig")
    summary=output_dir/DEFAULT_VALIDATION_SUMMARY
    with summary.open("w",encoding="utf-8") as f:
        f.write("決算後期待値エンジン v5 時系列検証\n")
        f.write("重要: 過去だけを使用。PRICE_EXCESS / PRICE_PLUS_ACH / PRICE_PLUS_ACH_GAPを同一テストで比較。\n")
        base_cases = df[df["model"] == "PRICE_EXCESS"].copy()
        if not base_cases.empty:
            ach_avail = pd.to_numeric(base_cases.get("ach_n"), errors="coerce").fillna(0).gt(0)
            gap_op_avail = pd.to_numeric(base_cases.get("forecast_op_gap_pct"), errors="coerce").notna()
            gap_eps_avail = pd.to_numeric(base_cases.get("forecast_eps_gap_pct"), errors="coerce").notna()
            f.write(f"unique_test_cases={len(base_cases)}\n")
            f.write(f"achievement_available={int(ach_avail.sum())} ({ach_avail.mean()*100:.1f}%)\n")
            f.write(f"forecast_op_gap_available={int(gap_op_avail.sum())} ({gap_op_avail.mean()*100:.1f}%)\n")
            f.write(f"forecast_eps_gap_available={int(gap_eps_avail.sum())} ({gap_eps_avail.mean()*100:.1f}%)\n")
            if "forecast_gap_source" in base_cases.columns:
                src_counts = base_cases.loc[gap_op_avail | gap_eps_avail, "forecast_gap_source"].fillna("").astype(str).value_counts()
                for src_name, src_n in src_counts.items():
                    if src_name:
                        f.write(f"forecast_gap_source[{src_name}]={int(src_n)}\n")

        mets={}
        for model_name,gmodel in df.groupby("model",sort=False):
            met=_validation_metrics(gmodel); mets[model_name]=met
            f.write(f"\n=== {model_name} ===\n")
            for k,v in met.items():
                if np.isfinite(v): f.write(f"{k}={v:.4f}\n")
            for age,ga in gmodel.groupby("age",sort=True):
                ma=_validation_metrics(ga)
                f.write(f"age={age}: n={len(ga)}, corr_excess={ma.get('corr_excess',np.nan):.4f}, top_minus_bottom={ma.get('top_minus_bottom',np.nan):.4f}, brier_gain={ma.get('brier_gain',np.nan):.4f}\n")
            f.write("score bands:\n")
            for band,gb in gmodel.groupby("スコア帯",sort=False):
                act=pd.to_numeric(gb["実現5日リターン"],errors="coerce")
                f.write(f"  {band}: n={len(gb)}, actual5_mean={act.mean():.3f}, actual5_pos={(act>0).mean()*100:.2f}\n")
        def _compare_models(base_name: str, new_name: str, label: str):
            if base_name not in mets or new_name not in mets:
                return
            b, a = mets[base_name], mets[new_name]
            dc = a.get("corr_excess", np.nan) - b.get("corr_excess", np.nan)
            dt = a.get("top_minus_bottom", np.nan) - b.get("top_minus_bottom", np.nan)
            db = a.get("brier_gain", np.nan) - b.get("brier_gain", np.nan)

            raw_vote = int(dc > 0) + int(dt > 0) + int(db > 0)
            meaningful_vote = (
                int(dc >= 0.02)
                + int(dt >= 0.50)
                + int(db >= 0.002)
            )

            age_improved = 0
            age_tested = 0
            gb = df[df["model"] == base_name]
            ga = df[df["model"] == new_name]
            for age in sorted(set(gb["age"]).intersection(set(ga["age"]))):
                mb = _validation_metrics(gb[gb["age"] == age])
                ma = _validation_metrics(ga[ga["age"] == age])
                if not mb or not ma:
                    continue
                age_tested += 1
                d_top = ma.get("top_minus_bottom", np.nan) - mb.get("top_minus_bottom", np.nan)
                d_corr = ma.get("corr_excess", np.nan) - mb.get("corr_excess", np.nan)
                # Age-level "improved" needs both ranking spread and correlation to avoid one-metric luck.
                if np.isfinite(d_top) and np.isfinite(d_corr) and d_top > 0 and d_corr > 0:
                    age_improved += 1

            gap_coverage = None
            if new_name == "PRICE_PLUS_ACH_GAP" and not base_cases.empty:
                gap_coverage = float(
                    pd.to_numeric(base_cases.get("forecast_op_gap_pct"), errors="coerce").notna().mean()
                )

            if gap_coverage is not None and gap_coverage < 0.15:
                rec = "カバレッジ不足で採用保留"
            elif meaningful_vote >= 2 and age_improved >= max(2, math.ceil(age_tested / 2)):
                rec = "採用候補"
            elif raw_vote >= 2:
                rec = "微改善だが採用保留"
            else:
                rec = "現時点では不採用"

            f.write(f"\n=== {label} ===\n")
            f.write(f"delta_corr_excess={dc:.4f}\n")
            f.write(f"delta_top_minus_bottom={dt:.4f}\n")
            f.write(f"delta_brier_gain={db:.4f}\n")
            f.write(f"raw_vote={raw_vote}/3\n")
            f.write(f"meaningful_vote={meaningful_vote}/3\n")
            f.write(f"age_joint_improved={age_improved}/{age_tested}\n")
            f.write(f"recommendation={rec}\n")

        _compare_models("PRICE_EXCESS", "PRICE_PLUS_ACH", "ACH_INCREMENT_vs_PRICE")
        _compare_models("PRICE_PLUS_ACH", "PRICE_PLUS_ACH_GAP", "GAP_INCREMENT_vs_ACH")
    log(f"[validate] CSV: {out}")
    log(f"[validate] summary: {summary}")
    return df


# -----------------------------------------------------------------------------
# main run
# -----------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="決算後セットアップの統計期待値を過去類似ケースから算出")
    ap.add_argument("--db", default=None, help="kani2.db")
    ap.add_argument("--input", default=None, help="決算後上昇_全母集団.csv")
    ap.add_argument("--output-dir", default=None)
    ap.add_argument("--history-years", type=float, default=4.0, help="類似ケース履歴年数")
    ap.add_argument("--min-neighbors", type=int, default=35)
    ap.add_argument("--max-neighbors", type=int, default=120)
    ap.add_argument("--allow-date-only", action="store_true", help="時刻不明のquarterly_actual_historyも教師へ含める（非推奨）")
    ap.add_argument("--validate", action="store_true", help="時系列holdout検証も実施")
    ap.add_argument("--validate-limit", type=int, default=600)
    ap.add_argument(
        "--model",
        choices=["price", "price-ach", "price-ach-gap"],
        default="price",
        help="通常出力モデル。price-ach / price-ach-gap は時系列検証で改善確認後に使用",
    )
    ap.add_argument("--top", type=int, default=100)
    args = ap.parse_args()

    t0 = time.perf_counter()
    output_dir = resolve_output_dir(args.output_dir)
    db_path = resolve_db_path(args.db)
    input_path = resolve_input_path(args.input, output_dir)
    log(f"[DB] {db_path}")
    log(f"[INPUT] {input_path}")
    log(f"[OUTPUT] {output_dir}")

    current = load_current_candidates(input_path)
    log(f"[1/7] current candidates={len(current):,}")

    # Only ages actually needed by current candidates, plus validation ages if requested.
    age_series = pd.to_numeric(current.get("決算後営業日数"), errors="coerce")
    needed_ages = sorted({int(x) for x in age_series.dropna() if 1 <= int(x) <= MAX_AGE})
    if args.validate:
        needed_ages = sorted(set(needed_ages) | {1, 3, 5, 10})

    history_start = (datetime.now(ZoneInfo("Asia/Tokyo")).date() - timedelta(days=int(args.history_years * 365.25))).isoformat()
    price_start = (pd.Timestamp(history_start) - pd.Timedelta(days=60)).date().isoformat()

    with connect_readonly(db_path) as conn:
        t = time.perf_counter()
        events = load_historical_events(conn, history_start, allow_date_only=args.allow_date_only)
        log(f"[2/7] historical events={len(events):,} / {time.perf_counter()-t:.2f}s")
        if events.empty:
            raise RuntimeError("過去決算イベントを構築できません")

        t = time.perf_counter()
        prices = load_price_history(conn, price_start)
        log(f"[3/7] price_history={len(prices):,} rows / {time.perf_counter()-t:.2f}s")
        if prices.empty:
            raise RuntimeError("price_historyを取得できません")
        ach_map = load_achievement_history(conn)
        log(f"      forecast_achievement stocks={len(ach_map):,}")
        forecast_history_map = load_forecast_history(conn)
        xbrl_exact, xbrl_by_day, xbrl_history = load_xbrl_forecast_map(conn, history_start)
        log(
            f"      forecast_history stocks={len(forecast_history_map):,} / "
            f"xbrl_forecast rows={len(xbrl_exact):,} / "
            f"xbrl_prior_series={len(xbrl_history):,}"
        )

    t = time.perf_counter()
    groups = build_price_groups(prices)
    aligned = align_events(events, groups)
    enrich_aligned_achievement(aligned, ach_map)
    enrich_aligned_forecast_gap(aligned, forecast_history_map, xbrl_exact, xbrl_by_day, xbrl_history)
    gap_n = sum(np.isfinite(ev.get("forecast_op_gap_pct", np.nan)) for ev in aligned)
    log(
        f"[4/7] aligned events={len(aligned):,} / stocks={len(groups):,} / "
        f"forecast_gap={gap_n:,} / {time.perf_counter()-t:.2f}s"
    )

    # Price bank no longer needs the original giant DataFrame.
    del prices

    bank_cache: dict[int, PreparedBank] = {}
    results = []
    age_s = pd.to_numeric(current["決算後営業日数"], errors="coerce")
    # 期待値は決算後日数と価格パスから計算できる独立軸。
    # 「解析状態」は旧スクリーナー固有の表示分類であり、ここで足切りすると
    # D1/D3/D5を持つ有効銘柄まで期待値欠損になるため、年齢だけで可否を決める。
    usable = current[age_s.ge(1) & age_s.le(MAX_AGE)].copy()
    current_ages = sorted({max(1, min(MAX_AGE, int(x))) for x in age_s.dropna() if int(x) >= 1})
    all_bank_ages = sorted(set(current_ages) | ({1,3,5,10} if args.validate else set()))
    log(f"[5/7] expectation target={len(usable):,} / current unique ages={len(current_ages)}")

    bt = time.perf_counter()
    matrix = build_multi_age_matrix(aligned, groups, all_bank_ages)
    log(f"      [trajectory] 一括構築={time.perf_counter()-bt:.2f}s / max_age={matrix.max_age}")
    for age in all_bank_ages:
        at = time.perf_counter()
        raw_bank = matrix.bank(age)
        bank_cache[age] = prepare_bank(raw_bank)
        log(f"      [bank] age={age}: n={len(raw_bank):,} / prepare={time.perf_counter()-at:.2f}s")
    del matrix

    for n, (idx, row) in enumerate(usable.iterrows(), 1):
        agev = num(row.get("決算後営業日数"))
        if agev is None:
            continue
        age = max(1, min(MAX_AGE, int(agev)))
        bank = bank_cache[age]
        feats = current_features(
            row, groups, ach_map,
            forecast_history_map, xbrl_exact, xbrl_by_day, xbrl_history,
        )
        active_weights = MODEL_FEATURE_WEIGHTS[args.model]
        active_name = {
            "price": "PRICE_EXCESS",
            "price-ach": "PRICE_PLUS_ACH",
            "price-ach-gap": "PRICE_PLUS_ACH_GAP",
        }[args.model]
        ex = compute_expectation_prepared(
            bank, feats, str(row.get("決算種別", "") or ""),
            min_neighbors=args.min_neighbors,
            max_neighbors=args.max_neighbors,
            feature_weights=active_weights,
            model_name=active_name,
        )
        ex["予想達成履歴件数"] = int(feats.get("ach_n", 0) or 0)
        ex["予想達成率"] = round(feats.get("ach_hit_rate"), 1) if np.isfinite(feats.get("ach_hit_rate", np.nan)) else np.nan
        ex["平均予想達成度"] = round(feats.get("ach_avg_pct"), 1) if np.isfinite(feats.get("ach_avg_pct", np.nan)) else np.nan
        ex["会社予想OP修正率"] = round(feats.get("forecast_op_gap_pct"), 2) if np.isfinite(feats.get("forecast_op_gap_pct", np.nan)) else np.nan
        ex["会社予想EPS修正率"] = round(feats.get("forecast_eps_gap_pct"), 2) if np.isfinite(feats.get("forecast_eps_gap_pct", np.nan)) else np.nan
        ex["予想ギャップソース"] = str(feats.get("forecast_gap_source", "") or "")
        source = str(row.get("現在値ソース", "") or "")
        anchor_type = "LIVE_PROXY" if "screener.現在値" in source else "EOD_CLOSE"
        results.append({
            "_idx": idx,
            "期待値アンカー": f"POST_DAY_{age}_CLOSE_ANALOG",
            "基準価格種別": anchor_type,
            "期待値注意": "MIDDAY現在値は過去日足終値とのproxy比較。最終ENTRY NOWではない。" if anchor_type == "LIVE_PROXY" else "",
            **ex,
        })
        if n % 100 == 0 or n == len(usable):
            log(f"      expectation {n}/{len(usable)}")

    expdf = pd.DataFrame(results)
    merged = current.copy()
    if not expdf.empty:
        expdf = expdf.set_index("_idx")
        for c in expdf.columns:
            merged.loc[expdf.index, c] = expdf[c]

    # Sort output by statistical edge; retain all current rows.
    merged["_exp_score_sort"] = pd.to_numeric(merged.get("期待値スコア"), errors="coerce").fillna(-999)
    merged["_exp_conf_sort"] = pd.to_numeric(merged.get("期待値信頼度"), errors="coerce").fillna(-999)
    merged = merged.sort_values(["_exp_score_sort", "_exp_conf_sort"], ascending=[False, False], kind="mergesort").drop(columns=["_exp_score_sort", "_exp_conf_sort"])

    out_path = output_dir / DEFAULT_OUTPUT_NAME
    top_path = output_dir / DEFAULT_TOP_NAME
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    top_cols = [c for c in [
        "コード", "銘柄名", "市場", "発表日時", "決算種別", "現在値", "決算後営業日数",
        "期待値モデル", "期待値スコア", "優位性ランク", "期待値信頼度", "信頼度ランク", "類似ケース数", "有効類似ケース数", "類似距離中央値", "類似条件",
        "5日基準リターン", "5日超過期待値", "10日基準リターン", "10日超過期待値", "5日基準上昇率", "5日上昇確率超過",
        "予想達成履歴件数", "予想達成率", "平均予想達成度",
        "会社予想OP修正率", "会社予想EPS修正率", "予想ギャップソース",
        "1日後上昇確率", "1日後平均リターン", "2日後上昇確率", "2日後平均リターン",
        "3日後上昇確率", "3日後平均リターン", "5日後上昇確率", "5日後平均リターン", "5日後中央値リターン",
        "10日後上昇確率", "10日後平均リターン", "10日後中央値リターン",
        "10日MFE平均", "10日MFE中央値", "10日MAE平均", "10日MAE中央値", "MFE_MAE比",
        "10日以内+3%到達率", "10日以内+5%到達率", "10日以内+10%到達率", "10日以内+15%到達率",
        "高値到達日平均", "高値到達日中央値", "高値1日目割合", "高値2日目割合", "高値3日目割合", "高値5日以内割合", "高値10日以内割合",
        "今からスコア", "短期低リスクスコア", "値幅×低リスクスコア", "期待値アンカー", "基準価格種別", "期待値注意",
    ] if c in merged.columns]
    merged[top_cols].head(args.top).to_csv(top_path, index=False, encoding="utf-8-sig")

    audit_path = output_dir / DEFAULT_AUDIT_NAME
    with audit_path.open("w", encoding="utf-8") as f:
        f.write("決算後期待値エンジン v4 精度監査\n")
        f.write(f"active_model={args.model}\n")
        f.write("v4は同age/決算種別の母集団基準差を使う。個人特性・板・歩み値・信用需給は未使用。\n")
        f.write("price-achは予想達成履歴、price-ach-gapはさらに会社予想ギャップを追加する実験モデル。--validateで改善確認後に採用。\n")
        f.write("MIDDAYのLIVE現在値はhistorical EOD close proxyであり、ENTRY NOWではない。\n")
        f.write(f"current_rows={len(current)}\n")
        f.write(f"expectation_target={len(usable)}\n")
        f.write(f"historical_events={len(events)}\n")
        f.write(f"aligned_events={len(aligned)}\n")
        f.write(f"history_start={history_start}\n")
        f.write(f"age_banks={','.join(map(str, sorted(bank_cache)))}\n")
        if "期待値スコア" in merged:
            s = pd.to_numeric(merged["期待値スコア"], errors="coerce")
            f.write(f"score_nonnull={int(s.notna().sum())}\n")
            rank_s = merged["優位性ランク"].astype(str) if "優位性ランク" in merged.columns else pd.Series("", index=merged.index)
            for rank in ("S", "A", "B", "C", "D"):
                f.write(f"rank_{rank}={int((rank_s==rank).sum())}\n")

    log(f"[6/7] output: {out_path}")
    log(f"      top: {top_path}")
    log(f"      audit: {audit_path}")

    if args.validate:
        log("[7/7] chronological validation")
        run_validation(
            aligned, groups, output_dir,
            min_neighbors=args.min_neighbors,
            max_neighbors=args.max_neighbors,
            limit=args.validate_limit,
            bank_cache=bank_cache,
        )
    else:
        log("[7/7] validation skipped (--validateで実行)")

    log(f"[完了] {time.perf_counter()-t0:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
