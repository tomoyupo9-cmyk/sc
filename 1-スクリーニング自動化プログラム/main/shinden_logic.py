# -*- coding: utf-8 -*-
# === v23 FAST 決算前79点正規化 + 決算直後100点維持 ===
# N+1個別SQLを廃止し、履歴/TDnet本文/価格履歴を一括取得
# 0期=信頼性0点、1期<=5点、2期<=10点、3期<=15点、4期以上<=20点
# 未織り込み: 終点騰落率だけでなく「20営業日内の最高値 / 20日前終値」のピーク先回りも減点
# forecast_history等はfetch_all.py専管。ここでは作成・変更しない。
"""
shinden_logic.py
================
シンデン型スコアの「純粋な判定ロジック」だけを持つモジュール。

データ取得はしない。
- 株探ファンダ.py        : 会社予想/実績/予想スナップショット
- fetch_all.py           : TDnet本文/根拠資料
- 自動スクリーニング.py : 株価/需給/最終統合
からDBに蓄積された情報を読み、screenerへ結果を書き戻す。

需給はスコアに含めず、注釈のみ。
"""

from __future__ import annotations
import json
import argparse
import os
from pathlib import Path
import math
import re
import sqlite3
import statistics
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

# -----------------------------
# スコア配分（100点）
# -----------------------------
MAX_GAP = 30.0
MAX_RELIABILITY = 20.0
MAX_BASIS = 15.0
MAX_VISIBILITY = 20.0
MAX_UNPRICED = 15.0

BASIS_KEYWORDS = {
    "受注残": 4.0, "受注獲得": 4.0, "案件獲得": 4.0, "大型受注": 4.0,
    "契約締結": 4.0, "新規契約": 3.5, "量産開始": 4.0, "稼働開始": 4.0,
    "新工場": 3.0, "生産能力増強": 3.0, "設備増強": 3.0,
    "新店": 3.0, "出店": 2.5, "移転": 2.0,
    "値上げ": 3.0, "価格改定": 3.0, "単価上昇": 3.0, "価格上昇": 3.0,
    "子会社化": 3.0, "連結子会社": 3.0, "M&A": 2.5,
    "市場回復": 2.5, "市況回復": 2.5, "需要回復": 2.0, "需要拡大": 2.0,
    "生産性向上": 2.0, "原材料合理化": 2.5, "コスト改善": 2.0,
    "一過性費用": 2.0, "固定費減少": 2.0, "価格転嫁": 2.5,
}

VISIBILITY_KEYWORDS = {
    "受注済": 5.0, "受注残": 4.5, "獲得しました": 4.5, "契約締結": 5.0,
    "採用されました": 5.0, "採用決定": 5.0, "量産開始": 5.0,
    "稼働開始": 5.0, "稼働しました": 5.0, "開業": 4.5, "オープン": 4.0,
    "実施済": 4.5, "価格改定を実施": 4.5, "値上げを実施": 4.5,
    "子会社化しました": 4.5, "完了しました": 4.0, "寄与": 2.5,
    "増加しました": 2.5, "回復しました": 3.0, "上昇しました": 2.5,
}

def _sf(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if isinstance(v, str):
            s = (v.strip().replace(",", "").replace("%", "").replace("％", "")
                 .replace("▲", "-").replace("△", "-"))
            if not s or s in {"-", "--", "N/A", "nan", "None"}:
                return None
            v = s
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def _growth(new: Any, old: Any) -> Optional[float]:
    n, o = _sf(new), _sf(old)
    if n is None or o is None or o <= 0:
        return None
    return (n / o - 1.0) * 100.0

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}


def ensure_shinden_schema(conn: sqlite3.Connection) -> None:
    """
    screener側の出力列だけを保証する。

    重要:
    forecast_history / forecast_achievement_history / tdnet_documents は
    fetch_all.py が正式管理するため、ここではCREATE/ALTERしない。
    shinden_logic.py は履歴データを「読むだけ」にする。
    """
    sc = _cols(conn, "screener")
    if sc:
        additions = [
            ("シンデン総合スコア", "REAL"),
            ("シンデン判定", "TEXT"),
            ("予想ギャップスコア", "REAL"),
            ("予想信頼性スコア", "REAL"),
            ("予想根拠スコア", "REAL"),
            ("予想可視性スコア", "REAL"),
            ("未織り込みスコア", "REAL"),
            ("予想達成履歴信頼度", "TEXT"),
            ("予想達成履歴期数", "INTEGER"),
            ("営業益予想ギャップ_pct", "REAL"),
            ("EPS予想ギャップ_pct", "REAL"),
            ("予想平均達成率_pct", "REAL"),
            ("予想最低達成率_pct", "REAL"),
            ("シンデン判定理由", "TEXT"),
            ("シンデン需給注釈", "TEXT"),
            ("次期転換期待スコア", "REAL"),
            ("次期転換判定", "TEXT"),
            ("反動余地スコア", "REAL"),
            ("履歴土台スコア", "REAL"),
            ("根拠可視性スコア", "REAL"),
            ("次期未織込スコア", "REAL"),
            ("回復兆候スコア", "REAL"),
            ("次期転換理由", "TEXT"),
        ]
        for col, typ in additions:
            if col not in sc:
                try:
                    conn.execute(f'ALTER TABLE screener ADD COLUMN "{col}" {typ}')
                except sqlite3.OperationalError:
                    pass
    conn.commit()

def _score_gap(forecast_op, actual_op, forecast_eps, actual_eps) -> tuple[float, Optional[float], Optional[float]]:
    op_g = _growth(forecast_op, actual_op)
    eps_g = _growth(forecast_eps, actual_eps)
    s = 0.0
    if op_g is not None:
        if op_g >= 80: s += 22
        elif op_g >= 50: s += 20
        elif op_g >= 30: s += 16
        elif op_g >= 20: s += 13
        elif op_g >= 10: s += 8
        elif op_g > 0: s += 4
    elif _sf(actual_op) is not None and _sf(actual_op) <= 0 and (_sf(forecast_op) or 0) > 0:
        # 黒転は巨大前年比として扱わず、再建型として控えめ
        s += 8
    if eps_g is not None:
        if eps_g >= 100: s += 8
        elif eps_g >= 60: s += 7
        elif eps_g >= 30: s += 5
        elif eps_g >= 15: s += 3
        elif eps_g > 0: s += 1
    return _clamp(s, 0, MAX_GAP), op_g, eps_g


def _achievement_stats(
    conn: sqlite3.Connection,
    code: str
) -> tuple[float, Optional[float], Optional[float], int, int, str, str, int]:
    """
    過去の期初会社予想→実績から「予想信頼性」を計算。

    履歴不足を中立点で救済しない。
      0期: 0/20, DATA不足
      1期: 最大 5/20, LOW
      2期: 最大10/20, MEDIUM
      3期: 最大15/20, MEDIUM
      4-5期: 最大20/20, HIGH

    return:
      score, avg_ratio, min_ratio, up_count, down_count,
      confidence, detail, sample_count
    """
    if not _table_exists(conn, "forecast_achievement_history"):
        return 0.0, None, None, 0, 0, "DATA不足", "0期 履歴テーブルなし・信頼性加点なし", 0

    cols = _cols(conn, "forecast_achievement_history")
    required = {"コード", "fiscal_key", "initial_forecast_op", "actual_op"}
    if not required.issubset(cols):
        return 0.0, None, None, 0, 0, "DATA不足", "0期 履歴列不足・信頼性加点なし", 0

    up_expr = "upward_revisions" if "upward_revisions" in cols else "0"
    down_expr = "downward_revisions" if "downward_revisions" in cols else "0"

    rows = conn.execute(f"""
        SELECT initial_forecast_op,actual_op,{up_expr},{down_expr}
        FROM forecast_achievement_history
        WHERE コード=?
        ORDER BY fiscal_key DESC
        LIMIT 5
    """, (code,)).fetchall()

    vals, up, down = [], 0, 0
    for f, a, u, d in rows:
        f, a = _sf(f), _sf(a)
        # 率比較が意味を持つ「期初営業益 > 0」の年度だけサンプルに採用。
        if f is not None and a is not None and f > 0:
            vals.append(a / f)
            up += int(_sf(u) or 0)
            down += int(_sf(d) or 0)

    n = len(vals)
    if n == 0:
        return 0.0, None, None, up, down, "DATA不足", "0期 有効な達成履歴なし・信頼性加点なし", 0

    avg = statistics.mean(vals)
    mn = min(vals)

    # まず5期揃っている場合と同じ「生の信頼性」を計算。
    raw = 0.0
    if 0.98 <= avg <= 1.20:
        raw += 10
    elif 0.90 <= avg < 0.98:
        raw += 7
    elif 1.20 < avg <= 1.50:
        raw += 8
    elif 0.80 <= avg < 0.90:
        raw += 4
    else:
        raw += 2

    if mn >= 0.95:
        raw += 7
    elif mn >= 0.90:
        raw += 6
    elif mn >= 0.80:
        raw += 3
    elif mn >= 0.70:
        raw += 1

    # 下方修正は強めに減点、上方修正は小さく加点。
    raw -= min(down * 2.5, 8)
    raw += min(up * 0.8, 2.5)
    raw = _clamp(raw, 0, MAX_RELIABILITY)

    # ★履歴数による上限。少数サンプルで20点近く取らせない。
    cap_by_n = {1: 5.0, 2: 10.0, 3: 15.0}
    sample_cap = cap_by_n.get(n, MAX_RELIABILITY)
    score = min(raw, sample_cap)

    if n == 1:
        conf = "LOW"
    elif n in (2, 3):
        conf = "MEDIUM"
    else:
        conf = "HIGH"

    detail = (
        f"{n}期 平均{avg*100:.1f}% 最低{mn*100:.1f}% "
        f"上方{up}/下方{down} 信頼性上限{sample_cap:.0f}/20"
    )
    return score, avg, mn, up, down, conf, detail, n

def _score_evidence(conn: sqlite3.Connection, code: str, op_yoy: Optional[float], progress: Optional[float]) -> tuple[float,float,list[str],list[str]]:
    text = ""
    if _table_exists(conn, "tdnet_documents"):
        rows = conn.execute("""
            SELECT 本文 FROM tdnet_documents
            WHERE コード=? AND 本文 IS NOT NULL
            ORDER BY 提出時刻 DESC LIMIT 12
        """, (code,)).fetchall()
        text = "\n".join(str(r[0] or "") for r in rows)
    basis, bh = 0.0, []
    for kw, pts in BASIS_KEYWORDS.items():
        if kw in text:
            basis += pts; bh.append(kw)
    vis, vh = 0.0, []
    for kw, pts in VISIBILITY_KEYWORDS.items():
        if kw in text:
            vis += pts; vh.append(kw)
    if op_yoy is not None:
        if op_yoy >= 50: vis += 5; vh.append(f"直近営業益YoY+{op_yoy:.1f}%")
        elif op_yoy >= 20: vis += 3; vh.append(f"直近営業益YoY+{op_yoy:.1f}%")
        elif op_yoy > 0: vis += 1
    if progress is not None and progress >= 35:
        vis += 2; vh.append(f"進捗{progress:.1f}%")
    return _clamp(basis,0,MAX_BASIS), _clamp(vis,0,MAX_VISIBILITY), bh[:10], vh[:10]

def _get_returns(conn: sqlite3.Connection, code: str) -> tuple[Optional[float],Optional[float],Optional[float],Optional[float]]:
    """
    5/20/60営業日の終点騰落率に加え、20営業日内のピーク先回り率を返す。

    peak20 の定義:
        20営業日前の終値を基準に、そこから現在までの最高値が何%上だったか。
        高値列が無いDBでは終値を代用する。

    これにより、
      20日前 ≒ 現在値 だが途中で一度大きく買われた
    という「往って来い」の事前織り込みを見落とさない。
    """
    if not _table_exists(conn, "price_history"):
        return None,None,None,None
    pc = _cols(conn, "price_history")
    code_col = "コード" if "コード" in pc else None
    date_col = "日付" if "日付" in pc else ("date" if "date" in pc else None)
    close_col = "終値" if "終値" in pc else ("close" if "close" in pc else None)
    high_col = "高値" if "高値" in pc else ("high" if "high" in pc else None)
    if not all((code_col,date_col,close_col)):
        return None,None,None,None
    high_expr = f'"{high_col}"' if high_col else f'"{close_col}"'
    df = pd.read_sql_query(
        f'SELECT "{date_col}" d,"{close_col}" c,{high_expr} h '
        f'FROM price_history WHERE CAST("{code_col}" AS TEXT)=? '
        f'ORDER BY "{date_col}" DESC LIMIT 70',
        conn, params=(code,)
    )
    if df.empty: return None,None,None,None
    df["c"] = pd.to_numeric(df["c"], errors="coerce")
    df["h"] = pd.to_numeric(df["h"], errors="coerce")
    df["h"] = df["h"].fillna(df["c"])
    df = df.dropna(subset=["c"]).reset_index(drop=True)
    if df.empty: return None,None,None,None
    cur=float(df.iloc[0]["c"])
    def ret(n):
        if len(df)<=n or float(df.iloc[n]["c"])==0: return None
        return (cur/float(df.iloc[n]["c"])-1)*100
    def peak_from_start(n):
        if len(df)<=n:
            return None
        base=float(df.iloc[n]["c"])
        if base==0:
            return None
        peak=pd.to_numeric(df.iloc[:n+1]["h"], errors="coerce").max()
        if pd.isna(peak):
            return None
        return (float(peak)/base-1)*100
    return ret(5),ret(20),ret(60),peak_from_start(20)

def _peak20_penalty(peak20: Optional[float]) -> float:
    """20営業日内に一度どこまで先回り買いされたかの減点。"""
    if peak20 is None:
        return 0.0
    if peak20 >= 30:
        return 4.0
    if peak20 >= 20:
        return 3.0
    if peak20 >= 15:
        return 2.0
    if peak20 >= 10:
        return 1.0
    return 0.0

def _score_unpriced(conn: sqlite3.Connection, code: str, day_pct: Optional[float]) -> tuple[float,str]:
    r5,r20,r60,peak20 = _get_returns(conn, code)
    s=0.0
    if day_pct is not None:
        if day_pct <= 0: s+=5
        elif day_pct <=1: s+=4
        elif day_pct <=2: s+=3
        elif day_pct <=3: s+=1.5
        elif day_pct >=5: s-=2
    if r5 is not None:
        if r5<=0:s+=4
        elif r5<=3:s+=3
        elif r5<=7:s+=1
        elif r5>=15:s-=2
    if r20 is not None:
        if r20<=2:s+=4
        elif r20<=7:s+=2.5
        elif r20<=12:s+=1
        elif r20>=25:s-=3
    if r60 is not None:
        if r60<=10:s+=2
        elif r60>=40:s-=1.5
    peak_pen = _peak20_penalty(peak20)
    s -= peak_pen
    peak_txt = f"{peak20:.1f}%" if peak20 is not None else "-"
    pen_txt = f"-{peak_pen:.1f}" if peak_pen > 0 else "0"
    return (
        _clamp(s,0,MAX_UNPRICED),
        f"前日{day_pct if day_pct is not None else '-'}% / "
        f"5日{r5 if r5 is not None else '-'}% / "
        f"20日{r20 if r20 is not None else '-'}% / "
        f"60日{r60 if r60 is not None else '-'}% / "
        f"20日内ピーク先回り{peak_txt}(減点{pen_txt})"
    )

def _demand_note(row: dict) -> str:
    """
    信用・出来高・売買代金・機関空売りはこのロジックでは使わない。
    ダッシュボード側の独立した需給列と併用する。
    """
    return "需給は別評価（シンデンロジック未使用）"



def _label(
    score: float,
    history_n: int|None=None,
    post_release: bool=False,
    upward: bool=False,
    quarter_kind: str="",
) -> str:
    """
    v23:
      決算後の格付けを4522件の実反応で校正。

    検証結果:
      - 上方修正あり: 60点以上で強く、70点以上は特に高反応。
      - 上方修正なし: 高得点でも分離力が弱い。
      - 例外的に1Qは「修正なし」でも55点以上が比較的強かった。

    数値スコアそのものは変更せず、格付けだけを校正する。
    """
    n = int(history_n or 0)

    if post_release:
        kind = str(quarter_kind or "")

        if upward:
            if score >= 70:
                return "S：上方修正・即反応最重要"
            if score >= 60:
                return "A：上方修正・強反応候補"
            if score >= 55:
                return "B：上方修正・好決算候補"
            if score >= 50:
                return "C：上方修正・監視"
            return "D：弱い/材料不足"

        # 上方修正なしは過去検証で高得点帯の分離力が弱かった。
        # A/Sを原則出さず、1QのみBを許可する。
        if kind == "1Q":
            if score >= 55:
                return "B：1Q好決算・修正なし"
            if score >= 50:
                return "C：1Q監視・修正なし"
            return "D：弱い/材料不足"

        if score >= 50:
            return "C：好決算だが修正なし"
        return "D：弱い/材料不足"

    if n == 0:
        return "参考：履歴なし（DATA不足）"
    if n == 1:
        return "参考：履歴1期のみ（LOW）"

    # 決算前は79点満点へ正規化済み。
    if score >= 63.2:      # 80 * 0.79
        base = "S：決算前・最重要"
    elif score >= 55.3:    # 70 * 0.79
        base = "A：かなり近い"
    elif score >= 47.4:    # 60 * 0.79
        base = "B：候補"
    elif score >= 39.5:    # 50 * 0.79
        base = "C：監視"
    else:
        base = "D：弱い/材料不足"
    return base


def _apply_reliability_gate(
    raw_total: float,
    reliability_score: float,
    min_achievement_ratio: Optional[float],
    history_n: int,
) -> tuple[float, str]:
    """
    シンデン型の最終信頼性ゲート。

    目的:
      予想Gap・根拠・可視性・未織り込みだけで、
      過去予想を全く達成できていない会社がA/Sになるのを防ぐ。

    上限:
      信頼性 = 0       -> C上限 59.9
      0 < 信頼性 < 10  -> B上限 69.9
      10 <= 信頼性 <15 -> A上限 79.9
      信頼性 >= 15     -> 信頼性による上限なし

    追加安全弁:
      過去最低達成率 < 0% -> 最大C（59.9）

    履歴0-1期は _label() 側の「参考」表示を維持する。
    """
    raw = _clamp(_sf(raw_total) or 0.0, 0, 100)
    rel = _clamp(_sf(reliability_score) or 0.0, 0, MAX_RELIABILITY)
    n = int(history_n or 0)

    caps = []
    reasons = []

    # 信頼性スコアによる上限
    if rel <= 0.0:
        caps.append(59.9)
        reasons.append("信頼性0→C上限")
    elif rel < 10.0:
        caps.append(69.9)
        reasons.append(f"信頼性{rel:.1f}<10→B上限")
    elif rel < 15.0:
        caps.append(79.9)
        reasons.append(f"信頼性{rel:.1f}<15→A上限")

    # 一度でも「正の期初営業益予想に対して実績が赤字」なら最大C
    mn = _sf(min_achievement_ratio)
    if mn is not None and mn < 0:
        caps.append(59.9)
        reasons.append(f"最低達成率{mn*100:.1f}%<0→C上限")

    if not caps:
        return raw, "なし"

    cap = min(caps)
    gated = min(raw, cap)

    if gated < raw:
        detail = f"{raw:.1f}→{gated:.1f} ({' / '.join(reasons)})"
    else:
        detail = f"上限{cap:.1f} ({' / '.join(reasons)})"

    # 履歴不足の参考表示自体は既存仕様に任せる
    if n <= 1:
        detail += f" / 履歴{n}期は参考判定"

    return gated, detail



def _apply_gap_gate(
    total_after_reliability: float,
    gap_score: float,
    operating_gap_pct: Optional[float],
    eps_gap_pct: Optional[float],
) -> tuple[float, str]:
    """
    シンデン型の最終予想Gapゲート。

    「予想の信頼性・根拠・可視性が高いだけの優良企業」が
    シンデン型上位へ来るのを防ぎ、未来利益ギャップを必須条件にする。

    ルール:
      営業益Gap と EPS Gap がともにマイナス -> D固定相当（49.9上限）
      Gapスコア < 5                         -> C上限（59.9）
      5 <= Gapスコア < 10                  -> B上限（69.9）
      10 <= Gapスコア < 15                 -> A上限（79.9）
      Gapスコア >= 15                      -> Gapによる上限なし

    ※営業益/EPSの片方が欠損の場合は「両方マイナス」判定を行わず、
      Gapスコアによる上限だけを適用する。
    """
    total = _clamp(_sf(total_after_reliability) or 0.0, 0, 100)
    gap = _clamp(_sf(gap_score) or 0.0, 0, MAX_GAP)
    opg = _sf(operating_gap_pct)
    epsg = _sf(eps_gap_pct)

    caps = []
    reasons = []

    # 明確な減益予想: シンデン型から除外
    if opg is not None and epsg is not None and opg < 0 and epsg < 0:
        caps.append(49.9)
        reasons.append(f"営業{opg:.1f}%・EPS{epsg:.1f}%ともにマイナス→D上限")

    # Gapスコア自体による格付け上限
    if gap < 5.0:
        caps.append(59.9)
        reasons.append(f"Gap{gap:.1f}<5→C上限")
    elif gap < 10.0:
        caps.append(69.9)
        reasons.append(f"Gap{gap:.1f}<10→B上限")
    elif gap < 15.0:
        caps.append(79.9)
        reasons.append(f"Gap{gap:.1f}<15→A上限")

    if not caps:
        return total, "なし"

    cap = min(caps)
    gated = min(total, cap)

    if gated < total:
        detail = f"{total:.1f}→{gated:.1f} ({' / '.join(reasons)})"
    else:
        detail = f"上限{cap:.1f} ({' / '.join(reasons)})"

    return gated, detail



# ----------------------------------------------------------------------
# v23 FAST: 実行時インデックス / 日内インクリメンタル
# ----------------------------------------------------------------------
def _ensure_fast_indexes(conn: sqlite3.Connection) -> None:
    """
    既存テーブルの検索を速くする内部インデックス。
    ダッシュボード列は増やさない。CREATE IF NOT EXISTSなので2回目以降はほぼ無コスト。
    """
    candidates = [
        ("price_history", "idx_shinden_price_code_date", '"コード","日付" DESC'),
        ("tdnet_documents", "idx_shinden_tdnet_code_pub", '"コード","提出時刻" DESC'),
        ("forecast_achievement_history", "idx_shinden_ach_code_fiscal", '"コード","fiscal_key" DESC'),
        ("earnings_reaction_labels", "idx_shinden_reaction_code_pub", '"コード","発表日時" DESC'),
    ]
    for table, idx_name, cols in candidates:
        try:
            if _table_exists(conn, table):
                tc = _cols(conn, table)
                needed = [x.strip('" ') for x in cols.replace(" DESC","").split(",")]
                if all(c in tc for c in needed):
                    conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}"({cols})')
        except Exception as e:
            print(f"[shinden][index][WARN] {idx_name}: {e}")
    conn.commit()


def _ensure_runtime_state(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shinden_runtime_state(
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)
    conn.commit()


def _state_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    _ensure_runtime_state(conn)
    row = conn.execute(
        "SELECT value FROM shinden_runtime_state WHERE key=?",
        (key,)
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _state_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    _ensure_runtime_state(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO shinden_runtime_state(key,value,updated_at)
        VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, value, now))
    conn.commit()


def _codes_with_new_earnings_since(
    conn: sqlite3.Connection,
    since_ts: str,
    valid_codes: set[str],
) -> list[str]:
    if not _table_exists(conn, "tdnet_documents"):
        return []
    cols = _cols(conn, "tdnet_documents")
    if not {"コード","提出時刻","タイトル","本文"}.issubset(cols):
        return []
    rows = conn.execute("""
        SELECT DISTINCT コード
        FROM tdnet_documents
        WHERE 提出時刻 > ?
          AND 本文 IS NOT NULL
          AND タイトル LIKE '%決算短信%'
          AND タイトル NOT LIKE '%訂正%'
    """, (since_ts,)).fetchall()
    out = []
    for (c,) in rows:
        s = str(c or "").strip()
        if s in valid_codes:
            out.append(s)
    return sorted(set(out))


def _bulk_achievement_map(conn: sqlite3.Connection, codes: list[str]) -> dict[str, list[tuple]]:
    if not codes or not _table_exists(conn, "forecast_achievement_history"):
        return {}
    cols = _cols(conn, "forecast_achievement_history")
    required = {"コード", "fiscal_key", "initial_forecast_op", "actual_op"}
    if not required.issubset(cols):
        return {}
    up_expr = '"upward_revisions"' if "upward_revisions" in cols else "0"
    down_expr = '"downward_revisions"' if "downward_revisions" in cols else "0"
    qmarks = ",".join(["?"] * len(codes))

    # CASTをWHERE/ORDER BYから外し、(コード,fiscal_key) indexを使わせる。
    rows = conn.execute(f"""
        WITH ranked AS (
            SELECT コード, fiscal_key, initial_forecast_op, actual_op,
                   {up_expr} AS uprev, {down_expr} AS downrev,
                   ROW_NUMBER() OVER (
                       PARTITION BY コード ORDER BY fiscal_key DESC
                   ) AS rn
            FROM forecast_achievement_history
            WHERE コード IN ({qmarks})
        )
        SELECT コード, fiscal_key, initial_forecast_op, actual_op, uprev, downrev
        FROM ranked
        WHERE rn <= 5
        ORDER BY コード, fiscal_key DESC
    """, codes).fetchall()

    out = {}
    for row in rows:
        code = str(row[0]).strip()
        out.setdefault(code, []).append((row[2], row[3], row[4], row[5]))
    return out


def _achievement_stats_from_rows(rows: list[tuple]) -> tuple[float, Optional[float], Optional[float], int, int, str, str, int]:
    vals, up, down = [], 0, 0
    for f, a, u, d in rows or []:
        f, a = _sf(f), _sf(a)
        if f is not None and a is not None and f > 0:
            vals.append(a / f)
            up += int(_sf(u) or 0)
            down += int(_sf(d) or 0)
    n = len(vals)
    if n == 0:
        return 0.0, None, None, up, down, "DATA不足", "0期 有効な達成履歴なし・信頼性加点なし", 0
    avg = statistics.mean(vals)
    mn = min(vals)
    raw = 0.0
    if 0.98 <= avg <= 1.20:
        raw += 10
    elif 0.90 <= avg < 0.98:
        raw += 7
    elif 1.20 < avg <= 1.50:
        raw += 8
    elif 0.80 <= avg < 0.90:
        raw += 4
    else:
        raw += 2
    if mn >= 0.95:
        raw += 7
    elif mn >= 0.90:
        raw += 6
    elif mn >= 0.80:
        raw += 3
    elif mn >= 0.70:
        raw += 1
    raw -= min(down * 2.5, 8)
    raw += min(up * 0.8, 2.5)
    raw = _clamp(raw, 0, MAX_RELIABILITY)
    cap_by_n = {1: 5.0, 2: 10.0, 3: 15.0}
    sample_cap = cap_by_n.get(n, MAX_RELIABILITY)
    score = min(raw, sample_cap)
    if n == 1:
        conf = "LOW"
    elif n in (2, 3):
        conf = "MEDIUM"
    else:
        conf = "HIGH"
    detail = (
        f"{n}期 平均{avg*100:.1f}% 最低{mn*100:.1f}% "
        f"上方{up}/下方{down} 信頼性上限{sample_cap:.0f}/20"
    )
    return score, avg, mn, up, down, conf, detail, n


def _bulk_tdnet_text_map(conn: sqlite3.Connection, codes: list[str]) -> dict[str, str]:
    if not codes or not _table_exists(conn, "tdnet_documents"):
        return {}
    cols = _cols(conn, "tdnet_documents")
    if not {"コード", "本文"}.issubset(cols):
        return {}
    qmarks = ",".join(["?"] * len(codes))

    if "提出時刻" in cols:
        rows = conn.execute(f"""
            WITH ranked AS (
                SELECT コード, 本文,
                       ROW_NUMBER() OVER (
                           PARTITION BY コード ORDER BY 提出時刻 DESC
                       ) AS rn
                FROM tdnet_documents
                WHERE コード IN ({qmarks})
                  AND 本文 IS NOT NULL
            )
            SELECT コード, 本文
            FROM ranked
            WHERE rn <= 12
            ORDER BY コード, rn
        """, codes).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT コード, 本文
            FROM tdnet_documents
            WHERE コード IN ({qmarks})
              AND 本文 IS NOT NULL
        """, codes).fetchall()

    out_parts = {}
    for code, body in rows:
        code = str(code).strip()
        out_parts.setdefault(code, []).append(str(body or ""))
    return {k: "\n".join(v) for k, v in out_parts.items()}


def _score_evidence_from_text(text: str, op_yoy: Optional[float], progress: Optional[float]) -> tuple[float,float,list[str],list[str]]:
    basis, bh = 0.0, []
    for kw, pts in BASIS_KEYWORDS.items():
        if kw in text:
            basis += pts
            bh.append(kw)
    vis, vh = 0.0, []
    for kw, pts in VISIBILITY_KEYWORDS.items():
        if kw in text:
            vis += pts
            vh.append(kw)
    if op_yoy is not None:
        if op_yoy >= 50:
            vis += 5
            vh.append(f"直近営業益YoY+{op_yoy:.1f}%")
        elif op_yoy >= 20:
            vis += 3
            vh.append(f"直近営業益YoY+{op_yoy:.1f}%")
        elif op_yoy > 0:
            vis += 1
    if progress is not None and progress >= 35:
        vis += 2
        vh.append(f"進捗{progress:.1f}%")
    return _clamp(basis,0,MAX_BASIS), _clamp(vis,0,MAX_VISIBILITY), bh[:10], vh[:10]


def _bulk_returns_map(conn: sqlite3.Connection, codes: list[str]) -> dict[str, tuple]:
    """
    株価の事前織り込みだけを測る。出来高・売買代金は使わない。

    v23 FAST:
      旧版は price_history 全期間を全銘柄ぶん読み、その後Pythonで70行に切っていた。
      今版は直近140暦日だけSQLで取得し、各銘柄70営業日に限定する。
    """
    if not codes or not _table_exists(conn, "price_history"):
        return {}
    pc = _cols(conn, "price_history")
    code_col = "コード" if "コード" in pc else None
    date_col = "日付" if "日付" in pc else ("date" if "date" in pc else None)
    close_col = "終値" if "終値" in pc else ("close" if "close" in pc else None)
    high_col = "高値" if "高値" in pc else ("high" if "high" in pc else None)
    if not all((code_col, date_col, close_col)):
        return {}

    qmarks = ",".join(["?"] * len(codes))
    high_expr = f'"{high_col}"' if high_col else f'"{close_col}"'
    cutoff = (datetime.now() - timedelta(days=140)).strftime("%Y-%m-%d")

    # 140日で通常90前後の営業日があるため60営業日リターンに十分。
    sql = (
        f'SELECT "{code_col}" AS code, "{date_col}" AS d, '
        f'"{close_col}" AS c, {high_expr} AS h '
        f'FROM price_history '
        f'WHERE "{code_col}" IN ({qmarks}) AND "{date_col}" >= ? '
        f'ORDER BY "{code_col}", "{date_col}" DESC'
    )
    params = list(codes) + [cutoff]
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return {}

    df["code"] = df["code"].astype(str).str.strip()
    df["c"] = pd.to_numeric(df["c"], errors="coerce")
    df["h"] = pd.to_numeric(df["h"], errors="coerce")
    df["h"] = df["h"].fillna(df["c"])
    df = df.dropna(subset=["c"])

    out = {}
    for code, g in df.groupby("code", sort=False):
        g = g.head(70).reset_index(drop=True)
        if g.empty:
            out[code] = (None, None, None, None, None)
            continue

        cur = float(g.iloc[0]["c"])

        def ret(n):
            if len(g) <= n:
                return None
            old = float(g.iloc[n]["c"])
            if old == 0:
                return None
            return (cur / old - 1) * 100

        peak20 = None
        drawdown20 = None
        if len(g) > 20:
            base = float(g.iloc[20]["c"])
            peak = pd.to_numeric(g.iloc[:21]["h"], errors="coerce").max()
            if base != 0 and pd.notna(peak) and float(peak) > 0:
                peak20 = (float(peak) / base - 1) * 100
                drawdown20 = (cur / float(peak) - 1) * 100

        out[code] = (ret(5), ret(20), ret(60), peak20, drawdown20)
    return out


def _score_unpriced_from_returns(day_pct: Optional[float], rets) -> tuple[float,str]:
    """
    「すでに上がったか」を終点だけで判定しない。

    - 前日
    - 5日
    - 20日
    - 60日
    - 20日内ピーク先回り
    - そのピークから現在までの押し

    を同時に使う。
    """
    r5, r20, r60, peak20, dd20 = rets or (None, None, None, None, None)
    s = 0.0

    if day_pct is not None:
        if day_pct <= 0: s += 5.0
        elif day_pct <= 1: s += 4.0
        elif day_pct <= 2: s += 3.0
        elif day_pct <= 3: s += 1.5
        elif day_pct >= 5: s -= 2.5

    if r5 is not None:
        if r5 <= 0: s += 4.0
        elif r5 <= 3: s += 3.0
        elif r5 <= 7: s += 1.0
        elif r5 >= 15: s -= 2.5

    if r20 is not None:
        if r20 <= 2: s += 4.0
        elif r20 <= 7: s += 2.5
        elif r20 <= 12: s += 1.0
        elif r20 >= 20: s -= 2.0
        if r20 >= 30: s -= 2.0

    if r60 is not None:
        if r60 <= 10: s += 2.0
        elif r60 >= 40: s -= 1.5
        if r60 >= 70: s -= 1.5

    # 一度大きく買われた事実は残す。ただし十分押していれば一部だけ減点を戻す。
    peak_pen = _peak20_penalty(peak20)
    relief = 0.0
    if dd20 is not None and peak_pen > 0:
        if dd20 <= -15:
            relief = min(2.0, peak_pen * 0.55)
        elif dd20 <= -8:
            relief = min(1.0, peak_pen * 0.35)
    effective_peak_pen = max(0.0, peak_pen - relief)
    s -= effective_peak_pen

    peak_txt = f"{peak20:.1f}%" if peak20 is not None else "-"
    dd_txt = f"{dd20:.1f}%" if dd20 is not None else "-"
    pen_txt = f"-{effective_peak_pen:.1f}" if effective_peak_pen > 0 else "0"

    return (
        _clamp(s, 0, MAX_UNPRICED),
        f"前日{day_pct if day_pct is not None else '-'}% / "
        f"5日{r5 if r5 is not None else '-'}% / "
        f"20日{r20 if r20 is not None else '-'}% / "
        f"60日{r60 if r60 is not None else '-'}% / "
        f"20日内ピーク{peak_txt} / ピーク後{dd_txt} / 先回り減点{pen_txt}"
    )


# ----------------------------------------------------------------------
# v9: 過去1600件の「決算本文×実反応」から軽量な反応辞書を自己校正
# ----------------------------------------------------------------------
REACTION_TERMS = [
    "上方修正","増額修正","業績予想の修正","最高益","過去最高益","増配","復配",
    "黒字転換","受注残","大型受注","受注獲得","引き合い","需要拡大","需要回復",
    "価格改定","値上げ","価格転嫁","量産開始","稼働開始","生産能力増強",
    "下方修正","減額修正","減配","赤字転落","特別損失","特損","減損",
    "前倒し","後ずれ","後ズレ","期ずれ","期ズレ","一時的","一過性",
    "為替影響","為替の影響","為替差益","為替差損","M&A","子会社化",
    "売却益","特別利益","固定資産売却益","補助金","反動減","先行投資",
    "販管費","研究開発費","人件費増","原材料高","物流費増"
]


def _learn_reaction_term_weights(conn: sqlite3.Connection) -> tuple[dict[str,float], dict[str,tuple]]:
    """
    hydratedした +5% / -5% 教師本文から語の出現率差を自己校正。

    v10:
      教師集合が変わらない限りJSONをDB内部キャッシュし、毎回1600本文を再走査しない。
    """
    if not (_table_exists(conn, "earnings_reaction_labels") and _table_exists(conn, "tdnet_documents")):
        return {}, {}

    _ensure_runtime_state(conn)

    try:
        sig_row = conn.execute("""
            SELECT
              COUNT(*) AS n,
              COALESCE(MAX(r.発表日時),'') AS mx
            FROM earnings_reaction_labels r
            JOIN tdnet_documents d
              ON r.コード=d.コード
             AND r.発表日時=d.提出時刻
             AND r.タイトル=d.タイトル
            WHERE r.引け後=1
              AND r.反応ラベル IN (2,-2)
              AND d.本文 IS NOT NULL
              AND length(d.本文)>=50
        """).fetchone()
        signature = f"{int(sig_row[0] or 0)}|{str(sig_row[1] or '')}"
    except Exception:
        signature = ""

    cached_sig = _state_get(conn, "teacher_signature")
    cached_json = _state_get(conn, "teacher_weights_json")
    if signature and cached_sig == signature and cached_json:
        try:
            obj = json.loads(cached_json)
            weights = {str(k): float(v) for k,v in (obj.get("weights") or {}).items()}
            stats = {str(k): tuple(v) for k,v in (obj.get("stats") or {}).items()}
            return weights, stats
        except Exception:
            pass

    try:
        rows = conn.execute("""
            SELECT r.反応ラベル, d.本文
            FROM earnings_reaction_labels r
            JOIN tdnet_documents d
              ON r.コード=d.コード
             AND r.発表日時=d.提出時刻
             AND r.タイトル=d.タイトル
            WHERE r.引け後=1
              AND r.反応ラベル IN (2,-2)
              AND d.本文 IS NOT NULL
              AND length(d.本文)>=50
        """).fetchall()
    except Exception:
        return {}, {}

    if not rows:
        return {}, {}

    npos = sum(1 for lab,_ in rows if int(lab)==2)
    nneg = sum(1 for lab,_ in rows if int(lab)==-2)
    if npos < 30 or nneg < 30:
        return {}, {}

    ph = Counter()
    nh = Counter()
    for lab, body in rows:
        text = str(body or "")
        target = ph if int(lab)==2 else nh
        for term in REACTION_TERMS:
            if term in text:
                target[term] += 1

    weights = {}
    stats = {}
    for term in REACTION_TERMS:
        pr = (ph[term] + 1.0) / (npos + 2.0)
        nr = (nh[term] + 1.0) / (nneg + 2.0)
        diff_pt = (pr - nr) * 100.0
        w = _clamp(diff_pt / 5.0, -2.5, 2.5)
        if abs(diff_pt) < 1.0:
            w = 0.0
        weights[term] = w
        stats[term] = (ph[term], nh[term], pr*100.0, nr*100.0, diff_pt)

    if signature:
        try:
            _state_set(conn, "teacher_signature", signature)
            _state_set(
                conn, "teacher_weights_json",
                json.dumps({"weights":weights, "stats":stats}, ensure_ascii=False)
            )
        except Exception:
            pass

    return weights, stats


def _bulk_latest_earnings_docs(conn: sqlite3.Connection, codes: list[str]) -> dict[str, tuple[str,str,str,str,str]]:
    """
    v19: 数値ソースと補助IRを分離する。

    code -> (
      決算短信提出時刻,
      決算短信タイトル,
      決算短信本文,      # YoY等の数値は原則ここだけから読む
      同日補助IRタイトル,
      同日補助IR本文,    # 上方修正/配当/前倒し等のイベント専用
    )
    """
    if not codes or not _table_exists(conn, "tdnet_documents"):
        return {}
    cols = _cols(conn, "tdnet_documents")
    if not {"コード","提出時刻","タイトル","本文"}.issubset(cols):
        return {}

    qmarks = ",".join(["?"] * len(codes))
    latest_rows = conn.execute(f"""
        WITH ranked AS (
            SELECT コード, 提出時刻, タイトル, 本文,
                   ROW_NUMBER() OVER (
                       PARTITION BY コード ORDER BY 提出時刻 DESC
                   ) AS rn
            FROM tdnet_documents
            WHERE コード IN ({qmarks})
              AND 本文 IS NOT NULL
              AND タイトル LIKE '%決算短信%'
              AND タイトル NOT LIKE '%訂正%'
        )
        SELECT コード, 提出時刻, タイトル, 本文
        FROM ranked
        WHERE rn=1
    """, codes).fetchall()

    out = {}
    for code,pub,title,body in latest_rows:
        code = str(code or "").strip()
        pub = str(pub or "")
        title = str(title or "")
        body = str(body or "")
        day = pub[:10]

        comp_titles = []
        comp_bodies = []
        try:
            rows = conn.execute("""
                SELECT 提出時刻, タイトル, 本文
                FROM tdnet_documents
                WHERE コード=?
                  AND substr(提出時刻,1,10)=?
                  AND 本文 IS NOT NULL
                  AND タイトル NOT LIKE '%訂正%'
                ORDER BY 提出時刻 ASC
            """, (code,day)).fetchall()
            for _p,t,b in rows:
                tt = str(t or "")
                if "決算短信" in tt:
                    continue
                if not any(k in tt for k in (
                    "業績予想","業績予想の修正","修正に関する",
                    "上方修正","下方修正",
                    "配当予想","剰余金の配当","増配","減配",
                    "決算説明","説明資料",
                    "中期経営","中期計画","事業計画",
                    "受注","大型案件"
                )):
                    continue
                comp_titles.append(f"[{tt}]")
                comp_bodies.append(f"=== {tt} ===\n{str(b or '')}")
        except Exception:
            pass

        out[code] = (
            pub, title, body,
            "\n".join(comp_titles),
            "\n\n".join(comp_bodies),
        )
    return out


def _bulk_recent_forecast_revision_map(conn: sqlite3.Connection, codes: list[str]) -> dict[str, dict]:
    """
    forecast_history の最新2値から「今回の予想修正幅」を直接復元。
    PDF本文に『13.8%上方修正』という文章がなくても表の数値履歴で拾う。
    """
    if not codes or not _table_exists(conn, "forecast_history"):
        return {}
    cols = _cols(conn, "forecast_history")
    need = {"コード","fiscal_key","forecast_date","forecast_op"}
    if not need.issubset(cols):
        return {}

    qmarks = ",".join(["?"] * len(codes))
    id_expr = "id" if "id" in cols else "rowid"
    rows = conn.execute(f"""
        WITH ranked AS (
            SELECT コード, fiscal_key, forecast_date, forecast_op, forecast_eps,
                   ROW_NUMBER() OVER (
                       PARTITION BY コード, fiscal_key
                       ORDER BY forecast_date DESC, {id_expr} DESC
                   ) AS rn
            FROM forecast_history
            WHERE コード IN ({qmarks})
              AND forecast_op IS NOT NULL
        )
        SELECT コード, fiscal_key, forecast_date, forecast_op, forecast_eps, rn
        FROM ranked
        WHERE rn <= 4
        ORDER BY コード, fiscal_key DESC, rn ASC
    """, codes).fetchall()

    by = {}
    for code,fk,fd,op,eps,rn in rows:
        c = str(code or "").strip()
        by.setdefault(c, {}).setdefault(str(fk or ""), []).append(
            (str(fd or ""), _sf(op), _sf(eps), int(rn))
        )

    out = {}
    for code, fks in by.items():
        # 最も新しいforecast_dateを持つ年度を採用
        best_fk = None
        best_date = ""
        for fk, vals in fks.items():
            d = max((v[0] for v in vals), default="")
            if d > best_date:
                best_date = d
                best_fk = fk
        if not best_fk:
            continue
        vals = sorted(fks[best_fk], key=lambda z: z[3])  # rn 1,2...
        if not vals:
            continue
        latest_date, latest_op, latest_eps, _ = vals[0]
        prev = next((v for v in vals[1:] if v[1] is not None and latest_op is not None and abs(v[1]-latest_op) > 1e-9), None)
        d = {
            "fiscal_key": best_fk,
            "date": latest_date,
            "latest_op": latest_op,
            "latest_eps": latest_eps,
            "previous_op": prev[1] if prev else None,
            "previous_eps": prev[2] if prev else None,
            "revision_pct": None,
            "direction": None,
        }
        if prev and prev[1] not in (None, 0) and latest_op is not None:
            rp = (latest_op / prev[1] - 1.0) * 100.0
            d["revision_pct"] = rp
            d["direction"] = "up" if rp > 0.05 else ("down" if rp < -0.05 else "flat")
        out[code] = d
    return out


def _parse_pub_dt(v: str) -> Optional[datetime]:
    s = str(v or "").replace("T"," ").replace("+09:00"," ").strip()
    for fmt, n in (("%Y-%m-%d %H:%M:%S",19),("%Y-%m-%d %H:%M",16),("%Y-%m-%d",10)):
        try:
            return datetime.strptime(s[:n], fmt)
        except Exception:
            pass
    return None


def _is_fresh_release(pub: str, max_age_days: int=2) -> bool:
    dt = _parse_pub_dt(pub)
    if dt is None:
        return False
    now = datetime.now()
    age = now - dt
    return timedelta(days=-1) <= age <= timedelta(days=max_age_days)


def _first_pct_near(text: str, keyword: str, window: int=160) -> Optional[float]:
    p = text.find(keyword)
    if p < 0:
        return None
    s = text[max(0,p-40):p+window].replace("％","%").replace("▲","-").replace("△","-")
    vals = re.findall(r'([+-]?\d+(?:\.\d+)?)\s*%', s)
    for v in vals:
        try:
            x=float(v)
            if -500 <= x <= 2000:
                return x
        except Exception:
            pass
    return None


def _parse_release_signals(title: str, text: str) -> dict:
    """
    発表直後に読める決算本文だけから、アルゴが拾いやすいヘッドラインを抽出。
    """
    X = (str(title or "") + "\n" + str(text or "")).replace("％","%").replace("▲","-").replace("△","-")
    out = {
        "upward": any(k in X for k in ("上方修正","増額修正","上期予想修正（増額）","通期予想修正（増額）")),
        "downward": any(k in X for k in ("下方修正","減額修正","通期予想修正（減額）")),
        "div_up": any(k in X for k in ("増配","復配","配当予想の修正（増額）")),
        "div_down": any(k in X for k in ("減配","配当予想の修正（減額）")),
        "record": any(k in X for k in ("最高益","過去最高益","連続最高益")),
        "black_turn": "黒字転換" in X,
        "red_turn": "赤字転落" in X,
        "forward_shift": any(k in X for k in ("前倒し","前倒し計上","前倒し引渡")),
        "expense_delay": any(k in X for k in ("販管費の下期","費用の下期","後ずれ","後ズレ","期ずれ","期ズレ")),
        "fx": any(k in X for k in ("為替影響","為替の影響","為替差益","為替差損")),
        "ma": any(k in X for k in ("M&A","子会社化","連結子会社")),
        "oneoff": any(k in X for k in ("一時的","一過性","一時益","一過性利益","売却益","特別利益","固定資産売却益")),
        "orders": any(k in X for k in ("受注残","大型受注","受注獲得","引き合い","案件獲得")),
    }

    # 明記された修正幅: 「13.8%上方修正」等
    m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*%\s*(?:の)?上方修正', X)
    out["revision_pct"] = float(m.group(1)) if m else None

    # 修正後の増益率: 「増益率が...→73.1%増」
    m = re.search(r'増益率[^。\n]{0,100}?[→⇒]\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*増', X)
    out["guidance_growth_pct"] = float(m.group(1)) if m else None

    # 下期成長: 「下期...前年同期比85.3%増」
    m = re.search(r'下期[^。\n]{0,180}?前年同期比\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*増', X)
    out["lower_half_growth_pct"] = float(m.group(1)) if m else None

    # 進捗率
    m = re.search(r'進捗率[^0-9]{0,30}([0-9]+(?:\.[0-9]+)?)\s*%', X)
    out["progress"] = float(m.group(1)) if m else None

    # 営業利益率の改善
    m = re.search(
        r'(?:売上)?営業利益率[^0-9]{0,50}([0-9]+(?:\.[0-9]+)?)\s*%\s*[→⇒]\s*([0-9]+(?:\.[0-9]+)?)\s*%',
        X
    )
    if m:
        out["margin_delta_pt"] = float(m.group(2))-float(m.group(1))
    else:
        out["margin_delta_pt"] = None

    # YoY: 文章中の典型表現を優先。
    yoy = {}
    for key, aliases in {
        "sales": ("売上高","売上収益"),
        "op": ("営業利益",),
        "ordinary": ("経常利益",),
        "net": ("純利益","当期利益"),
    }.items():
        val = None
        for alias in aliases:
            # 「営業利益は前年同期比63.1%増」
            mm = re.search(
                re.escape(alias) + r'[^。\n]{0,120}?前年同期比\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*(増|減)',
                X
            )
            if mm:
                x=float(mm.group(1))
                val = x if mm.group(2)=="増" else -x
                break
            # 「前年同期比3.5倍」等は巨大増益として扱う
            mm = re.search(
                re.escape(alias) + r'[^。\n]{0,120}?前年同期比\s*([0-9]+(?:\.[0-9]+)?)\s*倍',
                X
            )
            if mm:
                val = (float(mm.group(1))-1.0)*100.0
                break
        yoy[key]=val
    out["yoy"]=yoy
    return out



def _norm_numtext(s: str) -> str:
    x = str(s or "")
    trans = str.maketrans({
        "０":"0","１":"1","２":"2","３":"3","４":"4",
        "５":"5","６":"6","７":"7","８":"8","９":"9",
        "Ａ":"A","Ｂ":"B","ａ":"a","ｂ":"b",
        "（":"(","）":")","％":"%","，":",","．":".",
        "＋":"+","－":"-","−":"-","▲":"-","△":"-",
        "／":"/","　":" ",
    })
    return x.translate(trans)


def _current_period_kind_from_title(title: str) -> tuple[Optional[int], Optional[int], str]:
    t = str(title or "").replace("１","1").replace("２","2").replace("３","3").replace("４","4")
    ym = re.search(r"(20\d{2})年\s*(\d{1,2})月期", t)
    fy = int(ym.group(1)) if ym else None
    fm = int(ym.group(2)) if ym else None
    if "第1四半期" in t: return fy, fm, "1Q"
    if "第2四半期" in t or "中間" in t: return fy, fm, "2Q"
    if "第3四半期" in t: return fy, fm, "3Q"
    return fy, fm, "FY"


def _summary_row_yoy(title: str, text: str) -> dict[str, Optional[float]]:
    """
    決算短信1ページ目の当期行から、最初4つの増減率を
    sales/op/ordinary/net として読む。
    """
    X = _norm_numtext(text)
    fy, fm, q = _current_period_kind_from_title(title)
    if fy is None:
        return {}

    if q == "1Q":
        qpat = r"(?:第\s*1\s*四半期|第１四半期)"
    elif q == "2Q":
        qpat = r"(?:第\s*2\s*四半期|第２四半期|中間期?|第2四半期)"
    elif q == "3Q":
        qpat = r"(?:第\s*3\s*四半期|第３四半期)"
    else:
        qpat = None

    if qpat:
        pat = re.compile(rf"{fy}\s*年\s*{fm}\s*月期[^0-9%\n]{{0,50}}{qpat}", re.I)
    else:
        pat = re.compile(rf"{fy}\s*年\s*{fm}\s*月期")

    candidates = []
    for m in list(pat.finditer(X))[:12]:
        seg = X[m.end():m.end()+1000]
        nxt = re.search(r"20\d{2}\s*年\s*\d{1,2}\s*月期", seg)
        if nxt and nxt.start() > 40:
            seg = seg[:nxt.start()]
        pcts = []
        for pm in re.finditer(r"(?<![\d.])(-?\d+(?:\.\d+)?)\s*%", seg):
            try:
                v = float(pm.group(1))
            except Exception:
                continue
            if -1000 <= v <= 5000:
                pcts.append(v)
            if len(pcts) >= 6:
                break
        if len(pcts) >= 4:
            candidates.append(pcts[:4])

    if not candidates:
        return {}
    vals = candidates[0]
    return {"sales": vals[0], "op": vals[1], "ordinary": vals[2], "net": vals[3]}


def _revision_table_candidates(text: str) -> list[dict]:
    """
    標準修正表の「前回発表予想(A)」「今回修正予想(B)」から
    sales/op/ordinary/net の修正率を計算。
    """
    X = _norm_numtext(text)
    labels = list(re.finditer(
        r"(前回発表予想|前回予想|従来予想|今回修正予想|今回予想|修正予想)\s*(?:\([ABＡＢ]\))?",
        X
    ))
    rows = []
    for i,m in enumerate(labels):
        label = m.group(1)
        end = labels[i+1].start() if i+1 < len(labels) else min(len(X), m.end()+800)
        seg = X[m.end():end]
        nums = []
        for nm in re.finditer(r"(?<![\d.])(-?\d[\d,]*(?:\.\d+)?)", seg):
            try:
                v = float(nm.group(1).replace(",",""))
            except Exception:
                continue
            if abs(v) >= 10:
                nums.append(v)
            if len(nums) >= 7:
                break
        if len(nums) >= 4:
            rows.append((label, nums))

    prev = next((n for lab,n in rows if lab in ("前回発表予想","前回予想","従来予想")), None)
    new = next((n for lab,n in rows if lab in ("今回修正予想","今回予想","修正予想")), None)
    if not prev or not new:
        return []

    out = []
    for idx,name in enumerate(("sales","op","ordinary","net")):
        if idx < len(prev) and idx < len(new) and prev[idx] != 0:
            rp = (new[idx] / prev[idx] - 1.0) * 100.0
            if -90 <= rp <= 500:
                out.append({"metric":name,"previous":prev[idx],"new":new[idx],"revision_pct":rp})
    return out


def _best_revision_signal(companion_text: str) -> Optional[dict]:
    cands = _revision_table_candidates(companion_text)
    if not cands:
        return None
    for metric in ("op","ordinary","net","sales"):
        x = next((c for c in cands if c["metric"] == metric), None)
        if x:
            return x
    return cands[0]



def _period_kind_from_title(title: str) -> str:
    t = str(title or "").replace("１","1").replace("２","2").replace("３","3").replace("４","4")
    if "第1四半期" in t:
        return "1Q"
    if "第2四半期" in t or "中間" in t:
        return "2Q"
    if "第3四半期" in t:
        return "3Q"
    return "FY"


def _fiscal_key_from_title_local(title: str) -> Optional[str]:
    t = str(title or "")
    m = re.search(r"(20\d{2})年\s*(\d{1,2})月期", t)
    if not m:
        return None
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"


def _prev_fiscal_key_local(fk: str|None) -> Optional[str]:
    if not fk or "-" not in fk:
        return None
    try:
        y,m = fk.split("-",1)
        return f"{int(y)-1:04d}-{int(m):02d}"
    except Exception:
        return None


def _xbrl_current_metrics(conn: sqlite3.Connection, code: str, pub: str, title: str) -> dict:
    if not _table_exists(conn, "tdnet_xbrl_metrics"):
        return {}
    cols = _cols(conn, "tdnet_xbrl_metrics")
    need = {"コード","提出時刻","タイトル"}
    if not need.issubset(cols):
        return {}

    wanted = (
        "actual_fiscal_key","actual_op","actual_eps",
        "forecast_fiscal_key","forecast_op","forecast_eps",
        "parse_method"
    )
    select_cols = [f'"{c}"' if c in cols else f'NULL AS "{c}"' for c in wanted]
    sql = (
        "SELECT " + ",".join(select_cols) +
        " FROM tdnet_xbrl_metrics WHERE コード=? AND 提出時刻=? AND タイトル=? LIMIT 1"
    )
    row = conn.execute(sql, (code,pub,title)).fetchone()
    if not row:
        return {}
    return {k: row[i] for i,k in enumerate(wanted)}


def _prior_same_period_xbrl_op(conn: sqlite3.Connection, code: str, current_title: str, current_pub: str) -> Optional[float]:
    if not (_table_exists(conn, "tdnet_documents") and _table_exists(conn, "tdnet_xbrl_metrics")):
        return None

    kind = _period_kind_from_title(current_title)
    fk = _fiscal_key_from_title_local(current_title)
    prev_fk = _prev_fiscal_key_local(fk)
    if not prev_fk:
        return None

    cols = _cols(conn, "tdnet_xbrl_metrics")
    if not {"actual_fiscal_key","actual_op"}.issubset(cols):
        return None

    sql = """
        SELECT x.actual_op, d.タイトル, x.提出時刻
        FROM tdnet_xbrl_metrics x
        JOIN tdnet_documents d
          ON d.コード=x.コード
         AND d.提出時刻=x.提出時刻
         AND d.タイトル=x.タイトル
        WHERE x.コード=?
          AND x.actual_fiscal_key=?
          AND x.actual_op IS NOT NULL
          AND x.提出時刻 < ?
        ORDER BY x.提出時刻 DESC
        LIMIT 12
    """
    rows = conn.execute(sql, (code, prev_fk, current_pub)).fetchall()

    for op,t,_p in rows:
        if _period_kind_from_title(str(t or "")) == kind:
            return _sf(op)
    return None


def _prior_forecast_before_release(conn: sqlite3.Connection, code: str, fiscal_key: str|None, pub: str) -> Optional[float]:
    if not fiscal_key or not _table_exists(conn, "forecast_history"):
        return None
    cols = _cols(conn, "forecast_history")
    if not {"コード","fiscal_key","forecast_date","forecast_op"}.issubset(cols):
        return None

    order_tail = ", id DESC" if "id" in cols else ""
    sql = (
        "SELECT forecast_op FROM forecast_history "
        "WHERE コード=? AND fiscal_key=? AND forecast_op IS NOT NULL AND forecast_date < ? "
        "ORDER BY forecast_date DESC" + order_tail + " LIMIT 1"
    )
    row = conn.execute(sql, (code,fiscal_key,pub)).fetchone()
    return _sf(row[0]) if row else None



def _extract_forecast_op_from_doc_text(text: str) -> Optional[float]:
    """
    業績予想修正/決算短信の本文から「今回予想」の営業利益を拾う保守的fallback。
    まず標準表ラベルを探す。取れない場合はNoneを返し、推測しない。
    """
    X = _norm_numtext(text)

    # 典型: 今回修正予想(B) 売上高 営業利益 経常利益 ...
    labels = list(re.finditer(
        r"(今回修正予想|今回予想|修正予想|通期予想|業績予想)\s*(?:\([BＢ]\))?",
        X
    ))
    for m in labels[:12]:
        seg = X[m.end():m.end()+700]
        # 次のラベルまで
        nxt = re.search(r"(前回発表予想|前回予想|従来予想|今回修正予想|今回予想|修正予想)", seg)
        if nxt and nxt.start() > 20:
            seg = seg[:nxt.start()]
        nums = []
        for nm in re.finditer(r"(?<![\d.])(-?\d[\d,]*(?:\.\d+)?)", seg):
            try:
                v = float(nm.group(1).replace(",",""))
            except Exception:
                continue
            # 年月日/率を避けるため金額らしい値だけ
            if abs(v) >= 10:
                nums.append(v)
            if len(nums) >= 6:
                break
        # 標準表は [売上, 営業, 経常, 純益, EPS...] の順
        if len(nums) >= 2:
            op = nums[1]
            if 0 < abs(op) < 1e9:
                return float(op)
    return None


def _prior_forecast_op_from_documents(
    conn: sqlite3.Connection,
    code: str,
    pub: str,
    current_fiscal_key: str|None,
) -> tuple[Optional[float], str]:
    """
    forecast_history欠落時に、発表前のTDnet文書から最新の会社予想OPを復元。
    「業績予想修正」→「本決算短信」の順で探す。
    """
    if not _table_exists(conn, "tdnet_documents"):
        return None, ""

    rows = conn.execute(
        """
        SELECT 提出時刻, タイトル, 本文
        FROM tdnet_documents
        WHERE コード=?
          AND 提出時刻 < ?
          AND 本文 IS NOT NULL
          AND (
               タイトル LIKE '%業績予想%'
            OR タイトル LIKE '%予想の修正%'
            OR タイトル LIKE '%決算短信%'
          )
        ORDER BY 提出時刻 DESC
        LIMIT 30
        """,
        (code, pub)
    ).fetchall()

    for p,t,b in rows:
        title = str(t or "")
        # current年度と明らかに違う決算短信は飛ばす
        fk = _fiscal_key_from_title_local(title)
        if current_fiscal_key and fk and fk != current_fiscal_key:
            # 本決算短信はタイトル上は前期だが翌期予想を含むため、1年前だけ許容
            try:
                cy,cm = [int(x) for x in current_fiscal_key.split("-")]
                fy,fm = [int(x) for x in fk.split("-")]
                if not (fm == cm and fy == cy-1 and "決算短信" in title):
                    continue
            except Exception:
                continue

        op = _extract_forecast_op_from_doc_text(str(b or ""))
        if op is not None:
            return op, f"{str(p)[:10]} {title[:28]}"
    return None, ""


def _quarter_yoy_from_earnings_text(title: str, text: str) -> tuple[dict,str]:
    """
    current四半期のYoYを標準短信の連結経営成績表から復元する改良版。
    1) 当期ラベル近傍
    2) 「売上高 営業利益 経常利益 ...」見出し直後の最初の当期行
    の順で試す。
    """
    X = _norm_numtext(text)
    fy,fm,q = _current_period_kind_from_title(title)

    # まず既存の当期行パーサ
    y = _summary_row_yoy(title, text)
    if y and _sf(y.get("op")) is not None:
        # 誤抽出ガード: 売上/営業/経常/純益の4率のうち、
        # 極端に符号がバラバラな場合は疑わしいので次へ。
        vals = [y.get(k) for k in ("sales","op","ordinary","net") if _sf(y.get(k)) is not None]
        if len(vals) >= 3:
            return y, "短信当期行"

    # 見出しをアンカーに、当期年度/Qラベルの後ろから%を抽出
    if fy is not None:
        kind_pat = {
            "1Q": r"(?:第\s*1\s*四半期|第１四半期)",
            "2Q": r"(?:第\s*2\s*四半期|第２四半期|中間期?)",
            "3Q": r"(?:第\s*3\s*四半期|第３四半期)",
            "FY": r"",
        }[q]
        if q != "FY":
            pats = [
                rf"{fy}\s*年\s*{fm}\s*月期[^0-9]{{0,80}}{kind_pat}",
                rf"{fy}\s*年[^0-9]{{0,20}}{kind_pat}",
            ]
            for pat in pats:
                m = re.search(pat, X)
                if not m:
                    continue
                seg = X[m.end():m.end()+900]
                # 次年度/前年行で切る
                nxt = re.search(r"20\d{2}\s*年\s*\d{1,2}\s*月期", seg)
                if nxt and nxt.start() > 50:
                    seg = seg[:nxt.start()]
                pcts = []
                for pm in re.finditer(r"(-?\d+(?:\.\d+)?)\s*%", seg):
                    try:
                        v=float(pm.group(1))
                    except Exception:
                        continue
                    if -1000 <= v <= 5000:
                        pcts.append(v)
                    if len(pcts) >= 4:
                        break
                if len(pcts) >= 4:
                    return {
                        "sales":pcts[0],"op":pcts[1],
                        "ordinary":pcts[2],"net":pcts[3]
                    }, "短信期ラベル"

    return {}, ""



_AMOUNT_RE = r"(?:\d{1,3}(?:,\d{3})+|\d+)"
_PCT_RE = r"(?:△|-)?\d+(?:\.\d+)?"


def _current_period_label_regex(title: str) -> Optional[str]:
    """
    決算短信タイトルから、1ページ目の当期行ラベル用regexを作る。
    例:
      2027年3月期 第1四半期
      2026年12月期 中間期
    """
    t = (
        str(title or "")
        .replace("１","1").replace("２","2").replace("３","3").replace("４","4")
        .replace("　"," ")
    )
    m = re.search(r"(20\d{2})年\s*(\d{1,2})月期", t)
    if not m:
        return None
    fy, fm = int(m.group(1)), int(m.group(2))

    if "第1四半期" in t:
        suffix = r"(?:第\s*1\s*四半期|第１四半期)"
    elif "第2四半期" in t or "中間" in t:
        suffix = r"(?:第\s*2\s*四半期|第２四半期|中間期?)"
    elif "第3四半期" in t:
        suffix = r"(?:第\s*3\s*四半期|第３四半期)"
    else:
        suffix = ""

    if suffix:
        return rf"{fy}\s*年\s*{fm}\s*月期\s*{suffix}"
    return rf"{fy}\s*年\s*{fm}\s*月期"



def _parse_narrative_actual_yoy(text: str) -> tuple[dict, str]:
    """
    決算短信本文の定性的説明から current YoY を読む。
    PDF抽出で金額と括弧の間に改行が入っても許容する。
    """
    X = _norm_numtext(text)

    aliases = {
        "sales": ("連結売上高", "売上高", "売上収益"),
        "op": ("連結営業利益", "営業利益"),
        "ordinary": ("連結経常利益", "経常利益"),
        "net": (
            "親会社株主に帰属する四半期純利益",
            "親会社株主に帰属する中間純利益",
            "親会社株主に帰属する当期純利益",
            "四半期純利益", "中間純利益", "当期純利益",
        ),
    }

    out = {}
    for key, names in aliases.items():
        val = None
        for name in names:
            # [\s\S] で改行を含める。最短一致で当該利益の直後の増減率を拾う。
            pat = (
                re.escape(name)
                + r"[\s\S]{0,180}?"
                + r"(?:前年同期比|前年同四半期比|前年中間期比|同)\s*"
                + r"(-?\d+(?:\.\d+)?)\s*%\s*(増|減)"
            )
            m = re.search(pat, X)
            if m:
                x = float(m.group(1))
                val = x if m.group(2) == "増" else -x
                break
        out[key] = val

    if out.get("op") is not None:
        return out, "短信本文"
    return {}, ""


def _parse_compact_actual_row(title: str, text: str) -> tuple[dict, str]:
    """
    v19: 1ページ目の当期行を、年度/Qラベルの直後から8数値
    [売上,売上YoY,営業,営業YoY,経常,経常YoY,純益,純益YoY]
    として読む。
    """
    X = _norm_numtext(text)
    compact = re.sub(r"\s+", "", X)

    t = _norm_numtext(title)
    mt = re.search(r"(20\d{2})年(\d{1,2})月期", re.sub(r"\s+","",t))
    if not mt:
        return {}, ""
    fy, fm = int(mt.group(1)), int(mt.group(2))

    if "第1四半期" in t:
        suffix = r"第1四半期"
    elif "第2四半期" in t or "中間" in t:
        suffix = r"(?:第2四半期(?:\(中間期\))?|中間期)"
    elif "第3四半期" in t:
        suffix = r"第3四半期"
    else:
        suffix = ""

    label = rf"{fy}年{fm}月期{suffix}"
    m = re.search(label, compact)
    if not m:
        return {}, ""

    seg = compact[m.end():m.end()+500]
    # 次年度/前年行の開始で切る
    nxt = re.search(r"20\d{2}年\d{1,2}月期", seg)
    if nxt and nxt.start() > 20:
        seg = seg[:nxt.start()]

    # 金額はカンマ付き、率は小数。改行消失でも交互に並ぶ。
    pair_re = re.compile(
        r"(?P<amt>\d{1,3}(?:,\d{3})+|\d+)"
        r"(?P<pct>-?\d+(?:\.\d+)?)"
    )
    pairs = []
    for pm in pair_re.finditer(seg):
        try:
            amt = float(pm.group("amt").replace(",",""))
            pct = float(pm.group("pct"))
        except Exception:
            continue
        # 財務額らしさ・YoYらしさ
        if amt >= 1 and -1000 <= pct <= 5000:
            pairs.append((amt,pct))
        if len(pairs) >= 4:
            break

    if len(pairs) < 4:
        return {}, ""

    return {
        "sales": pairs[0][1],
        "op": pairs[1][1],
        "ordinary": pairs[2][1],
        "net": pairs[3][1],
    }, "短信標準表"


def _parse_current_full_year_forecast(text: str) -> dict:
    """
    決算短信1ページ目の通期予想行:
      通期6,40068.43,03071.83,03973.01,98772.3235.37
    から営業利益・経常利益・成長率を読む。
    """
    X = _norm_numtext(text).replace(" ", "")
    pat = re.compile(
        r"通期"
        rf"(?P<sales>{_AMOUNT_RE})(?P<sales_pct>{_PCT_RE})"
        rf"(?P<op>{_AMOUNT_RE})(?P<op_pct>{_PCT_RE})"
        rf"(?P<ord>{_AMOUNT_RE})(?P<ord_pct>{_PCT_RE})"
        rf"(?P<net>{_AMOUNT_RE})(?P<net_pct>{_PCT_RE})"
    )
    matches = list(pat.finditer(X))
    if not matches:
        return {}

    # 1ページ目の「3.連結業績予想」付近の最後の通期行が通常本命。
    m = matches[-1]

    def amt(name):
        try:
            return float(m.group(name).replace(",",""))
        except Exception:
            return None

    def pct(name):
        try:
            return float(m.group(name).replace("△","-"))
        except Exception:
            return None

    return {
        "sales": amt("sales"),
        "sales_growth_pct": pct("sales_pct"),
        "op": amt("op"),
        "op_growth_pct": pct("op_pct"),
        "ordinary": amt("ord"),
        "ordinary_growth_pct": pct("ord_pct"),
        "net": amt("net"),
        "net_growth_pct": pct("net_pct"),
    }


def _extract_full_year_revision_rows(text: str) -> dict:
    """
    通期業績予想修正表を空白・改行除去後に読む。

    PDF抽出では
      今回修正予想(B)5,8002,6602,6701,750207.29
    のように列間区切りが消えるため、カンマ付き金額トークンを
    独立して切り出す。
    """
    X = _norm_numtext(text)
    compact = re.sub(r"\s+", "", X)

    hits = list(re.finditer(r"通期(?:連結)?業績予想(?:数値)?の修正", compact))
    if hits:
        sec = compact[hits[-1].start():]
    else:
        hits = list(re.finditer(r"通期(?:連結)?業績予想", compact))
        if not hits:
            return {}
        sec = compact[hits[-1].start():]

    def grab(label_regex: str) -> Optional[list[float]]:
        m = re.search(label_regex, sec)
        if not m:
            return None
        seg = sec[m.end():]
        n = re.search(
            r"(前回発表予想|前回予想|今回修正予想|今回予想|"
            r"増減額|増減率|ご参考|参考|前期実績)",
            seg
        )
        if n and n.start() > 0:
            seg = seg[:n.start()]

        # 「5,8002,6602,6701,750207.29」から
        # 5,800 / 2,660 / 2,670 / 1,750 / 207.29 を取れる。
        toks = re.findall(
            r"\d{1,3}(?:,\d{3})+|\d+\.\d+|\d+",
            seg
        )
        vals = []
        for raw in toks:
            try:
                v = float(raw.replace(",", ""))
            except Exception:
                continue
            # 百万円単位の4本を優先。EPSは5番目。
            if abs(v) >= 10:
                vals.append(v)
            if len(vals) >= 5:
                break
        return vals if len(vals) >= 4 else None

    prev = grab(r"(?:前回発表予想|前回予想)\(?A\)?")
    new = grab(r"(?:今回修正予想|今回予想)\(?B\)?")
    if not prev or not new:
        return {}

    return {
        "previous_sales": prev[0],
        "previous_op": prev[1],
        "previous_ordinary": prev[2],
        "previous_net": prev[3],
        "new_sales": new[0],
        "new_op": new[1],
        "new_ordinary": new[2],
        "new_net": new[3],
    }


def _explicit_revision_status(earnings_body: str, companion_titles: str, companion_body: str) -> Optional[bool]:
    """
    今回「業績予想修正」が本当にある時だけrevision計算を許可。
    決算短信に
      直近に公表されている業績予想からの修正の有無：有/無
    があればそれを最優先。
    """
    X = _norm_numtext(earnings_body).replace(" ","")
    m = re.search(r"業績予想からの修正の有無[:：]?(有|無)", X)
    if m:
        return m.group(1) == "有"

    C = str(companion_titles or "") + "\n" + str(companion_body or "")
    if any(k in C for k in ("業績予想の修正","通期連結業績予想の修正","業績予想及び通期")):
        return True
    return None


def _latest_prior_full_year_forecast_from_docs(
    conn: sqlite3.Connection,
    code: str,
    pub: str,
) -> tuple[Optional[float], Optional[float], str]:
    """
    発表直前の会社通期予想をTDnet本文から復元。
    最新文書から順に、
      1) 業績予想修正PDFの今回修正予想(B)
      2) 本決算短信の翌期通期予想
    を採用する。
    """
    if not _table_exists(conn, "tdnet_documents"):
        return None, None, ""

    rows = conn.execute(
        """
        SELECT 提出時刻, タイトル, 本文
        FROM tdnet_documents
        WHERE コード=?
          AND 提出時刻 < ?
          AND 本文 IS NOT NULL
          AND (
               タイトル LIKE '%業績予想%'
            OR タイトル LIKE '%予想の修正%'
            OR タイトル LIKE '%決算短信%'
          )
        ORDER BY 提出時刻 DESC
        LIMIT 50
        """,
        (code, pub)
    ).fetchall()

    for p,t,b in rows:
        title = str(t or "")
        body = str(b or "")

        if "予想" in title and "修正" in title:
            d = _extract_full_year_revision_rows(body)
            op = _sf(d.get("new_op"))
            ordinary = _sf(d.get("new_ordinary"))
            if op is not None:
                return op, ordinary, f"{str(p)[:10]}修正PDF"

        if "決算短信" in title:
            d = _parse_current_full_year_forecast(body)
            op = _sf(d.get("op"))
            ordinary = _sf(d.get("ordinary"))
            if op is not None:
                return op, ordinary, f"{str(p)[:10]}本決算"

    return None, None, ""


def _canonical_current_signals(
    conn: sqlite3.Connection,
    code: str,
    pub: str,
    title: str,
    earnings_body: str,
    companion_titles: str,
    companion_body: str,
) -> tuple[dict,list[str]]:
    """
    v16 canonical:
      - 今回実績YoY: 決算短信標準表を最優先
      - 今回通期予想: 決算短信の通期行
      - 修正幅: 「今回修正あり」の時だけ、発表直前TDnet通期予想と比較
      - XBRLがあれば補助だが、無くても成立する
    """
    base = _parse_release_signals(title, earnings_body)
    comp = _parse_release_signals(companion_titles, companion_body)
    sig = _merge_event_only_signals(base, comp)
    audit = []

    # 1) 今回実績YoY
    narrative_yoy, narrative_src = _parse_narrative_actual_yoy(earnings_body)
    if narrative_yoy:
        sig["yoy"] = {
            **(sig.get("yoy") or {}),
            **{k:v for k,v in narrative_yoy.items() if v is not None},
        }
        audit.append(
            f"{narrative_src}営業YoY{_sf(sig['yoy'].get('op')):+.1f}%"
        )
    else:
        compact_yoy, compact_src = _parse_compact_actual_row(title, earnings_body)
        if compact_yoy:
            sig["yoy"] = compact_yoy
            audit.append(
                f"{compact_src}営業YoY{_sf(compact_yoy.get('op')):+.1f}%"
            )
        else:
            # XBRLがある場合のみ最後に利用
            xm = _xbrl_current_metrics(conn, code, pub, title)
            cur_op = _sf(xm.get("actual_op"))
            prev_op = _prior_same_period_xbrl_op(conn, code, title, pub)
            if cur_op is not None and prev_op not in (None,0):
                yoy_op = (cur_op/prev_op - 1.0)*100.0
                sig.setdefault("yoy", {})["op"] = yoy_op
                audit.append(f"XBRL営業YoY{yoy_op:+.1f}%")

    # 2) 今回会社通期予想
    current_fc = _parse_current_full_year_forecast(earnings_body)
    cur_op_fc = _sf(current_fc.get("op"))
    cur_ord_fc = _sf(current_fc.get("ordinary"))

    # 修正後の成長率も短信の営業利益成長率を正式採用
    if _sf(current_fc.get("op_growth_pct")) is not None:
        sig["guidance_growth_pct"] = _sf(current_fc.get("op_growth_pct"))

    # 3) 今回修正の有無を明示ゲート
    revision_status = _explicit_revision_status(
        earnings_body, companion_titles, companion_body
    )

    if revision_status is False:
        # 伯東・146Aのように「修正なし」なら架空の修正率を生成しない。
        sig["upward"] = False
        sig["downward"] = False
        sig["revision_pct"] = None
        audit.append("会社予想修正なし")

    elif revision_status is True:
        # まず同日修正PDFに前回(A)/今回(B)があれば直接使う。
        same_day_rev = _extract_full_year_revision_rows(companion_body)
        prev_op = _sf(same_day_rev.get("previous_op"))
        new_op = _sf(same_day_rev.get("new_op"))
        prev_ord = _sf(same_day_rev.get("previous_ordinary"))
        new_ord = _sf(same_day_rev.get("new_ordinary"))
        src = "同日修正PDF"

        # 同日修正PDFがDBに無い/パースできない時は、
        # current短信の通期予想と、発表直前TDnet予想を比較。
        if prev_op in (None,0) or new_op is None:
            prev_op, prev_ord, src = _latest_prior_full_year_forecast_from_docs(
                conn, code, pub
            )
            new_op = cur_op_fc
            new_ord = cur_ord_fc

        # 営業利益を主基準にする
        if new_op is not None and prev_op not in (None,0):
            rp = (new_op/prev_op - 1.0)*100.0
            if -80 <= rp <= 300:
                if rp >= 0.5:
                    sig["upward"] = True
                    sig["downward"] = False
                    sig["revision_pct"] = rp
                    audit.append(f"営業予想修正+{rp:.1f}%[{src}]")
                elif rp <= -0.5:
                    sig["downward"] = True
                    sig["upward"] = False
                    sig["revision_pct"] = abs(rp)
                    audit.append(f"営業予想修正{rp:.1f}%[{src}]")
                else:
                    sig["revision_pct"] = 0.0

        # 経常利益の修正率も監査表示。421Aなら約+13.8%になる。
        if new_ord is not None and prev_ord not in (None,0):
            rp_ord = (new_ord/prev_ord - 1.0)*100.0
            if -80 <= rp_ord <= 300:
                audit.append(f"経常予想修正{rp_ord:+.1f}%[{src}]")

    return sig, audit


def _merge_event_only_signals(base: dict, companion: dict) -> dict:
    """
    companion資料からはイベントだけ採用し、YoYは絶対に上書きしない。
    """
    out = dict(base or {})
    out["yoy"] = dict((base or {}).get("yoy") or {})

    for k in (
        "upward","downward","div_up","div_down","record","black_turn","red_turn",
        "forward_shift","expense_delay","fx","ma","oneoff","orders"
    ):
        out[k] = bool(out.get(k)) or bool((companion or {}).get(k))

    for k in ("revision_pct","guidance_growth_pct","lower_half_growth_pct",
              "progress","margin_delta_pt"):
        if _sf(out.get(k)) is None and _sf((companion or {}).get(k)) is not None:
            out[k] = _sf((companion or {}).get(k))
    return out


def _score_current_release(signals: dict, text: str, term_weights: dict[str,float]) -> tuple[float,float,float,list[str]]:
    """
    current release を 3成分へ落とす:
      surprise 0-30 : 数字・上方修正・成長の強さ
      quality  0-15 : 本業の質（前倒し/一時益等を減点）
      visibility 0-20: 修正後も伸びる、受注、最高益、教師反応語
    """
    s = 0.0
    q = 7.0  # 中立から開始
    v = 3.0
    flags = []
    yoy = signals.get("yoy") or {}

    # 良い決算そのもの
    profs = [x for x in (yoy.get("op"), yoy.get("ordinary"), yoy.get("net")) if x is not None]
    best = max(profs) if profs else None
    worst = min(profs) if profs else None

    if best is not None:
        if best >= 300: s += 14; flags.append(f"利益YoY最大+{best:.0f}%")
        elif best >= 150: s += 12; flags.append(f"利益YoY最大+{best:.0f}%")
        elif best >= 80: s += 10; flags.append(f"利益YoY最大+{best:.0f}%")
        elif best >= 50: s += 7; flags.append(f"利益YoY最大+{best:.0f}%")
        elif best >= 20: s += 4
        elif best > 0: s += 2

        # 営業・経常・純益の複数ラインが同時に強い決算を追加評価
        strong_lines = sum(1 for x in profs if x >= 50)
        positive_lines = sum(1 for x in profs if x > 0)
        if strong_lines >= 3:
            s += 4; q += 2; flags.append("主要3利益そろって強い")
        elif strong_lines >= 2:
            s += 2.5; q += 1
        elif positive_lines >= 3:
            q += 1
    if worst is not None and worst < 0:
        if worst <= -30: s -= 7; q -= 3; flags.append(f"利益YoY悪化{worst:.0f}%")
        else: s -= 3; q -= 1

    sy = yoy.get("sales")
    if sy is not None:
        if sy >= 20: s += 3; q += 1.5
        elif sy >= 5: s += 1.5
        elif sy < 0: s -= 1.5

    # 421A型の核: 今回新たな上方修正 + 修正後も高成長
    if signals.get("upward"):
        s += 8; v += 4; flags.append("今回上方修正")
        rp = _sf(signals.get("revision_pct"))
        if rp is not None:
            if rp >= 20: s += 5
            elif rp >= 10: s += 4
            elif rp >= 5: s += 2
            flags.append(f"修正幅+{rp:.1f}%")

    if signals.get("downward"):
        s -= 18; q -= 5; v -= 4; flags.append("今回下方修正")
    if signals.get("div_up"):
        s += 1.5; v += 1; flags.append("増配/復配")
    if signals.get("div_down"):
        s -= 3; flags.append("減配")
    if signals.get("record"):
        s += 2; v += 2; flags.append("最高益")
    if signals.get("black_turn"):
        s += 3; q += 1; flags.append("黒転")
    if signals.get("red_turn"):
        s -= 6; q -= 3; flags.append("赤転")

    gg = _sf(signals.get("guidance_growth_pct"))
    if gg is not None:
        if gg >= 50: s += 5; v += 4
        elif gg >= 25: s += 4; v += 3
        elif gg >= 10: s += 2; v += 2
        elif gg < 0: s -= 4
        flags.append(f"修正後増益率{gg:.1f}%")

    lh = _sf(signals.get("lower_half_growth_pct"))
    if lh is not None:
        if lh >= 50: s += 4; v += 4
        elif lh >= 20: s += 2.5; v += 2.5
        elif lh > 0: s += 1
        flags.append(f"下期YoY+{lh:.1f}%")

    # 進捗率は絶対値だけでは季節性を誤るので控えめに使用。
    # 大幅な高進捗だけを加点し、季節調整差は呼び出し側で別途補正する。
    pg = _sf(signals.get("progress"))
    if pg is not None:
        if pg >= 90: s += 3; v += 1.5; flags.append(f"進捗{pg:.1f}%")
        elif pg >= 70: s += 2
        elif pg >= 55: s += 1

    md = _sf(signals.get("margin_delta_pt"))
    if md is not None:
        if md >= 5: q += 4; s += 2; flags.append(f"利益率+{md:.1f}pt")
        elif md >= 2: q += 2; s += 1
        elif md <= -3: q -= 4; s -= 2; flags.append(f"利益率{md:.1f}pt")

    if signals.get("orders"):
        q += 2; v += 3; flags.append("受注/引合い")

    # 見た目だけ良い決算を割り引く
    if signals.get("forward_shift"):
        q -= 4.0; s -= 2.0; flags.append("前倒し補正")
    if signals.get("expense_delay"):
        q -= 3.5; s -= 1.5; flags.append("費用後ずれ補正")
    if signals.get("oneoff"):
        q -= 3.0; flags.append("一時要因補正")
    if signals.get("fx"):
        q -= 1.5; flags.append("為替寄与補正")
    if signals.get("ma"):
        q -= 1.0; flags.append("M&A寄与補正")

    # 過去1600件の実反応で語の方向を補正。最大±5点だけ。
    rx = 0.0
    matched = []
    for term, w in (term_weights or {}).items():
        if w and term in (text or ""):
            rx += float(w)
            matched.append((term,float(w)))
    rx = _clamp(rx, -5.0, 5.0)
    v += rx
    if matched and abs(rx) >= 0.5:
        top = sorted(matched, key=lambda z: abs(z[1]), reverse=True)[:3]
        flags.append("教師語:" + ",".join(f"{k}{w:+.1f}" for k,w in top))

    return _clamp(s,0,30), _clamp(q,0,15), _clamp(v,0,20), flags


def _score_next_turn_candidate(
    op_gap: Optional[float], eps_gap: Optional[float],
    reliability: Optional[float], history_n: int,
    avg_attain_pct: Optional[float], min_attain_pct: Optional[float],
    basis: Optional[float], visibility: Optional[float],
    unpriced: Optional[float], op_yoy: Optional[float],
) -> tuple[float, str, float, float, float, float, float, str]:
    """6340型：今期Gapが弱くても次期ガイダンス急回復を狙う別系統スコア。

    既存シンデン（今期上振れ型）とは混ぜない。0-100点。
    """
    def c(x, lo, hi):
        return max(lo, min(hi, x))

    def rebound_one(g):
        if g is None or not math.isfinite(float(g)):
            return 8.0
        g = float(g)
        if g <= -50: return 2.0
        if g < -20:  return 2.0 + ((g + 50.0) / 30.0) * 13.0
        if g <= 0:   return 15.0 + ((g + 20.0) / 20.0) * 10.0
        if g <= 10:  return 25.0 - g
        if g <= 20:  return 15.0 - (g - 10.0) * 0.7
        return 5.0

    op_ok = op_gap is not None and math.isfinite(float(op_gap))
    eps_ok = eps_gap is not None and math.isfinite(float(eps_gap))
    op_reb = rebound_one(op_gap)
    eps_reb = rebound_one(eps_gap)
    if op_ok and eps_ok:
        rebound = c(op_reb * 0.7 + eps_reb * 0.3, 0, 25)
    elif op_ok:
        rebound = c(op_reb, 0, 25)
    else:
        rebound = c(eps_reb, 0, 25)

    rel = float(reliability) if reliability is not None and math.isfinite(float(reliability)) else 0.0
    conf_part = c(rel / 20.0, 0, 1) * 10.0
    min_part = c((float(min_attain_pct) - 70.0) / 30.0, 0, 1) * 8.0 if min_attain_pct is not None and math.isfinite(float(min_attain_pct)) else 0.0
    avg_part = c((float(avg_attain_pct) - 90.0) / 30.0, 0, 1) * 4.0 if avg_attain_pct is not None and math.isfinite(float(avg_attain_pct)) else 0.0
    hist_part = c(float(history_n or 0) / 5.0, 0, 1) * 3.0
    history = c(conf_part + min_part + avg_part + hist_part, 0, 25)

    b = float(basis) if basis is not None and math.isfinite(float(basis)) else 0.0
    v = float(visibility) if visibility is not None and math.isfinite(float(visibility)) else 0.0
    basis_vis = c(c(b / 15.0, 0, 1) * 10.0 + c(v / 20.0, 0, 1) * 15.0, 0, 25)

    unpriced_part = c(float(unpriced), 0, 15) if unpriced is not None and math.isfinite(float(unpriced)) else 0.0

    recovery = 4.0
    if op_yoy is not None and math.isfinite(float(op_yoy)):
        oy = float(op_yoy)
        if oy <= -30: recovery = 0.0
        elif oy <= -10: recovery = 3.0
        elif oy < 0: recovery = 6.0
        elif oy < 10: recovery = 7.0
        elif oy < 20: recovery = 8.5
        else: recovery = 10.0

    total = c(rebound + history + basis_vis + unpriced_part + recovery, 0, 100)
    if not history_n or history_n <= 0: total = min(total, 59.9)
    elif history_n == 1: total = min(total, 64.9)
    elif history_n == 2: total = min(total, 69.9)
    if min_attain_pct is not None and math.isfinite(float(min_attain_pct)) and float(min_attain_pct) < 50:
        total = min(total, 69.9)
    if basis_vis < 10:
        total = min(total, 64.9)

    judge = (
        "S：次期急回復・最重要" if total >= 80 else
        "A：次期転換候補" if total >= 70 else
        "B：監視候補" if total >= 60 else
        "C：弱め" if total >= 50 else
        "D：材料不足"
    )
    def ff(x):
        return "-" if x is None or not math.isfinite(float(x)) else f"{float(x):.1f}"
    reason = (
        f"6340型 / 今期Gap:営業{ff(op_gap)}% EPS{ff(eps_gap)}% / "
        f"反動{rebound:.1f}/25 / 履歴{history:.1f}/25 "
        f"(平均{ff(avg_attain_pct)}% 最低{ff(min_attain_pct)}% {int(history_n or 0)}期) / "
        f"根拠可視{basis_vis:.1f}/25 / 未織込{unpriced_part:.1f}/15 / 回復{recovery:.1f}/10"
    )
    return total, judge, rebound, history, basis_vis, unpriced_part, recovery, reason


def apply_shinden_pattern_metrics(conn: sqlite3.Connection, verbose: bool = True, force_full: bool = False) -> int:
    """
    v9 統合版。

    pre-release:
      会社予想Gap / 過去達成履歴 / 前回までの業績 / 株価先回り
      から「跨ぐ候補」を評価。

    post-release（当日〜2日）:
      今回の決算短信を直接読み、
      良決算 + 新規上方修正 + 修正後成長 + 質補正 + 過去反応教師
      を追加して「アルゴが即反応しやすい決算」を評価。

    信用・出来高・売買代金・空売りは一切スコアに使わない。
    """
    t0 = datetime.now()
    ensure_shinden_schema(conn)
    _ensure_fast_indexes(conn)
    _ensure_runtime_state(conn)
    sc = _cols(conn, "screener")
    fn = _cols(conn, "finance_notes")
    if not sc or not fn:
        if verbose:
            print("[shinden] screener/finance_notes不足")
        return 0

    def s(name, aliases):
        c = next((x for x in aliases if x in sc), None)
        return f's."{c}" AS "{name}"' if c else f'NULL AS "{name}"'

    def f(name, aliases):
        c = next((x for x in aliases if x in fn), None)
        return f'f."{c}" AS "{name}"' if c else f'NULL AS "{name}"'

    # 需給列はSELECTすらしない。
    select = [
        s("コード", ["コード"]),
        s("前日比", ["前日終値比率_raw", "前日終値比率"]),
        f("forecast_op", ["forecast_op"]),
        f("forecast_eps", ["forecast_eps"]),
        f("actual_op", ["actual_operating_profit"]),
        f("actual_eps", ["actual_eps"]),
        f("progress", ["progress_percent"]),
        s("op_yoy", ["直近営業益YoY"]),
        s("sales_yoy", ["直近売上YoY"]),
        s("seasonal_progress_gap", ["季節調整済進捗差分"]),
    ]

    df = pd.read_sql_query(
        "SELECT " + ",".join(select) +
        ' FROM screener s LEFT JOIN finance_notes f '
        'ON CAST(s.コード AS TEXT)=CAST(f.コード AS TEXT)',
        conn
    )
    if df.empty:
        if verbose:
            print("[shinden] 対象銘柄なし")
        return 0

    df["コード"] = df["コード"].astype(str).str.strip()
    all_codes = [c for c in df["コード"].tolist() if c]

    # ----------------------------------------------------------
    # v10 AUTO:
    # 1日の最初は全銘柄（決算前スコアと株価先回りを更新）
    # 同日2回目以降は、新しく決算短信が入った銘柄だけ再採点。
    # ----------------------------------------------------------
    today_s = datetime.now().strftime("%Y-%m-%d")
    last_full_day = _state_get(conn, "last_full_day")
    last_run_ts = _state_get(conn, "last_run_ts")

    incremental = (
        (not force_full)
        and last_full_day == today_s
        and bool(last_run_ts)
    )

    if incremental:
        changed_codes = _codes_with_new_earnings_since(
            conn, last_run_ts, set(all_codes)
        )
        if not changed_codes:
            _state_set(conn, "last_run_ts", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            if verbose:
                print("[shinden] v23 FAST: 同日再実行 / 新規決算短信0銘柄 → 再計算なし")
            return 0
        df = df[df["コード"].isin(changed_codes)].copy()
        codes = changed_codes
        if verbose:
            print(
                f"[shinden] v23 FAST 日内差分 / 新規決算 {len(codes)}銘柄 "
                f"(全体{len(all_codes)}銘柄)"
            )
    else:
        codes = all_codes
        if verbose:
            print(f"[shinden] v23 FAST 全銘柄更新 / 対象 {len(codes)}銘柄")

    _st = time.perf_counter()
    achievement_map = _bulk_achievement_map(conn, codes)
    if verbose: print(f"[shinden][timer] 履歴 {time.perf_counter()-_st:.2f}s")

    _st = time.perf_counter()
    tdnet_text_map = _bulk_tdnet_text_map(conn, codes)
    if verbose: print(f"[shinden][timer] TDnet根拠 {time.perf_counter()-_st:.2f}s")

    _st = time.perf_counter()
    latest_earnings_map = _bulk_latest_earnings_docs(conn, codes)
    if verbose: print(f"[shinden][timer] 最新決算束 {time.perf_counter()-_st:.2f}s")

    _st = time.perf_counter()
    revision_map = _bulk_recent_forecast_revision_map(conn, codes)
    if verbose: print(f"[shinden][timer] 予想修正履歴 {time.perf_counter()-_st:.2f}s")

    _st = time.perf_counter()
    returns_map = _bulk_returns_map(conn, codes)
    if verbose: print(f"[shinden][timer] 株価先回り {time.perf_counter()-_st:.2f}s")

    _st = time.perf_counter()
    term_weights, term_stats = _learn_reaction_term_weights(conn)
    if verbose: print(f"[shinden][timer] 教師語 {time.perf_counter()-_st:.2f}s")

    if verbose:
        posw = sorted(((k,v) for k,v in term_weights.items() if v>0), key=lambda x:x[1], reverse=True)[:5]
        negw = sorted(((k,v) for k,v in term_weights.items() if v<0), key=lambda x:x[1])[:5]
        print(
            f"[shinden] load: 履歴{len(achievement_map)} / TDnet{len(tdnet_text_map)} / "
            f"最新決算{len(latest_earnings_map)} / 修正履歴{len(revision_map)} / 価格{len(returns_map)} / 教師語{len(term_weights)}"
        )
        if posw or negw:
            print("[shinden] 教師語+:", ", ".join(f"{k}{v:+.1f}" for k,v in posw) or "-")
            print("[shinden] 教師語-:", ", ".join(f"{k}{v:+.1f}" for k,v in negw) or "-")

    updates = []
    post_count = 0

    for _, r in df.iterrows():
        code = str(r["コード"]).strip()

        pre_gap, opg, epsg = _score_gap(
            r["forecast_op"], r["actual_op"],
            r["forecast_eps"], r["actual_eps"]
        )
        rel, avg, mn, up, down, conf, reldetail, history_n = _achievement_stats_from_rows(
            achievement_map.get(code, [])
        )
        pre_basis, pre_vis, bh, vh = _score_evidence_from_text(
            tdnet_text_map.get(code, ""),
            _sf(r["op_yoy"]),
            _sf(r["progress"])
        )
        unp, unpdetail = _score_unpriced_from_returns(
            _sf(r["前日比"]),
            returns_map.get(code)
        )

        # ----------------------------------------------------------
        # fresh current release があれば「良い決算そのもの」を直接評価
        # ----------------------------------------------------------
        post_release = False
        post_upward = False
        post_quarter_kind = ""
        current_flags = []
        latest = latest_earnings_map.get(code)
        gap, basis, vis = pre_gap, pre_basis, pre_vis

        if latest:
            pub, title, earnings_body, companion_titles, companion_body = latest
            if _is_fresh_release(pub, max_age_days=2):
                post_release = True
                post_count += 1

                # 数値は決算短信、イベントは同日補助IR。
                sig, canonical_audit = _canonical_current_signals(
                    conn, code, pub, title, earnings_body, companion_titles, companion_body
                )
                post_upward = bool(sig.get("upward"))
                post_quarter_kind = _period_kind_from_title(title)
                if verbose and code in ("421A","7433","146A"):
                    print(
                        f"[shinden][audit] {code} pub={pub[:16]} "
                        f"signals={';'.join(canonical_audit) or 'fallback/no-canonical'}"
                    )

                row_yoy = {}
                yy0 = sig.setdefault("yoy", {})

                table_rev = None
                if _sf(sig.get("revision_pct")) is None:
                    table_rev = _best_revision_signal(companion_body)
                if table_rev is not None:
                    trp = _sf(table_rev.get("revision_pct"))
                    if trp is not None:
                        if trp >= 0.5:
                            sig["upward"] = True
                            sig["downward"] = False
                            sig["revision_pct"] = trp
                        elif trp <= -0.5:
                            sig["downward"] = True
                            sig["upward"] = False
                            sig["revision_pct"] = abs(trp)

                # v19: 修正率はcanonical側だけで確定する。
                # forecast_history単独の古い値からは推測しない。
                # C) screener値は最後のfallbackだけ。
                #    stale値で短信の正しい数値を上書きしない。
                yy = sig.setdefault("yoy", {})
                sc_op = _sf(r.get("op_yoy"))
                sc_sales = _sf(r.get("sales_yoy"))
                if _sf(yy.get("op")) is None and sc_op is not None:
                    yy["op"] = sc_op
                if _sf(yy.get("sales")) is None and sc_sales is not None:
                    yy["sales"] = sc_sales

                # D) 進捗率も現在値で補完
                if _sf(sig.get("progress")) is None and _sf(r.get("progress")) is not None:
                    sig["progress"] = _sf(r.get("progress"))

                # E) 上方修正後の成長率が本文に無ければ、現在会社予想の前年比で補完
                if sig.get("upward") and _sf(sig.get("guidance_growth_pct")) is None and opg is not None:
                    sig["guidance_growth_pct"] = opg

                # 教師語は同日IR全体でよい。ただしYoY抽出には使わない。
                teacher_text = earnings_body + "\n" + companion_body
                cur_surprise, cur_quality, cur_vis, current_flags = _score_current_release(
                    sig, teacher_text, term_weights
                )

                # 季節調整進捗差
                sp_gap_now = _sf(r.get("seasonal_progress_gap"))
                if sp_gap_now is not None:
                    if sp_gap_now >= 25:
                        cur_surprise += 5; cur_quality += 2
                        current_flags.append(f"季節進捗+{sp_gap_now:.1f}pt")
                    elif sp_gap_now >= 15:
                        cur_surprise += 3.5; cur_quality += 1.5
                        current_flags.append(f"季節進捗+{sp_gap_now:.1f}pt")
                    elif sp_gap_now >= 8:
                        cur_surprise += 2
                        current_flags.append(f"季節進捗+{sp_gap_now:.1f}pt")
                    elif sp_gap_now <= -10:
                        cur_surprise -= 4; cur_quality -= 2
                        current_flags.append(f"季節進捗{sp_gap_now:.1f}pt")

                cur_surprise = _clamp(cur_surprise, 0, MAX_GAP)
                cur_quality = _clamp(cur_quality, 0, MAX_BASIS)
                cur_vis = _clamp(cur_vis, 0, MAX_VISIBILITY)

                # 入力ソースを理由欄へ残す
                for x in canonical_audit:
                    if x not in current_flags:
                        current_flags.append(x)
                if not canonical_audit and _sf(sig.get("yoy",{}).get("op")) is not None:
                    current_flags.append(f"営業YoY{_sf(sig['yoy']['op']):+.1f}%")
                if sig.get("upward") and _sf(sig.get("revision_pct")) is not None:
                    # canonical_auditに修正ソースが無い場合だけ簡易表示
                    if not any("予想修正" in z for z in canonical_audit):
                        current_flags.append(f"修正幅+{_sf(sig.get('revision_pct')):.1f}%")

                # current release主役。ただし抽出欠落だけで強い事前Gapを消さない。
                gap = _clamp(
                    max(
                        cur_surprise,
                        pre_gap * 0.85,
                        cur_surprise*0.82 + pre_gap*0.25,
                    ),
                    0, MAX_GAP
                )
                basis = _clamp(cur_quality + pre_basis*0.20, 0, MAX_BASIS)
                vis = _clamp(cur_vis + pre_vis*0.20, 0, MAX_VISIBILITY)

        rel_for_total = rel
        if post_release:
            # 新規上場を「履歴0だから20点欠損」にしない。
            # 強い今回決算だけ、履歴枠の一部を決算確度で代替する。
            if gap >= 26:
                rel_for_total = max(rel, 14.0)
            elif gap >= 22:
                rel_for_total = max(rel, 10.0)
            elif gap >= 18:
                rel_for_total = max(rel, 7.0)

        raw_total = _clamp(gap + rel_for_total + basis + vis + unp, 0, 100)

        # ----------------------------------------------------------
        # pre-releaseだけ履歴/未来Gapの厳格gate。
        # post-releaseは今回決算を直接読めているため、上場歴の浅さで封印しない。
        # ----------------------------------------------------------
        if post_release:
            total = raw_total
            reliability_gate_detail = "post-release: 履歴は加点のみ・上限なし"
            if rel_for_total > rel:
                reliability_gate_detail += f" / 今回決算確度で{rel_for_total:.1f}/20扱い"
            gap_gate_detail = "post-release: 今回決算サプライズで判定"
        else:
            total_after_reliability, reliability_gate_detail = _apply_reliability_gate(
                raw_total, rel, mn, history_n
            )
            total, gap_gate_detail = _apply_gap_gate(
                total_after_reliability, gap, opg, epsg
            )

        # ----------------------------------------------------------
        # pre-release health guard。
        # fresh決算を読めている場合は今回の決算を優先し、前回Qで二重減点しない。
        # ----------------------------------------------------------
        health_gate_detail = "なし"
        if not post_release:
            op_yoy = _sf(r.get("op_yoy"))
            sales_yoy = _sf(r.get("sales_yoy"))
            sp_gap = _sf(r.get("seasonal_progress_gap"))
            health_pen = 0.0
            health_flags = []

            if op_yoy is not None:
                if op_yoy <= -30:
                    health_pen += 18.0; health_flags.append(f"営業益YoY{op_yoy:.1f}%")
                elif op_yoy < 0:
                    health_pen += 10.0; health_flags.append(f"営業益YoY{op_yoy:.1f}%")

            if sp_gap is not None:
                if sp_gap <= -10:
                    health_pen += 15.0; health_flags.append(f"季節調整進捗差{sp_gap:.1f}pt")
                elif sp_gap <= -5:
                    health_pen += 8.0; health_flags.append(f"季節調整進捗差{sp_gap:.1f}pt")

            if sales_yoy is not None and sales_yoy < 0 and op_yoy is not None and op_yoy < 0:
                health_pen += 5.0; health_flags.append(f"売上YoY{sales_yoy:.1f}%")

            total = max(0.0, total - health_pen)
            severe_health = (
                op_yoy is not None and op_yoy < 0 and
                sp_gap is not None and sp_gap <= -5
            )
            if severe_health:
                total = min(total, 59.9)
                health_gate_detail = "C上限: " + ", ".join(health_flags)
            elif health_flags:
                health_gate_detail = f"減点-{health_pen:.1f}: " + ", ".join(health_flags)

        # post-releaseに明確な下方修正/弱い決算がある場合、履歴加点で救済しない。
        if post_release and current_flags:
            joined = " ".join(current_flags)
            if "今回下方修正" in joined:
                total = min(total, 49.9)

        # v23: 決算前と決算直後の尺度を分離。
        # 決算前は内部raw/gate計算を100点尺度のまま完了させ、
        # DBへ保存する表示スコアだけ79点満点へ線形正規化する。
        # これにより順位・相対差を保持しつつ、決算後100点尺度と意味を分離する。
        pre_normalization_detail = "なし"
        if not post_release:
            pre_score_100 = _clamp(total, 0, 100)
            total = _clamp(pre_score_100 * 0.79, 0, 79.0)
            pre_normalization_detail = f"{pre_score_100:.2f}/100→{total:.2f}/79"

        mode = "決算直後" if post_release else "決算前"
        if post_release:
            calibration_detail = (
                f"上方修正={'あり' if post_upward else 'なし'}"
                f"・種別={post_quarter_kind or '-'}"
            )
        else:
            calibration_detail = "決算前"
        dnote = _demand_note(r.to_dict())
        reason = (
            f"主モード:{mode} / "
            f"予想Gap:営業{opg if opg is not None else '-'}% EPS{epsg if epsg is not None else '-'}% / "
            f"今回決算:{','.join(current_flags[:8]) if current_flags else '-'} / "
            f"信頼性:{reldetail} / "
            f"根拠:{','.join(bh[:4]) or '-'} / 可視性:{','.join(vh[:4]) or '-'} / "
            f"未織込:{unpdetail} / "
            f"信頼性ゲート:{reliability_gate_detail} / "
            f"Gapゲート:{gap_gate_detail} / "
            f"直近ヘルス:{health_gate_detail} / "
            f"決算前スコア正規化:{pre_normalization_detail} / "
            f"実反応校正:{calibration_detail}"
        )

        avg_pct = avg * 100 if avg is not None else None
        min_pct = mn * 100 if mn is not None else None
        (
            next_total, next_judge, next_rebound, next_history, next_basis_vis,
            next_unpriced, next_recovery, next_reason
        ) = _score_next_turn_candidate(
            opg, epsg, rel, history_n, avg_pct, min_pct,
            pre_basis, pre_vis, unp, _sf(r.get("op_yoy"))
        )

        updates.append((
            total, _label(
                total,
                history_n,
                post_release=post_release,
                upward=post_upward,
                quarter_kind=post_quarter_kind,
            ),
            gap, rel, basis, vis, unp,
            conf, history_n, opg, epsg,
            avg * 100 if avg is not None else None,
            mn * 100 if mn is not None else None,
            reason, dnote,
            next_total, next_judge, next_rebound, next_history, next_basis_vis,
            next_unpriced, next_recovery, next_reason,
            code
        ))

    conn.executemany("""
        UPDATE screener SET
          シンデン総合スコア=?,シンデン判定=?,予想ギャップスコア=?,予想信頼性スコア=?,
          予想根拠スコア=?,予想可視性スコア=?,未織り込みスコア=?,予想達成履歴信頼度=?,
          予想達成履歴期数=?,営業益予想ギャップ_pct=?,EPS予想ギャップ_pct=?,
          予想平均達成率_pct=?,予想最低達成率_pct=?,
          シンデン判定理由=?,シンデン需給注釈=?,
          次期転換期待スコア=?,次期転換判定=?,反動余地スコア=?,履歴土台スコア=?,
          根拠可視性スコア=?,次期未織込スコア=?,回復兆候スコア=?,次期転換理由=?
        WHERE CAST(コード AS TEXT)=?
    """, updates)
    conn.commit()

    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _state_set(conn, "last_run_ts", now_s)
    if not incremental:
        _state_set(conn, "last_full_day", datetime.now().strftime("%Y-%m-%d"))

    if verbose:
        elapsed = (datetime.now() - t0).total_seconds()
        print(
            f"[shinden] v23 FAST 更新={len(updates)}銘柄 / fresh決算={post_count}銘柄 / "
            f"{elapsed:.2f}秒"
        )
    return len(updates)


# ----------------------------------------------------------------------
# v23: 過去の決算後反応でスコア帯を校正
# ----------------------------------------------------------------------
def _pick_col(cols: set[str], candidates: tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _reaction_schema(conn: sqlite3.Connection) -> dict[str, Optional[str]]:
    cols = _cols(conn, "earnings_reaction_labels")
    return {
        "code": _pick_col(cols, ("コード","code")),
        "pub": _pick_col(cols, ("発表日時","提出時刻","published_at")),
        "title": _pick_col(cols, ("タイトル","title")),
        "label": _pick_col(cols, ("反応ラベル","reaction_label","label")),
        "d1": _pick_col(cols, ("D1騰落率","翌日騰落率","D1_pct","d1_pct","D1")),
        "d3high": _pick_col(cols, ("D3高値騰落率","3日高値騰落率","D3high_pct","d3_high_pct","D3high")),
        "kind": _pick_col(cols, ("決算種別","quarter_type","種別")),
        "after": _pick_col(cols, ("引け後","after_close")),
    }


def _load_validation_events(
    conn: sqlite3.Connection,
    limit: Optional[int]=None,
) -> list[dict]:
    """
    earnings_reaction_labels と hydrated済み tdnet_documents を exact join。
    本文が取れている過去決算だけを検証対象にする。
    """
    if not (_table_exists(conn, "earnings_reaction_labels") and _table_exists(conn, "tdnet_documents")):
        return []

    rs = _reaction_schema(conn)
    if not all((rs["code"], rs["pub"], rs["label"])):
        return []

    rcols = _cols(conn, "earnings_reaction_labels")
    dcols = _cols(conn, "tdnet_documents")
    if not {"コード","提出時刻","タイトル","本文"}.issubset(dcols):
        return []

    code = rs["code"]; pub = rs["pub"]; lab = rs["label"]
    title_expr = f'r."{rs["title"]}"' if rs["title"] else "NULL"
    d1_expr = f'r."{rs["d1"]}"' if rs["d1"] else "NULL"
    d3_expr = f'r."{rs["d3high"]}"' if rs["d3high"] else "NULL"
    kind_expr = f'r."{rs["kind"]}"' if rs["kind"] else "NULL"

    after_where = ""
    if rs["after"]:
        after_where = f' AND COALESCE(r."{rs["after"]}",1)=1 '

    # titleが存在するテーブルは exact title join、無ければ code+timestamp。
    if rs["title"]:
        join_title = f' AND r."{rs["title"]}"=d.タイトル '
    else:
        join_title = ""

    lim = f" LIMIT {int(limit)}" if limit and int(limit) > 0 else ""

    sql = f"""
        SELECT
          r."{code}" AS code,
          r."{pub}" AS pub,
          {title_expr} AS reaction_title,
          d.タイトル AS doc_title,
          d.本文 AS body,
          r."{lab}" AS reaction_label,
          {d1_expr} AS d1,
          {d3_expr} AS d3high,
          {kind_expr} AS kind
        FROM earnings_reaction_labels r
        JOIN tdnet_documents d
          ON CAST(r."{code}" AS TEXT)=CAST(d.コード AS TEXT)
         AND r."{pub}"=d.提出時刻
         {join_title}
        WHERE d.本文 IS NOT NULL
          AND length(d.本文)>=50
          AND d.タイトル LIKE '%決算短信%'
          AND d.タイトル NOT LIKE '%訂正%'
          {after_where}
        ORDER BY r."{pub}" ASC
        {lim}
    """
    rows = conn.execute(sql).fetchall()
    out = []
    for row in rows:
        out.append({
            "code": str(row[0] or "").strip(),
            "pub": str(row[1] or ""),
            "reaction_title": str(row[2] or ""),
            "title": str(row[3] or ""),
            "body": str(row[4] or ""),
            "label": int(_sf(row[5]) or 0),
            "d1": _sf(row[6]),
            "d3high": _sf(row[7]),
            "kind": str(row[8] or ""),
        })
    return out


def _same_day_companion_docs(
    conn: sqlite3.Connection,
    code: str,
    pub: str,
    current_title: str,
) -> tuple[str,str]:
    day = str(pub)[:10]
    rows = conn.execute("""
        SELECT タイトル,本文
        FROM tdnet_documents
        WHERE コード=?
          AND substr(提出時刻,1,10)=?
          AND 本文 IS NOT NULL
          AND タイトル<>?
          AND タイトル NOT LIKE '%訂正%'
        ORDER BY 提出時刻 ASC
    """, (code,day,current_title)).fetchall()
    titles, bodies = [], []
    for t,b in rows:
        tt = str(t or "")
        if not any(k in tt for k in (
            "業績予想","予想の修正","上方修正","下方修正",
            "配当予想","増配","減配","決算説明","説明資料",
            "中期経営","中期計画","事業計画","受注","大型案件"
        )):
            continue
        titles.append(f"[{tt}]")
        bodies.append(f"=== {tt} ===\n{str(b or '')}")
    return "\n".join(titles), "\n\n".join(bodies)


def _historical_price_rows(conn: sqlite3.Connection, code: str) -> pd.DataFrame:
    """
    1銘柄につき1回だけ全価格履歴をロードする用途。
    validate時のイベント毎SQLを避ける。
    """
    if not _table_exists(conn, "price_history"):
        return pd.DataFrame()
    pc = _cols(conn, "price_history")
    cc = "コード" if "コード" in pc else None
    dc = "日付" if "日付" in pc else ("date" if "date" in pc else None)
    cl = "終値" if "終値" in pc else ("close" if "close" in pc else None)
    hi = "高値" if "高値" in pc else ("high" if "high" in pc else None)
    if not all((cc,dc,cl)):
        return pd.DataFrame()
    hiexpr = f'"{hi}"' if hi else f'"{cl}"'
    df = pd.read_sql_query(
        f'SELECT "{dc}" d,"{cl}" c,{hiexpr} h FROM price_history '
        f'WHERE "{cc}"=? ORDER BY "{dc}" ASC',
        conn, params=(code,)
    )
    if df.empty:
        return df
    df["d"] = pd.to_datetime(df["d"], errors="coerce")
    df["c"] = pd.to_numeric(df["c"], errors="coerce")
    df["h"] = pd.to_numeric(df["h"], errors="coerce").fillna(df["c"])
    return df.dropna(subset=["d","c"]).reset_index(drop=True)


def _historical_realized_reaction(
    prices: pd.DataFrame,
    pub: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    reaction tableにD1/D3列が無い場合の価格履歴fallback。

    引け後決算:
      base = 発表日終値
      D1   = 次営業日終値 / base - 1
      D3high = 次3営業日の高値最大 / base - 1
    """
    if prices is None or prices.empty:
        return None, None

    dt = _parse_pub_dt(pub)
    if dt is None:
        return None, None

    day = pd.Timestamp(dt.date())
    before = prices[prices["d"] <= day]
    after = prices[prices["d"] > day].sort_values("d", ascending=True)

    if before.empty or after.empty:
        return None, None

    base = _sf(before.iloc[-1]["c"])
    if base in (None, 0):
        return None, None

    d1_close = _sf(after.iloc[0]["c"])
    d1 = None if d1_close is None else (d1_close/base - 1.0)*100.0

    g3 = after.head(3)
    d3 = None
    if not g3.empty:
        h = pd.to_numeric(g3["h"], errors="coerce").max()
        if pd.notna(h):
            d3 = (float(h)/base - 1.0)*100.0

    return d1, d3


def _historical_unpriced(
    prices: pd.DataFrame,
    pub: str,
) -> tuple[float,str]:
    """
    引け後決算なので発表日終値までを「事前価格」として使う。
    """
    if prices is None or prices.empty:
        return 0.0, "価格履歴なし"

    dt = _parse_pub_dt(pub)
    if dt is None:
        return 0.0, "発表日時不明"

    d = pd.Timestamp(dt.date())
    g = prices[prices["d"] <= d].tail(70).copy()
    if g.empty:
        return 0.0, "価格履歴なし"

    # _score_unpriced_from_returns は新しい順のreturnsを想定するため同じ定義を再現。
    g = g.sort_values("d", ascending=False).reset_index(drop=True)
    cur = float(g.iloc[0]["c"])

    def ret(n):
        if len(g) <= n:
            return None
        old = float(g.iloc[n]["c"])
        if old == 0:
            return None
        return (cur/old-1.0)*100.0

    day_pct = ret(1)
    peak20 = dd20 = None
    if len(g) > 20:
        base = float(g.iloc[20]["c"])
        peak = pd.to_numeric(g.iloc[:21]["h"], errors="coerce").max()
        if base != 0 and pd.notna(peak) and float(peak) > 0:
            peak20 = (float(peak)/base-1.0)*100.0
            dd20 = (cur/float(peak)-1.0)*100.0

    return _score_unpriced_from_returns(
        day_pct, (ret(5),ret(20),ret(60),peak20,dd20)
    )


def _historical_reliability(
    achievement_rows: list[tuple],
    pub: str,
) -> tuple[float,str,int]:
    """
    forecast_achievement_history のうち、発表時点より前に終了している年度だけ使う。
    rows: fiscal_key, initial_forecast_op, actual_op, uprev, downrev
    """
    dt = _parse_pub_dt(pub)
    if dt is None:
        return 0.0, "0期", 0

    usable = []
    cutoff = dt.date()
    for fk,f,a,u,d in achievement_rows or []:
        m = re.match(r"(\d{4})-(\d{2})", str(fk or ""))
        if not m:
            continue
        fy, fm = int(m.group(1)), int(m.group(2))
        # fiscal_keyを決算月の末日近似として扱い、発表日より前のみ。
        try:
            end_approx = datetime(fy, fm, 28).date()
        except Exception:
            continue
        if end_approx < cutoff:
            usable.append((f,a,u,d))

    rel, avg, mn, up, down, conf, detail, n = _achievement_stats_from_rows(usable[:5])
    return rel, detail, n


def _load_all_achievement_rows(conn: sqlite3.Connection, codes: list[str]) -> dict[str,list[tuple]]:
    if not codes or not _table_exists(conn, "forecast_achievement_history"):
        return {}
    cols = _cols(conn, "forecast_achievement_history")
    if not {"コード","fiscal_key","initial_forecast_op","actual_op"}.issubset(cols):
        return {}
    up = '"upward_revisions"' if "upward_revisions" in cols else "0"
    down = '"downward_revisions"' if "downward_revisions" in cols else "0"
    out = {}
    # SQLite parameter limit対策
    for i in range(0,len(codes),500):
        chunk = codes[i:i+500]
        q = ",".join("?" for _ in chunk)
        rows = conn.execute(f"""
            SELECT コード,fiscal_key,initial_forecast_op,actual_op,{up},{down}
            FROM forecast_achievement_history
            WHERE コード IN ({q})
            ORDER BY コード,fiscal_key DESC
        """, chunk).fetchall()
        for c,fk,f,a,u,d in rows:
            out.setdefault(str(c).strip(), []).append((fk,f,a,u,d))
    return out


def _historical_post_release_score(
    conn: sqlite3.Connection,
    event: dict,
    term_weights: dict[str,float],
    prices: pd.DataFrame,
    achievement_rows: list[tuple],
) -> dict:
    """
    発表直後時点だけで再構築する v21 検証用スコア。
    現在screenerの値は使わないので、将来情報リークを避ける。
    """
    code = event["code"]
    pub = event["pub"]
    title = event["title"]
    body = event["body"]

    comp_titles, comp_body = _same_day_companion_docs(conn, code, pub, title)
    sig, audit = _canonical_current_signals(
        conn, code, pub, title, body, comp_titles, comp_body
    )

    combined = body + "\n" + comp_body
    cur_gap, cur_quality, cur_vis, flags = _score_current_release(
        sig, combined, term_weights
    )

    yy = sig.get("yoy") or {}
    op_yoy = _sf(yy.get("op"))
    progress = _sf(sig.get("progress"))
    pre_basis, pre_vis, _, _ = _score_evidence_from_text(
        combined, op_yoy, progress
    )

    gap = _clamp(cur_gap, 0, MAX_GAP)
    basis = _clamp(cur_quality + pre_basis*0.20, 0, MAX_BASIS)
    vis = _clamp(cur_vis + pre_vis*0.20, 0, MAX_VISIBILITY)

    rel, rel_detail, history_n = _historical_reliability(
        achievement_rows, pub
    )
    rel_for_total = rel
    if gap >= 26:
        rel_for_total = max(rel, 14.0)
    elif gap >= 22:
        rel_for_total = max(rel, 10.0)
    elif gap >= 18:
        rel_for_total = max(rel, 7.0)

    unpriced, unpriced_detail = _historical_unpriced(prices, pub)

    total = _clamp(gap + rel_for_total + basis + vis + unpriced, 0, 100)

    joined = " ".join(flags)
    if "今回下方修正" in joined:
        total = min(total, 49.9)

    return {
        "score": total,
        "gap": gap,
        "rel": rel,
        "rel_used": rel_for_total,
        "basis": basis,
        "vis": vis,
        "unpriced": unpriced,
        "history_n": history_n,
        "upward": bool(sig.get("upward")),
        "downward": bool(sig.get("downward")),
        "audit": ";".join(audit),
        "flags": ",".join(flags[:8]),
        "unpriced_detail": unpriced_detail,
        "rel_detail": rel_detail,
    }


def _score_bucket(score: float) -> str:
    s = float(score)
    if s < 50: return "<50"
    if s < 55: return "50-55"
    if s < 60: return "55-60"
    if s < 65: return "60-65"
    if s < 70: return "65-70"
    if s < 75: return "70-75"
    if s < 80: return "75-80"
    if s < 85: return "80-85"
    return "85+"


def _print_validation_group(df: pd.DataFrame, title: str) -> None:
    print(f"\n=== {title} ===")
    if df.empty:
        print("対象なし")
        return

    order = ["<50","50-55","55-60","60-65","65-70","70-75","75-80","80-85","85+"]
    rows = []
    for b in order:
        g = df[df["score_bucket"] == b]
        if g.empty:
            continue
        n = len(g)
        pos = float((g["label"] == 2).mean()*100.0)
        neg = float((g["label"] == -2).mean()*100.0)
        d1 = g["d1"].mean() if "d1" in g else float("nan")
        d3 = g["d3high"].mean() if "d3high" in g else float("nan")
        rows.append((b,n,pos,neg,d1,d3,pos-neg))

    print("score帯      n    +5%以上   -5%以下    D1平均   D3高値平均   net")
    for b,n,pos,neg,d1,d3,net in rows:
        d1s = "-" if pd.isna(d1) else f"{d1:+.2f}%"
        d3s = "-" if pd.isna(d3) else f"{d3:+.2f}%"
        print(f"{b:<10} {n:>5}  {pos:>7.1f}%  {neg:>7.1f}%  {d1s:>8}  {d3s:>10}  {net:>+6.1f}pt")


def _recommend_thresholds(df: pd.DataFrame) -> None:
    """
    閾値以上の累積成績からS/A候補を表示。
    自動書換えはしない。まず観測して人間が採用する。
    """
    print("\n=== 閾値以上の累積成績 ===")
    print("閾値    n    +5%以上   -5%以下    D1平均    net")
    candidates = []
    for th in (55,60,65,70,75,80,85):
        g = df[df["score"] >= th]
        if g.empty:
            continue
        n = len(g)
        pos = float((g["label"] == 2).mean()*100.0)
        neg = float((g["label"] == -2).mean()*100.0)
        d1 = g["d1"].mean()
        net = pos-neg
        print(f"{th:>3}+  {n:>5}  {pos:>7.1f}%  {neg:>7.1f}%  {d1:>+8.2f}%  {net:>+6.1f}pt")
        if n >= 50:
            candidates.append((th,n,pos,neg,net))

    if candidates:
        best = max(candidates, key=lambda x: (x[4], x[2], x[1]))
        print(
            f"\n[calibration] n>=50でnet(+5率--5率)最大: "
            f"{best[0]}点以上 / n={best[1]} / +5={best[2]:.1f}% / "
            f"-5={best[3]:.1f}% / net={best[4]:+.1f}pt"
        )



def _segment_threshold_stats(
    df: pd.DataFrame,
    min_n: int=12,
) -> pd.DataFrame:
    """
    上方修正有無 × 決算種別ごとに、各閾値以上の累積成績を作る。
    v22の全体集計だけでは「1Qかつ上方修正なし」等の閾値が見えないため、
    v23で交互作用を直接観測する。
    """
    rows = []
    for upflag, uplabel in ((True,"上方あり"),(False,"上方なし")):
        for kind in ("1Q","2Q/H1","3Q","FY"):
            seg = df[(df["upward"] == upflag) & (df["kind"].astype(str) == kind)]
            if seg.empty:
                continue
            for th in (45,50,55,60,65,70,75,80):
                g = seg[seg["score"] >= th]
                if len(g) < min_n:
                    continue
                pos = float((g["label"] == 2).mean()*100.0)
                neg = float((g["label"] == -2).mean()*100.0)
                d1 = float(g["d1"].mean()) if g["d1"].notna().any() else float("nan")
                d3 = float(g["d3high"].mean()) if g["d3high"].notna().any() else float("nan")
                rows.append({
                    "upward": upflag,
                    "upward_label": uplabel,
                    "kind": kind,
                    "threshold": th,
                    "n": len(g),
                    "pos5_pct": pos,
                    "neg5_pct": neg,
                    "net_pt": pos-neg,
                    "d1_mean": d1,
                    "d3high_mean": d3,
                })
    return pd.DataFrame(rows)


def _print_segment_score_bands(df: pd.DataFrame, min_total_n: int=20) -> None:
    print("\n=== 上方修正 × 決算種別 × スコア帯 ===")
    for upflag, uplabel in ((True,"上方あり"),(False,"上方なし")):
        for kind in ("1Q","2Q/H1","3Q","FY"):
            g = df[(df["upward"] == upflag) & (df["kind"].astype(str) == kind)]
            if len(g) < min_total_n:
                continue
            print(f"\n--- {uplabel} {kind} / n={len(g)} ---")
            _print_validation_group(g, f"{uplabel} {kind}")


def _print_segment_threshold_recommendations(stats: pd.DataFrame) -> None:
    print("\n=== セグメント別 閾値候補 ===")
    if stats.empty:
        print("対象なし")
        return

    print("区分                 推奨閾値    n    +5%以上   -5%以下    D1平均    net")
    for (uplabel,kind), g in stats.groupby(["upward_label","kind"], sort=False):
        # 過学習防止: n>=20を優先。無ければn>=12。
        gg = g[g["n"] >= 20]
        if gg.empty:
            gg = g[g["n"] >= 12]
        if gg.empty:
            continue

        # netを第一、+5率、D1平均、nを順に評価。
        gg = gg.copy()
        gg["_d1"] = gg["d1_mean"].fillna(-999)
        best = gg.sort_values(
            ["net_pt","pos5_pct","_d1","n"],
            ascending=[False,False,False,False]
        ).iloc[0]
        print(
            f"{uplabel+' '+kind:<20} "
            f"{int(best['threshold']):>3}+      "
            f"{int(best['n']):>4}  "
            f"{best['pos5_pct']:>7.1f}%  "
            f"{best['neg5_pct']:>7.1f}%  "
            f"{best['d1_mean']:>+8.2f}%  "
            f"{best['net_pt']:>+6.1f}pt"
        )


def validate_reaction_scores(
    conn: sqlite3.Connection,
    limit: Optional[int]=None,
    output_dir: Optional[str]=None,
) -> int:
    """
    `shinden_logic.py --validate-reactions`

    過去の引け後決算について、発表直後時点の情報だけで
    v21 post-release scoreを再構築し、実際のD1反応と照合する。

    DBのscreenerは変更しない。
    """
    t0 = time.perf_counter()
    rs = _reaction_schema(conn)
    print(
        "[validate][schema] "
        f"label={rs.get('label')} / D1={rs.get('d1')} / "
        f"D3high={rs.get('d3high')} / kind={rs.get('kind')}"
    )

    events = _load_validation_events(conn, limit=limit)
    if not events:
        print("[validate] hydrated済み反応教師が見つかりません")
        return 0

    term_weights, _ = _learn_reaction_term_weights(conn)
    codes = sorted(set(e["code"] for e in events))
    ach = _load_all_achievement_rows(conn, codes)
    price_cache: dict[str,pd.DataFrame] = {}

    print(
        f"[validate] 対象={len(events)}件 / 銘柄={len(codes)} / "
        f"教師語={len(term_weights)}"
    )

    out = []
    for i,e in enumerate(events,1):
        code = e["code"]
        if code not in price_cache:
            price_cache[code] = _historical_price_rows(conn, code)

        try:
            s = _historical_post_release_score(
                conn, e, term_weights,
                price_cache[code], ach.get(code, [])
            )
        except Exception as ex:
            if i <= 5:
                print(f"[validate][WARN] {code} {e['pub']}: {type(ex).__name__}: {ex}")
            continue

        row = {**e, **s}

        # v23: reaction tableのD1/D3列名が未検出でも、
        # price_historyから実現値を再計算して平均リターンを表示する。
        if row.get("d1") is None or row.get("d3high") is None:
            rd1, rd3 = _historical_realized_reaction(
                price_cache[code], e["pub"]
            )
            if row.get("d1") is None:
                row["d1"] = rd1
            if row.get("d3high") is None:
                row["d3high"] = rd3

        row["score_bucket"] = _score_bucket(s["score"])
        out.append(row)

        if i % 100 == 0 or i == len(events):
            print(f"[validate] {i}/{len(events)}")

    df = pd.DataFrame(out)
    if df.empty:
        print("[validate] 再現スコアを作れませんでした")
        return 0

    # labelの意味は既存reaction生成仕様: 2=+5%以上, -2=-5%以下
    _print_validation_group(df, "決算後スコア帯 × 実反応")
    _recommend_thresholds(df)

    # 決算種別別
    if "kind" in df.columns and df["kind"].astype(str).str.len().gt(0).any():
        for kind in ("1Q","2Q/H1","3Q","FY"):
            g = df[df["kind"].astype(str) == kind]
            if len(g) >= 30:
                _print_validation_group(g, f"{kind}のみ")

    # v23: 上方修正有無 × 決算種別の直接集計
    print("\n=== 上方修正 × 決算種別（全スコア） ===")
    print("区分                   n    +5%以上   -5%以下    D1平均   D3高値平均   net")
    kinds = ("1Q","2Q/H1","3Q","FY")
    for upflag, uplabel in ((True,"上方あり"),(False,"上方なし")):
        for kind in kinds:
            g = df[(df["upward"] == upflag) & (df["kind"].astype(str) == kind)]
            if g.empty:
                continue
            n = len(g)
            posr = float((g["label"] == 2).mean()*100.0)
            negr = float((g["label"] == -2).mean()*100.0)
            d1m = g["d1"].mean()
            d3m = g["d3high"].mean()
            net = posr-negr
            print(
                f"{uplabel+' '+kind:<22} {n:>5}  "
                f"{posr:>7.1f}%  {negr:>7.1f}%  "
                f"{d1m:>+8.2f}%  {d3m:>+10.2f}%  {net:>+6.1f}pt"
            )

    # v23: 上方修正 × 決算種別 × スコア帯まで分解
    _print_segment_score_bands(df, min_total_n=20)
    segment_stats = _segment_threshold_stats(df, min_n=12)
    _print_segment_threshold_recommendations(segment_stats)

    # 上方修正あり/なし
    up = df[df["upward"] == True]
    no_up = df[df["upward"] == False]
    if len(up) >= 30:
        _print_validation_group(up, "今回上方修正あり")
    if len(no_up) >= 30:
        _print_validation_group(no_up, "今回上方修正なし")

    out_dir = Path(output_dir) if output_dir else Path(__file__).resolve().parent / "reaction_analysis_v23"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "shinden_post_release_validation.csv"
    summary_path = out_dir / "shinden_post_release_validation_summary.txt"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    segment_csv = out_dir / "shinden_segment_thresholds.csv"
    if 'segment_stats' in locals() and not segment_stats.empty:
        segment_stats.to_csv(segment_csv, index=False, encoding="utf-8-sig")
        print(f"[validate] segment thresholds: {segment_csv}")

    # summaryは機械可読な最低限だけ保存
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("shinden v23 post-release validation\n")
        f.write(f"events={len(df)}\n")
        f.write(f"positive_5pct={(df['label']==2).sum()}\n")
        f.write(f"negative_5pct={(df['label']==-2).sum()}\n")
        f.write(f"d1_mean={df['d1'].mean() if 'd1' in df else float('nan')}\n")
        for th in (55,60,65,70,75,80,85):
            g = df[df["score"]>=th]
            if len(g):
                f.write(
                    f"threshold_{th}: n={len(g)},"
                    f"pos={(g['label']==2).mean()*100:.2f},"
                    f"neg={(g['label']==-2).mean()*100:.2f},"
                    f"d1={g['d1'].mean():.3f}\n"
                )

    print(f"\n[validate] CSV: {csv_path}")
    print(f"[validate] summary: {summary_path}")
    print(f"[validate] 完了: {len(df)}件 / {time.perf_counter()-t0:.1f}秒")
    return len(df)


# ----------------------------------------------------------------------
# 単独実行CLI（日次バッチ用）
# ----------------------------------------------------------------------
def _resolve_default_db_path() -> str:
    """
    優先順位:
      1) 環境変数 SHINDEN_DB_PATH / KABU_DB_PATH
      2) shinden_logic.py と同階層の db/kani2.db
      3) 既知の作業パス候補
    """
    candidates = []

    env_path = (os.environ.get("SHINDEN_DB_PATH", "") or os.environ.get("KABU_DB_PATH", "")).strip()
    if env_path:
        candidates.append(Path(env_path))

    here = Path(__file__).resolve().parent
    candidates.extend([
        here / "db" / "kani2.db",
        here.parent / "db" / "kani2.db",
        Path(r"D:\kabu\main\1-スクリーニング自動化プログラム\main\db\kani2.db"),
        Path(r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"),
    ])

    for p in candidates:
        try:
            if p.exists():
                return str(p)
        except Exception:
            pass

    return str(here / "db" / "kani2.db")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="シンデン型スコアを単独更新する外部判定エンジン"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="kani2.db のパス。省略時はSHINDEN_DB_PATHまたは既知パスから自動検出"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="進捗ログを最小化"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="同日でも全銘柄を強制再計算。LIVE_MATERIALSでは価格先回り反映のため使用"
    )
    parser.add_argument(
        "--validate-reactions",
        action="store_true",
        help="過去の決算後スコアを再現し、実際の翌日反応でスコア帯を校正する（screenerは変更しない）"
    )
    parser.add_argument(
        "--validate-limit",
        type=int,
        default=None,
        help="検証件数の上限。動作確認用。省略時はhydrated済み全件"
    )
    parser.add_argument(
        "--validate-output",
        default=None,
        help="検証CSV/summaryの出力フォルダ。省略時はreaction_analysis_v23"
    )
    args = parser.parse_args(argv)

    db_path = str(args.db or _resolve_default_db_path())
    if not os.path.exists(db_path):
        print(f"[shinden][ERROR] DBが見つかりません: {db_path}")
        print(r'例: py shinden_logic.py --db "D:\kabu\main\1-スクリーニング自動化プログラム\main\db\kani2.db"')
        return 2

    conn = sqlite3.connect(db_path, timeout=60.0)
    try:
        conn.execute("PRAGMA busy_timeout=60000;")

        if args.validate_reactions:
            print(f"[shinden] v23 反応実績校正開始: {db_path}")
            n = validate_reaction_scores(
                conn,
                limit=args.validate_limit,
                output_dir=args.validate_output,
            )
            print(f"[shinden] v23 反応実績校正完了: {n}件")
            return 0

        print(f"[shinden] 日次バッチ開始: {db_path}")
        n = apply_shinden_pattern_metrics(conn, verbose=not args.quiet, force_full=args.full)
        print(f"[shinden] 日次バッチ完了: {n}銘柄")

        # Task Scheduler / system_jobs から --full で呼ばれた場合、
        # 0件更新は「正常な差分0件」ではなく、入力DB不足・対象消失などの
        # silent failure の可能性が高い。外部オーケストレータへ非0で返す。
        # 通常の増分実行では新規決算0件が正当なので従来どおり0終了。
        if args.full and int(n or 0) <= 0:
            print("[shinden][ERROR] --full 実行なのに更新0件。fresh snapshot として扱いません。")
            return 3
        return 0
    except KeyboardInterrupt:
        print("\\n[shinden] 中断しました")
        return 130
    except Exception as e:
        print(f"[shinden][ERROR] {type(e).__name__}: {e}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

