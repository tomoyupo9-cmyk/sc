# -*- coding: utf-8 -*-

"""
===============================================================================
日本株 汎用・動的適正株価算出エンジン (ローカルDB連携版)
===============================================================================

【目的】
日本株の全銘柄に対して、同一ロジックで適正株価を計算する。

【データ取得元】
    1. ローカルDB (kani2.db の finance_notes テーブル)
       ※別プログラムで取得済みのファンダメンタル・財務データを利用
      ↓
    2. yahooquery (現在株価などの補完)
      ↓
    3. 欠損値対応 (データがない場合は安全にスキップ)

【重要】
株探への直接アクセス（スクレイピング）は行わず、
DBに保存されたキャッシュデータを利用することで高速に動作します。

===============================================================================
"""

# =============================================================================
# 標準ライブラリ
# =============================================================================

import os
import re
import sys
import math
import time
import sqlite3
import logging
import traceback
from pathlib import Path
from datetime import datetime, date
from typing import Any, Optional, Dict, List

# =============================================================================
# 外部ライブラリ
# =============================================================================

import pandas as pd
import numpy as np
from yahooquery import Ticker


# =============================================================================
# 設定
# =============================================================================

DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db"

OUTPUT_DIR = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data"

# 全銘柄コード
MASTER_CODES_PATH = (
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム"
    r"\main\input_data\株コード番号.txt"
)

# yahooquery
YQ_SLEEP = 0.15

# テストモード
TEST_MODE = False
TEST_LIMIT = 50

# =============================================================================
# ログ
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# =============================================================================
# 共通ユーティリティ
# =============================================================================

def safe_float(value: Any) -> Optional[float]:
    """
    数値を安全にfloatへ変換。
    """

    if value is None:
        return None

    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return None
        return float(value)

    if isinstance(value, (int, np.integer)):
        return float(value)

    s = str(value).strip()

    if not s:
        return None

    s = s.replace(",", "")
    s = s.replace("円", "")
    s = s.replace("%", "")
    s = s.replace("倍", "")

    # △・▲など
    s = s.replace("▲", "-")
    s = s.replace("△", "-")

    # ―
    if s in ("-", "－", "―", "--", "N/A", "NA"):
        return None

    try:
        return float(s)
    except Exception:
        return None


def normalize_code(code: Any) -> Optional[str]:
    """
    銘柄コードを4桁へ正規化。
    """

    if code is None:
        return None

    m = re.search(r"\d{4}", str(code))

    if not m:
        return None

    return m.group(0)


def safe_div(a: Any, b: Any) -> Optional[float]:

    a = safe_float(a)
    b = safe_float(b)

    if a is None or b is None:
        return None

    if abs(b) < 1e-12:
        return None

    return a / b


def clamp(value: float, low: float, high: float) -> float:

    return max(low, min(high, value))


def growth_rate(current: Any, previous: Any) -> Optional[float]:

    current = safe_float(current)
    previous = safe_float(previous)

    if current is None or previous is None:
        return None

    if abs(previous) < 1e-12:
        return None

    return current / previous - 1.0


# =============================================================================
# ローカルDBからのデータ取得
# =============================================================================

# =============================================================================
# ローカルDBからのデータ取得
# =============================================================================

class LocalDataFetcher:
    """
    株探取得プログラム（1つ目のコード）が finance_notes テーブルに保存した
    財務データや予想データを読み込むクラス。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def fetch(self, code: str) -> Dict[str, Any]:

        result = {
            "code": code,
            "kabutan_ok": 0, # DBにデータが存在すれば1とする
        }

        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            cur.execute("SELECT * FROM finance_notes WHERE コード = ?", (code,))
            row = cur.fetchone()
            
            if row:
                result["kabutan_ok"] = 1
                
                # DBのカラム名そのまま、自動ですべてを辞書に格納する！
                for key in row.keys():
                    val = row[key]
                    if val is not None:
                        # 適正株価エンジン側の変数名に合わせて一つだけ変換
                        if key == "forecast_op":
                            result["forecast_operating_profit"] = safe_float(val)
                        else:
                            # 数値にできるものは変換して格納
                            result[key] = safe_float(val) if isinstance(val, (int, float, str)) else val

        except sqlite3.OperationalError as e:
            logger.warning("[%s] Local DB fetch error: テーブルが存在しないかアクセスできません (%s)", code, e)
        except Exception as e:
            logger.warning("[%s] Local DB fetch error: %s", code, e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return result
# =============================================================================
# YahooQuery フォールバック
# =============================================================================

class YahooQueryFallback:

    def get(self, code: str) -> Dict[str, Any]:

        result = {}

        symbol = f"{code}.T"

        try:

            ticker = Ticker(
                symbol,
                asynchronous=False,
            )

            # -----------------------------------------------------------------
            # summary_detail
            # -----------------------------------------------------------------

            try:

                summary = ticker.summary_detail

                if isinstance(summary, dict):

                    data = (
                        summary.get(symbol)
                        or summary.get(code)
                    )

                    if isinstance(data, dict):

                        result["current_price"] = safe_float(
                            data.get("regularMarketPrice")
                        )

                        result["trailing_pe"] = safe_float(
                            data.get("trailingPE")
                        )

                        result["forward_pe"] = safe_float(
                            data.get("forwardPE")
                        )

                        result["dividend_yield"] = safe_float(
                            data.get("dividendYield")
                        )

            except Exception:
                pass

            # -----------------------------------------------------------------
            # key_stats
            # -----------------------------------------------------------------

            try:

                stats = ticker.key_stats

                if isinstance(stats, dict):

                    data = (
                        stats.get(symbol)
                        or stats.get(code)
                    )

                    if isinstance(data, dict):

                        if result.get("trailing_pe") is None:

                            result["trailing_pe"] = safe_float(
                                data.get("trailingPE")
                            )

                        if result.get("forward_pe") is None:

                            result["forward_pe"] = safe_float(
                                data.get("forwardPE")
                            )

                        result["roe"] = safe_float(
                            data.get("returnOnEquity")
                        )

                        result["book_value"] = safe_float(
                            data.get("bookValue")
                        )

            except Exception:
                pass

            # -----------------------------------------------------------------
            # price
            # -----------------------------------------------------------------

            try:

                price = ticker.price

                if isinstance(price, dict):

                    data = (
                        price.get(symbol)
                        or price.get(code)
                    )

                    if isinstance(data, dict):

                        if result.get("current_price") is None:

                            result["current_price"] = safe_float(
                                data.get("regularMarketPrice")
                            )

            except Exception:
                pass

        except Exception as e:

            logger.warning(
                "[yahooquery] %s %s",
                code,
                e,
            )

        return result


# =============================================================================
# 適正株価計算エンジン
# =============================================================================

class FairValueEngine:

    """
    汎用的な日本株用適正株価モデル。

    基本思想：

        適正株価
        =
        予想EPS
        ×
        適正PER

    ただしPERを固定しない。

    適正PERは

        成長性
        収益性
        ROE
        財務
        EPS成長
        利益率
        配当

    によって動的に決定する。

    さらにBPSを使ったPBR評価を併用する。
    """

    def calculate(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:

        result = {}

        # =====================================================================
        # 基本データ
        # =====================================================================

        price = data.get("current_price")

        eps = data.get("forecast_eps")

        if eps is None:
            eps = data.get("actual_eps")

        bps = data.get("bps")

        # =====================================================================
        # 成長率
        # =====================================================================

        eps_growth = growth_rate(
            data.get("actual_eps"),
            data.get("prev_eps"),
        )

        operating_growth = growth_rate(
            data.get("actual_operating_profit"),
            data.get("prev_operating_profit"),
        )

        revenue_growth = growth_rate(
            data.get("actual_revenue"),
            data.get("prev_revenue"),
        )

        # =====================================================================
        # 予想利益成長
        # =====================================================================

        forecast_op_growth = growth_rate(
            data.get("forecast_operating_profit"),
            data.get("actual_operating_profit"),
        )

        forecast_net_growth = growth_rate(
            data.get("forecast_net_profit"),
            data.get("actual_net_profit"),
        )

        # =====================================================================
        # 営業利益率
        # =====================================================================

        operating_margin = safe_div(
            data.get("actual_operating_profit"),
            data.get("actual_revenue"),
        )

        forecast_operating_margin = safe_div(
            data.get("forecast_operating_profit"),
            data.get("forecast_revenue"),
        )

        # =====================================================================
        # ROE
        # =====================================================================

        roe = None

        if (
            data.get("actual_net_profit") is not None
            and data.get("equity") is not None
        ):

            roe = safe_div(
                data.get("actual_net_profit"),
                data.get("equity"),
            )

        # YahooのROEがあれば優先
        if data.get("roe") is not None:

            y_roe = safe_float(data.get("roe"))

            if y_roe is not None:

                # Yahooは0.15形式の場合がある
                if abs(y_roe) <= 2:
                    roe = y_roe

        # =====================================================================
        # 成長率統合
        # =====================================================================

        growth_candidates = []

        for value in [
            eps_growth,
            operating_growth,
            revenue_growth,
            forecast_op_growth,
            forecast_net_growth,
        ]:

            if value is not None and np.isfinite(value):

                # 極端な値は除外
                if -0.80 <= value <= 2.00:

                    growth_candidates.append(value)

        if growth_candidates:

            growth_score = (
                sum(growth_candidates)
                / len(growth_candidates)
            )

        else:

            growth_score = 0.0

        # =====================================================================
        # 成長の質
        # =====================================================================

        growth_quality = 0.0

        # EPS成長
        if eps_growth is not None:

            growth_quality += clamp(
                eps_growth * 20,
                -10,
                20,
            )

        # 営業利益成長
        if operating_growth is not None:

            growth_quality += clamp(
                operating_growth * 20,
                -10,
                20,
            )

        # 予想営業利益成長
        if forecast_op_growth is not None:

            growth_quality += clamp(
                forecast_op_growth * 25,
                -15,
                25,
            )

        # =====================================================================
        # 収益性
        # =====================================================================

        profitability_score = 0.0

        if operating_margin is not None:

            profitability_score += clamp(
                operating_margin * 100,
                -20,
                20,
            )

        if forecast_operating_margin is not None:

            profitability_score += clamp(
                forecast_operating_margin * 100,
                -20,
                20,
            )

        if roe is not None:

            profitability_score += clamp(
                roe * 50,
                -20,
                20,
            )

        # =====================================================================
        # 適正PER
        # =====================================================================

        # 日本株全体の基準PER
        base_per = 15.0

        # 成長による加算
        growth_per_adjustment = clamp(
            growth_score * 15,
            -7,
            15,
        )

        # 成長品質
        quality_adjustment = clamp(
            growth_quality * 0.15,
            -6,
            10,
        )

        # 収益性
        profitability_adjustment = clamp(
            profitability_score * 0.12,
            -4,
            7,
        )

        # ROE
        roe_adjustment = 0.0

        if roe is not None:

            roe_adjustment = clamp(
                (roe - 0.08) * 20,
                -3,
                5,
            )

        fair_per = (
            base_per
            + growth_per_adjustment
            + quality_adjustment
            + profitability_adjustment
            + roe_adjustment
        )

        # =====================================================================
        # 特殊調整
        # =====================================================================

        # 赤字
        if eps is not None and eps <= 0:

            fair_per = None

        # 利益率悪化
        if (
            operating_margin is not None
            and forecast_operating_margin is not None
            and forecast_operating_margin
            < operating_margin * 0.70
        ):

            if fair_per is not None:

                fair_per *= 0.85

        # EPS急減
        if (
            eps_growth is not None
            and eps_growth < -0.30
        ):

            if fair_per is not None:

                fair_per *= 0.80

        # =====================================================================
        # PER法
        # =====================================================================

        per_fair_value = None

        if (
            eps is not None
            and fair_per is not None
        ):

            per_fair_value = eps * fair_per

        # =====================================================================
        # PBR法
        # =====================================================================

        pbr_fair_value = None
        fair_pbr = None

        if bps is not None:

            # ROEによって適正PBRを変える
            if roe is not None:

                fair_pbr = clamp(
                    1.0
                    + (roe - 0.08) * 8,
                    0.60,
                    3.50,
                )

            else:

                fair_pbr = 1.0

            pbr_fair_value = bps * fair_pbr

        # =====================================================================
        # PER/PBRの統合
        # =====================================================================

        if (
            per_fair_value is not None
            and pbr_fair_value is not None
        ):

            # EPS成長企業はPER側を重くする
            if growth_score >= 0.15:

                final_fair_value = (
                    per_fair_value * 0.75
                    + pbr_fair_value * 0.25
                )

            elif growth_score <= 0:

                final_fair_value = (
                    per_fair_value * 0.55
                    + pbr_fair_value * 0.45
                )

            else:

                final_fair_value = (
                    per_fair_value * 0.65
                    + pbr_fair_value * 0.35
                )

        elif per_fair_value is not None:

            final_fair_value = per_fair_value

        elif pbr_fair_value is not None:

            final_fair_value = pbr_fair_value

        else:

            final_fair_value = None

        # =====================================================================
        # 保守・標準・強気
        # =====================================================================

        conservative = None
        standard = None
        aggressive = None

        if final_fair_value is not None:

            conservative = final_fair_value * 0.80
            standard = final_fair_value
            aggressive = final_fair_value * 1.20

        # =====================================================================
        # 現在株価との乖離
        # =====================================================================

        upside = None

        if (
            price is not None
            and final_fair_value is not None
            and price > 0
        ):

            upside = (
                final_fair_value / price
                - 1
            )

        # =====================================================================
        # 判定
        # =====================================================================

        if upside is None:

            valuation = "判定不能"

        elif upside >= 0.30:

            valuation = "大幅割安"

        elif upside >= 0.15:

            valuation = "割安"

        elif upside >= -0.10:

            valuation = "適正"

        elif upside >= -0.25:

            valuation = "やや割高"

        else:

            valuation = "割高"

        # =====================================================================
        # 信頼度
        # =====================================================================

        available = 0

        for key in [
            "forecast_eps",
            "actual_eps",
            "prev_eps",
            "forecast_operating_profit",
            "actual_operating_profit",
            "prev_operating_profit",
            "actual_revenue",
            "prev_revenue",
            "bps",
            "equity",
        ]:

            if data.get(key) is not None:

                available += 1

        confidence = clamp(
            available / 10,
            0,
            1,
        )

        # =====================================================================
        # 出力
        # =====================================================================

        result.update({

            "eps_growth": eps_growth,

            "operating_growth": operating_growth,

            "revenue_growth": revenue_growth,

            "forecast_op_growth": forecast_op_growth,

            "forecast_net_growth": forecast_net_growth,

            "growth_score": growth_score,

            "operating_margin": operating_margin,

            "forecast_operating_margin":
                forecast_operating_margin,

            "roe": roe,

            "fair_per": fair_per,

            "fair_pbr": fair_pbr,

            "per_fair_value": per_fair_value,

            "pbr_fair_value": pbr_fair_value,

            "conservative_fair_value":
                conservative,

            "fair_value":
                standard,

            "aggressive_fair_value":
                aggressive,

            "upside":
                upside,

            "valuation":
                valuation,

            "confidence":
                confidence,
        })

        return result


# =============================================================================
# DB
# =============================================================================

class Database:

    def __init__(self, path: str):

        self.path = path

        Path(path).parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.conn = sqlite3.connect(
            path,
            timeout=60,
        )

        self.create_table()

    def create_table(self):

        sql = """
        CREATE TABLE IF NOT EXISTS fair_value (
            code TEXT PRIMARY KEY,

            updated_at TEXT,

            current_price REAL,

            forecast_eps REAL,
            actual_eps REAL,
            prev_eps REAL,

            forecast_revenue REAL,
            actual_revenue REAL,
            prev_revenue REAL,

            forecast_operating_profit REAL,
            actual_operating_profit REAL,
            prev_operating_profit REAL,

            forecast_net_profit REAL,
            actual_net_profit REAL,
            prev_net_profit REAL,

            bps REAL,

            equity_ratio REAL,
            equity REAL,
            assets REAL,

            interest_debt_ratio REAL,

            roe REAL,

            eps_growth REAL,
            operating_growth REAL,
            revenue_growth REAL,

            forecast_op_growth REAL,
            forecast_net_growth REAL,

            growth_score REAL,

            operating_margin REAL,
            forecast_operating_margin REAL,

            fair_per REAL,
            fair_pbr REAL,

            per_fair_value REAL,
            pbr_fair_value REAL,

            conservative_fair_value REAL,
            fair_value REAL,
            aggressive_fair_value REAL,

            upside REAL,

            valuation TEXT,

            confidence REAL,

            kabutan_ok INTEGER,
            yahoo_fallback INTEGER
        )
        """

        self.conn.execute(sql)

        self.conn.commit()

    def save(
        self,
        data: Dict[str, Any],
    ):

        columns = [
            "code",
            "updated_at",
            "current_price",

            "forecast_eps",
            "actual_eps",
            "prev_eps",

            "forecast_revenue",
            "actual_revenue",
            "prev_revenue",

            "forecast_operating_profit",
            "actual_operating_profit",
            "prev_operating_profit",

            "forecast_net_profit",
            "actual_net_profit",
            "prev_net_profit",

            "bps",

            "equity_ratio",
            "equity",
            "assets",

            "interest_debt_ratio",

            "roe",

            "eps_growth",
            "operating_growth",
            "revenue_growth",

            "forecast_op_growth",
            "forecast_net_growth",

            "growth_score",

            "operating_margin",
            "forecast_operating_margin",

            "fair_per",
            "fair_pbr",

            "per_fair_value",
            "pbr_fair_value",

            "conservative_fair_value",
            "fair_value",
            "aggressive_fair_value",

            "upside",

            "valuation",

            "confidence",

            "kabutan_ok",
            "yahoo_fallback",
        ]

        values = [
            data.get(col)
            for col in columns
        ]

        placeholders = ",".join(
            "?"
            for _ in columns
        )

        sql = f"""
        INSERT OR REPLACE INTO fair_value
        (
            {",".join(columns)}
        )
        VALUES
        (
            {placeholders}
        )
        """

        self.conn.execute(
            sql,
            values,
        )

    def commit(self):

        self.conn.commit()

    def close(self):

        self.conn.close()


# =============================================================================
# CSV出力
# =============================================================================

def export_csv(
    db_path: str,
    output_dir: str,
):

    Path(output_dir).mkdir(
        parents=True,
        exist_ok=True,
    )

    conn = sqlite3.connect(
        db_path
    )

    try:

        df = pd.read_sql_query(
            """
            SELECT *
            FROM fair_value
            ORDER BY
                upside DESC
            """,
            conn,
        )

        path = (
            Path(output_dir)
            / "fair_value_all.csv"
        )

        df.to_csv(
            path,
            index=False,
            encoding="utf-8-sig",
        )

        logger.info(
            "CSV出力: %s",
            path,
        )

        # ---------------------------------------------------------------------
        # ランキング
        # ---------------------------------------------------------------------

        rank_cols = [
            "code",
            "current_price",
            "fair_value",
            "upside",
            "fair_per",
            "fair_pbr",
            "eps_growth",
            "operating_growth",
            "valuation",
            "confidence",
        ]

        available = [
            c
            for c in rank_cols
            if c in df.columns
        ]

        ranking = df[
            available
        ].copy()

        ranking_path = (
            Path(output_dir)
            / "fair_value_ranking.csv"
        )

        ranking.to_csv(
            ranking_path,
            index=False,
            encoding="utf-8-sig",
        )

        logger.info(
            "ランキング出力: %s",
            ranking_path,
        )

    finally:

        conn.close()


# =============================================================================
# 銘柄コード読み込み
# =============================================================================

def load_codes(
    path: str,
) -> List[str]:

    if not os.path.exists(path):

        raise FileNotFoundError(
            f"銘柄コードファイルがありません: {path}"
        )

    codes = []

    with open(
        path,
        "r",
        encoding="utf-8-sig",
        errors="ignore",
    ) as f:

        for line in f:

            code = normalize_code(
                line
            )

            if code:

                codes.append(code)

    # 重複除去
    codes = list(
        dict.fromkeys(
            codes
        )
    )

    return codes


# =============================================================================
# 1銘柄処理
# =============================================================================

def process_stock(
    code: str,
    local_db: LocalDataFetcher,
    yahoo: YahooQueryFallback,
    engine: FairValueEngine,
) -> Dict[str, Any]:

    logger.info(
        "[%s] ローカルDBからデータ取得開始",
        code,
    )

    # =====================================================================
    # DB連携
    # =====================================================================

    data = local_db.fetch(code)
    data["yahoo_fallback"] = 0

    # =====================================================================
    # yahooquery (株価や不足データの補完)
    # =====================================================================

    yahoo_needed = [
        "current_price",
        "forecast_eps",
        "bps",
    ]

    need_yahoo = any(
        data.get(key) is None
        for key in yahoo_needed
    )

    if need_yahoo:

        logger.info(
            "[%s] yahooquery fallback",
            code,
        )

        try:

            ydata = yahoo.get(
                code
            )

            for key, value in ydata.items():

                if data.get(key) is None:

                    data[key] = value

            data["yahoo_fallback"] = 1

        except Exception as e:

            logger.warning(
                "[%s] yahoo fallback error: %s",
                code,
                e,
            )

    # =====================================================================
    # 現在株価がなければ終了
    # =====================================================================

    if data.get("current_price") is None:

        logger.warning(
            "[%s] 現在株価取得失敗",
            code,
        )

    # =====================================================================
    # 適正株価
    # =====================================================================

    fair = engine.calculate(
        data
    )

    data.update(
        fair
    )

    data["updated_at"] = (
        datetime.now().isoformat(
            timespec="seconds"
        )
    )

    return data


# =============================================================================
# メイン
# =============================================================================

def main():

    print()
    print("=" * 80)
    print("日本株 汎用・動的適正株価算出エンジン (ローカルDB連携版)")
    print("=" * 80)
    print()

    # -------------------------------------------------------------------------
    # 銘柄コード
    # -------------------------------------------------------------------------

    codes = load_codes(
        MASTER_CODES_PATH
    )

    if TEST_MODE:

        codes = codes[
            :TEST_LIMIT
        ]

    logger.info(
        "処理銘柄数: %s",
        len(codes),
    )

    # -------------------------------------------------------------------------
    # オブジェクト
    # -------------------------------------------------------------------------

    # KabutanClientとParserの代わりにDBから読み込むクラスを使用
    local_db = LocalDataFetcher(DB_PATH)

    yahoo = YahooQueryFallback()

    engine = FairValueEngine()

    db = Database(
        DB_PATH
    )

    # -------------------------------------------------------------------------
    # 全銘柄
    # -------------------------------------------------------------------------

    success = 0
    failed = 0

    started = time.time()

    try:

        for i, code in enumerate(
            codes,
            1,
        ):

            print(
                f"[{i}/{len(codes)}] {code}",
                flush=True,
            )

            try:

                data = process_stock(
                    code,
                    local_db,
                    yahoo,
                    engine,
                )

                db.save(
                    data
                )

                success += 1

                # -------------------------------------------------------------
                # 重要銘柄だけログ
                # -------------------------------------------------------------

                fair = data.get(
                    "fair_value"
                )

                price = data.get(
                    "current_price"
                )

                upside = data.get(
                    "upside"
                )

                if (
                    fair is not None
                    and price is not None
                ):

                    if upside is not None:

                        print(
                            f"    株価={price:.1f} "
                            f"適正={fair:.1f} "
                            f"乖離={upside * 100:+.1f}% "
                            f"{data.get('valuation', '')}"
                        )

            except Exception as e:

                failed += 1

                logger.error(
                    "[%s] ERROR: %s",
                    code,
                    e,
                )

                if os.environ.get(
                    "FAIR_VALUE_DEBUG"
                ):

                    traceback.print_exc()

            # -----------------------------------------------------------------
            # commit
            # -----------------------------------------------------------------

            if i % 50 == 0:

                db.commit()

                elapsed = (
                    time.time()
                    - started
                )

                logger.info(
                    "%s/%s 完了 %.1f秒",
                    i,
                    len(codes),
                    elapsed,
                )

            # -----------------------------------------------------------------
            # アクセス間隔
            # -----------------------------------------------------------------

            # YahooQueryへのアクセスがあった場合のみ少し待機
            time.sleep(
                YQ_SLEEP
            )

    finally:

        db.commit()

        db.close()

    # -------------------------------------------------------------------------
    # CSV
    # -------------------------------------------------------------------------

    export_csv(
        DB_PATH,
        OUTPUT_DIR,
    )

    # -------------------------------------------------------------------------
    # 終了
    # -------------------------------------------------------------------------

    elapsed = (
        time.time()
        - started
    )

    print()
    print("=" * 80)
    print("完了")
    print("=" * 80)
    print(
        f"成功: {success}"
    )
    print(
        f"失敗: {failed}"
    )
    print(
        f"時間: {elapsed:.1f} 秒"
    )
    print()
    print(
        f"DB: {DB_PATH}"
    )
    print(
        f"CSV: {OUTPUT_DIR}"
    )
    print()


# =============================================================================
# 起動
# =============================================================================

if __name__ == "__main__":

    main()