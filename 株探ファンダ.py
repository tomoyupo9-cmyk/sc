import re
# -*- coding: utf-8 -*-
# 株探ファンダ_v11.py（直感評価＆欠損時ロジック対応版）
import argparse
import sys
import os
import sqlite3
import asyncio 
from datetime import datetime
from io import StringIO
from textwrap import fill

import aiohttp 
import pandas as pd
import math
import numpy as np
import matplotlib 
matplotlib.use("Agg") 
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from bs4 import BeautifulSoup

# ===== 定数・日本語フォント自動設定 =====
def _set_japanese_font():
    """matplotlib/その他の日本語表示を設定"""
    preferred = ["Meiryo", "Yu Gothic", "YuGothic", "IPAexGothic",
                 "Noto Sans CJK JP", "Noto Sans JP", "TakaoGothic", "MS Gothic"]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in preferred:
        if name in available:
            rcParams["font.family"] = name
            break
    rcParams["axes.unicode_minus"] = False
_set_japanese_font()

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36")
}

ASYNC_CONCURRENCY_LIMIT = 20

MASTER_CODES_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\株コード番号.txt" 


# デバッグ用HTML出力ヘルパー
def _print_html_snippet_on_error(code: str, html_content: str, func_name: str):
    """HTML解析エラー時にデバッグ用のHTMLスニペットを出力"""
    start_index = html_content.find('<table')
    if start_index != -1:
        snippet = html_content[start_index:start_index + 500]
        print(f"[DEBUG_HTML] {code} in {func_name}: Table snippet: {snippet}...")
    else:
        print(f"[DEBUG_HTML] {code} in {func_name}: Table not found near start. Snippet: {html_content[:500]}...")


# =========================================================
# データ取得関数 (非同期化)
# =========================================================

async def fetch_quarterly_financials(code: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    """3ヵ月決算【実績】テーブルからデータを非同期で取得する (進捗率計算用)"""
    # code は元の数英字コード（例: 8035A, 102A）をそのまま使用
    url = f"https://kabutan.jp/stock/finance?code={code}"
    print(f"[INFO] fetching quarterly data: {url}")
    
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as res:
            res.raise_for_status() 
            html_content = await res.text(encoding=res.get_encoding() or "utf-8")

        soup = BeautifulSoup(html_content, "lxml")
        target = None
        target_heading = soup.find(string=lambda t: t and '3ヵ月決算【実績】' in t)
        
        if target_heading:
            target = target_heading.find_next('table')
        
        if target is None:
            return pd.DataFrame() 

        df = pd.read_html(StringIO(str(target)), flavor="lxml", header=0)[0]
        df = df.drop(columns=[col for col in df.columns if "損益率" in col], errors='ignore')
        
        # 修正済み: TypeErrorを解消
        df.columns = df.columns.str.replace(" ", "", regex=False).str.replace("　", "", regex=False)

        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年同期比|予|通期", na=False)
            df = df[mask].copy()

        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)

        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if "発表日" in df.columns:
            df["発表日"] = pd.to_datetime(df["発表日"].astype(str).str.split(r'\(| ').str[0].str.strip(), format="%y/%m/%d", errors='coerce')
            df.dropna(subset=["発表日"], inplace=True)
            
        df = df.sort_values(by="発表日", ascending=True).reset_index(drop=True)
        return df
    
    except Exception as e:
        _print_html_snippet_on_error(code, html_content if 'html_content' in locals() else "", "fetch_quarterly_financials")
        print(f"[ERROR] {code}: 3ヵ月実績データ解析中にエラーが発生しました - {type(e).__name__}: {e}")
        return pd.DataFrame() 


async def fetch_full_year_financials(code: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    """通期業績推移テーブルから実績データと最新の予想データを非同期で取得する。"""
    # code は元の数英字コード（例: 8035A, 102A）をそのまま使用
    url = f"https://kabutan.jp/stock/finance?code={code}"
    print(f"[INFO] fetching full year actual data: {url}")
    
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as res:
            res.raise_for_status()
            html_content = await res.text(encoding=res.get_encoding() or "utf-8")

        soup = BeautifulSoup(html_content, "lxml")
        tables = soup.find_all('table')
        target = None
        
        for table in tables:
            header_row = table.find('tr')
            if header_row and header_row.find(string=lambda t: t and ('決算期' in t or '通期' in t)):
                target = table
                break
        
        if target is None:
            print(f"[WARN] 通期業績推移テーブルが見つかりませんでした (code={code})。空のDataFrameを返します。")
            return pd.DataFrame() 

        df = pd.read_html(StringIO(str(target)), flavor="lxml", header=0)[0]
        df.columns = df.columns.astype(str).str.replace(r'\s+', '', regex=True)
        
        if "決算期" in df.columns:
            mask = ~df["決算期"].astype(str).str.contains("前期比|前年比", na=False)
            df = df[mask].copy()

        df = df[df["決算期"].notna()].copy()
        df["決算期"] = df["決算期"].astype(str).str.strip()
        df.reset_index(drop=True, inplace=True)

        for col in ["売上高", "営業益", "経常益", "最終益", "修正1株益", "修正1株配"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        df = df.drop(columns=["発表日"], errors='ignore')
        df = df.sort_values(by="決算期", ascending=True).reset_index(drop=True)
        
        return df
    
    except Exception as e:
        _print_html_snippet_on_error(code, html_content if 'html_content' in locals() else "", "fetch_full_year_financials")
        print(f"[ERROR] {code}: 通期実績データ解析中にエラーが発生しました - {type(e).__name__}: {e}")
        return pd.DataFrame() 

# =========================================================
# DB/判定/描画関数
# =========================================================

def _safe_growth(latest, prev):
    if pd.isna(latest) or pd.isna(prev):
        return None
    if prev == 0:
        return 100 if latest > 0 else (0 if latest == 0 else -100)
    return ((latest - prev) / prev) * 100

def _get_overall_verdict_label(score: int) -> str:
    """スコアに基づいて大分類の直感的な評価ラベルを返す"""
    if score >= 8:
        return "超優良 (S++)"
    elif score >= 5:
        return "優良 (A+)"
    elif score >= 2:
        return "成長期待 (B)"
    elif score >= 0:
        return "現状維持 (C)"
    else:
        return "要注意 (D-)"

def _format_verdict_with_progress(qp: tuple[str, float, str, float] | None, raw_verdict: str, score: int) -> str:
    """
    直感評価、進捗率、スコアを判定コメントの先頭に配置するよう修正した関数。
    """
    
    # 1. 直感評価ラベルの生成
    overall_label = _get_overall_verdict_label(score)
    
    # 2. 進捗率ヘッダーの生成
    prog_header = ""
    if qp is not None:
        latest_label, progress, status, _ = qp
        if status == "OK":
            prog_header = f"[{latest_label}：進捗{progress}%]"
        elif status == "予想未発表":
            prog_header = f"[{latest_label}：進捗率 (予想未発表)]"
        elif status == "予想ゼロ":
            prog_header = f"[{latest_label}：進捗率 (予想ゼロ)]"
        elif status == "予想データ欠損":
            prog_header = f"[{latest_label}：進捗率 (予想データ欠損)]"
    
    # 3. スコアヘッダーの生成
    score_header = f"スコア {score} 点"
    
    # 4. コメント本文の抽出と整形
    raw_verdict_safe = raw_verdict or ""
    
    # コメント本文を処理（成長継続コメントの切り出しは不要になりました）
    body_lines = [s.strip() for s in raw_verdict_safe.splitlines() if s.strip()] 
    
    # 5. 最終的な判定コメントの結合 
    
    # 判定コメントのヘッダー部分
    header_parts = [f"【総合評価】{overall_label}"] 
    if prog_header:
        header_parts.append(prog_header)
    header_parts.append(score_header) 
    
    final_comment_lines = []
    final_comment_lines.append(" ".join(header_parts))
    final_comment_lines.append("--- 詳細 ---")
    final_comment_lines.extend(body_lines)

    result = "\n".join(final_comment_lines)
    return result

def judge_and_score_performance(df: pd.DataFrame) -> tuple[str, int]:
    """
    通期実績データに基づき、最新の通期予想と前年度実績を比較し、業績を判定してスコアを計算する。
    通期予想が欠損している場合、直近2期の実績を比較するロジックを追加。
    """
    if df.empty or len(df) < 2:
        return "データ不足のため判定できません。", 0
    
    # 最新のデータが「予」かどうかをチェック
    is_forecast = "予" in df.iloc[-1]["決算期"]
    
    # 比較対象の特定
    if is_forecast:
        # 1. 通期予想（最新） vs. 前期実績（その前）
        df_latest = df.iloc[-1]
        df_prev = df.iloc[-2]
        comparison_period = "通期予想 vs 前期実績"
    else:
        # 2. 最新実績（最新） vs. その前実績（その前）
        if len(df) < 2:
             return "直近2期の実績データがありません。", 0
        df_latest = df.iloc[-1]
        df_prev = df.iloc[-2]
        comparison_period = "直近実績 vs 前期実績 (通期予想データ欠損)"

    latest_sales = df_latest.get("売上高")
    latest_op = df_latest.get("営業益")
    latest_eps = df_latest.get("修正1株益")
    
    prev_sales = df_prev.get("売上高")
    prev_op = df_prev.get("営業益")
    prev_eps = df_prev.get("修正1株益")
    
    op_growth = _safe_growth(latest_op, prev_op)
    eps_growth = _safe_growth(latest_eps, prev_eps)
    sales_growth = _safe_growth(latest_sales, prev_sales)

    msgs = []
    score = 0
    
    msgs.append(f"（比較期間：{comparison_period}）")

    # --- 成長率に基づくスコアリング ---
    
    # EPS
    if eps_growth is not None:
        if eps_growth > 20: score += 3; msgs.append(f"🟢 EPS成長率: {eps_growth:.1f}% (高成長)")
        elif eps_growth > 10: score += 2; msgs.append(f"🟡 EPS成長率: {eps_growth:.1f}% (安定成長)")
        elif eps_growth > 0: score += 1; msgs.append(f"⚪️ EPS成長率: {eps_growth:.1f}% (微増)")
        elif eps_growth == 0: msgs.append(f"⚫️ EPS成長率: 0.0% (横ばい)")
        elif eps_growth < 0: score -= 2; msgs.append(f"🔴 EPS成長率: {eps_growth:.1f}% (減益注意)")
    else: msgs.append("EPS成長率: データ欠損")
    
    # 営業益
    if op_growth is not None:
        if op_growth > 20: score += 2; msgs.append(f"🟢 営業益成長率: {op_growth:.1f}% (高成長)")
        elif op_growth > 10: score += 1; msgs.append(f"🟡 営業益成長率: {op_growth:.1f}% (安定成長)")
        elif op_growth < 0: score -= 1; msgs.append(f"🔴 営業益成長率: {op_growth:.1f}% (減益注意)")
        else: msgs.append(f"⚪️ 営業益成長率: {op_growth:.1f}%")
    else: msgs.append("営業益成長率: データ欠損")
    
    # 売上高
    if sales_growth is not None:
        if sales_growth > 10: score += 1; msgs.append(f"🟢 売上高成長率: {sales_growth:.1f}% (高成長)")
        elif sales_growth < 0: score -= 1; msgs.append(f"🔴 売上高成長率: {sales_growth:.1f}% (減収注意)")
        else: msgs.append(f"⚪️ 売上高成長率: {sales_growth:.1f}%")
    else: msgs.append("売上高成長率: データ欠損")
    
    # --- 総合判定コメント ---
    is_growing = (op_growth is not None and op_growth > 0) and \
                 (eps_growth is not None and eps_growth > 0)
    
    if is_growing and is_forecast:
        msgs.insert(1, "💡 通期予想は増収増益の見込みです。")
    elif is_growing and not is_forecast:
        msgs.insert(1, "💡 直近実績は増収増益でした。")

    if not msgs:
        msgs.append("最新の通期データに主要項目欠損。")
            
    final_verdict = "\n".join(msgs)
    
    return final_verdict, score

def calc_progress_from_df_op(quarterly_df: pd.DataFrame, forecast_series: pd.Series) -> tuple[str, float, str, float] | None:
    """
    四半期実績DFと通期予想Seriesから営業利益の進捗率を計算する
    戻り値に、DB保存用の進捗率の数値 (REAL型) を追加 (4番目の要素)。
    - 正常: 0.0 - 100.0+ の実数値
    - 予想データ欠損: -1.0
    - 予想ゼロ: -2.0
    - 予想未発表: -3.0
    """
    if quarterly_df.empty or "営業益" not in quarterly_df.columns.to_list() or "決算期" not in quarterly_df.columns.to_list():
        return None
    latest_op_row = quarterly_df.tail(1)
    if latest_op_row.empty:
        return None
        
    latest_label = str(latest_op_row["決算期"].iloc[0])
    latest_cumulative_op = quarterly_df["営業益"].sum()
    
    if forecast_series.empty or "営業益" not in forecast_series:
        return (latest_label, 0.0, "予想未発表", -3.0)

    full_op_val = pd.to_numeric(forecast_series.get("営業益"), errors="coerce")
    
    if pd.isna(full_op_val):
        return (latest_label, 0.0, "予想データ欠損", -1.0)
    
    # 予想がゼロの場合、進捗率は計算できないが、DBには特別なコード (-2.0) を入れる
    if full_op_val == 0:
        return (latest_label, 0.0, "予想ゼロ", -2.0)

    progress = latest_cumulative_op / full_op_val * 100.0
    # 正常な進捗率の数値はそのままDB保存用の値とする
    return (latest_label, round(progress, 1), "OK", progress)


def batch_record_to_sqlite(results: list[dict]):
    """
    メインプロセスで全ての実行結果を一括でSQLiteに記録する。
    DBスキーマに progress_percent REAL を追加。
    """
    DB_PATH = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\kani2.db" 
    
    if not results:
        print("[DB] 記録すべき成功結果がありません。")
        return

    try:
        # DB接続タイムアウトを20秒に延長（ロック対策）
        conn = sqlite3.connect(DB_PATH, timeout=20) 
        cur = conn.cursor()

        # 1. テーブル作成（コード TEXT PRIMARY KEY は数英字コードをそのまま受け入れる）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS finance_notes (
          コード TEXT PRIMARY KEY,
          財務コメント TEXT,
          score INTEGER,  
          progress_percent REAL, 
          html_path TEXT,
          updated_at TEXT
        );
        """)
        
        # 2. スキーマのマイグレーション: 既存のテーブルにカラムが存在しない場合に追加
        # ★ ここで ALTER TABLE 実行後に conn.commit() を即座に行うように修正 ★
        
        # score カラムのチェック
        migrated = False
        try:
            cur.execute("SELECT score FROM finance_notes LIMIT 1;")
        except sqlite3.OperationalError:
            print("[DB] スキーマ修正: 'score' カラムを追加します。")
            cur.execute("ALTER TABLE finance_notes ADD COLUMN score INTEGER;")
            migrated = True
            
        # progress_percent カラムのチェック
        try:
            cur.execute("SELECT progress_percent FROM finance_notes LIMIT 1;")
        except sqlite3.OperationalError:
            print("[DB] スキーマ修正: 'progress_percent' カラムを追加します。")
            cur.execute("ALTER TABLE finance_notes ADD COLUMN progress_percent REAL;")
            migrated = True
            
        if migrated:
            conn.commit() # カラム追加の変更をここで確定させる
            print("[DB] スキーマ修正をコミットしました。")


        # 3. データ挿入/更新
        ts = datetime.now().isoformat(timespec="seconds")
        
        data_to_insert = []
        
        for res in results:
            if res.get('status') == 'OK':
                # res['code'] (code_full) をそのまま DBのPRIMARY KEYとして使用
                data_to_insert.append((
                    res['code'], 
                    res['formatted_verdict'] or "", 
                    res['score'], 
                    res['progress_percent'], # 新しい進捗率の数値
                    res['out_html'] or "", 
                    ts
                ))
            
        if data_to_insert:
            cur.executemany("""
            INSERT INTO finance_notes (コード, 財務コメント, score, progress_percent, html_path, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(コード) DO UPDATE SET
              財務コメント   = excluded.財務コメント,
              score          = excluded.score,
              progress_percent = excluded.progress_percent,
              html_path      = excluded.html_path,
              updated_at     = excluded.updated_at;
            """, data_to_insert)

            conn.commit()
            print(f"[DB][INFO] {len(data_to_insert)}件の結果をSQLiteに一括記録しました。")

    except sqlite3.OperationalError as e:
        print(f"[DB][CRITICAL ERROR] SQLite DB操作エラー: DBがロックされています。処理をスキップしました。 - {e}")
    except Exception as e:
        print(f"[DB][CRITICAL ERROR] DB処理中に予期せぬエラーが発生しました: {type(e).__name__}: {e}")
    finally:
        try:
            if 'conn' in locals() and conn:
                conn.close()
        except Exception:
            pass

def export_html(df: pd.DataFrame, code: str, out_html: str = "report.html", qp_result: tuple[str, float, str, float] | None = None, verdict_str: str = ""):
    """
    グラフを生成し、判定結果文字列(verdict_str)をHTMLに書き込む（V5ロジック）。
    qp_result は4要素タプルを受け取るように変更。
    """
    
    # --- X軸ラベルの準備 ---
    x_actual_period = df["決算期"].tolist() if not df.empty and "決算期" in df.columns else []
    x_custom_labels = []
    for period in x_actual_period:
        label = f"{period}"
        if "予" in period:
            label += "\n(通期予想)"
        else:
            label += "\n(実績)" 
        x_custom_labels.append(label)
    x_full_plot = x_custom_labels
    
    # --- グラフ描画開始 ---
    body = ""
    if df.empty:
        print(f"[WARN] code={code}: データがないため、HTML出力はスキップされます。")
        fig = go.Figure().update_layout(title=f"通期実績データなし（code={code}）")
        try:
            body = fig.to_html(include_plotlyjs="cdn", full_html=False)
        except Exception as e:
            raise RuntimeError(f"Plotly HTML生成エラー (fig.to_html, empty DF): {type(e).__name__}: {e}")
            
    else:
        df_growth = df.copy()
        use_cols = [c for c in ["売上高","営業益","修正1株益"] if c in df_growth.columns]
        df_growth = df_growth.set_index("決算期")[use_cols].pct_change(fill_method=None)*100 if use_cols else pd.DataFrame(index=df_growth["決算期"])
        
        qp = qp_result
        
        title_suffix = "" 
        if qp:
            # qpは4要素タプル: (最新決算期, 進捗率表示値, ステータス文字列, 進捗率数値)
            latest_label, progress, status, _ = qp 
            if status == "OK":
                title_suffix = f" / {latest_label}：進捗{progress}%"
            elif status == "予想未発表":
                title_suffix = f" / 進捗率 (予想未発表)"
            elif status == "予想ゼロ":
                title_suffix = f" / 進捗率 (予想ゼロ)"
            elif status == "予想データ欠損":
                 title_suffix = f" / 進捗率 (予想データ欠損)"

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=False,
            specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
            vertical_spacing=0.12,
            subplot_titles=(f"通期実績推移（株探データ, code={code}）{title_suffix}", "成長率推移")
        )
        
        # 1. 売上高（実績 + 予想の棒グラフ）
        if "売上高" in df.columns and df["売上高"].notna().any():
            sales_full = (df["売上高"]/1e2).round(1).tolist()
            colors = ['rgba(173, 216, 230, 0.8)'] * (len(sales_full)-1) 
            if len(sales_full) > 0: colors.append('rgba(173, 216, 230, 0.4)' if pd.notna(sales_full[-1]) else 'rgba(173, 216, 230, 0.2)') 
            fig.add_trace(go.Bar(x=x_full_plot, y=sales_full, name="売上高（億円）", marker_color=colors, width=0.4, customdata=df["決算期"].tolist(), hovertemplate='<b>%{customdata}</b><br>売上高: %{y:.1f}億円<extra></extra>',), row=1, col=1, secondary_y=False)

        # 2. 営業益 (実績 + 予想の線グラフ)
        if "営業益" in df.columns and df["営業益"].notna().any():
            op_full = (df["営業益"]/1e2).round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=op_full[:-1], mode="lines+markers", name="営業益（実績）", marker_symbol="circle", marker_line_color='blue', line=dict(color='blue', dash='solid'), customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>営業益: %{y:.1f}億円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=False)
            if len(op_full) >= 2 and pd.notna(op_full[-1]): 
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[op_full[-2], op_full[-1]], mode="lines", name="営業益（予）接続", line=dict(color='blue', dash='dot'), showlegend=False), row=1, col=1, secondary_y=False)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[op_full[-1]], mode="markers", name="営業益（予）", marker=dict(symbol="circle-open", color='rgba(0,0,0,0)', line=dict(color='blue', width=2)), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>営業益(予): %{y:.1f}億円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=False)

        # 3. EPS (実績 + 予想の線グラフ)
        if "修正1株益" in df.columns and df["修正1株益"].notna().any():
            eps_full = df["修正1株益"].round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=eps_full[:-1], mode="lines+markers", name="EPS（円）", marker_symbol="diamond", marker_line_color='red', line=dict(color='red', dash='solid'), customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>EPS: %{y:.1f}円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=True)
            if len(eps_full) >= 2 and pd.notna(eps_full[-1]): 
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[eps_full[-2], eps_full[-1]], mode="lines", name="EPS（予）接続", line=dict(color='red', dash='dot'), showlegend=False), row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[eps_full[-1]], mode="markers", name="EPS（予）", marker=dict(symbol="diamond-open", color='rgba(0,0,0,0)', line=dict(color='red', width=2)), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>EPS(予): %{y:.1f}円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=True)

        # 4. 配当 (実績 + 予想の線グラフ)
        if "修正1株配" in df.columns and df["修正1株配"].notna().any():
            div_full = df["修正1株配"].round(1).tolist()
            fig.add_trace(go.Scatter(x=x_full_plot[:-1], y=div_full[:-1], mode="lines+markers", name="配当（円）", marker_symbol="x", marker_line_color='green', line=dict(color='green', dash='solid'), customdata=df["決算期"].tolist()[:-1], hovertemplate='<b>%{customdata}</b><br>配当: %{y:.1f}円<extra></extra>', showlegend=True), row=1, col=1, secondary_y=True)
            if len(div_full) >= 2 and pd.notna(div_full[-1]): 
                fig.add_trace(go.Scatter(x=[x_full_plot[-2], x_full_plot[-1]], y=[div_full[-2], div_full[-1]], mode="lines", name="配当（予）接続", line=dict(color='green', dash='dot'), showlegend=False), row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=[x_full_plot[-1]], y=[div_full[-1]], mode="markers", name="配当（予）", marker=dict(symbol="x-open", color='rgba(0,0,0,0)', line=dict(color='green', width=2)), customdata=[df["決算期"].iloc[-1]], hovertemplate='<b>%{customdata}</b><br>配当(予): %{y:.1f}円<extra></extra>', showlegend=False), row=1, col=1, secondary_y=True)

        max_sales_op = df[["売上高", "営業益"]].apply(lambda x: x / 1e2 if x.name in ["売上高", "営業益"] else x).stack().max()
        max_sales_op_range = max_sales_op * 1.1 if max_sales_op > 0.5 else None
        max_eps_div = df[["修正1株益", "修正1株配"]].stack().max()
        max_eps_div_range = max_eps_div * 1.1 if max_eps_div > 0.5 else None
        
        fig.update_xaxes(title_text="決算期", tickangle=0, type='category', ticktext=x_full_plot, tickvals=x_full_plot, row=1, col=1)
        fig.update_yaxes(title_text="金額（億円）", range=[None, max_sales_op_range], row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="EPS / 配当（円）", range=[None, max_eps_div_range], row=1, col=1, secondary_y=True)

        # 成長率グラフ
        x_full_growth_plot = x_full_plot[1:]
        
        if "売上高" in df_growth.columns and df_growth["売上高"].notna().any():
            sales_growth_full = df_growth["売上高"].tolist()
            fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=sales_growth_full[:-1], mode="lines+markers", name="売上高成長率（%）", line_color='blue', hovertemplate='売上高成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
            if len(sales_growth_full) >= 2 and pd.notna(sales_growth_full[-1]):
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[sales_growth_full[-2], sales_growth_full[-1]], mode="lines", name="売上高（予）接続（成長率）", line=dict(dash='dot', color='blue'), hovertemplate='売上高(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[sales_growth_full[-1]], mode="markers", name="売上高成長率（予）", marker_symbol="square-open", marker_line_color="blue", marker_color='rgba(0,0,0,0)', customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>売上高成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True ), row=2, col=1)

        if "営業益" in df_growth.columns and df_growth["営業益"].notna().any():
            op_growth_full = df_growth["営業益"].tolist()
            fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=op_growth_full[:-1], mode="lines+markers", name="営業益成長率（%）", line_color='red', hovertemplate='営業益成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
            if len(op_growth_full) >= 2 and pd.notna(op_growth_full[-1]):
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[op_growth_full[-2], op_growth_full[-1]], mode="lines", name="営業益（予）接続（成長率）", line=dict(dash='dot', color='red'), hovertemplate='営業益(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[op_growth_full[-1]], mode="markers", name="営業益成長率（予）", marker_symbol="circle-open", marker_line_color="red", marker_color='rgba(0,0,0,0)', customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>営業益成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True ), row=2, col=1)

        if "修正1株益" in df_growth.columns and df_growth["修正1株益"].notna().any():
            eps_growth_full = df_growth["修正1株益"].tolist()
            fig.add_trace(go.Scatter(x=x_full_growth_plot[:-1], y=eps_growth_full[:-1], mode="lines+markers", name="EPS成長率（%）", line_color='green', hovertemplate='EPS成長率: %{y:.1f}%%<extra></extra>', showlegend=True), row=2, col=1)
            if len(eps_growth_full) >= 2 and pd.notna(eps_growth_full[-1]):
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-2], x_full_growth_plot[-1]], y=[eps_growth_full[-2], eps_growth_full[-1]], mode="lines", name="EPS（予）接続（成長率）", line=dict(dash='dot', color='green'), hovertemplate='EPS(予)接続 (成長率)<extra></extra>', showlegend=False), row=2, col=1)
                 fig.add_trace(go.Scatter(x=[x_full_growth_plot[-1]], y=[eps_growth_full[-1]], mode="markers", name="EPS成長率（予）", marker_symbol="diamond-open", marker_line_color="green", marker_color='rgba(0,0,0,0)', customdata=[df["決算期"].iloc[-1]], hovertemplate=f'<b>%{{customdata}}</b><br>EPS成長率(予): %{{y:.1f}}%%<extra></extra>', showlegend=True), row=2, col=1)

        fig.update_xaxes(title_text="決算期", tickangle=0, type='category', ticktext=x_full_growth_plot, tickvals=x_full_growth_plot, row=2, col=1)
        fig.update_yaxes(title_text="成長率（%）", row=2, col=1)
        
        fig.update_layout(height=900, legend=dict(orientation="v"), margin=dict(l=60, r=40, t=60, b=100))
        
        try:
            body = fig.to_html(include_plotlyjs="cdn", full_html=False)
        except Exception as e:
            raise RuntimeError(f"Plotly HTML生成エラー (fig.to_html, full DF): {type(e).__name__}: {e}")
    
    # 判定文のHTML埋め込み
    verdict_html_content = verdict_str.replace('\n', '<br>')
    
    verdict_html = (
        f'<div id="verdict_placeholder" style="margin-top:20px; color:#c9302c; font-weight:600; text-align:left; white-space:pre-wrap; border:1px solid #ccc; padding:10px; margin-left:10%; margin-right:10%; background-color:#f9f9f9;">'
        f'{verdict_html_content}'
        f'</div>'
    )
    
    html = f"""<!doctype html><html lang="ja"><meta charset="utf-8">
    <title>Fundamentals {code}</title>
    <style>body{{font-family:system-ui,-apple-system,'Noto Sans JP',sans-serif;margin:16px;}}</style>
    {body}
    {verdict_html}
    </html>"""
    
    try:
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[INFO] saved HTML: {out_html}")
    except Exception as e:
        raise RuntimeError(f"HTMLファイル書き込みエラー (file.write): {type(e).__name__}: {e}")


# =========================================================
# 個別コード処理関数 (非同期ワーカー)
# =========================================================

async def process_single_code(code: str, out_dir: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> dict:
    """
    単一の銘柄コードのデータ取得、グラフ生成を行う非同期ワーカー。
    """
    # code_full は URL、DBキー、ファイル名に使用する元の数英字コード (例: '102A')
    code_full = str(code).strip() 
    
    # ユーザーの要件に従い、コードの形式チェックのみ行い、変換はしません
    if not code_full or not re.match(r'^[0-9A-Za-z]{4,}$', code_full):
        # 少なくとも4文字以上の数英字であることを確認
        print(f"[ERROR] {code_full}: 銘柄コードの形式不正（数英字4桁以上の形式ではありません）。スキップします。")
        return {'status': 'ERROR', 'code': code_full, 'message': "銘柄コードの形式不正", 'progress_percent': None} 
    
    async with semaphore:
        try:
            # 1. データ取得 (URLには数英字コード code_full を渡す)
            df_full_year, df_quarterly = await asyncio.gather(
                fetch_full_year_financials(code_full, session),
                fetch_quarterly_financials(code_full, session)
            )
            
            df_forecast_row = pd.Series()
            if not df_full_year.empty and "予" in df_full_year.iloc[-1]["決算期"]:
                df_forecast_row = df_full_year.iloc[-1]

            # DB/ファイル名には数英字コード code_full をそのまま使用
            out_html = os.path.join(out_dir, f"finance_{code_full}.html")
            
            # 2. 判定とスコア計算
            verdict, score = judge_and_score_performance(df_full_year) 
            
            # calc_progress_from_df_op は4要素タプルを返す
            qp = calc_progress_from_df_op(df_quarterly, df_forecast_row)
            
            # DB保存用の進捗率の数値 (失敗時は特殊な数値) を取得
            progress_percent_db = qp[3] if qp is not None else None 
            
            # コメント生成（直感評価、進捗率、スコアを先頭に配置）
            formatted_v = _format_verdict_with_progress(qp, verdict, score)

            # 3. HTMLをエクスポート (ファイル名とグラフタイトルには code_full をそのまま使用)
            export_html(df_full_year, code_full, out_html=out_html, qp_result=qp, verdict_str=formatted_v) 
            
            print(f"[OK] {code_full} done. (Score: {score} points, Progress: {progress_percent_db}%)")
            
            # 成功結果をディクショナリで返す (DB書き込み用)
            return {
                'status': 'OK',
                'code': code_full, # DBのPRIMARY KEYとして数英字コードをそのまま使用
                'score': score,
                'progress_percent': progress_percent_db, # 新しいDBカラムに保存する数値
                'formatted_verdict': formatted_v,
                'out_html': out_html,
            }
            
        except aiohttp.ClientResponseError as e:
            print(f"[ERROR] {code_full}: HTTPエラーが発生しました - {e.status}: {e.message}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"HTTPエラー: {e.status}", 'progress_percent': None}
        except aiohttp.ClientConnectorError as e:
            print(f"[ERROR] {code_full}: 接続エラーが発生しました - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"接続エラー: {type(e).__name__}", 'progress_percent': None}
        except RuntimeError as e:
            print(f"[ERROR] {code_full}: 処理エラーが発生しました - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"処理エラー: {type(e).__name__}", 'progress_percent': None}
        except Exception as e:
            print(f"[ERROR] {code_full}: 予期せぬエラーが発生しました - {type(e).__name__}: {e}")
            return {'status': 'ERROR', 'code': code_full, 'message': f"予期せぬエラー: {type(e).__name__}", 'progress_percent': None}


# =========================================================
# main関数 (非同期ランナー)
# =========================================================

async def main_async(target_code: str | None = None):
    
    out_dir = r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\screen_data\graph"
    os.makedirs(out_dir, exist_ok=True)
    
    codes = set()
    if target_code:
        codes = {target_code}
        print(f"[INFO] 指定コード {target_code} のみ処理します")
    else:
        try:
            # 読み込み時にコードを全て文字列として扱う
            master = pd.read_csv(MASTER_CODES_PATH, encoding="utf8", sep=",", engine="python", dtype={'コード': str}) 
            codes = set(master["コード"].astype(str))
            print(f"[INFO] {len(codes)} 銘柄を処理します（非同期処理開始）")
        except FileNotFoundError:
            print(f"[CRITICAL ERROR] マスターファイルが見つかりません: {MASTER_CODES_PATH}")
            return

    semaphore = asyncio.Semaphore(ASYNC_CONCURRENCY_LIMIT)
    print(f"[INFO] aiohttp + asyncioで最大 {ASYNC_CONCURRENCY_LIMIT} の並列リクエストを行います。")

    
    async with aiohttp.ClientSession(headers=HEADERS, trust_env=True) as session: 
        tasks = [
            process_single_code(code, out_dir, session, semaphore)
            for code in codes
        ]
        
        # return_exceptions=True により、エラーが発生しても他のタスクの完了を待機し、結果をリストに返す
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

    
    successful_results = []
    error_count = 0
    
    for result in all_results:
        if isinstance(result, dict) and result.get('status') == 'OK':
            successful_results.append(result)
        elif isinstance(result, dict) and result.get('status') == 'ERROR':
            error_count += 1
        elif isinstance(result, Exception):
            error_count += 1
            print(f"[SUMMARY] 予期せぬ例外が発生しました: {type(result).__name__}: {result}")
        
    if successful_results:
        # DB書き込みはメインスレッドで一括実行
        batch_record_to_sqlite(successful_results)
    
    print("=" * 30)
    print(f"[DONE] 処理完了。成功: {len(successful_results)}件, エラー: {error_count}件.")
    print("=" * 30)


def main(target_code: str | None = None):
    try:
        main_async_coro = main_async(target_code=target_code)
        
        # Windows環境でのasyncio実行に対応するため、SelectorEventLoopPolicyを使用
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        asyncio.run(main_async_coro)
        
    except Exception as e:
        print(f"[CRITICAL ERROR] メイン処理中に予期せぬエラーが発生しました: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='株探から財務データを取得し、グラフを生成します。（非同期・高速化対応）')
    parser.add_argument('code', nargs='?', default=None, help='処理する銘柄コード (省略した場合、マスターファイル内の全コードを処理)')
    args = parser.parse_args()

    try:
        main(target_code=args.code)
    except Exception:
        sys.exit(1)