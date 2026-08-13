# === v5.1 market psychology + watch-account network DB + stock-name fill 2026-08-12 ===
# v4心理局面 + 要注意人物プロフィールリンク/投稿銘柄横断DB/人物・銘柄ダッシュボード。
import os
import sys
import requests
import json
import time
import pandas as pd
import numpy as np
import webbrowser  # 生成完了後の自動オープン用
from datetime import datetime, timedelta, date
from playwright.sync_api import sync_playwright
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re # ★銘柄名の抽出・高度な辞書に使う正規表現ライブラリ
import sqlite3
import html as html_lib
from pathlib import Path
from curl_cffi import requests
try:
    import jpholiday
except Exception:
    jpholiday = None

# =========================================================================
# ★ 実行設定（BAN回避のためのスリープ・スレッド設定）
# =========================================================================
# マルチスレッドの最大同時実行数（多いほど高速ですがBANリスクが跳ね上がります。推奨: 2〜4）
MAX_THREADS_CONFIG = 3

# 1銘柄の処理を開始する前のランダム待機時間（秒）の最小・最大
# 複数スレッドが一斉にアクセスを開始するのを防ぎます
THREAD_START_SLEEP_MIN = 2.0
THREAD_START_SLEEP_MAX = 5.0

# 掲示板のページネーション（過去の投稿へ遡る）ごとの待機時間（秒）
# APIへの連続攻撃と判定されないための最重要インターバルです（推奨: 2.0〜3.0）
PAGE_FETCH_SLEEP = 1.5

# ★ 追加：バッチ処理（チャンク）設定（ALLモード時のBAN回避用）
CHUNK_SIZE = 10         # 1度に処理する銘柄数（10〜15程度を推奨）
LONG_SLEEP_MIN = 180.0  # バッチ間の休憩（秒）最小（推奨: 180 = 3分）
LONG_SLEEP_MAX = 300.0  # バッチ間の休憩（秒）最大（推奨: 300 = 5分）
PAGE_SIZE = 100
FETCH_RETRIES = 3
RESUME_CACHE_MAX_HOURS = 8

# ★要注意人物・横断銘柄ネットワーク
WATCH_DB_FILENAME = "yabb_watch.db"
PROFILE_HISTORY_MAX_PAGES = 2
PROFILE_HISTORY_REFRESH_HOURS = 24
PROFILE_HISTORY_SLEEP = 0.8
# =========================================================================

# ★Pandas/Numpyの特殊な数値をJSON保存できるようにするためのエンコーダー
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

# ★認証情報の使い回し（BAN回避・高速化）とスレッド排他ロックのためのグローバル変数
_AUTH_CACHE = {'jwt': None, 'cookie_b': None, 'timestamp': 0}
_AUTH_LOCK = threading.Lock()
_FILE_LOCK = threading.Lock() # 途中経過のファイル書き込み衝突を防ぐロック


def yahoo_user_profile_url(user_id):
    uid = str(user_id or "").strip()
    if not uid:
        return ""
    return f"https://finance.yahoo.co.jp/cm/personal/history/comment?user={uid}"


def _watch_db_connect(output_dir):
    db_path = os.path.join(output_dir, WATCH_DB_FILENAME)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    ensure_watch_db_schema(conn)
    return conn


def ensure_watch_db_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS watch_actors(
        user_id TEXT PRIMARY KEY,
        profile_url TEXT,
        latest_names TEXT,
        first_seen TEXT,
        last_seen TEXT,
        flagged_count INTEGER NOT NULL DEFAULT 0,
        last_direction TEXT,
        last_reason TEXT,
        profile_checked_at TEXT
    );

    CREATE TABLE IF NOT EXISTS watch_actor_stocks(
        user_id TEXT NOT NULL,
        code TEXT NOT NULL,
        stock_name TEXT,
        source TEXT NOT NULL DEFAULT 'analysis',
        first_seen TEXT,
        last_seen TEXT,
        observed_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(user_id, code)
    );

    CREATE INDEX IF NOT EXISTS idx_watch_actor_stocks_code
    ON watch_actor_stocks(code);

    CREATE INDEX IF NOT EXISTS idx_watch_actor_stocks_user
    ON watch_actor_stocks(user_id);
    """)
    conn.commit()


def _normalize_watch_code(code):
    c = str(code or "").strip().upper()
    m = re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", c)
    return c if m else ""


def _profile_stock_name_from_html(text_value, code):
    # 「会社名【6503】」のような公開投稿履歴表示から名前を拾う。
    plain = re.sub(r"<[^>]+>", " ", str(text_value or ""))
    plain = html_lib.unescape(re.sub(r"\s+", " ", plain)).strip()
    patterns = [
        rf"([^【】<>]{{1,80}})【\s*{re.escape(code)}\s*】",
        rf"([^【】<>]{{1,80}})\[\s*{re.escape(code)}\s*\]",
    ]
    for pat in patterns:
        m = re.search(pat, plain)
        if m:
            name = m.group(1).strip(" -|:：")
            if len(name) <= 80:
                return name
    return ""


def _extract_profile_stock_codes(page_html):
    """
    Yahoo公開投稿履歴ページから、確認できた投稿先銘柄を抽出する。
    取得ページ範囲内の履歴であり「生涯全投稿」を保証するものではない。
    """
    h = str(page_html or "")
    found = {}

    # 新掲示板の quote URL
    for m in re.finditer(r'/quote/((?:\d{4}|\d{3}[A-Za-z]))(?:\.[A-Za-z]+)?/(?:forum|bbs)', h, re.I):
        c = _normalize_watch_code(m.group(1))
        if c:
            found.setdefault(c, "")

    # 表示タイトル中の 【コード】
    for m in re.finditer(r'【\s*((?:\d{4}|\d{3}[A-Za-z]))\s*】', h, re.I):
        c = _normalize_watch_code(m.group(1))
        if c:
            name = _profile_stock_name_from_html(h[max(0, m.start()-180):m.end()+60], c)
            if name and not found.get(c):
                found[c] = name

    # 旧掲示板 /cm/message/1006503/... の通常ケース
    for m in re.finditer(r'/cm/message/100((?:\d{4}|\d{3}[A-Za-z]))(?:/|["\'])', h, re.I):
        c = _normalize_watch_code(m.group(1))
        if c:
            found.setdefault(c, "")

    return [{"code": c, "stock_name": n} for c, n in sorted(found.items())]


def fetch_user_profile_stocks(user_id, verbose=False):
    """
    公開プロフィール投稿履歴から「他に投稿している銘柄」を確認。
    ページ番号を順にたどり、新しい銘柄も新しいHTMLも無くなれば停止。
    """
    profile_url = yahoo_user_profile_url(user_id)
    if not profile_url:
        return [], {"complete": False, "pages": 0, "error": "no_user_id"}

    merged = {}
    seen_fingerprints = set()
    complete = True
    error = ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    for page in range(1, PROFILE_HISTORY_MAX_PAGES + 1):
        url = profile_url if page == 1 else f"{profile_url}&page={page}"
        try:
            res = requests.get(url, headers=headers, timeout=12, impersonate="chrome120")
            if res.status_code != 200:
                complete = False
                error = f"HTTP {res.status_code}"
                break

            body = res.text or ""
            fp = hash(body[:12000])
            if fp in seen_fingerprints:
                break
            seen_fingerprints.add(fp)

            rows = _extract_profile_stock_codes(body)
            before = len(merged)
            for row in rows:
                c = row["code"]
                n = row.get("stock_name") or ""
                if c not in merged or (not merged[c] and n):
                    merged[c] = n

            # 空ページ、または2ページ目以降で何も新規情報がないなら終了
            if not body.strip() or (page > 1 and len(merged) == before and not rows):
                break

            if verbose:
                print(f"[watch][PROFILE] {str(user_id)[:8]}... page={page} stocks={len(merged)}")
            time.sleep(PROFILE_HISTORY_SLEEP)

        except Exception as e:
            complete = False
            error = str(e)
            break

    return (
        [{"code": c, "stock_name": n} for c, n in sorted(merged.items())],
        {"complete": complete, "pages": len(seen_fingerprints), "error": error},
    )


def _watch_profile_due(conn, user_id):
    row = conn.execute(
        "SELECT profile_checked_at FROM watch_actors WHERE user_id=?",
        (str(user_id),),
    ).fetchone()
    if not row or not row["profile_checked_at"]:
        return True
    try:
        dt = datetime.fromisoformat(str(row["profile_checked_at"]))
        return (datetime.now() - dt).total_seconds() >= PROFILE_HISTORY_REFRESH_HOURS * 3600
    except Exception:
        return True


def persist_watch_targets(report, code, stock_name, output_dir, refresh_profiles=True):
    """
    今回のレポートで検出された要注意人物をDB保存し、
    公開プロフィール投稿履歴から他の投稿銘柄も補完する。
    report['blacklist'] 各要素に profile_url / known_stocks を追加する。
    """
    targets = report.get("blacklist", []) or []
    if not targets:
        return report

    now = datetime.now().isoformat(timespec="seconds")
    code = _normalize_watch_code(code) or str(code)
    conn = _watch_db_connect(output_dir)

    try:
        for target in targets:
            uid = str(target.get("userId") or "").strip()
            if not uid:
                continue

            names = str(target.get("names") or "").strip()
            direction = str(target.get("direction") or "")
            reason = str(target.get("reason") or "")
            profile_url = yahoo_user_profile_url(uid)

            conn.execute("""
                INSERT INTO watch_actors(
                    user_id,profile_url,latest_names,first_seen,last_seen,
                    flagged_count,last_direction,last_reason
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    profile_url=excluded.profile_url,
                    latest_names=excluded.latest_names,
                    last_seen=excluded.last_seen,
                    flagged_count=watch_actors.flagged_count+1,
                    last_direction=excluded.last_direction,
                    last_reason=excluded.last_reason
            """, (uid, profile_url, names, now, now, 1, direction, reason))

            conn.execute("""
                INSERT INTO watch_actor_stocks(
                    user_id,code,stock_name,source,first_seen,last_seen,observed_count
                ) VALUES(?,?,?,?,?,?,1)
                ON CONFLICT(user_id,code) DO UPDATE SET
                    stock_name=CASE WHEN excluded.stock_name<>'' THEN excluded.stock_name ELSE watch_actor_stocks.stock_name END,
                    source=CASE
                        WHEN watch_actor_stocks.source='analysis' THEN 'analysis'
                        WHEN excluded.source='analysis' THEN 'analysis'
                        ELSE watch_actor_stocks.source
                    END,
                    last_seen=excluded.last_seen,
                    observed_count=watch_actor_stocks.observed_count+1
            """, (uid, code, str(stock_name or ""), "analysis", now, now))

            # 公開プロフィール履歴の巡回は一定時間キャッシュ
            if refresh_profiles and _watch_profile_due(conn, uid):
                stocks, meta = fetch_user_profile_stocks(uid)
                for item in stocks:
                    c = _normalize_watch_code(item.get("code"))
                    if not c:
                        continue

                    # プロフィール履歴HTMLから銘柄名が取れないケースがあるため補完する。
                    sn = str(item.get("stock_name") or "").strip()
                    if not sn or sn == "不明":
                        try:
                            sn2 = get_stock_name(c)
                            if sn2 and sn2 != "不明":
                                sn = sn2
                        except Exception:
                            pass

                    conn.execute("""
                        INSERT INTO watch_actor_stocks(
                            user_id,code,stock_name,source,first_seen,last_seen,observed_count
                        ) VALUES(?,?,?,?,?,?,0)
                        ON CONFLICT(user_id,code) DO UPDATE SET
                            stock_name=CASE WHEN excluded.stock_name<>'' THEN excluded.stock_name ELSE watch_actor_stocks.stock_name END,
                            last_seen=MAX(watch_actor_stocks.last_seen, excluded.last_seen)
                    """, (uid, c, sn, "profile_history", now, now))
                conn.execute(
                    "UPDATE watch_actors SET profile_checked_at=? WHERE user_id=?",
                    (now, uid),
                )
                target["profile_fetch_meta"] = meta

            rows = conn.execute("""
                SELECT code,stock_name,source,observed_count,last_seen
                FROM watch_actor_stocks
                WHERE user_id=?
                ORDER BY CASE WHEN code=? THEN 0 ELSE 1 END, code
            """, (uid, code)).fetchall()

            known_stocks = []
            for rr in rows:
                d = dict(rr)
                sn = str(d.get("stock_name") or "").strip()
                cc = str(d.get("code") or "").strip()
                if cc and (not sn or sn == "不明"):
                    try:
                        n2 = get_stock_name(cc)
                        if n2 and n2 != "不明":
                            sn = n2
                            d["stock_name"] = n2
                            conn.execute(
                                "UPDATE watch_actor_stocks SET stock_name=? WHERE user_id=? AND code=?",
                                (n2, uid, cc)
                            )
                    except Exception:
                        pass
                known_stocks.append(d)

            target["profile_url"] = profile_url
            target["known_stocks"] = known_stocks
            target["known_stock_count"] = len(known_stocks)

        conn.commit()
    finally:
        conn.close()

    return report


def generate_watch_network_html(output_dir):
    """
    DBから
      1) 要注意人物一覧
      2) 銘柄ごとの要注意人物一覧
    の2HTMLを生成。
    """
    conn = _watch_db_connect(output_dir)
    try:
        actors = conn.execute("""
            SELECT a.*,
                   COUNT(s.code) AS stock_count
            FROM watch_actors a
            LEFT JOIN watch_actor_stocks s ON s.user_id=a.user_id
            GROUP BY a.user_id
            ORDER BY stock_count DESC, a.flagged_count DESC, a.last_seen DESC
        """).fetchall()

        actor_rows = []
        for a in actors:
            uid = a["user_id"]
            stocks = conn.execute("""
                SELECT code,stock_name,source,observed_count,last_seen
                FROM watch_actor_stocks
                WHERE user_id=?
                ORDER BY observed_count DESC, code
            """, (uid,)).fetchall()
            stock_links = []
            for s in stocks:
                raw_code = str(s["code"])
                raw_name = str(s["stock_name"] or "").strip()

                # 既存DBにコードだけで残っている古い行も表示時に補完
                if not raw_name or raw_name == "不明":
                    try:
                        n2 = get_stock_name(raw_code)
                        if n2 and n2 != "不明":
                            raw_name = n2
                            conn.execute(
                                "UPDATE watch_actor_stocks SET stock_name=? WHERE user_id=? AND code=?",
                                (raw_name, uid, raw_code)
                            )
                    except Exception:
                        pass

                c = html_lib.escape(raw_code)
                sn = html_lib.escape(raw_name)
                label = f"{c}{' '+sn if sn else ''}"
                stock_links.append(
                    f'<a href="report_{c}.html" target="_blank" '
                    f'style="margin-right:8px;">{label}</a>'
                )
            actor_rows.append(
                "<tr>"
                f"<td><a href=\"{html_lib.escape(a['profile_url'] or yahoo_user_profile_url(uid))}\" "
                f"target=\"_blank\"><b>{html_lib.escape(a['latest_names'] or uid[:10])}</b></a>"
                f"<br><small>{html_lib.escape(uid)}</small></td>"
                f"<td style='text-align:center;font-weight:bold;'>{int(a['stock_count'] or 0)}</td>"
                f"<td>{''.join(stock_links) or '-'}</td>"
                f"<td>{html_lib.escape(a['last_direction'] or '')}</td>"
                f"<td>{html_lib.escape(a['last_reason'] or '')}</td>"
                f"<td>{html_lib.escape(a['last_seen'] or '')}</td>"
                "</tr>"
            )

        stocks = conn.execute("""
            SELECT s.code,
                   MAX(CASE WHEN s.stock_name<>'' THEN s.stock_name ELSE '' END) AS stock_name,
                   COUNT(DISTINCT s.user_id) AS actor_count,
                   MAX(s.last_seen) AS last_seen
            FROM watch_actor_stocks s
            GROUP BY s.code
            ORDER BY actor_count DESC, s.code
        """).fetchall()

        stock_rows = []
        for s in stocks:
            c = str(s["code"])
            stock_name_value = str(s["stock_name"] or "").strip()

            if not stock_name_value or stock_name_value == "不明":
                try:
                    n2 = get_stock_name(c)
                    if n2 and n2 != "不明":
                        stock_name_value = n2
                        conn.execute(
                            "UPDATE watch_actor_stocks SET stock_name=? WHERE code=? AND (stock_name IS NULL OR stock_name='')",
                            (stock_name_value, c)
                        )
                except Exception:
                    pass

            people = conn.execute("""
                SELECT a.user_id,a.latest_names,a.profile_url,a.last_direction,a.last_reason
                FROM watch_actor_stocks x
                JOIN watch_actors a ON a.user_id=x.user_id
                WHERE x.code=?
                ORDER BY a.flagged_count DESC,a.last_seen DESC
            """, (c,)).fetchall()
            ppl = []
            for a in people:
                ppl.append(
                    f'<a href="{html_lib.escape(a["profile_url"] or yahoo_user_profile_url(a["user_id"]))}" '
                    f'target="_blank" title="{html_lib.escape(a["last_reason"] or "")}">'
                    f'{html_lib.escape(a["latest_names"] or a["user_id"][:10])}</a>'
                )
            stock_rows.append(
                "<tr>"
                f"<td><a href='report_{html_lib.escape(c)}.html' target='_blank'><b>{html_lib.escape(c)}</b></a></td>"
                f"<td>{html_lib.escape(stock_name_value)}</td>"
                f"<td style='text-align:center;font-size:18px;font-weight:900;color:#b91c1c;'>{int(s['actor_count'] or 0)}</td>"
                f"<td>{' / '.join(ppl) or '-'}</td>"
                f"<td>{html_lib.escape(s['last_seen'] or '')}</td>"
                "</tr>"
            )

        def wrap(title, intro, headers, rows):
            return f"""<!doctype html><html lang="ja"><head><meta charset="utf-8">
<title>{html_lib.escape(title)}</title>
<style>
body{{font-family:Arial,'Yu Gothic',sans-serif;background:#f1f5f9;padding:24px;color:#0f172a}}
.wrap{{max-width:1500px;margin:auto;background:white;padding:24px;border-radius:12px}}
table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #e2e8f0;vertical-align:top}}
th{{background:#0f172a;color:white;position:sticky;top:0}}tr:hover{{background:#f8fafc}}
a{{color:#2563eb;text-decoration:none}}small{{color:#64748b}}
.nav a{{display:inline-block;margin-right:12px;padding:7px 12px;border:1px solid #cbd5e1;border-radius:16px}}
</style></head><body><div class="wrap">
<div class="nav"><a href="report_WATCH_Actors.html">👤 要注意人物一覧</a><a href="report_WATCH_Stocks.html">📌 銘柄一覧</a></div>
<h1>{html_lib.escape(title)}</h1><p>{intro}</p>
<table><thead><tr>{''.join(f'<th>{html_lib.escape(h)}</th>' for h in headers)}</tr></thead>
<tbody>{''.join(rows) if rows else '<tr><td colspan="6">データなし</td></tr>'}</tbody></table>
</div></body></html>"""

        Path(output_dir, "report_WATCH_Actors.html").write_text(
            wrap(
                "👤 ヤフ板 要注意人物一覧",
                "公開Yahooアカウント単位。人物の実名や所属を推定するものではありません。投稿履歴ページで確認できた銘柄と、本スクリプトで要注意判定した銘柄を横断表示します。",
                ["アカウント", "銘柄数", "確認できた投稿銘柄", "直近方向", "直近検出理由", "最終確認"],
                actor_rows,
            ),
            encoding="utf-8",
        )
        Path(output_dir, "report_WATCH_Stocks.html").write_text(
            wrap(
                "📌 要注意人物が確認された銘柄一覧",
                "銘柄ごとに、DBで関連づいた要注意アカウント数を表示します。プロフィール履歴由来の関連も含みます。",
                ["コード", "銘柄名", "要注意人物数", "要注意人物", "最終確認"],
                stock_rows,
            ),
            encoding="utf-8",
        )
    finally:
        conn.close()

def get_stock_name(code):
    """
    ブラウザを立ち上げず、軽量かつ高速に銘柄名だけを確実に取得する専用関数
    User-Agentを付与してブロックを回避するよう強化
    """
    try:
        url = f"https://finance.yahoo.co.jp/quote/{code}.T/forum"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        #res = requests.get(url, headers=headers, timeout=10)
        res = requests.get(url, headers=headers, timeout=10, impersonate="chrome120")
        res.encoding = 'utf-8'
        
        # HTMLの <title>タグ から「【」の手前までを抽出
        match = re.search(r'<title>(.*?)【', res.text)
        if match:
            return match.group(1).replace('(株)', '').strip()
        else:
            title_match = re.search(r'<title>(.*?)</title>', res.text)
            return title_match.group(1).split('(')[0].strip() if title_match else "不明"
    except Exception:
        return "不明"

def get_latest_auth_info(code, force_refresh=False):
    """
    Playwrightを人間モードに偽装し、認証情報を取得。
    マルチスレッド衝突を防ぐためのロック機構を搭載し、
    30分以内であればキャッシュを再利用してBANを防ぐ。
    """
    global _AUTH_CACHE
    
    with _AUTH_LOCK: # ここから下は1度に1つのスレッドしか実行できない
        # 最後の取得から1800秒（30分）以内ならキャッシュを再利用
        if (not force_refresh) and time.time() - _AUTH_CACHE['timestamp'] < 1800 and _AUTH_CACHE['jwt']:
            return {'cookie_b': _AUTH_CACHE['cookie_b'], 'jwt_token': _AUTH_CACHE['jwt']}

        print(f"[{code}] ブラウザを起動して最新の認証情報を自動更新中...")
        auth_info = {'cookie_b': None, 'jwt_token': None}
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 800},
                locale='ja-JP',
                timezone_id='Asia/Tokyo',
                java_script_enabled=True
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
            page = context.new_page()
            
            # 画像やメディアの読み込みをブロックして高速化＆タイムアウト防止
            page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
            
            def handle_request(request):
                if "yahoo.co.jp" in request.url and 'x-jwt-token' in request.headers:
                    auth_info['jwt_token'] = request.headers['x-jwt-token']

            page.on("request", handle_request)
            
            try:
                # タイムアウトを60秒(60000ms)に延長してエラー落ちを防ぐ
                page.goto(f"https://finance.yahoo.co.jp/quote/{code}.T/forum", wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)
                
                for cookie in context.cookies():
                    if cookie['name'] == 'B':
                        auth_info['cookie_b'] = cookie['value']
                        break
                        
                # 取得に成功したらキャッシュを更新
                if auth_info['jwt_token'] and auth_info['cookie_b']:
                    _AUTH_CACHE = {'jwt': auth_info['jwt_token'], 'cookie_b': auth_info['cookie_b'], 'timestamp': time.time()}
                    
            except Exception as e:
                print(f"⚠️ Playwright通信エラー: {e}")
            finally:
                browser.close()
                
        return auth_info

def invalidate_auth_cache():
    global _AUTH_CACHE
    with _AUTH_LOCK:
        _AUTH_CACHE = {'jwt': None, 'cookie_b': None, 'timestamp': 0}

def _is_jpx_business_day(d: date) -> bool:
    """土日・日本の祝日・年末年始を除外した簡易JPX営業日判定。"""
    if d.weekday() >= 5:
        return False
    if (d.month, d.day) in {(1, 1), (1, 2), (1, 3), (12, 31)}:
        return False
    if jpholiday is not None:
        try:
            if jpholiday.is_holiday(d):
                return False
        except Exception:
            pass
    return True


def _previous_jpx_business_day(d: date) -> date:
    cur = d - timedelta(days=1)
    for _ in range(14):
        if _is_jpx_business_day(cur):
            return cur
        cur -= timedelta(days=1)
    return cur


def calculate_target_period(base_datetime=None):
    """
    平日かつ営業日の08:00〜23:59は「当日08:00〜現在」。
    それ以外は「直前のJPX営業日15:30〜現在」。
    月曜早朝・祝日明けも金曜/直前営業日まで正しく遡る。
    """
    if base_datetime is None:
        base_datetime = datetime.now()

    end_target = base_datetime
    time_val = base_datetime.hour * 100 + base_datetime.minute
    today = base_datetime.date()

    if _is_jpx_business_day(today) and 800 <= time_val <= 2359:
        start_target = base_datetime.replace(hour=8, minute=0, second=0, microsecond=0)
        return start_target, end_target

    prev_bday = _previous_jpx_business_day(today)
    start_target = datetime.combine(prev_bday, datetime.strptime('15:30', '%H:%M').time())
    return start_target, end_target


def fetch_yahoo_bbs_market_adaptive(code, verbose=False):
    """
    最新の対象期間だけを収集する。
    - page size=100
    - partで重複除去
    - 429/5xxは指数バックオフで再試行
    - 401/403は認証を1回破棄して再取得
    - 途中失敗時は fetch_meta['complete']=False を返し、分析側の確信度を下げる
    """
    start_target, end_target = calculate_target_period()
    if verbose:
        print(f"\n[相場カレンダー連動フィルター発動]")
        print(f"◆ 対象銘柄: {code}")
        print(f"◆ 現在日時: {end_target.strftime('%Y/%m/%d %H:%M:%S')}")
        print(f"◆ 収集範囲: {start_target} 〜 {end_target}")

    stock_name = get_stock_name(code)
    auth = get_latest_auth_info(code)
    meta = {
        'complete': False,
        'pages': 0,
        'errors': [],
        'status_code': None,
        'target_start': start_target.isoformat(timespec='minutes'),
        'target_end': end_target.isoformat(timespec='minutes'),
        'duplicates_removed': 0,
    }
    if not auth['cookie_b'] or not auth['jwt_token']:
        meta['errors'].append('auth_failed')
        if verbose:
            print(f"【エラー】[{code}] 認証情報の自動取得に失敗しました。")
        return pd.DataFrame(), stock_name, meta

    url = 'https://finance.yahoo.co.jp/bff-quote-stocks/v1/ajax/bbs/comment'
    filtered_by_part = {}
    current_mid = ''
    auth_refreshed = False

    for loop in range(1000):
        params = {'code': code, 'size': str(PAGE_SIZE), 'sort': '1'}
        if current_mid:
            params['mid'] = current_mid

        response = None
        for attempt in range(FETCH_RETRIES):
            cookies = {'B': auth['cookie_b']}
            headers = {
                'accept': '*/*',
                'referer': f'https://finance.yahoo.co.jp/quote/{code}.T/forum',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'x-jwt-token': auth['jwt_token'],
            }
            try:
                response = requests.get(url, params=params, cookies=cookies, headers=headers,
                                        impersonate="chrome120", timeout=15)
                meta['status_code'] = int(response.status_code)

                if response.status_code in (401, 403) and not auth_refreshed:
                    auth_refreshed = True
                    invalidate_auth_cache()
                    auth = get_latest_auth_info(code, force_refresh=True)
                    if auth['cookie_b'] and auth['jwt_token']:
                        continue

                if response.status_code == 200:
                    break

                if response.status_code == 429 or response.status_code >= 500:
                    wait = (2 ** attempt) + random.uniform(0.4, 1.2)
                    if verbose:
                        print(f"[{code}] HTTP {response.status_code} → {wait:.1f}秒後に再試行")
                    time.sleep(wait)
                    continue

                meta['errors'].append(f'http_{response.status_code}')
                response = None
                break
            except Exception as e:
                meta['errors'].append(f'request:{type(e).__name__}')
                if attempt + 1 < FETCH_RETRIES:
                    time.sleep((2 ** attempt) + random.uniform(0.3, 1.0))
                else:
                    response = None

        if response is None or response.status_code != 200:
            break

        try:
            items = response.json().get("response", {}).get("items", [])
        except Exception as e:
            meta['errors'].append(f'json:{type(e).__name__}')
            break

        meta['pages'] += 1
        if not items:
            meta['complete'] = True
            break

        for item in items:
            try:
                post_time = datetime.strptime(item['postDate'], '%Y/%m/%d %H:%M')
            except Exception:
                continue
            if start_target <= post_time <= end_target:
                part = str(item.get('part', ''))
                if part and part in filtered_by_part:
                    meta['duplicates_removed'] += 1
                else:
                    filtered_by_part[part or f"row_{len(filtered_by_part)}"] = item

        try:
            last_item_time = datetime.strptime(items[-1]['postDate'], '%Y/%m/%d %H:%M')
        except Exception:
            meta['errors'].append('invalid_last_post_time')
            break

        if last_item_time < start_target:
            meta['complete'] = True
            if verbose:
                print(f" -> ページ末尾（{last_item_time}）が対象期間より過去。収集完了。")
            break

        next_mid = str(items[-1].get("part", ""))
        if not next_mid or next_mid == current_mid:
            meta['errors'].append('pagination_stalled')
            break
        current_mid = next_mid

        if verbose:
            print(f" ループ {loop + 1}: 過去へ巡回中... (最古: {items[-1]['postDate']})")
        time.sleep(PAGE_FETCH_SLEEP + random.uniform(0.0, 0.5))
    else:
        meta['errors'].append('max_pages_reached')

    df = pd.DataFrame(list(filtered_by_part.values()))
    if verbose:
        state = '完全' if meta['complete'] else '部分取得'
        print(f"⇒ 【結果】{len(df)}件 / {state} / pages={meta['pages']} / dup除去={meta['duplicates_removed']}")
    return df, stock_name, meta


def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(v)))


def enrich_actor_psychology(report, df, fetch_meta=None):
    """
    v4:
      - 掲示板全体の方向 と 協調している側の方向 を分離
      - 上方向協調度 / 下方向協調度を個別算出
      - 心理誘導技法（FOMO・恐怖・緊急性など）を方向別に集約
      - 掲示板情報だけによる「上昇/下落示唆確率」を未較正ヒューリスティックとして算出

    注意: 機関・大口・特定グループの身元は掲示板単独では識別しない。
    """
    fetch_meta = fetch_meta or {}
    n = max(len(df), 1)
    unique_users = max(int(report.get('unique_users', 0)), 1)

    # ---------------------------------------------------------------
    # 1) 全体方向: 1ユーザーの連投で方向が決まらないようユーザー単位で±3 cap。
    #    さらに「方向を持つユーザーが全体の何割か」で減衰する。
    # ---------------------------------------------------------------
    user_net_map = {}
    user_post_count = {}
    multi_name_users = set()
    for uid, u_df in df.groupby('userId'):
        b = float(u_df['buy_count'].sum())
        se = float(u_df['sell_count'].sum())
        user_net_map[uid] = max(-3.0, min(3.0, b - se))
        user_post_count[uid] = len(u_df)
        try:
            if u_df['dispname'].dropna().nunique() > 1:
                multi_name_users.add(uid)
        except Exception:
            pass

    user_net = list(user_net_map.values())
    abs_net = sum(abs(x) for x in user_net)
    raw_direction_score = 0.0 if abs_net <= 1e-9 else 100.0 * sum(user_net) / abs_net
    directional_users = sum(1 for x in user_net if abs(x) > 1e-9)
    direction_participation = directional_users / max(unique_users, 1)
    participation_factor = 0.15 + 0.85 * np.sqrt(max(0.0, min(1.0, direction_participation)))
    direction_score = float(raw_direction_score * participation_factor)

    if direction_score >= 18:
        direction_label = '掲示板全体は上方向優勢'
    elif direction_score <= -18:
        direction_label = '掲示板全体は下方向優勢'
    else:
        direction_label = '掲示板全体は方向混在・中立'

    user_direction = {uid: (1 if net > 0 else (-1 if net < 0 else 0)) for uid, net in user_net_map.items()}
    df['_user_direction'] = df['userId'].map(user_direction).fillna(0).astype(int)

    # 全体投稿集中度（参考値）
    shares = sorted((c / n for c in user_post_count.values()), reverse=True)
    top1_share = shares[0] if shares else 0.0
    top3_share = sum(shares[:3]) if shares else 0.0
    hhi = sum(x*x for x in shares) if shares else 0.0

    # ---------------------------------------------------------------
    # 2) 方向別協調度
    # ---------------------------------------------------------------
    sync_items = report.get('extra_analysis_results', {}).get('stealth_sync', [])
    anomalies = report.get('anomalies', [])

    def _sync_direction(item):
        sb = float(item.get('source_buy_count', 0)); ss = float(item.get('source_sell_count', 0))
        tb = float(item.get('buy_count', 0)); ts = float(item.get('sell_count', 0))
        sdir = 1 if sb > ss else (-1 if ss > sb else 0)
        tdir = 1 if tb > ts else (-1 if ts > tb else 0)
        return sdir if sdir == tdir else 0

    def _directional_coord(sign):
        dir_users = [uid for uid, d in user_direction.items() if d == sign]
        dir_user_count = len(dir_users)
        if dir_user_count == 0:
            return {
                'score': 0.0, 'users': 0, 'posts': 0, 'post_share': 0.0,
                'sync_count': 0, 'max_flow': 0, 'concentration': 0.0,
                'sync_score': 0.0, 'burst_score': 0.0, 'reaction_score': 0.0,
                'identity_score': 0.0,
            }

        dir_df_users = df[df['_user_direction'] == sign]
        dir_posts = len(dir_df_users)
        dir_post_share = dir_posts / n
        counts = sorted((len(g) for _, g in dir_df_users.groupby('userId')), reverse=True)
        top1_all = (counts[0] / n) if counts else 0.0
        top3_all = (sum(counts[:3]) / n) if counts else 0.0

        # 「少数の同方向IDが掲示板全体を占有」したときに高くなる。
        concentration = _clamp(
            55 * max(0.0, (top3_all - 0.08) / 0.42) +
            25 * max(0.0, (top1_all - 0.04) / 0.26) +
            20 * max(0.0, (dir_post_share - 0.15) / 0.55)
        )

        sync_count = sum(1 for s in sync_items if _sync_direction(s) == sign)
        sync_score = _clamp((sync_count / max(dir_user_count, 1)) * 220)

        # 投稿そのものの方向で10分バーストを見る。
        post_dir_df = df[df['post_direction'] == sign]
        if not post_dir_df.empty:
            dir_resampled = post_dir_df.set_index('post_dt').resample('10min').size()
            dir_max_flow = int(dir_resampled.max()) if not dir_resampled.empty else 0
        else:
            dir_max_flow = 0
        burst_raw = _clamp((dir_max_flow - 4) * (100 / 24))
        within_dir_diversity = min(1.0, dir_user_count / max(dir_posts, 1))
        burst_score = burst_raw * (1.0 - 0.50 * within_dir_diversity)

        dir_anom = 0
        for a in anomalies:
            b = float(a.get('buy_count', 0)); s = float(a.get('sell_count', 0))
            adir = 1 if b > s else (-1 if s > b else 0)
            if adir == sign:
                dir_anom += 1
        reaction_score = _clamp((dir_anom / max(dir_posts, 1)) * 400)

        dir_multi = sum(1 for uid in dir_users if uid in multi_name_users)
        identity_score = _clamp((dir_multi / max(dir_user_count, 1)) * 250)

        score = _clamp(
            0.30 * concentration +
            0.30 * sync_score +
            0.18 * burst_score +
            0.12 * reaction_score +
            0.10 * identity_score
        )
        return {
            'score': float(score), 'users': dir_user_count, 'posts': dir_posts,
            'post_share': float(dir_post_share), 'sync_count': int(sync_count),
            'max_flow': int(dir_max_flow), 'concentration': float(concentration),
            'sync_score': float(sync_score), 'burst_score': float(burst_score),
            'reaction_score': float(reaction_score), 'identity_score': float(identity_score),
        }

    up_coord = _directional_coord(1)
    down_coord = _directional_coord(-1)
    up_coord_score = up_coord['score']
    down_coord_score = down_coord['score']
    coordination_score = max(up_coord_score, down_coord_score)

    if coordination_score < 25:
        organized_direction = '明確な協調方向なし'
    elif abs(up_coord_score - down_coord_score) < 10:
        organized_direction = '上下双方の対立・協調'
    elif up_coord_score > down_coord_score:
        organized_direction = '上方向への協調が優勢'
    else:
        organized_direction = '下方向への協調が優勢'

    if coordination_score < 25:
        actor_scale = '個人レベル・自然発生が中心'
    elif coordination_score < 50:
        actor_scale = '少人数・煽り屋グループの可能性'
    elif coordination_score < 70:
        actor_scale = '協調グループ的な行動と整合'
    else:
        actor_scale = '強い組織的行動パターンと整合（主体は特定不能）'

    # ---------------------------------------------------------------
    # 3) 心理誘導技法
    # ---------------------------------------------------------------
    psych_names = ['FOMO','恐怖','緊急性','確信演出','行動命令','価格アンカー','権威付け','群集圧力','安心誘導']
    psych_rates = {}
    for name in psych_names:
        col = 'psych_' + name
        psych_rates[name] = float((df[col] > 0).mean() * 100) if col in df else 0.0

    def _psych_pressure(sign):
        d = df[df['post_direction'] == sign]
        if d.empty:
            return 0.0
        cols = ['psych_' + n for n in psych_names if 'psych_' + n in d.columns]
        if not cols:
            return 0.0
        # 1投稿で4技法以上使っても上限100。煽り技法の「濃さ」を見る。
        density = d[cols].sum(axis=1).clip(upper=4) / 4.0
        return float(_clamp(density.mean() * 100))

    up_psych_pressure = _psych_pressure(1)
    down_psych_pressure = _psych_pressure(-1)
    dominant_techniques = [
        {'name': k, 'rate_pct': float(v)}
        for k, v in sorted(psych_rates.items(), key=lambda kv: kv[1], reverse=True)
        if v >= 3.0
    ][:5]

    despair_rate = float((df['despair_count'] > 0).mean() * 100) if 'despair_count' in df else 0.0
    cult_rate = float((df['cult_count'] > 0).mean() * 100) if 'cult_count' in df else 0.0
    noise_rate = float((df['noise_count'] > 0).mean() * 100) if 'noise_count' in df else 0.0
    max_flow = float(report.get('max_flow', 0))
    diversity_ratio = min(1.0, unique_users / n)

    if psych_rates.get('恐怖', 0) >= 12 and down_psych_pressure > up_psych_pressure + 8:
        psychology = '恐怖・狼狽を使った下方向誘導が優勢'
    elif (psych_rates.get('FOMO', 0) + psych_rates.get('緊急性', 0)) >= 18 and up_psych_pressure > down_psych_pressure + 8:
        psychology = 'FOMO・緊急性を使った上方向誘導が優勢'
    elif psych_rates.get('確信演出', 0) >= 10 or psych_rates.get('行動命令', 0) >= 8:
        psychology = '確信・命令型の行動誘導が目立つ'
    elif cult_rate >= 8:
        psychology = '熱狂・盲信優勢'
    elif despair_rate >= 12:
        psychology = '悲観・狼狽優勢'
    elif max_flow >= 40 and diversity_ratio >= 0.30:
        psychology = '群集過熱・祭り状態'
    elif noise_rate >= 20:
        psychology = 'ノイズ・煽り優勢'
    elif abs(direction_score) >= 55:
        psychology = '方向性の強い同調状態'
    else:
        psychology = '通常〜混在心理'


    # ---------------------------------------------------------------
    # 4) 群集心理フェーズ（逆張りレイヤー）
    #    DBや過去株価は使わず、「今回取得した投稿窓の内部」で心理の推移を読む。
    #    - お花畑/総楽観: 高値警戒の逆張り材料
    #    - 総悲観: 底接近の逆張り材料（即反転とは限らない）
    #    - 悲観後の静寂: セリングクライマックス後の底打ち候補
    #    - 固定ID常駐: 一方向の心理圧力が長時間残る状態
    # ---------------------------------------------------------------
    pos_users = sum(1 for d in user_direction.values() if d > 0)
    neg_users = sum(1 for d in user_direction.values() if d < 0)
    bull_breadth = 100.0 * pos_users / max(unique_users, 1)
    bear_breadth = 100.0 * neg_users / max(unique_users, 1)

    def _rate_score(rate_pct, full_at=25.0):
        return _clamp((float(rate_pct) / max(full_at, 1e-9)) * 100.0)

    # 「皆が同じ夢を見ている」ほど高くする。単なる買い語だけでは花畑にしない。
    euphoria_score = _clamp(
        0.38 * bull_breadth +
        0.17 * _rate_score(psych_rates.get('FOMO', 0), 20) +
        0.13 * _rate_score(cult_rate, 12) +
        0.12 * _rate_score(psych_rates.get('安心誘導', 0), 18) +
        0.10 * _rate_score(psych_rates.get('確信演出', 0), 15) +
        0.10 * max(0.0, direction_score)
    )

    # 悲観は「下落予想」だけではなく、群集が投げ切りに近づく逆張り材料にもなる。
    despair_phase_score = _clamp(
        0.38 * bear_breadth +
        0.20 * _rate_score(despair_rate, 18) +
        0.17 * _rate_score(psych_rates.get('恐怖', 0), 20) +
        0.10 * _rate_score(psych_rates.get('行動命令', 0), 15) +
        0.15 * max(0.0, -direction_score)
    )

    # ---- 固定/常駐アカウント圧 ----
    try:
        target_start_dt = pd.Timestamp(fetch_meta.get('target_start')) if fetch_meta.get('target_start') else df['post_dt'].min()
        target_end_dt = pd.Timestamp(fetch_meta.get('target_end')) if fetch_meta.get('target_end') else df['post_dt'].max()
    except Exception:
        target_start_dt, target_end_dt = df['post_dt'].min(), df['post_dt'].max()
    if pd.isna(target_start_dt): target_start_dt = df['post_dt'].min()
    if pd.isna(target_end_dt): target_end_dt = df['post_dt'].max()
    total_window_min = max(30.0, (target_end_dt - target_start_dt).total_seconds() / 60.0)

    resident_up_score = 0.0
    resident_down_score = 0.0
    resident_accounts = []
    for uid, u_df in df.groupby('userId'):
        if len(u_df) < 4:
            continue
        u_sorted = u_df.sort_values('post_dt')
        span_min = max(0.0, (u_sorted['post_dt'].max() - u_sorted['post_dt'].min()).total_seconds() / 60.0)
        span_ratio = min(1.0, span_min / total_window_min)
        share = len(u_sorted) / n
        hour_bins = int(u_sorted['post_dt'].dt.floor('60min').nunique())
        b = float(u_sorted['buy_count'].sum()); se = float(u_sorted['sell_count'].sum())
        directional_total = b + se
        consistency = abs(b - se) / directional_total if directional_total > 0 else 0.0
        rscore = _clamp(
            0.40 * _clamp((share / 0.22) * 100.0) +
            0.25 * (span_ratio * 100.0) +
            0.20 * _clamp((hour_bins / 4.0) * 100.0) +
            0.15 * (consistency * 100.0)
        )
        d = user_direction.get(uid, 0)
        if rscore >= 35:
            resident_accounts.append({
                'userId': str(uid), 'score': float(rscore), 'direction': int(d),
                'posts': int(len(u_sorted)), 'share_pct': float(share * 100.0),
                'span_ratio_pct': float(span_ratio * 100.0), 'hour_bins': hour_bins,
            })
        if d > 0:
            resident_up_score = max(resident_up_score, rscore)
        elif d < 0:
            resident_down_score = max(resident_down_score, rscore)
    resident_accounts.sort(key=lambda x: x['score'], reverse=True)

    # ---- 悲観 → 静寂 の時系列パターン ----
    # 「最後の投稿時刻」ではなく取得対象の終了時刻を基準にすることで、投稿が途絶えた時間も評価する。
    recent_minutes = 90
    prior_minutes = 360
    recent_start_dt = target_end_dt - pd.Timedelta(minutes=recent_minutes)
    prior_start_dt = recent_start_dt - pd.Timedelta(minutes=prior_minutes)
    recent_df = df[(df['post_dt'] >= recent_start_dt) & (df['post_dt'] <= target_end_dt)]
    prior_df = df[(df['post_dt'] >= prior_start_dt) & (df['post_dt'] < recent_start_dt)]

    recent_rate_per_hour = len(recent_df) / (recent_minutes / 60.0)
    prior_rate_per_hour = len(prior_df) / (prior_minutes / 60.0)
    if len(prior_df) >= 5 and prior_rate_per_hour > 0.5:
        flow_drop_pct = _clamp((1.0 - recent_rate_per_hour / prior_rate_per_hour) * 100.0)
        prior_neg = float((prior_df['post_direction'] < 0).mean() * 100.0)
        prior_desp = float((prior_df['despair_count'] > 0).mean() * 100.0)
        prior_fear = float((prior_df.get('psych_恐怖', pd.Series(0, index=prior_df.index)) > 0).mean() * 100.0)
        prior_despair_score = _clamp(
            0.45 * prior_neg +
            0.30 * _rate_score(prior_desp, 20) +
            0.25 * _rate_score(prior_fear, 22)
        )
        if prior_despair_score >= 35 and flow_drop_pct >= 25:
            silence_after_despair_score = _clamp(
                0.52 * prior_despair_score + 0.48 * flow_drop_pct
            )
        else:
            silence_after_despair_score = 0.0
    else:
        flow_drop_pct = 0.0
        prior_despair_score = 0.0
        silence_after_despair_score = 0.0

    cycle_flags = []
    if euphoria_score >= 62:
        cycle_flags.append('🌸 お花畑・総楽観：逆張りでは天井/売り警戒')
    if silence_after_despair_score >= 58:
        cycle_flags.append('🌙 悲観後の静寂：底打ち・売り枯れ候補')
    if despair_phase_score >= 58:
        cycle_flags.append('😰 総悲観：底接近候補（まだ投げ継続の可能性あり）')
    if resident_down_score >= 58:
        cycle_flags.append('🧱 下方向の固定ID常駐：じり安継続警戒')
    if resident_up_score >= 65:
        cycle_flags.append('📣 上方向の固定ID常駐：強気空気の固定化に注意')
    if not cycle_flags:
        cycle_flags.append('➖ 極端な群集心理フェーズなし')
    market_psychology_phase = ' / '.join(cycle_flags[:3])

    # ---------------------------------------------------------------
    # 5) データ確信度
    # ---------------------------------------------------------------
    sample_conf = 0.55 * min(1.0, n / 60.0) + 0.45 * min(1.0, unique_users / 18.0)
    if fetch_meta and not fetch_meta.get('complete', False):
        sample_conf *= 0.55
    conf_pct = _clamp(sample_conf * 100)
    confidence = 'HIGH' if conf_pct >= 70 else ('MEDIUM' if conf_pct >= 40 else 'LOW')

    # ---------------------------------------------------------------
    # 6) 上昇/下落「参考比率」
    #    方向・協調に加え、群集心理の逆張りフェーズを補正する。
    #    真の確率ではなく「今回の掲示板だけを見た時、どちらを警戒するか」の参考値。
    # ---------------------------------------------------------------
    tl = report.get('extra_analysis_results', {}).get('sentiment_timeline', [])
    if tl:
        recent_vals = [float(x.get('score', 0)) for x in tl[-2:]]
        recent_sentiment = sum(recent_vals) / max(len(recent_vals), 1)
    else:
        recent_sentiment = 0.0
    recent_norm = float(np.tanh(recent_sentiment / 8.0) * 100.0)

    coord_diff = up_coord_score - down_coord_score
    psych_diff = up_psych_pressure - down_psych_pressure
    direct_edge = (
        0.48 * (direction_score / 100.0) +
        0.25 * (coord_diff / 100.0) +
        0.17 * (psych_diff / 100.0) +
        0.10 * (recent_norm / 100.0)
    )

    # 極端な群集心理では「皆が言っている方向」をそのまま追わず、逆張り補正を入れる。
    # 悲観だけでは即反転とせず弱いプラス、悲観後の静寂を最も強い底打ち候補として扱う。
    # 逆張り補正は「普通の強気/弱気」では発動させず、50点を超えた極端心理だけに効かせる。
    euphoria_contra = _clamp((euphoria_score - 50.0) * 2.0) / 100.0
    despair_contra = _clamp((despair_phase_score - 50.0) * 2.0) / 100.0
    direct_scale = max(0.25, 1.0 - 0.65 * euphoria_contra - 0.45 * despair_contra)
    direct_edge *= direct_scale
    phase_edge = (
        +0.12 * despair_contra
        +0.40 * (silence_after_despair_score / 100.0)
        -0.70 * euphoria_contra
        -0.24 * (resident_down_score / 100.0)
        +0.04 * (resident_up_score / 100.0)
    )
    edge = max(-1.0, min(1.0, direct_edge + phase_edge))

    # サンプル不足・方向参加率不足なら50%へ強く縮める。
    confidence_scale = (conf_pct / 100.0) * (0.45 + 0.55 * np.sqrt(max(0.0, min(1.0, direction_participation))))
    # 「悲観後の静寂」は方向参加率が低くなること自体がシグナルなので、その分だけ減衰を緩める。
    if silence_after_despair_score >= 58:
        confidence_scale = max(confidence_scale, min(0.82, 0.45 + 0.004 * silence_after_despair_score))
    shrunk_edge = edge * confidence_scale
    rise_signal_pct = float(50.0 + 35.0 * np.tanh(1.65 * shrunk_edge))
    rise_signal_pct = float(max(15.0, min(85.0, rise_signal_pct)))
    fall_signal_pct = float(100.0 - rise_signal_pct)
    edge_strength = float(abs(rise_signal_pct - 50.0) * 2.0)

    if rise_signal_pct >= 65:
        price_direction_label = '参考判断は上方向優勢'
    elif rise_signal_pct >= 56:
        price_direction_label = '参考判断はやや上方向'
    elif rise_signal_pct <= 35:
        price_direction_label = '参考判断は下方向優勢'
    elif rise_signal_pct <= 44:
        price_direction_label = '参考判断はやや下方向'
    else:
        price_direction_label = '参考判断は上下拮抗'

    evidence = [
        f"全体方向 {direction_score:+.1f}/100（方向一致度 {raw_direction_score:+.1f}, 方向参加率 {direction_participation*100:.1f}%）",
        f"上方向協調 {up_coord_score:.1f}/100 vs 下方向協調 {down_coord_score:.1f}/100",
        f"上方向心理圧 {up_psych_pressure:.1f}/100 vs 下方向心理圧 {down_psych_pressure:.1f}/100",
        f"上位3ID投稿占有 {top3_share*100:.1f}%（最大ID {top1_share*100:.1f}%）",
        f"同期候補 上{up_coord['sync_count']}件 / 下{down_coord['sync_count']}件",
        f"10分方向別最大流速 上{up_coord['max_flow']}件 / 下{down_coord['max_flow']}件",
        f"群集心理: 花畑{euphoria_score:.1f} / 悲観{despair_phase_score:.1f} / 悲観後静寂{silence_after_despair_score:.1f}",
        f"固定ID常駐圧: 上{resident_up_score:.1f} / 下{resident_down_score:.1f}",
    ]
    if dominant_techniques:
        evidence.append('主要心理技法: ' + ', '.join(f"{x['name']} {x['rate_pct']:.1f}%" for x in dominant_techniques))
    if multi_name_users:
        evidence.append(f"同一IDの複数表示名 {len(multi_name_users)}件")
    if fetch_meta and not fetch_meta.get('complete', False):
        evidence.append('取得が途中終了のため確信度と上下示唆を50%へ減衰')

    actor_hypothesis = f"{organized_direction} / {actor_scale}"

    report['raw_direction_score'] = float(raw_direction_score)
    report['direction_participation_rate'] = float(direction_participation * 100.0)
    report['direction_score'] = float(direction_score)
    report['direction_label'] = direction_label
    report['coordination_score'] = float(coordination_score)  # 後方互換: 上下の高い方
    report['up_coordination_score'] = float(up_coord_score)
    report['down_coordination_score'] = float(down_coord_score)
    report['organized_direction'] = organized_direction
    report['up_coordination_detail'] = up_coord
    report['down_coordination_detail'] = down_coord
    report['actor_scale'] = actor_scale
    report['actor_confidence'] = confidence
    report['actor_confidence_pct'] = float(conf_pct)
    report['psychology_state'] = psychology
    report['psychology_factors'] = psych_rates
    report['dominant_psych_techniques'] = dominant_techniques
    report['up_psych_pressure'] = float(up_psych_pressure)
    report['down_psych_pressure'] = float(down_psych_pressure)
    report['actor_hypothesis'] = actor_hypothesis
    report['actor_evidence'] = evidence
    report['institutional_attribution'] = '掲示板単独では機関・大口・特定グループの身元は判定不能。ここでの主体規模感は投稿行動の協調性を示す。'
    report['fetch_meta'] = fetch_meta
    report['top1_post_share'] = float(top1_share)
    report['top3_post_share'] = float(top3_share)
    report['hhi_user_posts'] = float(hhi)
    report['euphoria_score'] = float(euphoria_score)
    report['despair_phase_score'] = float(despair_phase_score)
    report['silence_after_despair_score'] = float(silence_after_despair_score)
    report['prior_despair_score'] = float(prior_despair_score)
    report['recent_flow_drop_pct'] = float(flow_drop_pct)
    report['resident_up_score'] = float(resident_up_score)
    report['resident_down_score'] = float(resident_down_score)
    report['resident_accounts'] = resident_accounts[:10]
    report['market_psychology_phase'] = market_psychology_phase
    report['market_psychology_flags'] = cycle_flags

    report['bbs_rise_signal_pct'] = rise_signal_pct
    report['bbs_fall_signal_pct'] = fall_signal_pct
    report['bbs_price_direction_label'] = price_direction_label
    report['bbs_direction_edge_strength'] = edge_strength
    report['bbs_probability_status'] = 'REFERENCE_ONLY_NO_DB'
    report['bbs_probability_note'] = 'DB保存や学習を使わない参考比率です。実際の株価上昇確率ではなく、方向・協調・群集心理の逆張りフェーズをまとめた目安です。'

    # 後方互換: bullish_scoreは「上昇示唆% - 50」を2倍した方向スコアにする。
    report['bullish_score'] = float((rise_signal_pct - 50.0) * 2.0)
    return report

def analyze_bbs_manipulation(df, stock_name="不明", fetch_meta=None):
    """
    【究極版マルチ複合判定】全16パターン＋文脈理解（セマンティック処理）＋引用・アンケートスルー＋ブラックリスト/スパイク最適化
    """
    if df.empty:
        return {"is_manipulated": False, "intent": "データなし", "risk_level": "Low", "summary": ["対象枠内のデータが0件です。"], "anomalies": [], "max_flow": 0, "avg_len": 0, "fact_rate": 0, "blacklist": [], "extra_analysis_results": {}, "bullish_score": 0, "stock_name": stock_name, "direction_score": 0, "raw_direction_score": 0, "direction_participation_rate": 0, "direction_label": "データなし", "coordination_score": 0, "up_coordination_score": 0, "down_coordination_score": 0, "organized_direction": "判定不能", "actor_scale": "判定不能", "actor_confidence": "LOW", "psychology_state": "判定不能", "market_psychology_phase": "判定不能", "euphoria_score": 0, "despair_phase_score": 0, "silence_after_despair_score": 0, "resident_up_score": 0, "resident_down_score": 0, "bbs_rise_signal_pct": 50, "bbs_fall_signal_pct": 50, "bbs_probability_status": "REFERENCE_ONLY_NO_DB", "fetch_meta": fetch_meta or {}}

    df = df.copy()
    for _col, _default in (("good",0),("bad",0),("userId","unknown"),("dispname",""),("body",""),("part",""),("postDate","")):
        if _col not in df.columns:
            df[_col] = _default
    df["good"] = pd.to_numeric(df["good"], errors="coerce").fillna(0)
    df["bad"] = pd.to_numeric(df["bad"], errors="coerce").fillna(0)

    report = {
        'is_manipulated': False, 'intent': '正常（個人の雑談・ノイズベース）', 'risk_level': 'Low',
        'summary': [], 'total_posts': len(df), 'unique_users': df['userId'].nunique(),
        'anomalies': [], 'max_flow': 0, 'avg_len': 0, 'fact_rate': 0.0, 'blacklist': [], 'bullish_score': 0
    }
    
    # ===============================================================
    # ★精度向上①：テキストの前処理（引用返信の除去とタグ消去）
    # ===============================================================
    df['temp_body'] = df['body'].astype(str).str.replace('<br />', '\n', regex=False).str.replace('&gt;', '>', regex=False)
    # > または ＞ で始まる「引用行」を完全に削除する
    df['temp_body'] = df['temp_body'].apply(lambda x: re.sub(r'^[>＞].*$', '', x, flags=re.MULTILINE))
    
    # ===============================================================
    # ★精度向上②：アンケート（Poll）の検知フラグ
    # ===============================================================
    def check_poll(text):
        if ('はい' in text and 'いいえ' in text) or ('👍' in text and '👎' in text):
            return 1
        if '投票' in text and ('お願いします' in text or 'クリック' in text):
            return 1
        return 0
    df['is_poll'] = df['temp_body'].apply(check_poll)

    # ===============================================================
    # ★精度向上③：意味の反転（文脈を読むセマンティック処理）
    # ===============================================================
    def preprocess_semantics(text):
        # 1. 誤検知しやすい単語（買い煽り、売り残など）を透明化
        text = re.sub(r'買い煽|売り煽|買い残|売り残|信用買|信用売|買戻|買い戻', '', text)
        # 2. 「ガラッと」「ガラガラ」などの副詞・擬音語を透明化（「ガラ」暴落誤検知を防ぐ）
        text = re.sub(r'ガラッと|ガラガラ', '', text)
        # 3. 強力な打ち消し表現の反転
        # 句読点をまたいだ別文の「ない」を誤って否定と結びつけない。
        # 例: 「絶対上がる！今買わないと」は上方向のまま扱う。
        _gap = r'[^。！？!?、,\n]{0,5}'
        text = re.sub(r'(上が(?:ら|り|る|れ)?|高騰|爆上げ|反発|ストップ高|S高)' + _gap + r'(?:し)?(ない|ません|訳ない|わけない|状況では|厳しい|無理)', '下がる', text)
        text = re.sub(r'(下が(?:ら|り|る|れ)?|暴落|下落|ガラ|ストップ安|S安)' + _gap + r'(?:し)?(ない|ません|訳ない|わけない|状況では|厳しい|無理)', '上がる', text)
        return text

    # HTMLタグと改行を消し去り、純粋な「本人の言葉」だけにする
    df['clean_body'] = df['temp_body'].str.replace(r'<[^>]+>', '', regex=True).str.replace('\n', '').str.replace(' ', '')
    df['analyzed_body'] = df['clean_body'].apply(preprocess_semantics)
    df['body_len'] = df['analyzed_body'].str.len()

    # ★精度向上④：疑問形（？や?や「か」）の後に絵文字等があっても正確に質問と判定する
    def check_question(text):
        if re.search(r'[？\?か]([wWｗＷ草笑\s。、！\!\.😂😭🥺🙏💦🙇]*)$', text):
            return 1
        return 0
    df['is_question'] = df['analyzed_body'].apply(check_question)

    # ===============================================================
    # 正規表現を用いた高度な辞書群（表記揺れや活用を網羅）
    # ===============================================================
    pat_buy = re.compile(
        r'ストップ高|S高|Ｓ高|[買か]い気配|上が[るっり]|上げ[るてた]|爆上げ|ばくあげ|プラ転|反発|上昇|高騰|底打ち|'
        r'買い増|買[おえ]う|買っ[てた]|仕込|リバ|ホールド|握力|ガチホ|上値|青天井|割安|お宝|'
        r'期待|強い|最高|自社株買い|🚀|📈|助かる|救われる|耐え[ろる]|戻[るっ]|プラス|勝たん|買い時|追加|強気|ナンピン|押し目', 
        re.IGNORECASE
    )
    pat_sell = re.compile(
        r'ストップ安|S安|Ｓ安|[下さ]が[るっり]|下げ[るてた]|ガラ|ナイアガラ|暴落|急落|下落|マイ転|調整|'
        r'逃げ[ろてた]|空売り|売[ろお]う|売っ[てた]|損切り|損切|天井|下値|割高|'
        r'オワタ|終わった|クソ株|くそ株|紙切れ|弱い|最悪|ゴミ|退場|ヤバい|やばい|売ら|ヤラれ|焼かれ|全滅|減っ|📉|利確|逃避', 
        re.IGNORECASE
    )
    pat_squeeze = re.compile(r'踏み上げ|売り豚|燃料|丸焼け|担がれ|踏む|買い戻し', re.IGNORECASE)
    pat_fact = re.compile(r'決算|業績|四半期|進捗|コンセンサス|需給|PER|PBR|時価総額|中長期|指標|ファンダ|\d+日線|日足|月足', re.IGNORECASE)
    pat_noise = re.compile(r'脳死|全力|仕手|イナゴ', re.IGNORECASE)
    pat_despair = re.compile(r'ふざけんな|しね|氏ね|死ね|ゴミ|クソ|助けて|含み損|もう無理|退場|紙切れ|眠れ|南無|アーメン', re.IGNORECASE)
    pat_cult = re.compile(r'信じて|ついていき|神|流石|さすが|先生|天才', re.IGNORECASE)
    pat_migrating = re.compile(r'こっち|あっち|乗り換え|\d{4}.*?[行い]く', re.IGNORECASE)

    # 打ち消し表現
    pat_neg_sell = re.compile(r'下がらない|暴落しない|心配.*ない|問題.*ない|売らない|売るな|下げない|売らん|手離さない|手放さない|ガチホ', re.IGNORECASE)

    # ===============================================================
    # v3: 心理誘導技法。単なる強気/弱気とは別に「どう行動させようとしているか」を観測する。
    # ===============================================================
    psych_patterns = {
        'FOMO': re.compile(r'乗り遅れ|置いて[い行]かれ|今しか|今のうち|間に合わ|買わないと|売らないと|初動|祭り|チャンス', re.IGNORECASE),
        '恐怖': re.compile(r'逃げろ|逃げた方|危険|終わり|終わった|ストップ安|S安|暴落|紙切れ|退場|破産|地獄|助から|全滅', re.IGNORECASE),
        '緊急性': re.compile(r'今すぐ|今のうち|寄りで|寄り付き|PTS|急げ|間に合|今日中|明日まで|成[り]?行|すぐ買|すぐ売', re.IGNORECASE),
        '確信演出': re.compile(r'絶対|確実|間違いない|確定|100%|１００％|必ず|余裕|既定路線|決まり', re.IGNORECASE),
        '行動命令': re.compile(r'買え|買っとけ|買うべき|仕込め|握れ|ホールドしろ|売れ|売っとけ|逃げろ|損切れ|空売れ', re.IGNORECASE),
        '価格アンカー': re.compile(r'(?:株価|目標|明日|来週|年内)?[0-9０-９,，]{2,8}円|[0-9０-９,，]{2,8}(?:まで|超え|越え|割れ|いく|行く)', re.IGNORECASE),
        '権威付け': re.compile(r'機関|大口|外資|ファンド|証券|アナリスト|プロ|関係者|インサイダー|空売り機関', re.IGNORECASE),
        '群集圧力': re.compile(r'みんな|全員|総員|買い豚|売り豚|ホルダー|イナゴ|祭り|総悲観|総楽観|殺到|置いてかれる', re.IGNORECASE),
        '安心誘導': re.compile(r'大丈夫|安心|握って|ガチホ|助かる|戻る|心配ない|問題ない|耐えろ|放置で', re.IGNORECASE),
    }

    # 各投稿のスコアを正規表現で計算
    df['raw_buy_count'] = df['analyzed_body'].apply(lambda x: len(pat_buy.findall(x)))
    df['raw_sell_count'] = df['analyzed_body'].apply(lambda x: len(pat_sell.findall(x)))
    df['neg_sell_count'] = df['analyzed_body'].apply(lambda x: len(pat_neg_sell.findall(x)))

    # 打ち消しを考慮し、さらにアンケートや疑問形ならスコアを無効化する
    df['buy_count'] = (df['raw_buy_count'] + df['neg_sell_count']) * (1 - df['is_question']) * (1 - df['is_poll'])
    df['sell_count'] = (df['raw_sell_count'] - df['neg_sell_count']).clip(lower=0) * (1 - df['is_question']) * (1 - df['is_poll'])

    df['squeeze_count'] = df['analyzed_body'].apply(lambda x: len(pat_squeeze.findall(x)))
    df['fact_count'] = df['analyzed_body'].apply(lambda x: len(pat_fact.findall(x)))
    df['noise_count'] = df['analyzed_body'].apply(lambda x: len(pat_noise.findall(x)))
    df['despair_count'] = df['analyzed_body'].apply(lambda x: len(pat_despair.findall(x)))
    df['cult_count'] = df['analyzed_body'].apply(lambda x: len(pat_cult.findall(x)))
    df['mig_count'] = df['analyzed_body'].apply(lambda x: len(pat_migrating.findall(x)))

    # v3 心理誘導技法フラグ。質問・アンケートは「誘導」として数えない。
    _active_mask = (1 - df['is_question']) * (1 - df['is_poll'])
    psych_cols = []
    for _name, _pat in psych_patterns.items():
        _col = 'psych_' + _name
        df[_col] = df['analyzed_body'].apply(lambda x, p=_pat: 1 if p.search(str(x)) else 0) * _active_mask
        psych_cols.append(_col)
    df['psych_technique_count'] = df[psych_cols].sum(axis=1)
    df['post_net'] = df['buy_count'] - df['sell_count']
    df['post_direction'] = np.sign(df['post_net']).astype(int)
    
    # 時間帯フラグ
    df['post_dt'] = pd.to_datetime(df['postDate'])
    df['hour'] = df['post_dt'].dt.hour
    df['minute'] = df['post_dt'].dt.minute
    df['is_morning'] = (df['hour'] == 8) & (df['minute'] >= 30)
    df['is_midnight'] = (df['hour'] >= 1) & (df['hour'] <= 5)

    # ===============================================================
    # ユーザー分析（ブラックリスト判定の高度化）
    # ===============================================================
    for uid, u_df in df.groupby('userId'):
        u_posts_count = len(u_df)
        unique_names = u_df['dispname'].dropna().unique().tolist()
        
        u_buy = u_df['buy_count'].sum()
        u_sell = u_df['sell_count'].sum()
        u_avg_len = u_df['body_len'].mean()
        u_fact_total = u_df['fact_count'].sum()
        
        direction, dir_class = "方向不明", "badge-watch"
        if u_buy > u_sell: direction, dir_class = "買い誘導 (上)", "badge-buy"
        elif u_sell > u_buy: direction, dir_class = "売り誘導 (下)", "badge-sell"
        
        reasons = []
        if len(unique_names) > 1: reasons.append(f"【ネーム多重偽装】同一IDで複数名使用（{ ' -> '.join(map(str, unique_names)) }）")
        
        is_spammer = False
        if u_posts_count >= 5 or (u_posts_count / len(df) > 0.10):
            if u_avg_len < 40 and u_fact_total < 2:
                is_spammer = True 
            elif u_posts_count >= 15:
                is_spammer = True 
                
        if is_spammer:
            reasons.append(f"【高密度連投工作】短期間に {u_posts_count} 件の視覚占有工作")
            
        if reasons:
            report['blacklist'].append({'userId': str(uid), 'names': ", ".join(map(str, unique_names)), 'count': int(u_posts_count), 'direction': direction, 'dir_class': dir_class, 'reason': " / ".join(reasons)})

    report['user_occupancy'] = float(report['unique_users'] / report['total_posts']) if report['total_posts'] > 0 else 1.0
    time_resampled = df.set_index('post_dt').resample('10min').size()
    report['max_flow'] = int(time_resampled.max()) if not time_resampled.empty else 0
    report['avg_len'] = float(df['body_len'].mean())
    report['fact_rate'] = float((df['fact_count'] > 0).mean())
    
    df['total_reactions'] = df['good'] + df['bad']
    mean_react = df['total_reactions'].mean()
    std_react = df['total_reactions'].std()
    threshold = mean_react + (3 * std_react) if std_react > 0 else 50
    anomaly_df = df[df['total_reactions'] > threshold].copy()
    
    buy_score, sell_score, squeeze_score, fake_bear_score = 0, 0, 0, 0
    flag_hidden_fact, flag_sakura_good = False, False
    
    # ===============================================================
    # 異常スパイクの判定（秀逸な考察と荒らしの保護）
    # ===============================================================
    if not anomaly_df.empty:
        real_manipulation_count = 0 
        
        for _, row in anomaly_df.iterrows():
            if row['is_poll'] == 1:
                bias = "📊 掲示板アンケート（世論調査）"
            elif row['is_question'] == 1:
                bias = "🤔 狼狽・質問（投資家の迷い）"
            elif row['body_len'] >= 50 and row['fact_count'] >= 1 and row['good'] > row['bad']:
                bias = "💡 秀逸な考察（投資家からの高い支持）"
            elif row['body_len'] > 40 and row['fact_count'] > 0 and row['bad'] > row['good'] * 2:
                flag_hidden_fact = True
                bias = "🙈 ファクト長文へのBad偏重"
                real_manipulation_count += 1
            elif row['body_len'] < 10 and row['good'] > row['bad'] * 5 and row['good'] > 20:
                flag_sakura_good = True
                bias = "🤖 短文へのGood偏重"
                real_manipulation_count += 1
            elif row['good'] >= row['bad']:
                buy_score += (row['buy_count'] + 1) * (row['good'] - row['bad'])
                squeeze_score += row['squeeze_count'] * (row['good'] - row['bad'])
                bias = "強気投稿への反応スパイク"
                real_manipulation_count += 1
            else:
                # ★追加：Badが多い場合、短文で中身がないならただの「荒らし」としてスルーする
                if row['body_len'] < 40 and row['fact_count'] == 0:
                    bias = "🤬 荒らし・不快な投稿への反発（ノイズ）"
                else:
                    sell_score += (row['sell_count'] + 1) * (row['bad'] - row['good'])
                    if row['sell_count'] > 0:
                        fake_bear_score += (row['bad'] - row['good'])
                        bias = "弱気投稿への反応スパイク"
                    else:
                        bias = "弱気方向の反応スパイク"
                    real_manipulation_count += 1
                
            report['anomalies'].append({
                'part': str(row['part']), 'dispname': str(row['dispname']), 'postDate': str(row['postDate']), 
                'good': int(row['good']), 'bad': int(row['bad']), 'bias': bias, 'body': str(row['body']),
                'buy_count': int(row['buy_count']), 'sell_count': int(row['sell_count']),
                'is_poll': int(row.get('is_poll', 0)), 'is_question': int(row.get('is_question', 0))
            })

        if real_manipulation_count > 0:
            report['is_manipulated'] = True
            report['risk_level'] = 'High'
            report['summary'].append(f"【警告】ボタン統計に異常スパイク（しきい値 {threshold:.1f}票超）の工作疑い投稿を {real_manipulation_count} 件検知。")

    morning_rate = len(df[df['is_morning']]) / len(df) if len(df) > 0 else 0
    midnight_rate = len(df[df['is_midnight']]) / len(df) if len(df) > 0 else 0
    despair_rate = df['despair_count'].sum() / len(df) if len(df) > 0 else 0
    cult_rate = df['cult_count'].sum() / len(df) if len(df) > 0 else 0
    mig_rate = df['mig_count'].sum() / len(df) if len(df) > 0 else 0

    if report['max_flow'] >= 40:
        report['is_manipulated'] = True
        report['risk_level'] = 'High'
        if report['user_occupancy'] < 0.20:
            report['intent'] = '⑦【少数ID集中の高速投稿】協調投稿の可能性'
            report['summary'].append(f"【超高流速】10分間に最大 {report['max_flow']} 件の組織的連投。少数主体による協調的な世論形成と整合。")
        else:
            report['intent'] = '⑧【イナゴパニックお祭り相場】制御不能の殺到状態'
            report['summary'].append(f"【需給過熱】10分間に最大 {report['max_flow']} 件の書き込み。週明けは値幅が極限まで荒れるためエントリー要注意。")
            
    elif flag_hidden_fact:
        report['is_manipulated'] = True
        report['intent'] = '⑫【ファクト投稿へのBad集中】反対反応の偏りを検知'
        report['summary'].append("【反応偏り】ファクトを含む長文にBadが集中。組織的行為か単なる反論集中かは別判定が必要です。")
    elif flag_sakura_good:
        report['is_manipulated'] = True
        report['intent'] = '⑬【短文へのGood集中】不自然な支持反応'
        report['summary'].append("【反応偏り】短文にGoodが集中。人為的押し上げの可能性はあるが主体は特定できません。")
        
    elif report['is_manipulated']:
        if fake_bear_score > 0 and fake_bear_score > buy_score:
            report['intent'] = '⑤【弱気反応スパイク優勢】下方向の心理圧力を観測'
            report['summary'].append("【反応偏り】弱気投稿側の反応スパイクが優勢。目的や主体は断定せず、下方向協調度と併せて判定します。")
        elif squeeze_score > buy_score and squeeze_score > sell_score:
            report['intent'] = '⑥【踏み上げ言及集中】上方向モメンタム心理を観測'
            report['summary'].append("【踏み上げ心理】「売り豚」「燃料」等の語が反応とともに集中。上方向心理圧力として扱います。")
        elif report['user_occupancy'] < 0.25 and buy_score > sell_score:
            report['intent'] = '①【強気反応スパイク】上方向の反応偏りを観測'
            report['summary'].append("🚨【強気反応スパイク】少数主体への反応集中を検知。上方向協調度と投稿者集中度を併せて確認してください。")
            report['summary'].append("📈 チャートが底値圏なら初動と整合する場合がありますが、掲示板単独では玉集めや大口関与は判定しません。")
            report['summary'].append("📉 高値圏・急騰後ならFOMO型の過熱にもなり得るため、掲示板の強気だけを買い根拠にはしません。")
        elif report['user_occupancy'] < 0.25 and sell_score > buy_score:
            report['intent'] = '②【下方向誘導集中】恐怖・弱気投稿が少数IDに集中'
            report['summary'].append("【下方向心理圧】恐怖・弱気投稿が集中。安値集め目的か純粋な弱気かは掲示板単独では判定しません。")
        else:
            report['intent'] = '③【短期方向偏り】反応スパイクはあるが主体・目的は不明'
            
    elif morning_rate > 0.2:
        report['intent'] = '⑩【寄付前の誘導投稿集中】8:30以降の方向づけ'
        report['summary'].append("【寄付前心理】8:30以降の投稿比率が高い状態。気配に反応した自然投稿か誘導かは協調度と併せて判定します。")
    elif midnight_rate > 0.2 and report['user_occupancy'] < 0.3:
        report['intent'] = '⑪【深夜帯の少数ID集中】閑散時間の方向づけ'
        report['summary'].append("【深夜偏り】深夜帯に少数IDの投稿が集中。翌朝への方向づけの可能性があります。")
    elif cult_rate > 0.05:
        report['intent'] = '⑯【盲信・同調モード】特定人物への強い追随'
        report['summary'].append("【同調リスク】特定人物への追随語が増加。反転時に心理が一方向へ崩れやすい状態です。")
    elif mig_rate > 0.05:
        report['intent'] = '⑭【他銘柄への誘導言及】別銘柄への資金移動を促す投稿'
        report['summary'].append("【他銘柄誘導】別銘柄への乗り換えを促す投稿が増加。実際の資金移動は別データで確認が必要です。")
    elif despair_rate > 0.1:
        report['intent'] = '⑮【総悲観・狼狽優勢】心理的な投げムード'
        report['summary'].append("【悲観集中】罵倒・含み損・退場語が増加。反転余地もありますが、掲示板だけで底打ちは断定しません。")
            
    else:
        if report['user_occupancy'] >= 0.4 and report['avg_len'] >= 45 and df['fact_count'].sum() > df['noise_count'].sum():
            report['intent'] = '⑨【ファンダ言及型・長文議論】比較的多様な参加者'
            report['risk_level'] = 'Low'
            report['summary'].append(f"【長文議論】平均文字数 {report['avg_len']:.1f}文字。ファンダ関連語を含む投稿が比較的多い状態です。")
            report['summary'].append("参加者の多様性とファンダ言及は高めですが、内容の正しさや株価上昇を保証するものではありません。")
        elif report['user_occupancy'] < 0.3:
            report['intent'] = '④【少人数占有】過疎板で一部IDの影響が大きい'
            report['summary'].append("【少人数占有】投稿者が少なく、一部IDの発言が掲示板全体の印象を左右しやすい状態です。")
        else:
            report['intent'] = '正常（個人の自然な雑談ベース）'
            report['summary'].append("特筆すべき歪みや、際立った長文考察は見られません。標準的な個人の雑談ベースです。")
            
    # v3: 旧16分類は補助観測ラベル。ここから直接「買い/売り」を出さない。
    meta_map = {
        '①': {'action': '上方向観測', 'phase': '反応スパイク', 'class': 'action-buy'},
        '②': {'action': '下方向観測', 'phase': '恐怖/弱気集中', 'class': 'action-sell'},
        '③': {'action': '要確認', 'phase': '短期方向偏り', 'class': 'action-watch'},
        '④': {'action': '要確認', 'phase': '少人数占有', 'class': 'action-watch'},
        '⑤': {'action': '下方向観測', 'phase': '弱気反応偏り', 'class': 'action-sell'},
        '⑥': {'action': '上方向観測', 'phase': '踏み上げ心理', 'class': 'action-buy'},
        '⑦': {'action': '要警戒', 'phase': '少数ID高速投稿', 'class': 'action-watch'},
        '⑧': {'action': '要警戒', 'phase': '群集過熱', 'class': 'action-watch'},
        '⑨': {'action': '中立観測', 'phase': '長文/ファンダ議論', 'class': 'action-watch'},
        '⑩': {'action': '要確認', 'phase': '寄付前集中', 'class': 'action-watch'},
        '⑪': {'action': '要確認', 'phase': '深夜少数ID', 'class': 'action-watch'},
        '⑫': {'action': '要確認', 'phase': 'Bad反応偏り', 'class': 'action-watch'},
        '⑬': {'action': '要確認', 'phase': 'Good反応偏り', 'class': 'action-watch'},
        '⑭': {'action': '要確認', 'phase': '他銘柄誘導', 'class': 'action-watch'},
        '⑮': {'action': '下方向観測', 'phase': '悲観/狼狽', 'class': 'action-sell'},
        '⑯': {'action': '要警戒', 'phase': '盲信/同調', 'class': 'action-watch'}
    }
    report['action'], report['phase'], report['class'] = '様子見', '様子見', 'action-watch'
    for key, val in meta_map.items():
        if report['intent'].startswith(key):
            report['action'], report['phase'], report['class'] = val['action'], val['phase'], val['class']
            break
            
    def extra_analysis_modules(analysis_df):
        results = {'topic_list': [], 'stealth_sync': [], 'sentiment_timeline': []}
        topic_map = {
            'IR・公式発表': ['増資', '決算', '配当', '自社株買い', '上方修正', '下方修正', '適時開示', '中期経営計画', '中計', 'IR発表'],
            '個別材料・思惑': ['M&A', '業務提携', '資本提携', '買収', '新製品', '特許', '承認', '思惑', '材料出た', 'TOB', '株式分割'],
            'ニュース・市場': ['日経', '報道', '速報', 'ニュース', '株探', 'ロイター', '経済指標'],
            '政治・国策': ['政府', '自民', '野党', '総理', '規制', '法案', '補助金', '国策', '防衛'],
            '為替・金融': ['ドル円', '円安', '円高', '為替', '金利', '日銀', '植田', '政策金利']
        }
        for _, row in analysis_df.iterrows():
            tags = [t for t, ws in topic_map.items() if any(w in str(row['analyzed_body']) for w in ws)]
            if tags: 
                results['topic_list'].append({'postDate': str(row['postDate']), 'dispname': str(row['dispname']), 'part': str(row['part']), 'tags': tags, 'body': str(row['body']), 'buy_count': int(row['buy_count']), 'sell_count': int(row['sell_count']), 'is_poll': int(row['is_poll']), 'is_question': int(row['is_question'])})
        
        df_sorted = analysis_df.sort_values('post_dt')
        def get_bigrams(text):
            t = str(text)
            return set([t[i:i+2] for i in range(len(t)-1)]) if len(t) > 1 else set()

        best_matches = {}

        for i, row1 in df_sorted.iterrows():
            if len(str(row1['clean_body'])) < 25: continue
            
            window_end = row1['post_dt'] + pd.Timedelta(minutes=15)
            mask = (df_sorted['post_dt'] > row1['post_dt']) & (df_sorted['post_dt'] <= window_end)
            nearby_posts = df_sorted[mask]
            
            s1 = get_bigrams(row1['clean_body'])
            for j, row2 in nearby_posts.iterrows():
                if row1['userId'] == row2['userId']: continue
                if len(str(row2['clean_body'])) < 25: continue
                
                s2 = get_bigrams(row2['clean_body'])
                if not s1 or not s2: continue
                
                similarity = len(s1 & s2) / len(s1 | s2)
                if similarity >= 0.65:
                    target_part = str(row2['part'])
                    if target_part not in best_matches or best_matches[target_part]['similarity'] < similarity:
                        best_matches[target_part] = {
                            'postDate': str(row2['postDate']), 'dispname': str(row2['dispname']), 'part': str(row2['part']),
                            'similarity': float(similarity), 'source_id': str(row1['userId']), 'body': str(row2['body']),
                            'source_body': str(row1['body']), 'source_dispname': str(row1['dispname']),
                            'source_postDate': str(row1['postDate']), 'source_part': str(row1['part']),
                            'source_buy_count': int(row1['buy_count']), 'source_sell_count': int(row1['sell_count']),
                            'buy_count': int(row2['buy_count']), 'sell_count': int(row2['sell_count']),
                            'is_poll': int(row2.get('is_poll', 0)), 'is_question': int(row2.get('is_question', 0))
                        }
        
        results['stealth_sync'] = list(best_matches.values())

        temp_df = analysis_df.copy()
        temp_df['score'] = temp_df['buy_count'] - temp_df['sell_count']
        if not temp_df.empty:
            resampled = temp_df.set_index('post_dt').resample('30min')['score'].sum().fillna(0)
            results['sentiment_timeline'] = [{'time': ts.strftime('%m/%d %H:%M'), 'score': float(sc)} for ts, sc in resampled.items()]
        return results

    report['extra_analysis_results'] = extra_analysis_modules(df)
    
    score = 0
    inte = report['intent']
    if inte.startswith('⑨'): score += 10000    
    elif inte.startswith('⑥'): score += 9000    
    elif inte.startswith('②') or inte.startswith('⑮') or inte.startswith('⑫'): score += 8000    
    elif inte.startswith('①'): score += 5000    
    elif inte.startswith('③'): score += 3000    
    elif inte.startswith('④') or inte.startswith('⑪'): score += 2000    
    elif inte.startswith('⑧') or inte.startswith('⑩'): score += -1000   
    elif inte.startswith('⑦'): score += -2000   
    elif inte.startswith('⑤') or inte.startswith('⑬') or inte.startswith('⑭') or inte.startswith('⑯'): score += -5000   
    
    tl = report['extra_analysis_results'].get('sentiment_timeline', [])
    recent_sentiment = tl[-1]['score'] if tl else 0
    score += (report['fact_rate'] * 1000) + report['max_flow'] + (recent_sentiment * 10)        
    
    report['legacy_bullish_score'] = float(score)
    report['stock_name'] = stock_name
    report = enrich_actor_psychology(report, df, fetch_meta=fetch_meta)
    return report

def generate_html_report(df, report, code, filepath):
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    stock_name = report.get('stock_name', '不明')
    
    if report['risk_level'] == 'High': status_color, status_border, status_text = "#fff1f2", "#f43f5e", "#9f1239"
    elif '健全' in report['intent']: status_color, status_border, status_text = "#eff6ff", "#3b82f6", "#1e3a8a"
    else: status_color, status_border, status_text = "#f0fdf4", "#22c55e", "#166534"

    def get_dir_html(b_count, s_count, is_poll=0, is_question=0):
        if is_poll == 1:
            return "<span style='background-color:#fef3c7; color:#d97706; border:1px solid #fcd34d; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📊 アンケート</span>"
        elif is_question == 1:
            return "<span style='background-color:#e0f2fe; color:#0369a1; border:1px solid #7dd3fc; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>🤔 質問・迷い</span>"
        elif b_count > s_count:
            return "<span style='background-color:#dcfce7; color:#15803d; border:1px solid #86efac; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📈 買い誘導</span>"
        elif s_count > b_count:
            return "<span style='background-color:#fee2e2; color:#b91c1c; border:1px solid #fca5a5; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>📉 売り誘導</span>"
        else:
            return "<span style='background-color:#f1f5f9; color:#64748b; border:1px solid #cbd5e1; padding:4px 8px; border-radius:4px; font-size:11px; font-weight:bold; white-space:nowrap;'>➖ 様子見/中立</span>"

    unified_list = []
    
    for a in report['anomalies']:
        bc = "badge-trap" if any(x in a['bias'] for x in ["サクラ", "隠蔽", "買い嵌め", "売り崩"]) else "badge-watch"
        if "秀逸な考察" in a['bias']: bc = "badge-buy"
        if "荒らし" in a['bias']: bc = "badge-watch"
            
        unified_list.append({
            'date': a['postDate'], 'id_info': f"<b>{a['dispname']}</b><br><small style='color:#888;'>ID: {a['part']}</small>",
            'direction_html': get_dir_html(a.get('buy_count', 0), a.get('sell_count', 0), a.get('is_poll', 0), a.get('is_question', 0)),
            'badge': f"<span class='badge {bc}'>🚨 {a['bias']}</span>", 'body': a['body'],
            'link': f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{a['part']}"
        })
        
    for s in report['extra_analysis_results'].get('stealth_sync', []):
        sl = f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{s.get('source_part', '')}"
        tl = f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{s.get('part', '')}"
        ch = f"""
        <div style='background: #f8fafc; border-left: 4px solid #94a3b8; padding: 8px 12px; margin-bottom: 8px; border-radius: 4px;'>
            <div style='color: #475569; font-weight: bold; font-size: 0.85em; margin-bottom: 4px;'>📝 発信元の投稿 ({s.get('source_postDate', '')}) <a href='{sl}' target='_blank' style='margin-left:10px;'>🔗元へ</a></div>
            <div style='color: #475569; font-size: 0.9em;'>{s.get('source_body', '')}</div>
        </div>
        <div style='color: #7e22ce; font-weight: bold; font-size: 0.85em; margin-bottom: 4px;'>⬇️ 追従・コピペ投稿<a href='{tl}' target='_blank' style='margin-left:10px;'>🔗コピペへ</a></div>
        <div>{s['body']}</div>
        """
        unified_list.append({
            'date': s['postDate'], 'id_info': f"<b>{s['dispname']}</b><br><small style='color:#888;'>ID: {s['part']}</small>",
            'direction_html': get_dir_html(s.get('buy_count', 0), s.get('sell_count', 0), s.get('is_poll', 0), s.get('is_question', 0)),
            'badge': f"<span class='badge badge-stealth'>同期工作({s['similarity']*100:.0f}%)</span>", 'body': ch, 'link': tl
        })
        
    for t in report['extra_analysis_results'].get('topic_list', []):
        ts = "".join([f"<span class='badge badge-topic'>{tag}</span>" for tag in t['tags']])
        unified_list.append({
            'date': t['postDate'], 'id_info': f"<b>{t['dispname']}</b><br><small style='color:#888;'>ID: {t['part']}</small>",
            'direction_html': get_dir_html(t.get('buy_count', 0), t.get('sell_count', 0), t.get('is_poll', 0), t.get('is_question', 0)),
            'badge': ts, 'body': t['body'], 'link': f"https://finance.yahoo.co.jp/quote/{code}.T/forum/{t['part']}"
        })
        
    for item in unified_list: item['dt_obj'] = datetime.strptime(item['date'], '%Y/%m/%d %H:%M')
    unified_list.sort(key=lambda x: x['dt_obj'], reverse=True)
    
    integrated_rows = ""
    for item in unified_list:
        integrated_rows += f"<tr><td>{item['date']}</td><td>{item['id_info']}</td><td style='text-align:center; vertical-align:middle;'>{item['direction_html']}</td><td>{item['badge']}</td><td class='comment-body'>{item['body']}</td><td style='text-align:center; vertical-align:middle;'><a href=\"{item['link']}\" target=\"_blank\" style=\"text-decoration:none; font-size: 1.4em;\">🔗</a></td></tr>"
    if not integrated_rows: integrated_rows = "<tr><td colspan='6' style='text-align:center; color:#888; padding: 30px;'>異常検知や重要トピックはありませんでした。</td></tr>"

    blacklist_rows = ""
    for target in report['blacklist']:
        profile_url = target.get('profile_url') or yahoo_user_profile_url(target.get('userId'))
        known_stocks = target.get('known_stocks', []) or []
        stock_links = []
        for ks in known_stocks:
            kc = html_lib.escape(str(ks.get('code') or ''))
            kn = html_lib.escape(str(ks.get('stock_name') or ''))
            if not kc:
                continue
            label = f"{kc}{' ' + kn if kn else ''}"
            stock_links.append(f"<a href='report_{kc}.html' target='_blank' style='display:inline-block;margin:2px 7px 2px 0;'>{label}</a>")
        known_html = "".join(stock_links) or "<span style='color:#94a3b8;'>今回確認できた他銘柄なし</span>"
        name_html = (
            f"<a href='{html_lib.escape(profile_url)}' target='_blank' "
            f"style='font-weight:800;color:#2563eb;text-decoration:none;'>"
            f"{html_lib.escape(str(target.get('names') or target.get('userId') or ''))} ↗</a>"
        )
        blacklist_rows += (
            "<tr>"
            f"<td><code style='background:#f1f5f9; padding:2px 6px; border-radius:4px; font-weight:bold; color:#0f172a;'>{html_lib.escape(str(target.get('userId') or ''))}</code></td>"
            f"<td>{name_html}</td>"
            f"<td style='text-align:center; font-weight:bold; color:#e11d48;'>{target.get('count',0)} 件</td>"
            f"<td style='text-align:center;'><span class='badge {target.get('dir_class','badge-watch')}'>{html_lib.escape(str(target.get('direction') or ''))}</span></td>"
            f"<td style='min-width:260px;'>{known_html}</td>"
            f"<td style='color:#7f1d1d; font-size:12.5px; font-weight:600; background:#fef2f2;'>⚠️ {html_lib.escape(str(target.get('reason') or ''))}</td>"
            "</tr>"
        )
    if not blacklist_rows:
        blacklist_rows = "<tr><td colspan='6' style='text-align:center; color:#64748b; padding: 25px;'>検出基準に該当する高占有・表示名変更などの要注意条件に該当するアカウントはいません。</td></tr>"

    timeline = report['extra_analysis_results'].get('sentiment_timeline', [])
    if not timeline: chart_html, chart_labels, chart_scores = "<div style='text-align:center; width:100%; color:#888; padding: 30px;'>グラフ描画用データがありません</div>", "[]", "[]"
    else:
        chart_html = """<div style="position: relative; height: 260px; width: 100%;"><canvas id="sentimentChart"></canvas></div>"""
        chart_labels = json.dumps([item['time'] for item in timeline])
        chart_scores = json.dumps([item['score'] for item in timeline])

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>{stock_name}({code}) マルチ分析レポート</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            html {{ scroll-behavior: smooth; }}
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; background-color: #f0f2f5; color: #1e293b; margin: 0; padding: 20px; }}
            .header-main {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 4px solid #1e3a8a; padding-bottom: 12px; margin-bottom: 15px; }}
            .header-main h1 {{ color: #1e3a8a; margin: 0; font-size: 24px; }}
            .meta-info {{ color: #64748b; font-size: 0.9em; }}
            .toc-container {{ position: sticky; top: 0; background: rgba(240, 242, 245, 0.95); padding: 12px 0; z-index: 1000; border-bottom: 1px solid #cbd5e1; margin-bottom: 25px; display: flex; gap: 12px; flex-wrap: wrap; backdrop-filter: blur(5px); }}
            .toc-link {{ text-decoration: none; color: #1e3a8a; background: white; border: 1px solid #94a3b8; padding: 8px 16px; border-radius: 20px; font-size: 13px; font-weight: bold; transition: all 0.2s; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }}
            .toc-link:hover {{ background: #1e3a8a; color: white; border-color: #1e3a8a; }}
            .dashboard-top {{ display: flex; gap: 20px; margin-bottom: 25px; align-items: stretch; }}
            .col-left {{ flex: 1.2; display: flex; flex-direction: column; }}
            .col-right {{ flex: 1; display: flex; flex-direction: column; justify-content: space-between; gap: 12px; }}
            .summary-box {{ padding: 22px; border-radius: 8px; line-height: 1.7; box-shadow: inset 0 2px 4px rgba(0,0,0,0.02); height: 100%; box-sizing: border-box; }}
            .dynamic-status {{ background-color: {status_color}; border-left: 8px solid {status_border}; color: {status_text}; }}
            .intent-label {{ font-size: 0.85em; font-weight: bold; text-transform: uppercase; opacity: 0.8; margin-bottom: 6px; }}
            .intent-title {{ font-size: 1.35em; font-weight: 800; line-height: 1.4; }}
            .metrics-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; flex-grow: 1; }}
            .card {{ background: white; border: 1px solid #e2e8f0; padding: 14px; border-radius: 8px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.02); display: flex; flex-direction: column; justify-content: center; }}
            .card.full-width {{ grid-column: span 2; background: #f8fafc; border-style: dashed; }}
            .card .title {{ font-size: 0.82em; color: #64748b; font-weight: 600; margin-bottom: 4px; }}
            .card .value {{ font-size: 20px; font-weight: bold; color: #0f172a; }}
            .highlight-orange {{ color: #ea580c !important; }}
            .highlight-blue {{ color: #2563eb !important; }}
            .content-wrapper {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.04); margin-bottom: 25px; scroll-margin-top: 70px; }}
            h2 {{ font-size: 17px; color: #1e3a8a; margin-top: 0; margin-bottom: 15px; border-left: 5px solid #1e3a8a; padding-left: 10px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 6px; overflow: hidden; margin-top: 5px; }}
            th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #e2e8f0; font-size: 13px; vertical-align: top; }}
            th {{ background-color: #f1f5f9; color: #475569; font-weight: bold; font-size: 0.9em; }}
            tr:hover {{ background-color: #f8fafc; }}
            .badge {{ padding: 5px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; display: inline-block; margin-bottom: 4px; }}
            .badge-buy {{ background-color: #dcfce7; color: #15803d; border: 1px solid #86efac; }}
            .badge-sell {{ background-color: #fee2e2; color: #b91c1c; border: 1px solid #fca5a5; }}
            .badge-trap {{ background-color: #fef3c7; color: #d97706; border: 1px dashed #d97706; }}
            .badge-topic {{ background-color: #e0f2fe; color: #0369a1; border: 1px solid #7dd3fc; margin-right: 4px; }}
            .badge-stealth {{ background-color: #f3e8ff; color: #7e22ce; border: 1px solid #d8b4fe; }}
            .badge-watch {{ background-color: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; }}
            .comment-body {{ font-size: 0.95em; max-width: 550px; word-wrap: break-word; line-height: 1.5; }}
            .guide-table th {{ background-color: #1e3a8a; color: white; }}
            .bl-table th {{ background-color: #7f1d1d; color: white; }}
            a[title="Yahoo!ファイナンスの実際の投稿へ飛ぶ"]:hover {{ opacity: 0.7; transform: scale(1.1); display: inline-block; transition: all 0.2s; }}
        </style>
    </head>
    <body>
        <div style="max-width: 1250px; margin: 0 auto;">
            <div class="header-main">
                <h1>📊 {stock_name}({code}) 掲示板心理・方向別協調・上下示唆レポート</h1>
                <div class="meta-info">生成日時: {now_str} | 対象期間内の総スキャン数: {report['total_posts']} 件</div>
            </div>
            
            <div class="toc-container">
                <a href="#sec-dashboard" class="toc-link">🎯 誘導方向・主体規模サマリー</a>
                <a href="#sec-timeline" class="toc-link">📈 時系列心理グラフ</a>
                <a href="#sec-integrated" class="toc-link">🔍 統合検知リスト</a>
                <a href="#sec-blacklist" class="toc-link">⚠️ 高占有・要注意アカウント</a>
                <a href="#sec-matrix" class="toc-link">📊 判定マトリクス早見表</a>
            </div>
            
            <div class="dashboard-top" id="sec-dashboard">
                <div class="col-left">
                    <div class="summary-box dynamic-status">
                        <div class="intent-label">🎯 解析された掲示板の現状パターン</div>
                        <div style="margin: 12px 0; display: flex; gap: 8px;">
                            <span class="{report['class']}" style="padding: 5px 12px; border-radius: 4px; font-weight: bold; border: 2px solid currentColor; background: rgba(255,255,255,0.5);">
                                補助判定：{report['action']}
                            </span>
                            <span style="padding: 5px 12px; border-radius: 4px; font-weight: bold; border: 2px solid #64748b; background: rgba(255,255,255,0.5); color: #64748b;">
                                補助パターン：{report['phase']}
                            </span>
                        </div>
                        <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin:12px 0;">
                            <div style="background:rgba(255,255,255,.75);padding:11px;border-radius:6px;border:2px solid #22c55e;"><b>📈 上昇参考</b><br><span style="font-size:24px;font-weight:900;color:#15803d;">{report.get('bbs_rise_signal_pct',50):.1f}%</span></div>
                            <div style="background:rgba(255,255,255,.75);padding:11px;border-radius:6px;border:2px solid #ef4444;"><b>📉 下落参考</b><br><span style="font-size:24px;font-weight:900;color:#b91c1c;">{report.get('bbs_fall_signal_pct',50):.1f}%</span></div>
                            <div style="grid-column:span 2;background:rgba(255,255,255,.65);padding:9px;border-radius:6px;"><b>掲示板心理からの参考方向</b><br>{report.get('bbs_price_direction_label','-')} / エッジ {report.get('bbs_direction_edge_strength',0):.1f}</div>
                            <div style="background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>全体方向</b><br>{report.get('direction_label','-')} <b>({report.get('direction_score',0):+.1f})</b><br><small>方向参加率 {report.get('direction_participation_rate',0):.1f}%</small></div>
                            <div style="background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>協調している側</b><br>{report.get('organized_direction','-')}</div>
                            <div style="background:rgba(240,253,244,.85);padding:9px;border-radius:6px;"><b>上方向協調度</b><br>{report.get('up_coordination_score',0):.1f}/100</div>
                            <div style="background:rgba(254,242,242,.85);padding:9px;border-radius:6px;"><b>下方向協調度</b><br>{report.get('down_coordination_score',0):.1f}/100</div>
                            <div style="background:rgba(240,253,244,.65);padding:9px;border-radius:6px;"><b>上方向心理圧</b><br>{report.get('up_psych_pressure',0):.1f}/100</div>
                            <div style="background:rgba(254,242,242,.65);padding:9px;border-radius:6px;"><b>下方向心理圧</b><br>{report.get('down_psych_pressure',0):.1f}/100</div>
                            <div style="background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>主体規模感</b><br>{report.get('actor_scale','-')}</div>
                            <div style="background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>分析確信度</b><br>{report.get('actor_confidence','LOW')} ({report.get('actor_confidence_pct',0):.0f}%)</div>
                            <div style="grid-column:span 2;background:#fffbeb;padding:9px;border-radius:6px;border:1px solid #fde68a;"><b>群集心理フェーズ</b><br>{report.get('market_psychology_phase','-')}</div>
                            <div style="background:#fff7ed;padding:9px;border-radius:6px;"><b>🌸 花畑・総楽観</b><br>{report.get('euphoria_score',0):.1f}/100</div>
                            <div style="background:#fef2f2;padding:9px;border-radius:6px;"><b>😰 総悲観</b><br>{report.get('despair_phase_score',0):.1f}/100</div>
                            <div style="background:#f8fafc;padding:9px;border-radius:6px;"><b>🌙 悲観後の静寂</b><br>{report.get('silence_after_despair_score',0):.1f}/100</div>
                            <div style="background:#f8fafc;padding:9px;border-radius:6px;"><b>🧱 固定ID常駐圧</b><br>上 {report.get('resident_up_score',0):.1f} / 下 {report.get('resident_down_score',0):.1f}</div>
                            <div style="grid-column:span 2;background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>心理状態</b><br>{report.get('psychology_state','-')}</div>
                            <div style="grid-column:span 2;background:rgba(255,255,255,.55);padding:9px;border-radius:6px;"><b>主要心理誘導技法</b><br>{', '.join([f"{x['name']} {x['rate_pct']:.1f}%" for x in report.get('dominant_psych_techniques',[])]) or '目立つ技法なし'}</div>
                        </div>
                        <div style="font-size:12px;padding:8px;background:#fff7ed;border:1px solid #fed7aa;border-radius:6px;margin-bottom:8px;"><b>参考比率について:</b> {report.get('bbs_probability_note','')}</div>
                        <div style="font-size:12px;padding:8px;background:rgba(255,255,255,.45);border-radius:6px;margin-bottom:10px;">{report.get('institutional_attribution','')}</div>
                        <div class="intent-title">{report['intent']}</div>
                        <hr style="border: 0; border-top: 1px solid rgba(0,0,0,0.1); margin: 15px 0;">
                        <div class="intent-label">📋 スクリーニングシステム戦略分析ログ</div>
                        <ul style="margin: 5px 0 0 0; padding-left: 18px; font-size: 13.5px;">
                            {"".join([f"<li style='margin-bottom: 4px;'>{line}</li>" for line in (report['summary'] + ["観測根拠: " + e for e in report.get('actor_evidence', [])])])}
                        </ul>
                    </div>
                </div>
                <div class="col-right">
                    <div class="metrics-grid">
                        <div class="card"><div class="title">期間内総投稿数</div><div class="value">{report['total_posts']} 件</div></div>
                        <div class="card"><div class="title">最大流速 (10分ピーク)</div><div class="value {'highlight-orange' if report['max_flow'] >= 40 else ''}">{report['max_flow']} 件</div></div>
                        <div class="card"><div class="title">平均文字数 (テキスト密度)</div><div class="value {'highlight-blue' if report['avg_len'] >= 45 else ''}">{report['avg_len']:.1f} 文字</div></div>
                        <div class="card"><div class="title">ファクト分析密度スコア</div><div class="value">{report['fact_rate']:.2f}</div></div>
                        <div class="card full-width"><div class="title">ユーザー多様性</div><div class="value">{report['user_occupancy']:.2f} （ユニーク数: {report['unique_users']} 人）</div></div>
                        <div class="card full-width"><div class="title">取得品質</div><div class="value">{'COMPLETE' if report.get('fetch_meta',{}).get('complete') else 'PARTIAL'} / pages={report.get('fetch_meta',{}).get('pages',0)}</div></div>
                    </div>
                </div>
            </div>
            
            <div class="content-wrapper" id="sec-timeline">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h2 style="margin-bottom: 0;">📈 時系列心理分析トレンド（買い・売り優勢 / 30分間隔）</h2>
                    <div style="display: flex; gap: 12px; font-size: 12px; font-weight: bold; color: #475569; background: #f1f5f9; padding: 6px 12px; border-radius: 6px;">
                        <div style="display: flex; align-items: center; gap: 4px;"><span style="display:inline-block; width:12px; height:12px; background:#22c55e; border-radius:50%;"></span>買い優勢</div>
                        <div style="display: flex; align-items: center; gap: 4px;"><span style="display:inline-block; width:12px; height:12px; background:#ef4444; border-radius:50%;"></span>売り優勢</div>
                    </div>
                </div>
                <div style="background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0;">
                    {chart_html}
                </div>
            </div>
            
            <div class="content-wrapper" id="sec-integrated">
                <h2 style="color: #0369a1; border-left-color: #0369a1; margin-bottom: 5px;">🔍 統合検知リスト（スパイク・ステルス同期・マクロトピック）</h2>
                <p style="font-size: 12px; color: #64748b; margin-top: 0; margin-bottom: 15px;">※フラグは投稿・反応の偏りを示す観測シグナルです。機関・大口・特定グループの身元を直接証明するものではありません。</p>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 120px;">投稿日時</th>
                            <th style="width: 140px;">アカウント / ID</th>
                            <th style="width: 100px; text-align: center;">誘導方向</th>
                            <th style="width: 180px;">検知フラグ</th>
                            <th>投稿本文</th>
                            <th style="width: 60px; text-align: center;">リンク</th>
                        </tr>
                    </thead>
                    <tbody>{integrated_rows}</tbody>
                </table>
            </div>

            <div class="content-wrapper" id="sec-blacklist">
                <h2 style="color: #7f1d1d; border-left-color: #7f1d1d;">💀 掲示板占有・表示名変更などの要注意アカウント</h2>
                <table class="bl-table">
                    <thead><tr><th style="width: 140px;">userId（生ID）</th><th style="width: 180px;">使用ニックネーム</th><th style="width: 80px; text-align: center;">連投数</th><th style="width: 140px; text-align: center;">誘導ベクトル</th><th>⚠️ 要注意とした観測理由</th></tr></thead>
                    <tbody>{blacklist_rows}</tbody>
                </table>
            </div>
            
            <div class="content-wrapper" id="sec-matrix">
                <h2>🎯 ヤフ板心理・誘導パターン仮説マトリクス（観測→解釈）</h2>
                <table class="guide-table">
                    <thead><tr><th style="width: 260px;">パターン番号とタイトル</th><th style="width: 100px; text-align: center;">売買・行動</th><th style="width: 160px; text-align: center;">需給フェーズ</th><th>トレード戦略</th><th>心理・行動仮説（断定ではない）</th></tr></thead>
                    <tbody>
                        <tr><td><b>①【買い方向の集中スパイク】</b></td><td style="text-align:center;">上方向</td><td style="text-align:center;">要確認</td><td>少数ID・短時間・反応偏りが重なるほど協調的な上方向誘導の可能性が上がる。</td><td>自然な強気化でも起きるため、主体の身元や売買目的は掲示板単独では断定しない。</td></tr>
                        <tr><td><b>②【売り方向の集中】</b></td><td style="text-align:center;">下方向</td><td style="text-align:center;">要確認</td><td>恐怖語・売り語が少数IDに集中しているか、多数参加の自然悲観かを分離して見る。</td><td>「安く集めたい大口」とは自動断定せず、下方向への心理誘導として扱う。</td></tr>
                        <tr><td><b>③【方向混在・短期世論揺れ】</b></td><td style="text-align:center;">混在</td><td style="text-align:center;">様子見</td><td>買い・売り双方の投稿が拮抗し、明確な誘導方向がまだ出ていない状態。</td><td>個人同士の通常の意見対立で説明できることが多い。</td></tr>
                        <tr><td><b>④【少人数占有】</b></td><td style="text-align:center;">方向次第</td><td style="text-align:center;">低流動</td><td>投稿者が少なく、一部IDだけで掲示板の印象が大きく変わる状態。</td><td>協調度が低ければ個人レベル、高ければ小集団的な方向づけの可能性。</td></tr>
                        <tr><td><b>⑤【反応逆転・逆張り安心感】</b></td><td style="text-align:center;">要確認</td><td style="text-align:center;">反応偏り</td><td>投稿本文の方向とGood/Badの反応方向が大きく食い違う状態。</td><td>意図的な印象操作の可能性もあるが、単なる反論集中との区別が必要。</td></tr>
                        <tr><td><b>⑥【踏み上げ言説集中】</b></td><td style="text-align:center;">上方向</td><td style="text-align:center;">モメンタム</td><td>「踏み上げ」「売り豚」「燃料」などの上方向を煽る言説が集中。</td><td>実際の空売り需給は掲示板とは別データで確認する必要がある。</td></tr>
                        <tr><td><b>⑦【少数ID集中の高速投稿】</b></td><td style="text-align:center;">方向次第</td><td style="text-align:center;">高協調候補</td><td>10分流速が高いのにユニークIDが少ない状態。</td><td>協調グループ的行動と整合するが、機関・大口の身元は識別できない。</td></tr>
                        <tr><td><b>⑧【多数参加の祭り状態】</b></td><td style="text-align:center;">方向次第</td><td style="text-align:center;">群集過熱</td><td>高速投稿だが参加者も多く、自然な群集反応で説明しやすい状態。</td><td>組織工作より個人の殺到を優先仮説とする。</td></tr>
                        <tr><td><b>⑨【多様なファクト考察】</b></td><td style="text-align:center;">自然</td><td style="text-align:center;">低協調</td><td>多くのIDが長文・ファクト中心で議論している状態。</td><td>意図的誘導より自然な投資家議論の可能性を高く見る。</td></tr>
                        <tr><td><b>⑩【寄付前の誘導投稿集中】</b></td><td style="text-align:center;">方向次第</td><td style="text-align:center;">寄付前</td><td>8:30以降に方向性のある投稿が集中。</td><td>気配への自然反応か、注文心理を寄せる意図かを協調度と合わせて見る。</td></tr>
                        <tr><td><b>⑪【深夜帯の少数ID集中】</b></td><td style="text-align:center;">方向次第</td><td style="text-align:center;">閑散時間</td><td>閲覧者が少ない深夜に少数IDの方向性投稿が続く状態。</td><td>翌朝への方向づけ仮説は持てるが、身元や目的は断定しない。</td></tr>
                        <tr><td><b>⑫【ファクト投稿へのBad集中】</b></td><td style="text-align:center;">反応偏り</td><td style="text-align:center;">要確認</td><td>長文・ファクト投稿にBadが偏る状態。</td><td>反論集中・コミュニティ対立・印象操作の複数仮説を残す。</td></tr>
                        <tr><td><b>⑬【短文へのGood集中】</b></td><td style="text-align:center;">上方向候補</td><td style="text-align:center;">反応偏り</td><td>情報量の少ない短文に不釣り合いなGoodが集中。</td><td>人為的押し上げと整合するが、主体は特定不能。</td></tr>
                        <tr><td><b>⑭【他銘柄への誘導】</b></td><td style="text-align:center;">資金移動心理</td><td style="text-align:center;">宣伝</td><td>別銘柄コードや乗り換え表現が増える状態。</td><td>単なる情報共有か意図的な資金誘導かを投稿者集中度で見る。</td></tr>
                        <tr><td><b>⑮【総悲観・狼狽優勢】</b></td><td style="text-align:center;">下方向心理</td><td style="text-align:center;">悲観</td><td>含み損・退場・助けて等の悲観語が増加。</td><td>反転余地の心理指標にはなるが、底打ちそのものは断定しない。</td></tr>
                        <tr><td><b>⑯【盲信・同調モード】</b></td><td style="text-align:center;">上方向候補</td><td style="text-align:center;">同調</td><td>特定人物への称賛・盲信語が増え、判断が一方向化。</td><td>同調が崩れた際に心理が急反転しやすい点を警戒する。</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <script>
        const timelineScores = {chart_scores};
        if (timelineScores && timelineScores.length > 0) {{
            const ctx = document.getElementById('sentimentChart').getContext('2d');
            const labels = {chart_labels};
            const pointColors = timelineScores.map(val => val > 0 ? '#22c55e' : (val < 0 ? '#ef4444' : '#94a3b8'));
            new Chart(ctx, {{
                type: 'line',
                data: {{ labels: labels, datasets: [{{ label: '心理スコア', data: timelineScores, borderColor: '#94a3b8', backgroundColor: 'rgba(148, 163, 184, 0.1)', borderWidth: 2, pointBackgroundColor: pointColors, pointBorderColor: '#ffffff', pointBorderWidth: 1.5, pointRadius: 5, pointHoverRadius: 8, fill: true, tension: 0.4 }}] }},
                options: {{ responsive: true, maintainAspectRatio: false, scales: {{ y: {{ grid: {{ color: (ctx) => ctx.tick.value === 0 ? '#475569' : '#e2e8f0', lineWidth: (ctx) => ctx.tick.value === 0 ? 2 : 1 }} }} }}, plugins: {{ legend: {{ display: false }}, tooltip: {{ padding: 10, callbacks: {{ label: function(context) {{ let val = context.parsed.y; return val > 0 ? '📈 買い優勢 (スコア: +' + val + ')' : (val < 0 ? '📉 売り優勢 (スコア: ' + val + ')' : '➖ 中立 (スコア: 0)'); }} }} }} }} }}
            }});
        }}
        </script>
    </body>
    </html>
    """
    with open(filepath, 'w', encoding='utf-8') as f: f.write(html_content)
    print(f"📄 HTMLレポートを完全生成しました: {filepath}")

def generate_ranking_html(ranking_list, filepath):
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    table_rows = ""
    for i, rep in enumerate(ranking_list):
        code, stock_name = rep.get('code', 'N/A'), rep.get('stock_name', '不明')
        score, posts = float(rep.get('bullish_score', 0)), int(rep.get('total_posts', 0))
        rise_p = float(rep.get('bbs_rise_signal_pct', 50)); fall_p = float(rep.get('bbs_fall_signal_pct', 50))
        dir_score = float(rep.get('direction_score', 0)); dir_part = float(rep.get('direction_participation_rate', 0))
        up_coord = float(rep.get('up_coordination_score', 0)); down_coord = float(rep.get('down_coordination_score', 0))
        organized_dir = rep.get('organized_direction', '')
        actor_scale = rep.get('actor_scale', '')
        actor_conf = rep.get('actor_confidence', '')
        psych = rep.get('psychology_state', '')
        cycle = rep.get('market_psychology_phase', '')
        eup = float(rep.get('euphoria_score', 0)); desp = float(rep.get('despair_phase_score', 0)); silence = float(rep.get('silence_after_despair_score', 0))
        resident_down = float(rep.get('resident_down_score', 0))
        action, phase = rep.get('action', ''), rep.get('phase', '')
        intent = rep.get('intent', '')
        fact_rate, max_flow = float(rep.get('fact_rate', 0)), int(rep.get('max_flow', 0))
        
        target_aim = intent.split('】')[0].replace('【', '') if '】' in intent else intent
        
        if action == '買い': action_style = "background-color:#dcfce7; color:#15803d; border:1px solid #86efac"
        elif action == '要確認': action_style = "background-color:#fef3c7; color:#d97706; border:1px solid #fcd34d"
        elif action == '売り': action_style = "background-color:#fee2e2; color:#b91c1c; border:1px solid #fca5a5"
        else: action_style = "background-color:#f1f5f9; color:#64748b; border:1px solid #cbd5e1"
        
        row_style = "background-color: #f8fafc;" if posts < 10 else ""
        table_rows += f"""<tr style="{row_style}"><td style="text-align:center;">{i+1}</td><td style="text-align:center;">{stock_name}</td><td style="text-align:center;"><a href="report_{code}.html" target="_blank" style="font-weight:bold; color:#2563eb; text-decoration:none;">{code}</a></td><td style="text-align:center;font-weight:900;color:#15803d;">{rise_p:.1f}%</td><td style="text-align:center;font-weight:900;color:#b91c1c;">{fall_p:.1f}%</td><td style="text-align:center;font-weight:bold;">{dir_score:+.1f}</td><td style="text-align:center;">{dir_part:.1f}%</td><td style="text-align:center;font-weight:bold;color:#15803d;">{up_coord:.1f}</td><td style="text-align:center;font-weight:bold;color:#b91c1c;">{down_coord:.1f}</td><td style="font-size:12px;font-weight:bold;">{organized_dir}</td><td style="font-size:12px;">{actor_scale}</td><td style="text-align:center;">{actor_conf}</td><td style="font-size:12px;">{psych}</td><td style="font-size:12px;min-width:220px;">{cycle}</td><td style="text-align:center;">{eup:.0f}</td><td style="text-align:center;">{desp:.0f}</td><td style="text-align:center;">{silence:.0f}</td><td style="text-align:center;">{resident_down:.0f}</td><td style="text-align:center;">{posts}</td><td style="text-align:center;"><span style="{action_style}; padding:4px 8px; border-radius:4px; font-weight:bold; font-size:12px;">{action}</span></td><td style="text-align:center; font-size:13px; font-weight:bold;">{phase}</td><td style="font-size:13px; color:#334155; font-weight:bold;">{target_aim}</td><td style="text-align:center;">{fact_rate:.2f}</td><td style="text-align:center;">{max_flow}</td></tr>"""
        
    if not table_rows: table_rows = "<tr><td colspan='24' style='text-align:center; padding:30px; color:#64748b;'>解析可能なデータがありませんでした。</td></tr>"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>掲示板心理フェーズ・参考方向 スクリーニング全件ランキング</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/js/jquery.tablesorter.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/js/jquery.tablesorter.widgets.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/css/theme.default.min.css">
        <style>
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; background-color: #f0f2f5; color: #1e293b; margin: 0; padding: 30px; }} 
            .header-main {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 4px solid #ea580c; padding-bottom: 12px; margin-bottom: 20px; }} 
            .header-main h1 {{ color: #ea580c; margin: 0; font-size: 26px; }} 
            .meta-info {{ color: #64748b; font-size: 0.9em; }} 
            .content-wrapper {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.04); }} 
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }} 
            th {{ background-color: #1e3a8a !important; color: white !important; cursor: pointer; padding: 12px !important; text-align:center; }} 
            td {{ padding: 10px; border-bottom: 1px solid #e2e8f0; }} 
            tr:hover {{ background-color: #f8fafc; }}
            .tablesorter-filter {{ width: 95%; padding: 4px; border-radius: 4px; border: 1px solid #ccc; font-size: 12px; box-sizing: border-box; }}
            select.tablesorter-filter {{ height: 26px; }}
        </style>
    </head>
    <body>
        <div style="max-width: 1400px; margin: 0 auto;">
            <div class="header-main"><h1>🏆 ヤフ板・群集心理フェーズ / 方向別協調度 スクリーニング全件ランキング</h1><div class="meta-info">生成日時: {now_str} | 対象: {len(ranking_list)}銘柄</div></div>
            <div class="content-wrapper"><p style="font-size:14px; color:#475569;">※ 上昇/下落はDBや学習を使わない参考比率です。総楽観・総悲観・悲観後の静寂・固定ID常駐も逆張り補正しています。各列はソート・絞り込みできます。</p>
                <table id="rankTable" class="tablesorter">
                    <thead>
                        <tr>
                            <th data-filter="false">順位</th>
                            <th>銘柄名</th>
                            <th>コード</th>
                            <th data-filter="false">上昇参考%</th>
                            <th data-filter="false">下落参考%</th>
                            <th data-filter="false">全体方向</th>
                            <th data-filter="false">方向参加率</th>
                            <th data-filter="false">上協調</th>
                            <th data-filter="false">下協調</th>
                            <th class="filter-select">協調している側</th>
                            <th>主体規模感</th>
                            <th class="filter-select">確信度</th>
                            <th class="filter-select">心理状態</th>
                            <th class="filter-select">群集心理フェーズ</th>
                            <th data-filter="false">花畑</th>
                            <th data-filter="false">悲観</th>
                            <th data-filter="false">悲観後静寂</th>
                            <th data-filter="false">固定ID下圧</th>
                            <th data-filter="false">投稿数</th>
                            <th class="filter-select">判定</th>
                            <th class="filter-select">需給フェーズ</th>
                            <th>旧16パターン</th>
                            <th data-filter="false">ファクト密度</th>
                            <th data-filter="false">最大流速</th>
                        </tr>
                    </thead>
                    <tbody>{table_rows}</tbody>
                </table>
            </div>
        </div>
        <script>
            $(document).ready(function() {{
                $("#rankTable").tablesorter({{
                    theme: 'default',
                    widthFixed: true,
                    widgets: ['zebra', 'filter'],
                    widgetOptions: {{
                        filter_cssFilter: 'tablesorter-filter',
                        filter_columnFilters: true,
                        filter_placeholder: {{ search: '絞り込み...' }},
                        filter_saveFilters: false,
                        filter_reset: '.reset'
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    """
    with open(filepath, 'w', encoding='utf-8') as f: f.write(html_content)
    print(f"🏆 ランキングダッシュボードを生成しました: {filepath}")
    try: webbrowser.open(f"file:///{os.path.abspath(filepath)}")
    except: pass

if __name__ == "__main__":
    target_code = sys.argv[1].upper() if len(sys.argv) > 1 else '285A'
    
    # ★実行環境に依存しないよう、スクリプト自身のディレクトリを基準にパスを設定
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output_data")
    input_dir = os.path.join(base_dir, "input_data")
    
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    if not os.path.exists(input_dir): os.makedirs(input_dir)

    # all または ALL-RESET コマンド等の吸収
    if target_code.startswith('ALL'):
        input_file = os.path.join(input_dir, "ヤフ板コード番号.txt")
        cache_file = os.path.join(output_dir, 'all_reports_cache.jsonl')
        target_start_for_run, _ = calculate_target_period()
        current_session = target_start_for_run.isoformat(timespec='minutes')
        
        # ★エラー落ち防止：ファイルがない場合はダミーを自動生成して案内を出す
        if not os.path.exists(input_file):
            print(f"【エラー】銘柄リストファイルが見つかりません。")
            print(f"👉 以下の場所に新規作成しました:\n   {input_file}")
            print(f"監視したい銘柄コードを1行ずつ入力して、再度実行してください。")
            with open(input_file, 'w', encoding='utf-8') as f:
                f.write("9984\n7974\n6920\n")  # ダミーでソフトバンクG、任天堂、レーザーテック等を入れておく
            sys.exit()

        with open(input_file, 'r', encoding='utf-8') as f: codes = [line.strip() for line in f if line.strip()]
        
        all_reports, processed_codes = [], set()
        
        # ★コマンドライン引数で 'reset' や 'clear' が指定されたか判定
        force_clean = (target_code == 'ALL-RESET' or (len(sys.argv) > 2 and sys.argv[2].upper() in ['RESET', 'CLEAR', '-F']))
        force_resume = (target_code == 'ALL-RESUME' or (len(sys.argv) > 2 and sys.argv[2].upper() in ['RESUME', '-R']))
        
        if os.path.exists(cache_file):
            cache_age_h = (time.time() - os.path.getmtime(cache_file)) / 3600.0
            can_resume = cache_age_h <= RESUME_CACHE_MAX_HOURS

            # キャッシュ内の対象セッションが現在と一致するか確認
            cached_session = None
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            rep0 = json.loads(line)
                            cached_session = rep0.get('cache_session') or rep0.get('fetch_meta', {}).get('target_start')
                            break
            except Exception:
                cached_session = None
            can_resume = can_resume and (cached_session == current_session)

            if force_clean:
                print("\n🗑️ 【強制初期化】既存キャッシュを削除して最新データで再実行します。")
                os.remove(cache_file)
            else:
                resume = force_resume and can_resume
                if (not force_resume) and sys.stdin.isatty() and can_resume:
                    ans = input(f"\n🔄 同じ対象期間の中断キャッシュがあります（{cache_age_h:.1f}時間前）。\n続きから再開しますか？ [R:再開 / N:最新を取り直す] (デフォルト:N): ").strip().upper()
                    resume = ans in ('R','Y','YES','RESUME')

                if resume:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    rep = json.loads(line)
                                    if (rep.get('cache_session') or rep.get('fetch_meta', {}).get('target_start')) != current_session:
                                        continue
                                    all_reports.append(rep)
                                    processed_codes.add(rep['code'])
                                except Exception:
                                    pass
                    print(f"\n✅ 同一セッションの {len(processed_codes)} 銘柄を再利用して再開します。")
                else:
                    backup_file = cache_file.replace('.jsonl', f'_backup_{int(time.time())}.jsonl')
                    os.rename(cache_file, backup_file)
                    print("\n🆕 最新性を優先し、前回キャッシュを退避して全銘柄を取り直します。")

        codes_to_process = [c for c in codes if c not in processed_codes]
        if not codes_to_process: print(f"\n✅ 全 {len(codes)} 銘柄がすでに処理済みです。ランキングを再生成します。")
        else: print(f"\n🚀 ALLモード起動：残り {len(codes_to_process)} 銘柄のマルチスレッドスクリーニングを開始します。")

        def process_stock(code):
            time.sleep(random.uniform(THREAD_START_SLEEP_MIN, THREAD_START_SLEEP_MAX))
            try:
                df_market, stock_name, fetch_meta = fetch_yahoo_bbs_market_adaptive(code=code)
                if not df_market.empty:
                    report = analyze_bbs_manipulation(df_market, stock_name=stock_name, fetch_meta=fetch_meta)
                    report['code'] = code
                    report['cache_session'] = current_session
                    persist_watch_targets(report, code, stock_name, output_dir, refresh_profiles=True)
                    html_path = os.path.join(output_dir, f'report_{code}.html')
                    generate_html_report(df_market, report, code, html_path)
                    with _FILE_LOCK:
                        with open(cache_file, 'a', encoding='utf-8') as cf: cf.write(json.dumps(report, ensure_ascii=False, cls=NpEncoder) + '\n')
                    return report
                else:
                    empty_report = {'code': code, 'stock_name': stock_name, 'is_empty': True, 'bullish_score': -9999, 'total_posts': 0, 'cache_session': current_session, 'fetch_meta': fetch_meta}
                    with _FILE_LOCK:
                        with open(cache_file, 'a', encoding='utf-8') as cf: cf.write(json.dumps(empty_report, ensure_ascii=False, cls=NpEncoder) + '\n')
                    return empty_report
            except Exception as e: print(f"⚠️ [{code}] 予期せぬエラーでスキップされました: {e}"); return None

        if codes_to_process:
            print(f"⚡ {MAX_THREADS_CONFIG} スレッドでバッチ分割処理を実行します...")
            
            total_target = len(codes_to_process)
            completed_count = 0

            for i in range(0, total_target, CHUNK_SIZE):
                chunk = codes_to_process[i:i + CHUNK_SIZE]
                print(f"\n🚀 バッチ処理開始: {i+1} 〜 {min(i+CHUNK_SIZE, total_target)} / {total_target} 銘柄")

                with ThreadPoolExecutor(max_workers=MAX_THREADS_CONFIG) as executor:
                    future_to_code = {executor.submit(process_stock, code): code for code in chunk}
                    for future in as_completed(future_to_code):
                        code = future_to_code[future]
                        result = future.result()
                        completed_count += 1
                        
                        if result:
                            all_reports.append(result)
                            if result.get('is_empty'): 
                                print(f"➖ [{completed_count}/{total_target}] データなし/スキップ: {code}")
                            else: 
                                print(f"✅ [{completed_count}/{total_target}] 解析完了: {code} (Score: {result.get('bullish_score', 0):.0f})")
                        else: 
                            print(f"❌ [{completed_count}/{total_target}] エラーによる失敗: {code}")
                        time.sleep(0.5)

                # 最後のバッチ以外はWAF検知回避の長時間スリープを入れる
                if i + CHUNK_SIZE < total_target:
                    sleep_time = random.uniform(LONG_SLEEP_MIN, LONG_SLEEP_MAX)
                    print(f"☕ WAF検知回避のため {sleep_time:.0f} 秒間の待機に入ります...")
                    time.sleep(sleep_time)
                
        print("\n🏆 全銘柄のスクリーニングが完了しました。上昇期待値全件ランキングを計算・生成します...")
        valid_reports = [r for r in all_reports if not r.get('is_empty', False)]
        valid_reports.sort(key=lambda x: float(x.get('bbs_rise_signal_pct', 50)), reverse=True)
        generate_ranking_html(valid_reports, os.path.join(output_dir, 'report_ALL_Ranking.html'))
        generate_watch_network_html(output_dir)
        print("👤 要注意人物DB: report_WATCH_Actors.html / report_WATCH_Stocks.html を更新しました")

    else:
        df_market, stock_name, fetch_meta = fetch_yahoo_bbs_market_adaptive(code=target_code, verbose=True)
        if not df_market.empty:
            report = analyze_bbs_manipulation(df_market, stock_name=stock_name, fetch_meta=fetch_meta)
            persist_watch_targets(report, target_code, stock_name, output_dir, refresh_profiles=True)
            print("\n" + "="*60)
            print(f" 📊  YABB ADVANCED COMPOSITE ANALYSIS REPORT [{target_code}]")
            print("="*60)
            print(f"▶ 総合工作フラグ : {'【 TRUE (要警戒) 】' if report['is_manipulated'] else '【 FALSE (正常) 】'}")
            print(f"▶ 掲示板現状判定 : {report['intent']}")
            print(f"▶ 上昇/下落参考  : {report.get('bbs_rise_signal_pct',50):.1f}% / {report.get('bbs_fall_signal_pct',50):.1f}%  ※参考値")
            print(f"▶ 株価方向示唆   : {report.get('bbs_price_direction_label')}")
            print(f"▶ 掲示板全体方向 : {report.get('direction_label')} ({report.get('direction_score',0):+.1f}) / 参加率 {report.get('direction_participation_rate',0):.1f}%")
            print(f"▶ 上/下協調度    : {report.get('up_coordination_score',0):.1f} / {report.get('down_coordination_score',0):.1f} → {report.get('organized_direction')}")
            print(f"▶ 主体規模感     : {report.get('actor_scale')} / 確信度 {report.get('actor_confidence')}")
            print(f"▶ 心理状態       : {report.get('psychology_state')}")
            print(f"▶ 群集心理フェーズ: {report.get('market_psychology_phase')}")
            print(f"▶ 花畑/悲観/静寂 : {report.get('euphoria_score',0):.1f} / {report.get('despair_phase_score',0):.1f} / {report.get('silence_after_despair_score',0):.1f}")
            print(f"▶ 固定ID常駐圧   : 上 {report.get('resident_up_score',0):.1f} / 下 {report.get('resident_down_score',0):.1f}")
            print(f"▶ 心理誘導技法   : {', '.join([x['name'] for x in report.get('dominant_psych_techniques',[])]) or '目立つ技法なし'}")
            print(f"▶ 統計流速情報   : 総数 {report['total_posts']}件 / 最大流速 {report['max_flow']}件(10分)")
            print(f"▶ テキスト質評価 : 平均 {report['avg_len']:.1f}文字 / ファクト密度 {report['fact_rate']:.2f}")
            print(f"▶ ユーザー多様性 : 実人数 {report['unique_users']}人 / 占有比率 {report['user_occupancy']:.2f}")
            print("-"*60)
            print("▶ スクリーニング戦略ログ:")
            for line in report['summary']: print(f"  {line}")
            print("="*60 + "\n")
            html_path = os.path.join(output_dir, f'report_{target_code}.html')
            generate_html_report(df_market, report, target_code, html_path)
            generate_watch_network_html(output_dir)
            try: webbrowser.open(f"file:///{os.path.abspath(html_path)}")
            except: pass
        else: print("\n対象時間枠内に投稿データがありませんでした。")