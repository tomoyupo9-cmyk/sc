# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any
import json, html

def _json_for_script(obj: Any) -> str:
    try:
        s = json.dumps(obj or {}, ensure_ascii=False)
        return s.replace("</", "<\\/")
    except Exception:
        return "{}"

def render_dashboard_html(main: Dict[str, Any], bbs: Dict[str, Any], leak: Dict[str, Any], tob: Dict[str, Any]) -> str:
    """
    注目度ダッシュボード（TOB/漏れ以外は添付HTML準拠の見た目）
    受け取り:
      - main: {"rows": [...], "earnings": [...], "generated_at": "YYYY-MM-DD HH:MM:SS"}
      - bbs, leak, tob: そのまま JSON
    返り値: 完成HTML文字列
    """
    generated_at = (main or {}).get("generated_at") or (bbs or {}).get("generated_at") or ""
    data = {
        "generated_at": generated_at,
        "rows": (main or {}).get("rows", []),
        "earnings": (main or {}).get("earnings", []),
        "bbs": bbs or {},
        "leak": leak or {},
        "tob": tob or {},
    }

    html_tpl = """<!doctype html>
<html lang="ja">
<head>
<meta http-equiv="Cache-Control" content="no-store">
<meta http-equiv="Pragma" content="no-cache">
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>注目度ダッシュボード</title>
<style>
  :root { --br:#e5e7eb; --fg:#0f172a; --muted:#64748b; --bg:#fff; --pos:#16a34a; --neu:#6b7280; --neg:#dc2626; --tab:#3b82f6; }
  html,body{background:#fff}
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;margin:18px;color:#0f172a}
  h1{font-size:26px;margin:0 0 10px}
  .toolbar{display:flex;gap:10px;align-items:center;margin:6px 0 14px}
  .tabs{display:flex;gap:8px}
  .tab{padding:8px 12px;border:1px solid var(--br);border-radius:999px;background:#f8fafc;color:#111;font-size:13px;cursor:pointer}
  .tab.active{background:#e6f0ff;border-color:#bfdbfe;color:#1d4ed8;font-weight:600}
  .stamp{font-size:12px;color:#64748b;margin-left:8px}
  .card{border:1px solid var(--br);border-radius:12px;padding:12px;margin:14px 0;background:#fff}
  table{width:100%;border-collapse:collapse}
  th,td{border-bottom:1px solid #f1f5f9;padding:8px 10px;text-align:left;vertical-align:top}
  th{background:#f8fafc;color:#334155;font-weight:600;position:sticky;top:0}
  .code{display:inline-block;background:#f1f5f9;border-radius:999px;padding:2px 8px;font-size:12px;color:#334155}
  .badge{display:inline-block;padding:2px 10px;border-radius:999px;color:#fff;font-size:12px;white-space:nowrap}
  .pos{background:var(--pos)} .neu{background:var(--neu)} .neg{background:var(--neg)}
  .newslist{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;max-width:1100px}
  .newsitem{border:1px solid var(--br);border-radius:10px;padding:8px;background:#fff}
  .src{color:#64748b;font-size:12px}
  .btn{padding:8px 12px;border:1px solid var(--br);border-radius:10px;background:#f8fafc;cursor:pointer}
  .btn:hover{filter:brightness(0.98)}
  .small{font-size:12px;color:#64748b}
  a{color:#1d4ed8;text-decoration:none} a:hover{text-decoration:underline}
  .day-head{font-weight:700;margin:18px 0 8px}
  td.num{text-align:right}
  td.diff.plus{color:#16a34a;font-weight:600}
  td.diff.minus{color:#dc2626;font-weight:600}
  .jdg{ display:inline-flex; align-items:center; gap:6px; padding:2px 10px; border-radius:999px; font-weight:700; border:1px solid var(--br); line-height:1.9; }
  .jdg .score{ font-weight:600; opacity:.8; }
  .jdg.positive{ color:#065f46; background:#ecfdf5; border-color:#a7f3d0; }
  .jdg.neutral { color:#374151; background:#f3f4f6; border-color:#e5e7eb; }
  .jdg.negative{ color:#7f1d1d; background:#fef2f2; border-color:#fecaca; }
  .ev{ padding:4px 10px; border-radius:12px; line-height:1.9; background:#f0f9ff; color:#0c4a6e; border-left:4px solid #38bdf8; }
  .ev .chip{ display:inline-block; margin:2px 6px 2px 0; padding:2px 8px; border-radius:999px; background:#e0f2fe; border:1px solid #bae6fd; font-weight:600; }
</style>
</head>
<body>
<h1>注目度ダッシュボード</h1>
<div class="toolbar">
  <div class="tabs">
    <button class="tab active" data-mode="total">総合</button>
    <button class="tab" data-mode="bbs">掲示板</button>
    <button class="tab" data-mode="leak">漏れ</button>
    <button class="tab" data-mode="tob">TOB</button>
    <button class="tab" data-mode="earnings">決算</button>
    <button class="tab" data-mode="earnings_day">決算（日別）</button>
  </div>
  <span id="stamp" class="stamp">生成時刻: __GENERATED_AT__</span>
  <button class="btn" id="btn-rerender">再描画</button>
</div>

<!-- JSON payload -->
<script id="__DATA__" type="application/json">__DATA_JSON__</script>

<div class="card">
  <table id="tbl">
    <thead id="thead"></thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
  const DATA = JSON.parse(document.getElementById('__DATA__').textContent);
  let MODE = 'total';
  const tbody = document.getElementById('tbody');
  const thead = document.getElementById('thead');

  const esc = (str) => (str != null ? String(str).replace(/[&<>\"']/g, (s)=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;','\\'':'&#39;'}[s])) : '');

  const el = (tag, attrs = {}, ...children) => {
    const element = document.createElement(tag);
    for (const key in attrs) { element.setAttribute(key, attrs[key]); }
    children.forEach(child => {
      if (typeof child === 'string') { element.innerHTML += child; } 
      else if (child) { element.appendChild(child); }
    });
    return element;
  };

  const badgeLabel = (newsItem) => {
    if (!newsItem) return "neutral";
    const label = (newsItem.sentiment || '').toLowerCase();
    if (label === 'positive' || label === 'negative' || label === 'neutral') return label;
    return 'neutral';
  };

  const badge = (label) => {
    const textMap = { 'positive': 'positive', 'negative': 'negative', 'neutral': 'neutral' };
    return '<span class="badge ' + esc(label) + '">' + esc(textMap[label] || 'neutral') + '</span>';
  };

  const stockCell = (code, name) => {
    const ticker = esc(code);
    const stockName = esc(name || code);
    const url = 'https://finance.yahoo.co.jp/quote/' + ticker;
    return '<a href="' + url + '" target="_blank" rel="noopener">' + stockName + '</a> <span class="code">(' + ticker + ')</span>';
  };

  const offeringCell = (history) => history ? 'あり' : '';

  const toNewsCards = (items) => {
    if (!items || !items.length) return '';
    const cards = items.slice(0, 3).map(item => {
      const time = esc(item.published || '');
      const source = esc(item.source || '');
      const title = esc(item.title || '');
      const url = esc(item.link || '#');
      return '<div class="newsitem"><a href="' + url + '" target="_blank" rel="noopener">' + title + '</a><div class="src">' + source + '　' + time + '</div></div>';
    }).join('');
    return '<div class="newslist">' + cards + '</div>';
  };

  function renderTable(mode) {
    tbody.innerHTML = '';
    const j = DATA;

    if (mode === 'bbs') {
      thead.innerHTML = '<tr><th>#</th><th>銘柄</th><th>増資経歴</th><th class="num">BBS(24h)</th><th class="num">BBS(72h)</th><th class="num">24h/72h比率</th><th>Sentiment</th><th>最新ニュース</th></tr>';
      const rows = (j.rows || []).slice().sort((a, b) => ((b.bbs?.posts_72h || 0) - (a.bbs?.posts_72h || 0)));
      if (!rows.length) { tbody.appendChild(el('tr', {}, el('td', { colspan: '8', 'class': 'small' }, 'データがありません。'))); return; }
      rows.forEach((r, idx) => {
        const items = (r.news && r.news.items) ? r.news.items : [];
        const label = badgeLabel(items[0]);
        const b24 = r.bbs?.posts_24h || 0;
        const b72 = r.bbs?.posts_72h || 0;
        const ratio = (b72 > 0 && b24 >= 0) ? (b24 * 1.0) / (b72 * 1.0) : 0;
        const growth_cls = (ratio > 0.38) ? 'plus' : ((ratio < 0.25) ? 'minus' : '');
        const growth_txt = (ratio * 100).toFixed(1) + '%';
        const tr = el('tr', {},
          el('td', {}, String(idx + 1)),
          el('td', {}, stockCell(r.ticker, r.name)),
          el('td', {}, offeringCell(r.has_offering_history)),
          el('td', { 'class': 'num' }, String(b24)),
          el('td', { 'class': 'num' }, String(b72)),
          el('td', { 'class': 'num diff ' + growth_cls }, growth_txt),
          el('td', {}, badge(label)),
          el('td', {}, toNewsCards(items))
        ); tbody.appendChild(tr);
      });
      return;
    } else if (mode === 'leak' || mode === 'tob') {
      const source = (mode === 'leak') ? j.leak : j.tob;
      const title = (mode === 'leak') ? '漏れ？' : 'TOB？';
      const rows = (source.rows || []).slice().sort((a, b) => (b.score || 0) - (a.score || 0));
      thead.innerHTML = '<tr><th>#</th><th>銘柄</th><th>件数/' + title + 'スコア</th><th class="num">ポジティブ</th><th class="num">ネガティブ</th><th>最新コメント (最大3件)</th></tr>';
      if (!rows.length) { tbody.appendChild(el('tr', {}, el('td', { colspan: '6', 'class': 'small' }, 'データがありません。'))); return; }
      rows.forEach((r, idx) => {
        const cmt_list = (r.comments || []).slice(0, 3).map(c => {
          const text = esc(c.text || '').substring(0, 80) + '...';
          const link = esc(c.link || '#');
          const time = esc(c.published || '');
          const sentiment = badge(c.sentiment);
          return '<div style="margin-top: 6px;">' + sentiment + ' <a href="' + link + '" target="_blank" rel="noopener">' + text + '</a><div class="src">' + time + '</div></div>';
        }).join('');
        const score_txt = '件数: ' + (r.count || 0) + '<br>スコア: ' + (r.score != null ? Number(r.score).toFixed(3) : '0.000');
        const tr = el('tr', {},
          el('td', {}, String(idx + 1)),
          el('td', {}, stockCell(r.ticker, r.name)),
          el('td', {}, score_txt),
          el('td', { 'class': 'num' }, String(r.positive_count || 0)),
          el('td', { 'class': 'num' }, String(r.negative_count || 0)),
          el('td', {}, cmt_list)
        ); tbody.appendChild(tr);
      });
      return;
    } else if (mode === 'earnings' || mode === 'earnings_day') {
      thead.innerHTML = '<tr><th>#</th><th>銘柄</th><th>決算発表日</th><th>決算結果</th><th>判定理由</th><th>Sentiment</th><th>最新ニュース</th></tr>';
      let earnings_rows = (j.earnings || []).slice();
      if (mode === 'earnings_day') {
        earnings_rows.sort((a, b) => (a.announce_date || 'ZZZZ').localeCompare(b.announce_date || 'ZZZZ'));
      } else {
        earnings_rows.sort((a, b) => (b.score || 0) - (a.score || 0));
      }
      if (!earnings_rows.length) { tbody.appendChild(el('tr', {}, el('td', { colspan: '7', 'class': 'small' }, 'データがありません。'))); return; }
      let currentDay = '';
      earnings_rows.forEach((r, idx) => {
        const items = (r.news && r.news.items) ? r.news.items : [];
        const label = badgeLabel(items[0]);
        if (mode === 'earnings_day' && r.announce_date && r.announce_date !== currentDay) {
          currentDay = r.announce_date;
          tbody.appendChild(el('tr', {}, el('td', { colspan: '7', 'class': 'day-head' }, currentDay + ' 発表')));
        }
        const judgeText = esc(r.judge?.label || r.verdict || '判定なし');
        const judgeClass = String((r.judge?.label or r.verdict or '')).toLowerCase().replace(/[^a-z]/g, '');
        const judgeScore = (r.judge?.score != null) ? Number(r.judge.score).toFixed(2) : (r.verdict_score != null ? Number(r.verdict_score).toFixed(2) : '');
        const judgeHtml = '<div class="jdg ' + judgeClass + '"><span>' + judgeText + '</span>' + (judgeScore ? '<span class="score">(' + judgeScore + ')</span>' : '') + '</div>';
        const chips = (r.judge?.reason_chips) || (r.reasons || []);
        const reasonHtml = '<div class="ev">' + (chips || []).map(chip => '<span class="chip">' + esc(chip) + '</span>').join('') + '</div>';
        const tr = el('tr', {},
          el('td', {}, String(idx + 1)),
          el('td', {}, stockCell(r.ticker, r.name)),
          el('td', {}, esc(r.announce_date || '')),
          el('td', {}, judgeHtml),
          el('td', {}, reasonHtml),
          el('td', {}, badge(label)),
          el('td', {}, toNewsCards(items))
        ); tbody.appendChild(tr);
      });
      return;
    } else {
      thead.innerHTML = '<tr><th>#</th><th>銘柄</th><th>増資経歴</th><th class="num">スコア</th><th class="num">Trends</th><th class="num">BBS</th><th class="num">News(24h)</th><th>Sentiment</th><th>最新ニュース</th></tr>';
      const rows = (j.rows || []).slice().sort((a, b) => (b.score || 0) - (a.score || 0));
      if (!rows.length) { tbody.appendChild(el('tr', {}, el('td', { colspan: '9', 'class': 'small' }, 'データがありません。'))); return; }
      rows.forEach((r, idx) => {
        const items = (r.news && r.news.items) ? r.news.items : [];
        const label = badgeLabel(items[0]);
        const trendsValue = (r.trends && r.trends.latest != null) ? r.trends.latest : r.trends;
        const tr = el('tr', {},
          el('td', {}, String(idx + 1)),
          el('td', {}, stockCell(r.ticker, r.name)),
          el('td', {}, offeringCell(r.has_offering_history)),
          el('td', { 'class': 'num' }, Number(r.score || 0).toFixed(3)),
          el('td', { 'class': 'num' }, Number(trendsValue || 0).toFixed(3)),
          el('td', { 'class': 'num' }, String(r.bbs?.posts_24h || 0)),
          el('td', { 'class': 'num' }, String(r.news?.count_24h || 0)),
          el('td', {}, badge(label)),
          el('td', {}, toNewsCards(items))
        ); tbody.appendChild(tr);
      });
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('stamp').textContent = '生成時刻: ' + (DATA.generated_at || '');
    document.querySelectorAll('.tab').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        MODE = btn.getAttribute('data-mode');
        renderTable(MODE);
      });
    });
    renderTable(MODE);
    document.getElementById('btn-rerender').addEventListener('click', () => renderTable(MODE));
  });
</script>

</body>
</html>"""

    html_out = html_tpl.replace("__GENERATED_AT__", html.escape(generated_at)).replace("__DATA_JSON__", _json_for_script(data))
    return html_out
