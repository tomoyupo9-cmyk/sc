# report_render.py
# out/data.json を読み、人向けのHTMLレポートを生成（ニュース感情の一文説明つき）

import json, html
from pathlib import Path

ROOT = Path(__file__).resolve().parent
data_path = ROOT / "out" / "data.json"
out_html  = ROOT / "out" / "report.html"

def badge(label):
    color = {"positive":"#16a34a","neutral":"#6b7280","negative":"#dc2626"}.get(label,"#6b7280")
    return f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;background:{color};color:#fff;font-size:12px">{html.escape(label)}</span>'

def render():
    obj = json.loads(data_path.read_text(encoding="utf-8"))
    rows = obj.get("rows", [])
    ts = obj.get("generated_at","")

    html_head = """
<!doctype html><meta charset="utf-8">
<title>ニュース感情 レポート</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,'Noto Sans JP',sans-serif;margin:20px;color:#111;}
h1{font-size:20px;margin:0 0 12px}
.card{border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin:14px 0}
h2{font-size:16px;margin:0 0 6px}
.meta{color:#6b7280;font-size:12px;margin-bottom:8px}
table{width:100%;border-collapse:collapse}
th,td{border-bottom:1px solid #f1f5f9;padding:6px 8px;text-align:left;vertical-align:top}
th{color:#334155;background:#f8fafc}
.source{color:#64748b;font-size:12px}
small{color:#64748b}
a{color:#0d3b66;text-decoration:none}
a:hover{text-decoration:underline}
.counts{font-size:12px;color:#64748b;margin-left:8px}
</style>
"""
    body = [f"<h1>ニュース感情 レポート <small>{html.escape(ts)}</small></h1>"]

    for r in rows:
        name = r.get("name") or r.get("ticker")
        news = (r.get("news") or {})
        items = news.get("items") or []

        pos = sum(1 for it in items if it.get("sentiment")=="positive")
        neg = sum(1 for it in items if it.get("sentiment")=="negative")
        neu = sum(1 for it in items if it.get("sentiment")=="neutral")

        body.append('<div class="card">')
        body.append(f"<h2>{html.escape(name)} <span class='counts'>(24h {news.get('count_24h',0)})</span></h2>")
        body.append(f"<div class='meta'>pos {pos} / neu {neu} / neg {neg}</div>")
        body.append("<table><thead><tr><th>記事</th><th>感情</th><th>説明</th><th>媒体/日時</th></tr></thead><tbody>")
        for it in items:
            title = it.get("title","")
            link  = it.get("link","")
            lab   = it.get("sentiment","neutral")
            expl  = it.get("sentiment_explain") or ""
            src   = it.get("source","")
            pub   = it.get("published","")
            body.append("<tr>")
            body.append(f"<td><a href='{html.escape(link)}' target='_blank'>{html.escape(title)}</a></td>")
            body.append(f"<td>{badge(lab)}</td>")
            body.append(f"<td>{html.escape(expl)}</td>")
            body.append(f"<td><div class='source'>{html.escape(src)}</div><small>{html.escape(pub)}</small></td>")
            body.append("</tr>")
        body.append("</tbody></table></div>")

    (out_html).write_text(html_head + "\n".join(body), encoding="utf-8")
    print(f"[OK] {out_html}")

if __name__ == "__main__":
    render()
