import sys
import json
import os
import re
import math
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup, escape

# ==============================================================================
# --- 入出力パスの完全固定 ---
# ==============================================================================
INPUT_JSON_PATH = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\dashboard_data.json"
OUTPUT_FILEPATH = r"D:\kabu\main\1-スクリーニング自動化プログラム\main\output_data\index.html"

# template.html はこのスクリプトと同じ階層にある前提
TEMPLATE_FILEPATH = os.path.join(os.path.dirname(__file__), "template.html")
# ==============================================================================

# --- HTML内での数値や文字列の安全なフォーマット ---
def _to_float(v):
    if v is None: return None
    try:
        s = str(v).replace(',', '').replace('％','').replace('%','').strip()
        if s == '': return None
        x = float(s)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def _fmt_cell(v):
    """HTML混在・数値を安全に整形して返す"""
    try:
        if v is None or (isinstance(v,float) and math.isnan(v)): 
            return ""
        s = str(v)
        if any(tag in s for tag in ("<a ", "<img", "<span", "<br", "<div")):
            return Markup(s)
        if isinstance(v,int): 
            return f"{v:,}"
        if isinstance(v,float):
            if abs(v-round(v))<1e-9: 
                return f"{int(round(v)):,}"
            return f"{v:.2f}"
        fv = _to_float(v)
        if fv is not None: 
            return _fmt_cell(fv)
        return escape(s)
    except Exception:
        return escape(str(v)) if v is not None else ""


def generate_html(json_path, template_path, output_path):
    print(f"Loading JSON from: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data_json_str = f.read()

    # Jinja2 テンプレートの読み込み準備
    template_dir = os.path.dirname(os.path.abspath(template_path))
    template_name = os.path.basename(template_path)

    env = Environment(
        loader=FileSystemLoader(template_dir, encoding="utf-8"),
        autoescape=select_autoescape(["html"])
    )
    env.filters["fmt_cell"] = _fmt_cell

    build_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tpl = env.get_template(template_name)
    
    # テンプレートのレンダリング
    html_output = tpl.render(
        include_log=False,
        data_json="{}", 
        generated_at=build_id,
        build_id=build_id
    )

    # 異常なJSONデータをブラウザがクラッシュしないように安全化
    data_json_str = data_json_str.replace("<", "\\u003c").replace(">", "\\u003e")
    data_json_str = re.sub(r':\s*NaN', ': null', data_json_str)
    data_json_str = re.sub(r':\s*Infinity', ': null', data_json_str)
    data_json_str = re.sub(r':\s*-Infinity', ': null', data_json_str)

    # データを埋め込むスクリプトブロック
    json_script = f"""
<script id="__DATA__" type="application/json">
{data_json_str}
</script>
<script id="data_inline">
    try {{
        var el = document.getElementById('__DATA__');
        if (el) {{
            window.__DATA__ = JSON.parse(el.textContent);
            window.DATA = window.__DATA__;
        }}
    }} catch(e) {{
        console.error("データの読み込みに失敗しました:", e);
    }}
</script>
"""

    # 古いデータタグがあれば削除
    html_output = re.sub(r'<script\s+id="__DATA__"[^>]*>.*?</script>', '', html_output, flags=re.DOTALL | re.IGNORECASE)
    html_output = re.sub(r'<script\s+id="(data_inline|inline-data)"[^>]*>.*?</script>', '', html_output, flags=re.DOTALL | re.IGNORECASE)

    # </body> の直前にデータを挿入
    if "</body>" in html_output:
        html_output = html_output.replace("</body>", json_script + "\n</body>")
    else:
        html_output += json_script

    # 書き出し
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", buffering=8192) as f:
        f.write(html_output)
    
    print(f"✅ HTML generated successfully: {output_path}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_JSON_PATH):
        print(f"❌ エラー: JSONファイルが見つかりません -> {INPUT_JSON_PATH}")
        sys.exit(1)
        
    if not os.path.exists(TEMPLATE_FILEPATH):
        print(f"❌ エラー: テンプレートが見つかりません -> {TEMPLATE_FILEPATH}")
        sys.exit(1)

    # 固定パスで実行
    generate_html(INPUT_JSON_PATH, TEMPLATE_FILEPATH, OUTPUT_FILEPATH)