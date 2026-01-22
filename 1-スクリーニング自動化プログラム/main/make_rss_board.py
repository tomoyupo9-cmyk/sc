import xlwings as xw
import json
import os
import sys

# ==========================================================
# 設定エリア
# ==========================================================
WATCHLIST_FILE_CANDIDATES = [
    r"H:\Desktop\株攻略\1-スクリーニング自動化プログラム\main\output_data\rss_monitor_list.json",
    r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data\rss_monitor_list.json",
    "rss_monitor_list.json"
]

BOARD_FILENAME = "RSS_Monitor_Board.xlsx"

# ★重要：楽天RSSのDDEは一度に大量に登録すると固まります
# 安定動作のため、とりあえず上位500銘柄に制限します（必要なら増やしてください）
MAX_LIMIT = 500

# ==========================================================
# 処理
# ==========================================================
def find_watchlist_path():
    for path in WATCHLIST_FILE_CANDIDATES:
        if os.path.exists(path):
            return path
    return None

def main():
    print("=== RSS監視ボード生成ツール（フリーズ対策版） ===")
    
    # 1. JSON読み込み
    json_path = find_watchlist_path()
    if not json_path:
        print("【エラー】監視リスト(json)が見つかりません。")
        input("Enterキーで終了...")
        return

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            raw_targets = data.get("target_codes", [])
            # 重複削除・ソート
            targets = sorted(list(set([str(t).replace(".T", "") for t in raw_targets])))
    except Exception as e:
        print(f"【エラー】JSON読み込み失敗: {e}")
        return

    if not targets:
        print("監視対象が0件です。")
        return

    # 件数制限の適用
    total_count = len(targets)
    if total_count > MAX_LIMIT:
        print(f"【警告】銘柄数が多すぎます({total_count}件)。フリーズ防止のため上位{MAX_LIMIT}件に制限します。")
        targets = targets[:MAX_LIMIT]
    else:
        print(f"対象銘柄数: {total_count} 件")

    # 2. Excel起動
    print("Excelを起動中...（操作しないでください）")
    app = None
    try:
        # 非表示で起動すると高速かつ安定します
        app = xw.App(visible=False)
        
        # ★重要：自動計算を「手動」にする（これがフリーズ対策のキモ）
        app.calculation = 'manual'
        app.screen_updating = False
        app.display_alerts = False

        wb = app.books.add()
        sheet = wb.sheets[0]
        sheet.name = "RSS監視ボード"

        # ヘッダー
        headers = ["コード", "現在値", "前日比", "騰落率", "出来高", "始値", "高値", "安値", "判定"]
        sheet.range("A1").value = headers
        
        # 装飾
        sheet.range("A1:I1").color = (0, 0, 128)
        sheet.range("A1:I1").font.color = (255, 255, 255)
        sheet.range("A1:I1").font.bold = True

        # データ作成
        print(f"{len(targets)}件のデータを準備中...")
        data_rows = []
        for code in targets:
            rss_target = f"{code}.T"
            # 文字列として数式を埋め込む
            row = [
                code,
                f"=RSS|'{rss_target}'!現在値",
                f"=RSS|'{rss_target}'!前日比",
                f"=RSS|'{rss_target}'!騰落率",
                f"=RSS|'{rss_target}'!出来高",
                f"=RSS|'{rss_target}'!始値",
                f"=RSS|'{rss_target}'!高値",
                f"=RSS|'{rss_target}'!安値",
                "監視中"
            ]
            data_rows.append(row)

        # 一括書き込み
        print("Excelに書き込み中...")
        if data_rows:
            sheet.range("A2").value = data_rows
            sheet.range(f"A1:I{len(data_rows)+1}").columns.autofit()

        # 保存先のパス
        save_dir = os.path.dirname(json_path)
        save_path = os.path.join(save_dir, BOARD_FILENAME)

        # 既に開かれている場合のエラー対策
        if os.path.exists(save_path):
            try:
                os.remove(save_path)
            except:
                print("【注意】既存ファイルが開かれているため上書きできません。別名で保存します。")
                save_path = os.path.join(save_dir, f"RSS_Board_{datetime.datetime.now().strftime('%H%M%S')}.xlsx")

        # 保存
        wb.save(save_path)
        print(f"保存完了: {save_path}")

    except Exception as e:
        print(f"【エラー】作成中に問題が発生しました: {e}")
    finally:
        # 後始末
        try:
            if app:
                # 最後に設定を戻しておく（次にExcelを開くときのため）
                app.calculation = 'automatic'
                app.screen_updating = True
                app.quit() # アプリを閉じる
        except:
            pass

    print("-" * 40)
    print("【作成成功】")
    print("ファイルを開くと、最初はデータが更新されていないかもしれません。")
    print("Excelの「数式」タブ -> 「計算方法の設定」が「自動」になっているか確認してください。")
    print("-" * 40)

if __name__ == "__main__":
    main()