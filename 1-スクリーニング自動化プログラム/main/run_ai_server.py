import subprocess
import os
import sys

# サーバーディレクトリのパス
server_dir = r"D:\kabu\main\1-スクリーニング自動化プログラム\main"
command = [sys.executable, "-m", "uvicorn", "stock_ai_server:app", "--port", "8000"]

def start_server():
    try:
        # ディレクトリが存在するか確認
        if not os.path.exists(server_dir):
            print(f"ディレクトリが見つかりません: {server_dir}")
            return

        os.chdir(server_dir)
        
        # 【修正点1】ファイルハンドルを明示的に開き、バッファを無効化(buffering=0)
        # これにより、エラー発生時にログが即座にファイルへ書き込まれます
        log_file = open("server.log", "a", encoding="utf-8", buffering=1)
        
        print(f"サーバーを起動中... ログ出力先: {os.path.join(server_dir, 'server.log')}")
        
        subprocess.Popen(
            command,
            creationflags=subprocess.CREATE_NO_WINDOW,
            stdout=log_file,
            stderr=subprocess.STDOUT
        )
    except Exception as e:
        print(f"起動エラー: {e}")

if __name__ == "__main__":
    start_server()