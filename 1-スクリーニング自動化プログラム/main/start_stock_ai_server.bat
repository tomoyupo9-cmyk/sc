@echo off
cd /d "D:\kabu\main\1-スクリーニング自動化プログラム\main"
uvicorn stock_ai_server:app --port 8000
