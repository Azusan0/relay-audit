@echo off
cd /d "%~dp0"
echo.
echo   relay-audit  http://127.0.0.1:8737
echo   关闭此窗口即停止服务
echo.
start "" http://127.0.0.1:8737
python server.py 8737
pause
