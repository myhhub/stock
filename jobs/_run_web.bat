@echo off
cd %~dp0
cd ..
python web\main.py
echo 服务已启动，web地址 : http://localhost:9999/
pause
exit
