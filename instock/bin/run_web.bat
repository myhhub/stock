chcp 65001
@echo off
cd %~dp0
cd ..
cd web
python main.py
echo ------服务已启动，请不要关闭------
echo Web服务地址 : http://localhost:9999/
pause
exit
