chcp 65001
@echo off
cd %~dp0
cd ..
cd trade
python trade_service.py
echo ------交易服务已启动，请不要关闭------
pause
exit
