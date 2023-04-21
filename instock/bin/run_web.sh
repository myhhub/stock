#!/bin/bash

/usr/local/bin/python3 /data/InStock/instock/web/web_service.py

echo ------Web服务已启动，请不要关闭------
echo 访问地址 : http://localhost:9988/

#启动cron服务。在前台
/usr/sbin/cron -f