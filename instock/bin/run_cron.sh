#!/bin/sh

export PYTHONIOENCODING=utf-8
export LANG=zh_CN.UTF-8
export PYTHONPATH=/data/InStock
export LC_CTYPE=zh_CN.UTF-8

# 环境变量输出
# https://stackoverflow.com/questions/27771781/how-can-i-access-docker-set-environment-variables-from-a-cron-job
printenv | grep -v "no_proxy" >> /etc/environment

#启动cron服务。在前台
/usr/sbin/cron -f