#!/bin/sh


# https://stackoverflow.com/questions/27771781/how-can-i-access-docker-set-environment-variables-from-a-cron-job
# 解决环境变量输出问题。
printenv | grep -v "no_proxy" >> /etc/environment

# 第一次后台执行日数据。
nohup bash /data/InStock/cron/cron.daily/run_daily &

#启动cron服务。在前台
/usr/sbin/cron -f