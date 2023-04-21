# 基础镜像
# https://hub.docker.com/_/python/tags?page=1&name=3.11-rc-rc-slim
FROM docker.io/python:3.11-rc-slim

MAINTAINER myh

# 使用使用国内镜像地址加速。修改debian apt更新地址，pip地址，设置时区
# https://opsx.alibaba.com/mirror
# https://mirrors.tuna.tsinghua.edu.cn/help/pypi/
# cat /etc/apt/sources.list 参考原始地址，再确定怎么样替换
RUN sed -i "s@http://\(deb\|security\).debian.org@https://mirrors.aliyun.com@g" /etc/apt/sources.list && \
    echo  "[global]\n\
index-url = https://pypi.tuna.tsinghua.edu.cn/simple\n\
trusted-host = pypi.tuna.tsinghua.edu.cn" > /etc/pip.conf && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

#增加语言utf-8
ENV LANG=zh_CN.UTF-8
ENV LC_CTYPE=zh_CN.UTF-8
ENV LC_ALL=C
ENV PYTHONPATH=/data/InStock

# 安装 依赖库
# apt-get autoremove -y 删除没有用的依赖lib
# apt-get --purge remove 软件包名称 , 删除已安装包（不保留配置文件)
RUN apt-get update && \
    apt-get install -y net-tools procps cron gcc make python3-dev default-libmysqlclient-dev libxml2-dev libxslt1-dev curl && \
    pip install mysqlclient && \
    pip install arrow && \
    pip install numpy && \
    pip install SQLAlchemy && \
    pip install pymysql && \
    pip install Logbook && \
    pip install python_dateutil && \
    pip install bokeh && \
    pip install pandas && \
    pip install tornado && \
    pip install akshare && \
    pip install easytrader && \
    pip install supervisor && \
    curl -SL http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz | tar -xzC . && \
    cd ta-lib/  && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    pip install TA-Lib && \
    rm -rf ta-lib* && \
    apt-get --purge remove -y gcc make python3-dev default-libmysqlclient-dev libxml2-dev libxslt1-dev curl && \
    rm -rf /root/.cache/* && apt-get clean && apt-get autoremove -y

WORKDIR /data

EXPOSE 9988

#InStock软件
ADD InStock /data/InStock

#add cron sesrvice.
#每分钟，每小时1分钟，每天1点1分，每月1号执行
RUN mkdir -p /etc/cron.minutely && mkdir -p /etc/cron.hourly && mkdir -p /etc/cron.daily && \
    echo "SHELL=/bin/sh \n\
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin \n\
# min   hour    day     month   weekday command \n\
*/1     *       *       *       *       /bin/run-parts /etc/cron.minutely \n\
10       *       *       *       *       /bin/run-parts /etc/cron.hourly \n\
30       17       *       *       *       /bin/run-parts /etc/cron.daily \n" > /var/spool/cron/crontabs/root && \
    chmod 600 /var/spool/cron/crontabs/root

ADD InStock/cron/cron.minutely /etc/cron.minutely
ADD InStock/cron/cron.hourly /etc/cron.hourly
ADD InStock/cron/cron.daily /etc/cron.daily

RUN chmod 755 /data/InStock/instock/bin/run_* && \
    chmod 755 /etc/cron.minutely/* &&  \
    chmod 755 /etc/cron.hourly/* &&  \
    chmod 755 /etc/cron.daily/*

ENTRYPOINT ["supervisord","-n","-c","/data/InStock/supervisor/supervisord.conf"]