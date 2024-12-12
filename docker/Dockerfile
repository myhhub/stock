# 基础镜像
# https://hub.docker.com/_/python/tags?page=1&name=3.11-slim-bullseye
FROM docker.io/python:3.11-slim-bullseye

MAINTAINER myh
#增加语言utf-8
ENV LANG=zh_CN.UTF-8
ENV LC_CTYPE=zh_CN.UTF-8
ENV LC_ALL=C
ENV PYTHONPATH=/data/InStock
EXPOSE 9988


# 替换 Debian 源地址为阿里云
RUN sed -i "s@http://\\(deb\\|security\\).debian.org@https://mirrors.aliyun.com@g" /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl

# 配置 pip 使用阿里云源
RUN echo "[global]\nindex-url = https://mirrors.aliyun.com/pypi/simple\ntrusted-host = mirrors.aliyun.com" > /etc/pip.conf

# 设置时区为上海
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

# 安装必需的软件包
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cron \
        gcc \
        make \
        python3-dev \
        default-libmysqlclient-dev \
        build-essential \
        telnet \
        pkg-config \
        curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 安装 Python 包
RUN pip install --no-cache-dir \
    supervisor \
    mysqlclient \
    requests \
    arrow \
    numpy \
    SQLAlchemy \
    PyMySQL \
    Logbook \
    python_dateutil \
    py_mini_racer \
    tqdm \
    beautifulsoup4 \
    bokeh \
    pandas \
    tornado \
    mini-racer \
    easytrader


WORKDIR /data

COPY ta-lib-0.4.0-src.tar.gz ./

RUN tar -xf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr --build=$(uname -m)-unknown-linux-gnu && \
    make && \
    make install && \
    cd .. && \
    pip install --no-cache-dir TA-Lib && \
    # 清理安装文件以减少镜像大小
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# 清理开发工具和缓存
RUN apt-get --purge remove -y \
        gcc \
        make \
        python3-dev \
        default-libmysqlclient-dev \
        curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /root/.cache/* /var/lib/apt/lists/*

#InStock软件
COPY stock /data/InStock
COPY cron/cron.hourly /etc/cron.hourly
COPY cron/cron.workdayly /etc/cron.workdayly
COPY cron/cron.monthly /etc/cron.monthly

#add cron sesrvice.
#任务调度
RUN chmod 755 /data/InStock/instock/bin/run_*.sh && \
    chmod 755 /etc/cron.hourly/* && chmod 755 /etc/cron.workdayly/* && chmod 755 /etc/cron.monthly/* && \
    echo "SHELL=/bin/sh \n\
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin \n\
# min hour day month weekday command \n\
*/30 9,10,11,13,14,15 * * 1-5 /bin/run-parts /etc/cron.hourly \n\
30 17 * * 1-5 /bin/run-parts /etc/cron.workdayly \n\
30 10 * * 3,6 /bin/run-parts /etc/cron.monthly \n" > /var/spool/cron/crontabs/root && \
    chmod 600 /var/spool/cron/crontabs/root

ENTRYPOINT ["supervisord","-n","-c","/data/InStock/supervisor/supervisord.conf"]