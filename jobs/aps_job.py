#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from pytz import utc
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import libs.database as mdb

__author__ = 'myh '
__date__ = '2023/3/10 '

# doc : http://apscheduler.readthedocs.io/en/latest/modules/jobstores/sqlalchemy.html
jobstores = {
    # 可以配置多个存储
    # SQLAlchemyJobStore指定存储链接
    'default': SQLAlchemyJobStore(url=mdb.MYSQL_CONN_URL, tablename='apscheduler_jobs')
}

executors = {
    # 最大工作线程数
    # 最大工作进程数为5
    'default': {'type': 'threadpool', 'max_workers': 20},
    'processpool': ProcessPoolExecutor(max_workers=5)
}

job_defaults = {
    # 关闭新job的合并，当job延误或者异常原因未执行时
    # 并发运行新job默认最大实例多少
    'coalesce': False,
    'max_instances': 3
}


def job_func():
    print("Hello World")


# utc作为调度程序的时区
scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)
# 添加任务
# scheduler.add_job(job_func, trigger='interval', jobstore='default', executor='processpool', seconds=10)
scheduler.add_job(job_func, 'cron', day_of_week='mon-fri', hour=18, minute=0)

scheduler.start()
print("start ...")
