# DEVELOPMENT.md

InStock 开发与运行命令。所有命令来源于仓库实际文件，标注 `file:line`。

## 环境准备

- Python 3.11（`docker/Dockerfile:3` `python:3.11-slim-bullseye`）。
- `pip install -r requirements.txt`（`requirements.txt:1-18`，含 numpy/pandas/TA_Lib/tornado/PyMySQL/easytrader 等）。
- TA-Lib 需先编译 native 库再装 Python 包，编译步骤见 `docker/Dockerfile:46-53`（`curl` 下载 ta-lib tar.gz → `./configure && make && make install` → `pip install TA-Lib`）。本地需自行安装 ta-lib native。
- 容器内额外通过 `Dockerfile` 装了 supervisor、mysqlclient、cron 等（`docker/Dockerfile:27-45`）。

## 数据库

- 后端 MariaDB（`docker/docker-compose.yml:5` `library/mariadb:latest`，容器名 `InStockDbService`）。
- 配置走环境变量，默认 localhost:3306/root/root/instockdb，见 `instock/lib/database.py:14-36`：
  - `db_host`、`db_user`、`db_password`、`db_database`、`db_port` 覆盖默认（`database.py:22-36`）。
  - docker-compose 传入 `db_host: InStockDbService`（`docker/docker-compose.yml:20`）。
- 初始化 DB：`python instock/job/init_job.py`，先 `check_database` 再 `create_new_database`（`instock/job/init_job.py:52-60`、`init_job.py:20-30`）。

## 运行命令

### Web

```
python instock/web/web_service.py
```

端口 9988，见 `instock/web/web_service.py:76-77`。脚本版 `instock/bin/run_web.sh:3` 用 `/usr/local/bin/python3`。

### 全量作业

```
python instock/job/execute_daily_job.py
python instock/job/execute_daily_job.py 2023-03-01
python instock/job/execute_daily_job.py 2023-03-01,2023-03-02
python instock/job/execute_daily_job.py 2023-03-01 2023-03-21
```

参数语义见 `instock/bin/run_job.sh:9-12`；编排顺序见 `instock/job/execute_daily_job.py:40-59`（建库→基础数据→选股→其它基础数据/指标/K线/策略→回测→收盘数据，其中部分 job 当前被注释）。

### 单个作业

```
python instock/job/init_job.py
python instock/job/selection_data_daily_job.py
python instock/job/basic_data_daily_job.py
python instock/job/basic_data_other_daily_job.py
python instock/job/indicators_data_daily_job.py
python instock/job/klinepattern_data_daily_job.py
python instock/job/strategy_data_daily_job.py
python instock/job/backtest_data_daily_job.py
```

清单见 `instock/bin/run_job.sh:14-22`。每个 job 内部用 `run_template.run_with_args` 支持同款日期参数，见 `instock/lib/run_template.py:17-58`。

### 交易

```
python instock/trade/trade_service.py
```

`main()` 构造 `MainEngine(broker='gf_client', need_data, log_handler)`，加载策略后 `start()`，见 `instock/trade/trade_service.py:19-25`。券商账号配置在 `instock/config/trade_client.json`（`trade_service.py:10`）。

## PYTHONPATH

- 容器内 `PYTHONPATH=/data/InStock`（`docker/Dockerfile:10`、`instock/bin/run_cron.sh:4`）。
- 本地各入口脚本自行 `sys.path.append` 项目根，见 `instock/web/web_service.py:16-18`、`instock/job/execute_daily_job.py:13-15`、`instock/job/init_job.py:10-12`。

## Docker

```
cd docker && docker-compose up -d
```

- 起 MariaDB（`InStockDbService`）+ InStock（`InStock`，映射 9988 端口），见 `docker/docker-compose.yml:4-26`。
- InStock 镜像 `ENTRYPOINT supervisord -n -c /data/InStock/supervisor/supervisord.conf`（`docker/Dockerfile:77`）。

## supervisord

```
supervisord -c supervisor/supervisord.conf
```

三个 program（`supervisor/supervisord.conf:25-42`）：

- `run_job`：`/data/InStock/instock/bin/run_job.sh`，`autorestart=false`，priority 100。
- `run_web`：`/data/InStock/instock/bin/run_web.sh`，`autorestart=true`，priority 500。
- `run_cron`：`/data/InStock/instock/bin/run_cron.sh`，`autorestart=true`，priority 900。

## cron 调度

容器内 crontab（`docker/Dockerfile:69-74`）：

- `*/30 9,10,11,13,14,15 * * 1-5 /bin/run-parts /etc/cron.hourly` — 工作日盘中每 30 分钟跑 `cron/cron.hourly/run_hourly`，即 `basic_data_daily_job.py`（`cron/cron.hourly/run_hourly:3`）。
- `30 17 * * 1-5 /bin/run-parts /etc/cron.workdayly` — 工作日 17:30 跑 `cron/cron.workdayly/run_workdayly`，即 `execute_daily_job.py`（`cron/cron.workdayly/run_workdayly:3`）。
- `30 10 * * 3,6 /bin/run-parts /etc/cron.monthly` — 周三、六 10:00 跑 `cron/cron.monthly/run_monthly`，清缓存（`cron/cron.monthly/run_monthly:4`）。

cron 进程由 `instock/bin/run_cron.sh` 启动（`run_cron.sh:11` `/usr/sbin/cron -f`），并将环境变量写入 `/etc/environment` 供 cron 任务继承（`run_cron.sh:8`）。

## 配置文件

- `instock/config/eastmoney_cookie.txt` — 东方财富 Cookie 文件（`instock/core/eastmoney_fetcher.py:40-46`）。
- `instock/config/proxy.txt` — HTTP 代理列表，每行一个，`singleton_proxy.proxys` 随机选取（`instock/core/singleton_proxy.py:21-34`）。
- `instock/config/trade_client.json` — 券商账号配置，供 `easytrader` `prepare`（`instock/trade/trade_service.py:10`、`instock/trade/robot/engine/main_engine.py:34-36`；格式见 `instock/trade/usage.md`）。
- 环境变量 `EAST_MONEY_COOKIE`：优先级高于 cookie 文件（`instock/core/eastmoney_fetcher.py:34-37`）。

## lint

```
make lint-arch
```

架构 lint（harness 创建后可用）。
