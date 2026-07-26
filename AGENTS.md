# AGENTS.md

InStock 股票量化系统导航地图。本文档为入口索引，详细设计见 `docs/`。

## 1. 项目定位

InStock：基于 Python 3.11 + Tornado + MariaDB + TA-Lib + easytrader + supervisord 的 A 股量化系统，覆盖数据抓取、指标/形态计算、选股策略、回测、Web 展示与实盘交易。

## 2. 目录地图

- `instock/bin/` — 启动脚本（`run_web.sh`、`run_job.sh`、`run_cron.sh`），见 `instock/bin/run_web.sh:3`、`instock/bin/run_job.sh:6`、`instock/bin/run_cron.sh:9`。
- `instock/config/` — 配置文件：`eastmoney_cookie.txt`、`proxy.txt`、`trade_client.json`。
- `instock/core/` — 核心层：抓取 `crawling/`、指标 `indicator/`、K线 `kline/`、形态 `pattern/`、策略 `strategy/`、回测 `backtest/`，加 `eastmoney_fetcher.py`、`stockfetch.py`、`tablestructure.py`、`singleton_*.py`。
- `instock/job/` — 作业调度层：`execute_daily_job.py` 编排全部日作业，`init_job.py` 建库，其余 9 个 `*_daily_job.py` 各管一域。
- `instock/lib/` — 基础库：`database.py`（DB 读写）、`torndb.py`（Tornado DB 包装）、`run_template.py`（日期参数化批跑）、`trade_time.py`（交易时间）、`singleton_type.py`、`crypto_aes.py`、`version.py`。
- `instock/trade/` — 交易引擎：`robot/engine/`（`main_engine.py`、`clock_engine.py`、`event_engine.py`）、`robot/infrastructure/`（`strategy_template.py`、`default_handler.py`）、`strategies/`（`stagging.py` 等）、`trade_service.py`。
- `instock/web/` — Web 层：`web_service.py`（Tornado 入口，端口 9988）、`base.py`、`dataTableHandler.py`、`dataIndicatorsHandler.py`、`templates/`、`static/`。
- `cron/` — 容器内 cron 脚本：`cron.hourly/run_hourly`、`cron.workdayly/run_workdayly`、`cron.monthly/run_monthly`。
- `docker/` — `Dockerfile`、`docker-compose.yml`、`build.sh`。
- `supervisor/` — `supervisord.conf`，跑 `run_job`/`run_web`/`run_cron` 三个 program，见 `supervisor/supervisord.conf:25-42`。

## 3. 关键命令

详细命令与依赖说明见 `docs/DEVELOPMENT.md`。

- 初始化 DB：`python instock/job/init_job.py`
- 跑 web：`python instock/web/web_service.py`（端口 9988，`instock/web/web_service.py:76`）
- 跑全部 job：`python instock/job/execute_daily_job.py`，支持单日/批量/区间参数，见 `instock/bin/run_job.sh:9-12`
- 单个 job：`python instock/job/<job_name>.py`，例如 `python instock/job/basic_data_daily_job.py`
- Docker：`cd docker && docker-compose up -d`
- supervisord：`supervisord -c supervisor/supervisord.conf`
- lint：`make lint-arch`（创建后）

## 4. 架构入口

分层与边界调用统计见 `docs/ARCHITECTURE.md`。入口点：作业 `execute_daily_job.py:35`、Web `web_service.py:71`、交易 `trade_service.py:19`。

## 5. 约束与已知问题

- **lib→core 循环**：`instock/lib/trade_time.py:5` import `instock.core.singleton_trade_date`，linter 会标记；既有结构，harness 不修。
- **DB 配置走环境变量**：默认 localhost:3306/root/root/instockdb，可被 `db_host`/`db_user`/`db_password`/`db_database`/`db_port` 覆盖，见 `instock/lib/database.py:14-36`。
- **PYTHONPATH**：容器内为 `/data/InStock`（`docker/Dockerfile:10`、`instock/bin/run_cron.sh:4`）；本地运行时各入口脚本自行 `sys.path.append` 项目根，见 `instock/web/web_service.py:16-18`、`instock/job/execute_daily_job.py:13-15`。
- **TA-Lib 原生库**：需先编译安装 ta-lib native 再 `pip install TA-Lib`，编译步骤见 `docker/Dockerfile:46-53`。
- **东方财富 Cookie**：优先级 `EAST_MONEY_COOKIE` 环境变量 > `instock/config/eastmoney_cookie.txt` > 默认值，见 `instock/core/eastmoney_fetcher.py:34-49`。

## 6. 文档索引

- `docs/ARCHITECTURE.md` — 分层、边界调用统计、热点函数、入口点清单。
- `docs/DEVELOPMENT.md` — 环境准备、命令、cron 调度、配置文件。
- `docs/design-docs/data-pipeline.md` — 抓取→落库→指标/形态→选股/策略→回测管线。
- `docs/design-docs/trade-engine.md` — MainEngine / ClockEngine / StrategyTemplate / 事件引擎。
- `docs/design-docs/web-layer.md` — Tornado 路由与 handler 结构。
- `docs/design-docs/strategy-system.md` — 选股策略注册与调度机制。
