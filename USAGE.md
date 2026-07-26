# InStock 使用说明

InStock 是 A 股量化系统：抓取每日行情/基本面数据，计算技术指标与 K 线形态，综合选股、回测，提供 Web 展示与实盘交易。本文只讲**配置、构建、运行**。详细架构与开发见 `docs/`。

---

## 一、环境要求

| 依赖 | 说明 |
|---|---|
| Python | 3.11（Docker 镜像内置） |
| MariaDB / MySQL | 数据存储，默认 `localhost:3306` |
| TA-Lib native 库 | 技术指标计算依赖，需先编译安装 C 库再装 Python 包 |
| Redis 等其他服务 | 无 |

Python 依赖见 `requirements.txt`（含 numpy/pandas/TA_Lib/tornado/PyMySQL/SQLAlchemy/easytrader 等）。

---

## 二、配置

### 1. 数据库

配置读环境变量，未设则用默认值（见 `instock/lib/database.py:14-36`）：

| 环境变量 | 默认值 |
|---|---|
| `db_host` | `localhost` |
| `db_port` | `3306` |
| `db_user` | `root` |
| `db_password` | `root` |
| `db_database` | `instockdb` |

本地示例：

```sh
export db_host=127.0.0.1 db_user=root db_password=你的密码 db_database=instockdb
```

Docker 部署时 `docker-compose.yml` 已设 `db_host=InStockDbService`，密码默认 `root`。

### 2. 东方财富 Cookie（数据抓取需要）

优先级：环境变量 `EAST_MONEY_COOKIE` > `instock/config/eastmoney_cookie.txt` > 默认值（见 `instock/core/eastmoney_fetcher.py:34`）。

```sh
export EAST_MONEY_COOKIE='从浏览器复制的 cookie 串'
```

### 3. 配置文件（`instock/config/`）

| 文件 | 用途 |
|---|---|
| `eastmoney_cookie.txt` | cookie 备用存放（未用环境变量时） |
| `proxy.txt` | 抓取代理（可选，Docker 通过 volume 挂载 `/data/instockproxy.txt`） |
| `trade_client.json` | 自动交易客户端配置（仅用交易功能时需要） |

---

## 三、构建

### 方式 A：Docker（推荐，开箱即用）

```sh
# 拉取官方镜像
docker pull mayanghua/instock:latest

# 启动（含 MariaDB）
cd docker
docker-compose up -d
```

`docker-compose.yml` 会拉起两个容器：`InStockDbService`（MariaDB）和 `InStock`（Web+Job+Cron，端口 9988）。

### 方式 B：本地构建 Docker 镜像

```sh
cd docker
./build.sh        # 同步代码、构建并推送 mayanghua/instock:latest
# 仅构建不推送：docker build -f Dockerfile -t myinstock .
```

### 方式 C：本地源码运行

```sh
# 1. 安装 TA-Lib C 库（macOS: brew install ta-lib；Linux: 从源码 ./configure && make && make install）
# 2. 安装 Python 依赖
pip install -r requirements.txt
```

---

## 四、运行

### 1. 初始化数据库（首次必须）

```sh
python instock/job/init_job.py        # 创建库表
```

### 2. 启动 Web 服务

```sh
python instock/web/web_service.py     # 访问 http://localhost:9988/
```

### 3. 跑数据作业

```sh
# 全量当日作业
python instock/job/execute_daily_job.py

# 单日：python instock/job/execute_daily_job.py 2023-03-01
# 批量：python instock/job/execute_daily_job.py 2023-03-01,2023-03-02
# 区间：python instock/job/execute_daily_job.py 2023-03-01 2023-03-21

# 单个作业（如综合选股）
python instock/job/selection_data_daily_job.py
```

作业清单：`init_job`（建库）、`basic_data_daily_job`（基础数据实时）、`basic_data_other_daily_job`（非实时）、`indicators_data_daily_job`（指标）、`klinepattern_data_daily_job`（K 线形态）、`selection_data_daily_job`（选股）、`strategy_data_daily_job`（策略）、`backtest_data_daily_job`（回测）。

### 4. 实盘交易（可选）

```sh
python instock/trade/trade_service.py
```

需先配置 `instock/config/trade_client.json`，券商客户端用法见 `instock/trade/usage.md`。

### 5. 进程托管（Docker 内置，本地可选）

```sh
supervisord -c supervisor/supervisord.conf
```

托管三个进程：`run_job`（一次性作业）、`run_web`（Web，自动重启）、`run_cron`（cron 调度）。

Docker 内 cron 调度（见 `docker/Dockerfile`）：

```
*/30 9,10,11,13,14,15 * * 1-5   # 盘中每 30 分钟跑基础数据
30 17 * * 1-5                   # 收盘后跑全量作业
30 10 * * 3,6                   # 周三、周六清缓存
```

---

## 五、常用命令速查

| 操作 | 命令 |
|---|---|
| Docker 启动 | `cd docker && docker-compose up -d` |
| Docker 停止 | `cd docker && docker-compose down` |
| 建库 | `python instock/job/init_job.py` |
| 启 Web | `python instock/web/web_service.py` |
| 跑全量作业 | `python instock/job/execute_daily_job.py` |
| 跑区间作业 | `python instock/job/execute_daily_job.py 2023-03-01 2023-03-21` |
| 进程托管 | `supervisord -c supervisor/supervisord.conf` |
| Lint | `make lint-arch` |
| Web 访问 | http://localhost:9988/ |

> 入口脚本会自动把项目根加入 `sys.path`（见 `instock/web/web_service.py:16`、`instock/job/execute_daily_job.py:13`），本地源码运行无需手动设 `PYTHONPATH`；Docker 内 `PYTHONPATH=/data/InStock`（见 `docker/Dockerfile:10`）。
