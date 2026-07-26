# data-pipeline.md

InStock 数据管线：抓取 → 落库 → 指标/形态 → 选股/策略 → 回测。论断标注 `file:line`。

## 总流程

`execute_daily_job.main()` 编排顺序（`instock/job/execute_daily_job.py:40-59`）：

1. `init_job.main()` 建库（`execute_daily_job.py:40`）。
2. `basic_data_daily_job.main()` 实时行情（`execute_daily_job.py:42`）。
3. `selection_data_daily_job.main()` 综合选股（`execute_daily_job.py:44`）。
4. 线程池并发：`basic_data_other_daily_job.main()`（`execute_daily_job.py:47`），指标/K线/策略/回测当前被注释（`execute_daily_job.py:49-56`）。
5. `basic_data_after_close_daily_job.main()` 收盘数据（`execute_daily_job.py:59`）。

每个 job 内部由 `run_template.run_with_args` 处理日期参数：无参取最近交易日（`instock/lib/run_template.py:48-58`，调 `trade_time.get_trade_date_last`），单参批量，双参区间并用 `ThreadPoolExecutor` 提交（`run_template.py:17-46`）。函数名前缀决定调用分支：`save_nph*` 传 `before=False`（`run_template.py:51-52`），`save_after_close*` 走另一支（`run_template.py:53-54`）。

## 1. 数据抓取

抓取层在 `instock/core/crawling/`，全部走 `eastmoney_fetcher.make_request`（`instock/core/eastmoney_fetcher.py:86`），封装 Cookie、代理、重试（`eastmoney_fetcher.py:51-60` Retry 3 次退避）。

代表：`instock/core/crawling/stock_hist_em.py`

- `stock_zh_a_spot_em()` 沪深京 A 股实时行情，分页拉取（`stock_hist_em.py:21-61`），列名重命名 + `pd.to_numeric` 清洗（`stock_hist_em.py:64-188`）。
- `code_id_map_em()` 股票代码→市场 id 映射，`@lru_cache` 缓存（`stock_hist_em.py:191-310`）。
- `stock_zh_a_hist()` 日/周/月 K 线，按 `secid` 区分沪 1./深 0.（`stock_hist_em.py:313-385`）。
- `stock_zh_a_hist_min_em()` 分时（`stock_hist_em.py:388-515`）。

抓取代理来自 `singleton_proxy.proxys.get_proxies()`，随机选 `instock/config/proxy.txt` 中一行（`instock/core/singleton_proxy.py:30-34`）。

## 2. 落库

落库统一走 `instock/lib/database.py`，DataFrame 入库核心是 `insert_db_from_df` / `insert_other_db_from_df`（`database.py:69-113`）：

- 用 SQLAlchemy `engine()`（`database.py:50-51`，URL 在 `database.py:38-39`）。
- `data.to_sql(if_exists='append')`，按 `cols_type` 决定列类型：None 推断、`False` 全 NVARCHAR(255)、字典则按字典（`database.py:90-98`）。
- 落库后 `inspect.get_pk_constraint` 检查主键，无则 `ALTER TABLE ADD PRIMARY KEY`，并按 `indexs` 字典建索引（`database.py:103-111`）。
- 重跑先 `checkTableIsExist`（`database.py:158-168`）+ `executeSql` `DELETE FROM ... where date=...`（`database.py:172-178`）。

表结构与列类型来自 `instock/core/tablestructure.py`：`TABLE_CN_STOCK_SPOT`、`TABLE_CN_STOCK_INDICATORS`、`TABLE_CN_STOCK_SELECTION` 等常量 + `get_field_types`（`tablestructure.py:1064`）。代表用法见 `basic_data_daily_job.py:31-40`：取 `TABLE_CN_STOCK_SPOT['name']`、`get_field_types(TABLE_CN_STOCK_SPOT['columns'])`，再 `insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")`。

Web 层用 `torndb.Connection`（`instock/lib/torndb.py:47`）做查询，`Connection.query`（`torndb.py:136`）与 `Connection.get`（`torndb.py:157`），连接配置来自 `database.MYSQL_CONN_TORNDB`（`database.py:45-46`，在 `web_service.py:59` 注入 `self.db`）。

## 3. 指标 / 形态

指标作业 `instock/job/indicators_data_daily_job.py`：

- `prepare(date)` 取 `stock_hist_data(date).get_data()`（`indicators_data_daily_job.py:25-26`），`run_check` 用 `ThreadPoolExecutor(max_workers=40)` 并发调 `calculate_indicator.get_indicator`（`indicators_data_daily_job.py:61-83`），结果合并后落 `TABLE_CN_STOCK_INDICATORS`（`indicators_data_daily_job.py:33-55`）。
- 二次筛选：`guess_buy` 按 `kdjk>=80 and kdjd>=70 and kdjj>=100 and rsi_6>=80 and cci>=100 and cr>=300 and wr_6>=-20 and vr>=160` 选买入候选（`indicators_data_daily_job.py:96-98`），`guess_sell` 用对称阈值（`indicators_data_daily_job.py:131-133`），分别落 `TABLE_CN_STOCK_INDICATORS_BUY` / `_SELL`。

K 线形态在 `instock/core/pattern/pattern_recognitions.py`，由 `klinepattern_data_daily_job.py` 调度；指标计算核心在 `instock/core/indicator/calculate_indicator.py`，依赖 `talib`（`requirements.txt:5`）。

## 4. 选股 / 策略

- 综合选股：`selection_data_daily_job.py` 调 `stockfetch.fetch_stock_selection()`（`selection_data_daily_job.py:27`）落 `TABLE_CN_STOCK_SELECTION`（`selection_data_daily_job.py:31-41`）。
- 策略作业：`strategy_data_daily_job.py` 驱动 `instock/core/strategy/` 下各策略模块，每个策略暴露 `check(code_name, data, date, threshold=60)` 布尔函数，组合多个 `enter.check_*` 子条件。代表：
  - `turtle_trade.check_enter`：海龟法则，最近 60 日最高收盘价突破（`instock/core/strategy/turtle_trade.py:14-37`）。
  - `breakthrough_platform.check`：平台突破，60 日均线 + 放量上涨 + 偏离区间，内部调 `enter.check_volume`（`instock/core/strategy/breakthrough_platform.py:17-49`）。
  - `enter.check_volume`：放量上涨公共条件，5 日均量≥2 倍、成交额≥2 亿（`instock/core/strategy/enter.py:16-57`）。

## 5. 回测

`backtest_data_daily_job.py` 驱动 `instock/core/backtest/rate_stats.py`，对入选股票做收益率统计并落 `TABLE_CN_STOCK_BACKTEST_DATA`（列定义在 `tablestructure.TABLE_CN_STOCK_BACKTEST_DATA`，被指标作业 concat 引用见 `indicators_data_daily_job.py:115-116`）。

## 缓存

历史 K 线缓存目录 `instock/cache/hist/`，由 `stockfetch.py:29-31` 创建；`cron.monthly/run_monthly` 清空该目录（`cron/cron.monthly/run_monthly:4` `rm -rf /data/InStock/instock/cache/hist/*`）。
