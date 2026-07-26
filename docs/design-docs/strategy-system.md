# strategy-system.md

InStock 选股策略系统。论断标注 `file:line`。

## 模块组织

策略位于 `instock/core/strategy/`，每个文件一个独立策略，统一暴露 `check(code_name, data, date=None, threshold=60) -> bool` 布尔接口。`__init__.py` 为空（`instock/core/strategy/__init__.py:1-6`），仅作包标识，无注册表。

策略文件清单（`ls instock/core/strategy/`）：

- `turtle_trade.py` — 海龟突破
- `breakthrough_platform.py` — 平台突破
- `enter.py` — 公共子条件（放量上涨、海龟突破）
- `backtrace_ma250.py`、`climax_limitdown.py`、`high_tight_flag.py`、`keep_increasing.py`、`low_atr.py`、`low_backtrace_increase.py`、`parking_apron.py`

## 调度

由 `instock/job/strategy_data_daily_job.py` 驱动（`execute_daily_job.py:52` 调用，当前在主流程中被注释，`execute_daily_job.py:52`）。`strategy_data_daily_job.py` 走 `run_template.run_with_args`（与其它 job 同款，见 `indicators_data_daily_job.py:158`、`selection_data_daily_job.py:47` 的同构 main）。

策略不是插件式注册：调用方按需 import 各策略模块的 `check`，传入 `code_name=(date, code)` 与历史行情 DataFrame + 日期 + threshold=60，返回 True 即入选。

## 公共子条件 enter.py

`instock/core/strategy/enter.py` 提供复用条件：

- `check_volume(code_name, data, date=None, threshold=60)`（`enter.py:16`）放量上涨：
  - `p_change < 2` 或收盘<开盘 → False（`enter.py:28-29`）。
  - `vol_ma5 = tl.MA(volume, 5)`（`enter.py:31-32`）。
  - 成交额 `last_close * last_vol >= 2 亿`（`enter.py:46-47`）。
  - 量比 `last_vol / mean_vol >= 2`（`enter.py:53-55`）。
- `check_enter(code_name, data, date=None, threshold=60)`（`turtle_trade.py:14`，海龟法则子条件，实为同模块 `turtle_trade` 文件内）：最近 threshold 日最后收盘价 ≥ 区间最高收盘价（`turtle_trade.py:25-35`）。

注：`turtle_trade.py:14` 的 `check_enter` 与 `enter.py` 是不同文件；`breakthrough_platform.py:37` 调用的是 `enter.check_volume`（`from instock.core.strategy import enter`，`breakthrough_platform.py:7`）。

## 代表策略

### turtle_trade.py（海龟交易法则）

`instock/core/strategy/turtle_trade.py:14-37`：取 `data.tail(n=threshold)`，逐行求 `max_price`，最后收盘 `last_close >= max_price` 即入选。常量 `BALANCE = 200000`（`turtle_trade.py:9`）。

### breakthrough_platform.py（平台突破）

`instock/core/strategy/breakthrough_platform.py:17-49`：

1. `ma60 = tl.MA(close, 60)`（`breakthrough_platform.py:29-30`）。
2. 找 `open < ma60 <= close` 的突破日，且该日满足 `enter.check_volume`（`breakthrough_platform.py:35-39`）。
3. 突破日之前任意一天的 `(ma60 - close)/ma60` 必须落在 `(-0.05, 0.2)`（`breakthrough_platform.py:44-47`）。

### enter.py（放量上涨 + 海龟突破子条件）

见上文「公共子条件」。

## 与回测的衔接

入选股票写入 `TABLE_CN_STOCK_BACKTEST_DATA` 相关列（`indicators_data_daily_job.py:115-116` 用 `pd.concat([data, DataFrame(columns=TABLE_CN_STOCK_BACKTEST_DATA['columns'])])` 占位），由 `backtest_data_daily_job.py` 驱动 `instock/core/backtest/rate_stats.py` 做收益率统计。策略作业与回测作业在 `execute_daily_job.py:49-56` 当前均被注释，需手动启用。

## 数据依赖

策略入参 `data` 为单只股票的历史日 K DataFrame，由 `stock_hist_data(date).get_data()` 提供（`indicators_data_daily_job.py:26`），缓存于 `instock/cache/hist/`（`stockfetch.py:29-31`）。指标列（`kdjk/kdjd/kdjj/rsi_6/cci/cr/wr_6/vr`）由 `calculate_indicator.get_indicator` 用 `talib` 计算（`indicators_data_daily_job.py:69`）。
