# trade-engine.md

InStock 实盘交易引擎结构。论断标注 `file:line`。

## 入口

`instock/trade/trade_service.py:19` `main()`：

```python
broker = 'gf_client'
log_handler = DefaultLogHandler(name='交易服务', log_type='file', filepath=log_filepath)
m = MainEngine(broker, need_data, log_handler)
m.is_watch_strategy = True
m.load_strategy()
m.start()
```

- `broker='gf_client'`（`trade_service.py:20`），券商账号文件 `need_data = config/trade_client.json`（`trade_service.py:10`）。
- 日志路径 `log/stock_trade.log`（`trade_service.py:11`）。
- `is_watch_strategy=True` 开启策略文件改动自动重载，注释明确「不建议在生产环境下使用」（`trade_service.py:23`）。

支持的 broker 类型与 `easytrader.use` 用法见 `instock/trade/usage.md`（htzq_client / ht_client / gj_client / universal_client / ths / xq，`usage.md:9-49`）。

## MainEngine 主引擎

`instock/trade/robot/engine/main_engine.py:22` `class MainEngine`。

职责：登录券商、启动事件/时钟引擎、动态加载策略、退出 shutdown。

- 初始化（`main_engine.py:25-79`）：
  - `easytrader.use(broker)` + `user.prepare(need_data)`；账号文件不存在则告警并降级（`main_engine.py:33-38`）。`broker is None` 时进入无交易模式（`main_engine.py:39-41`）。
  - 构造 `EventEngine` 与 `ClockEngine`（`main_engine.py:43-44`）。
  - 策略容器 `strategies: OrderedDict`、`strategy_list: list`（`main_engine.py:47-48`）。
  - 退出信号 `SIGINT/SIGTERM`，非 Windows 加 `SIGHUP/SIGQUIT`，统一绑 `_shutdown`（`main_engine.py:68-77`）。
- `start()`（`main_engine.py:81-90`）：启动 EventEngine → sleep 10s 等账户加载 → 启动 ClockEngine，并把二者 stop 注册到 `main_shutdown`。
- `load_strategy(names=None)`（`main_engine.py:150-163`）：扫描 `strategies/` 目录下 `.py`（排除 `__init__.py`），逐个 `load`；`is_watch_strategy` 为真则启动 `_watch_thread` 轮询重载。
- `load(names, strategy_file)`（`main_engine.py:92-133`）：按 `os.path.getmtime` 检测改动；有改动则 `importlib.reload`，从模块取 `Strategy` 类实例化，注册事件监听（`main_engine.py:135-148`）。
- `_shutdown(sig, frame)`（`main_engine.py:201-231`）：依次 `before_shutdown` → `main_shutdown` → 各策略 `shutdown()` → `after_shutdown` → `sys.exit(1)`。

## EventEngine 事件引擎

`instock/trade/robot/engine/event_engine.py:19` `class EventEngine`。

- `Queue` + 单消费线程 `__run`，每条事件起独立 `__process` 线程执行 handlers（`event_engine.py:36-52`）。
- `register(event_type, handler)` / `unregister` 维护 `defaultdict(list)`（`event_engine.py:64-77`）。
- `Event(event_type, data)`（`event_engine.py:13-16`）。

## ClockEngine 时钟引擎

`instock/trade/robot/engine/clock_engine.py:99` `class ClockEngine`。

- `EventType = 'clock_tick'`（`clock_engine.py:104`），策略的 `clock(event)` 监听此类型（`main_engine.py:148`）。
- `now` 属性返回 `time.time()`（`clock_engine.py:154-160`），`now_dt` 返回带时区 arrow 时间（`clock_engine.py:162-167`）。
- 交易状态 `trading_state` 由 `trade_time.is_tradetime` + `is_trade_date` 判定（`clock_engine.py:119-120`）。
- 默认注册的时刻事件（`clock_engine.py:126-148`）：`open`(09:00)、`pause`(11:30)、`continue`(13:00)、`close`(15:00，置 `trading_state=False`)；间隔事件 0.5/1/5/15/30/60 分钟（`clock_engine.py:151-152`）。
- 两类 handler：`ClockMomentHandler`（时刻，`clock_engine.py:53-96`，支持仅交易日触发与补触发）、`ClockIntervalHandler`（间隔，`clock_engine.py:23-50`，可限定交易阶段）。
- 非交易日 `tock` 直接跳过（`clock_engine.py:177-181`）。

## StrategyTemplate 策略模板

`instock/trade/robot/infrastructure/strategy_template.py:9` `class StrategyTemplate`。

- 类属性 `name`（`strategy_template.py:10`）；`__init__(user, log_handler, main_engine)` 注入 `user`、`main_engine`、`clock_engine`，并调 `init()`（`strategy_template.py:12-18`）。
- 子类可重写：`init` 注册时钟事件、`strategy` 执行逻辑、`clock(event)` 响应时钟、`log_handler` 自定义日志、`shutdown` 收尾（`strategy_template.py:20-42`）。

## DefaultLogHandler

`instock/trade/robot/infrastructure/default_handler.py:15`。基于 `logbook`，`log_type='stdout'` 输出屏幕、`'file'` 写文件（`default_handler.py:27-33`）；`__getattr__` 代理到内部 `Logger`（`default_handler.py:35-36`）。

## 内置策略示例

`instock/trade/strategies/stagging.py:14` `class Strategy(StrategyTemplate)`：

- `name = 'stagging'`（`stagging.py:15`）。
- `init` 注册 10:00 时刻事件与 1.5 分钟间隔事件（`stagging.py:24-30`）。
- `strategy` 调 `self.user.auto_ipo()` 打新（`stagging.py:32-34`）。
- `clock` 在 `event.data.clock_event == self.name` 时触发 `strategy`（`stagging.py:36-42`）。

策略目录由 `MainEngine.load_strategy` 扫描 `strategies/`（`main_engine.py:153-156`），即 `instock/trade/strategies/`。

## broker 客户端

`easytrader`（`requirements.txt:15`）提供券商客户端封装，`MainEngine` 在 `__init__` 中 `easytrader.use(broker).prepare(need_data)`（`main_engine.py:33-36`）。`prepare` 配置文件格式见 `instock/trade/usage.md:92-123`（银河/国金、华泰、雪球三种）。
