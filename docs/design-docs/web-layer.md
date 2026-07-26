# web-layer.md

InStock Web 层基于 Tornado。论断标注 `file:line`。

## 入口与路由

`instock/web/web_service.py:35` `class Application(tornado.web.Application)`，`__init__` 注册路由表（`web_service.py:37-48`）：

| 路由 | Handler | 说明 |
|---|---|---|
| `/` | `HomeHandler` | 首页（`web_service.py:39`） |
| `/instock/` | `HomeHandler` | 首页（`web_service.py:40`） |
| `/instock/api_data` | `dataTableHandler.GetStockDataHandler` | 报表数据 JSON 接口（`web_service.py:42`） |
| `/instock/data` | `dataTableHandler.GetStockHtmlHandler` | 报表页面（`web_service.py:43`） |
| `/instock/data/indicators` | `dataIndicatorsHandler.GetDataIndicatorsHandler` | 股票指标/K 线（`web_service.py:45`） |
| `/instock/control/attention` | `dataIndicatorsHandler.SaveCollectHandler` | 加入/取消关注（`web_service.py:47`） |

配置（`web_service.py:49-56`）：模板 `templates/`、静态 `static/`、`xsrf_cookies=False`、`cookie_secret` 固定值、`debug=True`。

全局 DB 连接在 `Application.__init__` 末尾注入：`self.db = torndb.Connection(**mdb.MYSQL_CONN_TORNDB)`（`web_service.py:59`），连接参数来自 `instock/lib/database.py:45-46`。

## main()

`web_service.py:71` `main()`：关闭 tornado 日志（`web_service.py:73`），建 `HTTPServer` 监听 9988（`web_service.py:75-77`），启动 `IOLoop`（`web_service.py:82`）。脚本版 `instock/bin/run_web.sh:3`。

## BaseHandler

`instock/web/base.py:13` `class BaseHandler(tornado.web.RequestHandler, ABC)`。

- `db` property 每次访问先 `SELECT 1` 探活，失败则 `reconnect`（`base.py:14-22`）。
- `LeftMenu`（`base.py:25-28`）从 `singleton_stock_web_module_data.stock_web_module_data().get_data_list()` 取左侧菜单。
- `GetLeftMenu(url)` 工厂（`base.py:32-33`），被各 handler `render` 时传入。

## dataTableHandler

`instock/web/dataTableHandler.py`。

- `MyEncoder(json.JSONEncoder)`（`dataTableHandler.py:18-28`）：bytes → 是/否；`datetime.date` → `/OADate(...)/` 串。
- `GetStockHtmlHandler.get`（`dataTableHandler.py:33-43`）：按 `table_name` 取 `web_module_data`，根据 `is_realtime` 决定日期（实时用 `run_date_nph`，否则 `run_date`，调 `trade_time.get_trade_date_last`，`dataTableHandler.py:37-41`），渲染 `stock_web.html`。
- `GetStockDataHandler.get`（`dataTableHandler.py:47-71`）：按 `name`/`date` 拼 `SELECT *{order_columns} FROM {table_name} WHERE date=%s ORDER BY ...`，`self.db.query(sql, date)` 取数，`json.dumps(data, cls=MyEncoder)` 返回（`dataTableHandler.py:54-71`）。`order_by`/`order_columns` 来自 `web_module_data`（`dataTableHandler.py:60-66`）。

## dataIndicatorsHandler

`instock/web/dataIndicatorsHandler.py`。

- `GetDataIndicatorsHandler.get`（`dataIndicatorsHandler.py:16-40`）：按 `code` 前缀分流——`1`/`5` 开头调 `stockfetch.fetch_etf_hist`，否则 `fetch_stock_hist`（`dataIndicatorsHandler.py:24-27`）；再用 `kline.visualization.get_plot_kline` 生成图（`dataIndicatorsHandler.py:31-32`），渲染 `stock_indicators.html`。
- `SaveCollectHandler.get`（`dataIndicatorsHandler.py:44-65`）：按 `otype` 在 `TABLE_CN_STOCK_ATTENTION['name']` 上 `DELETE`/`INSERT`（`dataIndicatorsHandler.py:53-60`），统一返回 `{"data":[{}]}`。

## 依赖

Web 层 fan-out：→core(9)（`stockfetch`、`singleton_stock_web_module_data`、`kline.visualization`、`tablestructure`）、→lib（`torndb`、`database`、`trade_time`）。`Application.db` 走 torndb（`web_service.py:59`），`BaseHandler.db` 走 `self.application.db`（`base.py:14-22`）。
