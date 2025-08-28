# 股票历史数据获取与处理

本目录包含了完整的股票历史数据获取、聚合和存储解决方案，基于 baostock 数据源，支持 ClickHouse（推荐）与Mysql 数据库存储。

## 🎯 主要功能

### 1. 数据获取 - 使用 baostock 获取历史行情

#### 数据源优势
- **baostock**: 比 akshare 与 tushare 更稳定，无需额外注册
- **覆盖范围**: 上海证券交易所（sh）和深圳证券交易所（sz）
- **时间跨度**: 默认获取 2000年至今的完整历史数据
- **数据完整性**: 包含开高低收、成交量、成交额、换手率等完整指标

#### 使用方式

**逐步获取所有股票数据：**
```bash
python bao_all.py
```

**分片并发执行（推荐）：**
```bash
# 分成3个进程并发执行，提高数据获取效率
python bao_all.py --shard 0 &  # 处理索引 % 3 == 0 的股票
python bao_all.py --shard 1 &  # 处理索引 % 3 == 1 的股票  
python bao_all.py --shard 2 &  # 处理索引 % 3 == 2 的股票
```

**单只股票获取：**
```bash
python bao.py sh.600000  # 获取浦发银行数据
python bao.py sz.000001  # 获取平安银行数据
```

#### 状态管理特性
- 通过 `code_map.csv` 文件记录处理状态
- 支持断点续传，反复执行不会重复爬取已完成的股票
- 自动跳过失败或空数据的股票
- 实时显示处理进度和成功率

#### 数据存储格式
- 文件位置：`history_stock_data/` 目录
- 文件命名：`{market}-{code}.csv`（如：`sh-600000.csv`）
- 数据字段：日期、代码、开盘价、最高价、最低价、收盘价、前收盘价、成交量、成交额、复权标志、换手率、交易状态、涨跌幅、ST标记

### 2. 聚合数据 - 按时间维度整理

```bash
python download_process.py
```

#### 功能特点
- 自动读取 `history_stock_data/` 中的所有股票数据
- 按照 **5年维度** 聚合数据（便于后续分表存储）
- 并发读取文件，提高处理效率
- 自动更新 `code_map.csv` 中的处理状态

#### 输出文件
聚合后的数据保存在 `agg_data/` 目录：
```
agg_data/
├── data_2000_2004.csv  # 2000-2004年数据
├── data_2005_2009.csv  # 2005-2009年数据
├── data_2010_2014.csv  # 2010-2014年数据
├── data_2015_2019.csv  # 2015-2019年数据
├── data_2020_2024.csv  # 2020-2024年数据
└── data_2025_2025.csv  # 2025年至今数据
```

**MySQL 分表建议：** 可以基于这个5年维度进行分表，如 `stock_history_2020_2024` 等。

### 3. 写入 ClickHouse - 高性能分析数据库

#### 配置数据库连接

编辑 `db_engine.py` 文件，配置 ClickHouse 连接参数：

```python
CLICKHOUSE_CONFIG = {
    'host': '192.168.1.6',      # ClickHouse 服务器地址
    'port': 8123,               # HTTP 端口
    'username': 'root',         # 用户名
    'password': 'your_password', # 密码  
    'database': 'instockdb'     # 数据库名
}
```
执行导入数据的命令
```bash
python deal_clickhouse_data.py
```

## 4、写入mysql数据库
编辑 `db_engine.py` 文件，配置 Mysql 连接参数：

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'username': 'root',
    'password': 'your_password',
    'database': 'instockdb'
}
```

执行导入数据的命令
```bash
python deal_agg_data4mysql.py
``` 

#### ClickHouse 优势

**为什么选择 ClickHouse：**
- 🚀 **查询性能**: 比传统 MySQL 快 10-100 倍
- 💾 **存储效率**: 列式压缩，节省 80%+ 存储空间
- 📈 **分析能力**: 专为时间序列和大数据分析设计
- 🔄 **自动分区**: 按月自动分区，无需手动管理
- 📊 **物化视图**: 自动生成市场统计和股票基础信息

#### 表结构设计

**主表**: `cn_stock_history`
- 单表设计，支持全市场数据
- 按月自动分区（`PARTITION BY toYYYYMM(date)`）
- 主键排序优化（`ORDER BY (date, code)`）

**物化视图**:
- `cn_market_daily_stats`: 市场日度统计
- `cn_stock_basic_info`: 股票基础信息

#### 数据导入

```python
from db_engine import create_clickhouse_client, create_stock_history_table_clickhouse
from deal_clickhouse_data import import_data_to_clickhouse

# 1. 创建连接
client = create_clickhouse_client()

# 2. 创建表结构
create_stock_history_table_clickhouse(client)

# 3. 导入数据
import_data_to_clickhouse()
```

## 📁 文件说明

| 文件名 | 功能描述 |
|--------|----------|
| `bao_all.py` | 批量获取所有股票历史数据，支持分片并发 |
| `bao.py` | 单只股票数据获取工具 |
| `download_process.py` | 数据聚合工具，按5年维度整理 |
| `db_engine.py` | 数据库连接配置（MySQL + ClickHouse） |
| `deal_clickhouse_data.py` | ClickHouse 数据导入工具 |
| `code_map.csv` | 股票代码映射和处理状态记录 |
| `CLICKHOUSE_DESIGN.md` | ClickHouse 数据库设计文档 |

## 🚀 完整使用流程

### 步骤1: 安装依赖
```bash
pip install baostock pandas clickhouse-connect
```

### 步骤2: 获取股票数据
```bash
# 方式1: 单进程执行
python bao_all.py

# 方式2: 分片并发执行（推荐）
python bao_all.py --shard 0 &
python bao_all.py --shard 1 &  
python bao_all.py --shard 2 &
```

### 步骤3: 聚合数据
```bash
python download_process.py
```

### 步骤4: 配置并导入 ClickHouse
```bash
# 1. 编辑 db_engine.py 配置连接参数
# 2. 运行数据导入
python deal_clickhouse_data.py
```

## 💡 性能优化建议

1. **并发获取**: 使用 `--shard` 参数分片执行，充分利用多核性能
2. **网络稳定**: 确保网络连接稳定，避免数据获取中断
3. **磁盘空间**: 预留足够磁盘空间（全市场数据约需 10-20GB）
4. **ClickHouse 调优**: 根据查询需求调整分区策略和索引
5. **定期更新**: 建议每日增量更新最新交易数据

## 🔧 故障排除

**常见问题：**

1. **网络连接失败**: 检查网络连接，重试执行
2. **磁盘空间不足**: 清理空间或更换存储位置
3. **ClickHouse 连接失败**: 检查服务状态和连接配置
4. **数据格式错误**: 检查源数据文件格式和编码

**日志查看：**
- 处理过程会实时显示进度和错误信息
- 检查 `code_map.csv` 中的 flag 字段确认处理状态

---

