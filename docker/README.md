# Docker部署说明

## 启动服务

### 1. 启动所有服务
```bash
cd docker
docker-compose up -d
```

### 2. 查看服务状态
```bash
docker-compose ps
```

## 服务配置

### MariaDB (MySQL)
- 容器名: InStockDbService
- 端口: 3306 (内部)
- 用户: root
- 密码: root
- 数据目录: /data/mariadb/data

### ClickHouse
- 容器名: InStockClickHouse
- HTTP端口: 8123
- Native端口: 9000
- 用户: root
- 密码: 123456
- 数据库: instockdb (需要手动创建)
- 数据目录: /data/clickhouse/data
- 日志目录: /data/clickhouse/logs

### InStock应用
- 容器名: InStock
- 端口: 9988
- 依赖: MariaDB, ClickHouse

## 环境变量

在应用中可以使用以下环境变量配置ClickHouse连接：

```bash
CLICKHOUSE_HOST=clickhouse        # Docker内部主机名
CLICKHOUSE_PORT=8123             # HTTP端口
CLICKHOUSE_USER=root             # 用户名
CLICKHOUSE_PASSWORD=123456       # 密码
CLICKHOUSE_DB=instockdb          # 数据库名
```

## 使用示例

### Python客户端使用
```python
from instock.lib.clickhouse_client import create_clickhouse_client

# 使用默认配置连接
with create_clickhouse_client() as client:
    # 创建数据库
    client.execute_query("CREATE DATABASE IF NOT EXISTS instockdb")
    
    # 获取股票数据
    df = client.get_stock_data('000001', '2024-01-01', '2024-12-31')
    print(df.head())
```

## 故障排除

### 1. 检查容器日志
```bash
docker logs InStockClickHouse
docker logs InStockDbService
docker logs InStock
```

### 2. 连接测试
```bash
# 测试ClickHouse连接
docker exec -it InStockClickHouse clickhouse-client --user=root --password=123456

# 创建数据库
docker exec -it InStockClickHouse clickhouse-client --user=root --password=123456 --query="CREATE DATABASE IF NOT EXISTS instockdb"
```

### 3. 重建服务
```bash
docker-compose down
docker-compose up -d --force-recreate
```
