# ClickHouse vs MySQL 股票数据存储方案对比

## 当前MySQL方案分析

你当前的代码使用MySQL进行存储，采用了**分表策略**：
- 按年份范围分表：`cn_stock_history_2000_2004`, `cn_stock_history_2005_2009` 等
- 按市场分表：`cn_stock_history_sh_2000_2004`, `cn_stock_history_sz_2000_2004` 等
- 每个表都有独立的索引：date索引、code索引、联合索引

**MySQL方案的问题：**
1. **管理复杂**：需要手动创建和维护多个表
2. **查询复杂**：跨年份、跨市场查询需要UNION多个表
3. **存储效率低**：行式存储，压缩率不高
4. **分析性能差**：大数据量聚合查询性能不佳

## ClickHouse最佳实践方案

### 1. 单表设计 + 自动分区

```sql
CREATE TABLE cn_stock_history (
    date Date,
    code LowCardinality(String),        -- 使用LowCardinality优化高基数字符串
    market LowCardinality(String),       -- 市场：sh/sz/bj
    open Decimal(12, 4),
    high Decimal(12, 4),
    low Decimal(12, 4),
    close Decimal(12, 4),
    preclose Decimal(12, 4),
    volume UInt64,
    amount Decimal(20, 2),
    adjustflag UInt8,
    turn Decimal(8, 4),
    tradestatus UInt8,
    p_change Decimal(8, 4),
    isST UInt8
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)              -- 按月自动分区
ORDER BY (date, code)                    -- 主键，自动创建索引
SETTINGS index_granularity = 8192
```

### 2. 为什么不需要分表？

**ClickHouse的优势：**

1. **自动分区管理**
   - `PARTITION BY toYYYYMM(date)` 自动按月创建分区
   - 查询自动路由到相关分区，无需手动指定
   - 自动分区剪枝，只扫描相关分区

2. **列式存储**
   - 只读取查询涉及的列，大幅提升性能
   - 同类型数据聚集，压缩率高（通常80-90%）
   - 向量化执行，SIMD指令优化

3. **智能索引**
   - 主键自动创建稀疏索引
   - 支持跳数索引、布隆过滤器等
   - 自动优化查询路径

4. **水平扩展**
   - 支持分片集群，可线性扩展
   - 副本机制保证数据可靠性

### 3. 存储容量对比

**你的数据规模估算：**
- 5000+ 股票 × 20年 × 250个交易日 ≈ 2500万行数据
- 15个字段，每行约100-150字节

**MySQL存储：**
- 原始数据：~3.75GB
- 索引开销：~1.5GB  
- 总计：~5.25GB

**ClickHouse存储：**
- 压缩后数据：~0.75GB (压缩率80%)
- 索引开销：~0.1GB
- 总计：~0.85GB

**节省80%以上存储空间！**

### 4. 查询性能对比

**MySQL查询（跨表）：**
```sql
-- 查询2020-2024年平安银行数据，需要UNION多个表
SELECT * FROM cn_stock_history_2020_2024 WHERE code = '000001'
UNION ALL
SELECT * FROM cn_stock_history_sh_2020_2024 WHERE code = '000001'
-- 复杂且容易遗漏
```

**ClickHouse查询（单表）：**
```sql
-- 简单直观，自动分区剪枝
SELECT * FROM cn_stock_history 
WHERE code = '000001' AND date >= '2020-01-01'
```

### 5. 分析查询优势

**复杂分析场景：**
```sql
-- 计算各市场月度成交额
SELECT 
    market,
    toYYYYMM(date) as month,
    sum(amount) / 1e8 as total_amount_yi
FROM cn_stock_history 
WHERE date >= '2023-01-01'
GROUP BY market, month
ORDER BY month, market;

-- 找出每日涨停股票
SELECT 
    date,
    market, 
    count() as limit_up_count
FROM cn_stock_history 
WHERE p_change >= 9.8
GROUP BY date, market
ORDER BY date DESC;

-- 计算移动平均
SELECT 
    code,
    date,
    close,
    avg(close) OVER (PARTITION BY code ORDER BY date ROWS 19 PRECEDING) as ma20
FROM cn_stock_history 
WHERE code = '000001'
ORDER BY date;
```

### 6. 运维优势

1. **自动优化**
   - 后台自动合并小分区
   - 自动重新压缩数据
   - 智能缓存管理

2. **备份简单**
   - 单表备份，无需协调多表
   - 支持增量备份

3. **监控简单**
   - 统一的表监控
   - 分区级别的详细指标

### 7. 迁移建议

1. **保留MySQL作为OLTP**
   - 实时交易数据写入
   - 简单查询场景

2. **ClickHouse作为OLAP**
   - 历史数据分析
   - 复杂统计查询
   - 报表生成

3. **数据同步**
   - 定期从MySQL同步到ClickHouse
   - 或者双写策略

## 总结

对于5000+股票20年的历史数据：

| 方面 | MySQL分表 | ClickHouse单表 |
|------|-----------|----------------|
| 存储空间 | ~5.25GB | ~0.85GB |
| 查询复杂度 | 高（需UNION） | 低（单表查询） |
| 分析性能 | 差 | 优秀 |
| 运维复杂度 | 高 | 低 |
| 扩展性 | 有限 | 优秀 |

**建议采用ClickHouse单表方案**，无需分表，利用自动分区和列式存储的优势。
