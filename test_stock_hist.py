#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入需要测试的函数
from instock.core.crawling.stock_hist_em import code_id_map_em, stock_zh_a_hist

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_code_id_map_em():
    """测试code_id_map_em函数"""
    logger.info("开始测试code_id_map_em函数")
    code_id_dict = code_id_map_em()
    logger.info(f"成功获取股票代码映射，共 {len(code_id_dict)} 条记录")
    logger.info(f"前5个映射：{list(code_id_dict.items())[:5]}")
    
    # 测试获取平安银行的市场ID
    ping_an_code = "000001"
    get_market_id = code_id_dict["get_market_id"]
    market_id = get_market_id(ping_an_code)
    logger.info(f"平安银行({ping_an_code})的市场ID：{market_id}")
    
    # 测试获取贵州茅台的市场ID
    maotai_code = "600519"
    market_id = get_market_id(maotai_code)
    logger.info(f"贵州茅台({maotai_code})的市场ID：{market_id}")
    
    # 测试获取创业板股票的市场ID
    cyb_code = "300001"
    market_id = get_market_id(cyb_code)
    logger.info(f"创业板股票({cyb_code})的市场ID：{market_id}")
    
    # 测试获取科创板股票的市场ID
    kcb_code = "688001"
    market_id = get_market_id(kcb_code)
    logger.info(f"科创板股票({kcb_code})的市场ID：{market_id}")


def test_stock_zh_a_hist():
    """测试stock_zh_a_hist函数"""
    logger.info("开始测试stock_zh_a_hist函数")
    
    # 测试获取平安银行的历史数据
    symbol = "000001"
    start_date = "20251101"
    end_date = "20251130"
    
    try:
        df = stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        logger.info(f"成功获取{symbol}的历史数据")
        logger.info(f"数据形状：{df.shape}")
        
        if not df.empty:
            logger.info(f"数据日期范围：{df.index.min()} 到 {df.index.max()}")
            logger.info(f"数据示例：")
            logger.info(df.head())
        else:
            logger.warning("获取到的历史数据为空")
            
    except Exception as e:
        logger.error(f"获取{symbol}历史数据失败：{e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_code_id_map_em()
    test_stock_zh_a_hist()
