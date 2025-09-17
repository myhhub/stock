import tushare as ts
import pandas as pd
import os
import time
import argparse
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockDataFetcher:
    def __init__(self):
        """初始化tushare连接"""
        token = os.environ.get("TUSHARE_TOKEN")
        if not token:
            raise ValueError("TUSHARE_TOKEN环境变量未设置")
        
        ts.set_token(token)
        self.pro = ts.pro_api()
        
    def get_stock_list(self, exchange=None):
        """获取股票列表
        Args:
            exchange: 交易所代码 SSE-上交所 SZSE-深交所 BSE-北交所 None-全部
        """
        try:
            if exchange:
                stock_basic = self.pro.stock_basic(exchange=exchange, list_status='L', 
                                                 fields='ts_code,symbol,name,area,industry,list_date')
            else:
                stock_basic = self.pro.stock_basic(exchange='', list_status='L', 
                                                 fields='ts_code,symbol,name,area,industry,list_date')
            
            logger.info(f"获取到 {len(stock_basic)} 只股票 (交易所: {exchange or '全部'})")
            return stock_basic
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return None
    
    def get_stocks_by_exchange(self, exchange_type):
        """根据交易所类型获取股票列表"""
        exchange_map = {
            'sh': 'SSE',    # 上海证券交易所
            'sz': 'SZSE',   # 深圳证券交易所  
            'bj': 'BSE',    # 北京证券交易所
            'all': None     # 全部交易所
        }
        
        exchange = exchange_map.get(exchange_type.lower())
        if exchange_type.lower() not in exchange_map:
            raise ValueError(f"不支持的交易所类型: {exchange_type}")
        
        return self.get_stock_list(exchange)
    
    def load_stocks_from_csv(self, csv_file):
        """从CSV文件加载股票代码列表"""
        try:
            if not os.path.exists(csv_file):
                raise FileNotFoundError(f"文件不存在: {csv_file}")
            
            df = pd.read_csv(csv_file)
            logger.info(f"从 {csv_file} 读取到 {len(df)} 条记录")
            
            # 检查文件格式
            if 'clean_code' in df.columns and 'market' in df.columns:
                # 基于clean_code和market构造ts_code
                stock_codes = []
                for _, row in df.iterrows():
                    clean_code = str(row['clean_code']).zfill(6)  # 确保6位数字
                    market = row['market']
                    
                    # 根据市场构造完整的ts_code
                    if market == 'SH':
                        ts_code = f"{clean_code}.SH"
                    elif market == 'SZ':
                        ts_code = f"{clean_code}.SZ"
                    elif market == 'BJ':
                        ts_code = f"{clean_code}.BJ"
                    else:
                        logger.warning(f"未知市场类型: {market}")
                        continue
                    
                    stock_codes.append(ts_code)
                
                logger.info(f"成功解析 {len(stock_codes)} 个股票代码")
                return stock_codes
                
            elif 'ts_code' in df.columns:
                # 直接使用ts_code列
                stock_codes = df['ts_code'].tolist()
                logger.info(f"从ts_code列获取 {len(stock_codes)} 个股票代码")
                return stock_codes
                
            else:
                raise ValueError("CSV文件必须包含 'clean_code'+'market' 列或 'ts_code' 列")
                
        except Exception as e:
            logger.error(f"读取CSV文件失败: {e}")
            return None
    
    def parse_stock_codes(self, stock_codes_str):
        """解析股票代码字符串，支持逗号分隔的多个代码"""
        codes = [code.strip().upper() for code in stock_codes_str.split(',')]
        
        # 为不包含交易所后缀的代码自动添加后缀
        processed_codes = []
        for code in codes:
            if '.' not in code:
                # 根据代码前缀判断交易所
                if code.startswith(('000', '001', '002', '003', '300')):
                    code += '.SZ'  # 深圳
                elif code.startswith(('600', '601', '603', '605', '688')):
                    code += '.SH'  # 上海
                elif code.startswith(('430', '831', '832', '833', '834', '835', '836', '837', '838', '839', '870', '871', '872', '873', '920')):
                    code += '.BJ'  # 北京
            processed_codes.append(code)
        
        return processed_codes
    
    def fetch_stock_daily(self, ts_code, start_date='20050101', end_date=None):
        """获取单只股票的日线行情数据"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            # 获取日线行情
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                # 按日期升序排列
                df = df.sort_values('trade_date')
                df.reset_index(drop=True, inplace=True)
                logger.info(f"成功获取 {ts_code} 的 {len(df)} 条数据")
                return df
            else:
                logger.warning(f"{ts_code} 无数据")
                return None
                
        except Exception as e:
            logger.error(f"获取 {ts_code} 数据失败: {e}")
            return None
    
    def save_stock_data(self, df, ts_code, save_dir='data'):
        """保存股票数据到CSV文件"""
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        
        filename = f"{save_dir}/{ts_code}.csv"
        try:
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"数据已保存到 {filename}")
            return True
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            return False
    
    def fetch_stocks_batch(self, stock_codes, start_date='20050101', end_date=None, save_dir='data', delay=0.1):
        """批量获取指定股票的历史数据"""
        total_stocks = len(stock_codes)
        success_count = 0
        failed_codes = []
        
        logger.info(f"开始获取 {total_stocks} 只股票的历史数据")
        
        for index, ts_code in enumerate(stock_codes):
            logger.info(f"正在处理 ({index+1}/{total_stocks}) {ts_code}")
            
            # 获取股票数据
            df = self.fetch_stock_daily(ts_code, start_date, end_date)
            
            if df is not None:
                # 保存数据
                if self.save_stock_data(df, ts_code, save_dir):
                    success_count += 1
                else:
                    failed_codes.append(ts_code)
            else:
                failed_codes.append(ts_code)
            
            # 避免请求过于频繁
            time.sleep(delay)
            
            # 每50只股票输出一次进度
            if (index + 1) % 50 == 0:
                logger.info(f"已处理 {index + 1}/{total_stocks} 只股票，成功 {success_count} 只")
        
        logger.info(f"批量获取完成！总计处理 {total_stocks} 只股票，成功获取 {success_count} 只")
        
        if failed_codes:
            logger.warning(f"失败的股票代码 ({len(failed_codes)}): {failed_codes[:10]}...")  # 只显示前10个
            
        return success_count, failed_codes
    
    def fetch_by_exchange(self, exchange_type, start_date='20050101', end_date=None, save_dir='data', delay=0.1):
        """根据交易所获取股票数据"""
        stock_list = self.get_stocks_by_exchange(exchange_type)
        if stock_list is None or stock_list.empty:
            logger.error(f"未获取到 {exchange_type} 交易所的股票列表")
            return
        
        stock_codes = stock_list['ts_code'].tolist()
        return self.fetch_stocks_batch(stock_codes, start_date, end_date, save_dir, delay)

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='获取股票历史行情数据')
    
    # 模式选择（互斥）
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--single', type=str, help='获取单只股票数据，股票代码如: 000001.SZ')
    mode_group.add_argument('--codes', type=str, help='获取指定股票列表数据，用逗号分隔如: 000001.SZ,600000.SH')
    mode_group.add_argument('--csv-file', type=str, help='从CSV文件读取股票代码列表')
    mode_group.add_argument('--exchange', type=str, choices=['sh', 'sz', 'bj', 'all'], 
                           help='按交易所获取数据: sh(上交所), sz(深交所), bj(北交所), all(全部)')
    
    # 其他参数
    parser.add_argument('--start-date', type=str, default='20050101', 
                       help='开始日期，格式YYYYMMDD (默认: 20050101)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期，格式YYYYMMDD (默认: 今日)')
    parser.add_argument('--save-dir', type=str, default='data',
                       help='数据保存目录 (默认: data)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='请求间隔秒数 (默认: 0.1)')
    
    return parser.parse_args()

def main():
    """主函数"""
    args = parse_arguments()
    
    try:
        fetcher = StockDataFetcher()
        
        if args.single:
            # 获取单只股票数据
            logger.info(f"获取单只股票 {args.single} 的数据")
            df = fetcher.fetch_stock_daily(args.single, args.start_date, args.end_date)
            if df is not None:
                fetcher.save_stock_data(df, args.single, args.save_dir)
        
        elif args.codes:
            # 获取指定股票列表数据
            stock_codes = fetcher.parse_stock_codes(args.codes)
            logger.info(f"获取指定股票列表数据: {stock_codes}")
            fetcher.fetch_stocks_batch(stock_codes, args.start_date, args.end_date, args.save_dir, args.delay)
        
        elif args.csv_file:
            # 从CSV文件读取股票代码
            stock_codes = fetcher.load_stocks_from_csv(args.csv_file)
            if stock_codes:
                logger.info(f"从CSV文件获取 {len(stock_codes)} 个股票代码")
                success_count, failed_codes = fetcher.fetch_stocks_batch(
                    stock_codes, args.start_date, args.end_date, args.save_dir, args.delay
                )
                
                # 保存失败的股票代码
                if failed_codes:
                    failed_file = f"failed_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                    with open(failed_file, 'w') as f:
                        for code in failed_codes:
                            f.write(f"{code}\n")
                    logger.info(f"失败的股票代码已保存到: {failed_file}")
        
        elif args.exchange:
            # 按交易所获取数据
            logger.info(f"获取 {args.exchange} 交易所股票数据")
            success_count, failed_codes = fetcher.fetch_by_exchange(
                args.exchange, args.start_date, args.end_date, args.save_dir, args.delay
            )
            
            # 保存失败的股票代码
            if failed_codes:
                failed_file = f"failed_stocks_{args.exchange}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(failed_file, 'w') as f:
                    for code in failed_codes:
                        f.write(f"{code}\n")
                logger.info(f"失败的股票代码已保存到: {failed_file}")
        
        logger.info("任务完成！")
        
    except Exception as e:
        logger.error(f"执行失败: {e}")

if __name__ == "__main__":
    main()