
import os
import sys
import baostock as bs
import pandas as pd
from db_engine import HISTORY_DIR
def main():
    # 检查命令行参数
    if len(sys.argv) != 2:
        print("使用方法: python bao.py <股票代码>")
        print("示例: python bao.py sh.600000")
        sys.exit(1)
    
    # 获取股票代码参数
    stock_code = sys.argv[1]
    
    # 验证股票代码格式
    if not stock_code.startswith(('sh.', 'sz.')):
        print("错误: 股票代码必须以 'sh.' 或 'sz.'")
        sys.exit(1)
    
    # 生成输出文件名：将点号替换为连字符
    output_filename = os.path.join(HISTORY_DIR, stock_code.replace('.', '-') + '.csv')

    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code +" error message->" + lg.error_msg)

    if lg.error_code != '0':
        print("登录失败，程序退出")
        sys.exit(1)
    
    try:
        #### 获取沪深A股历史K线数据 ####
        # 详细指标参数，参见"历史行情指标参数"章节；"分钟线"参数与"日线"参数不同。"分钟线"不包含指数。
        # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
        # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
        rs = bs.query_history_k_data_plus(
            stock_code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date='2000-01-01', 
            end_date='2025-08-25',
            frequency="d", 
            adjustflag="3"
        )
        print('query_history_k_data_plus respond error_code:' + rs.error_code)
        print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
        
        if rs.error_code != '0':
            print("数据查询失败，程序退出")
            return
        
        #### 打印结果集 ####
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        
        #### 结果集输出到csv文件 ####   
        result.to_csv(output_filename, index=False)
        print(f"数据已成功保存到: {output_filename}")
        print(f"共获取 {len(result)} 条记录")
        
    finally:
        #### 登出系统 ####
        bs.logout()

if __name__ == "__main__":
    main()

