import pandas as pd
import subprocess
import os
import argparse
import sys
from typing import Optional
from db_engine import CODE_MAP_CSV

def normalize_stock_code(market, code):
    """标准化股票代码格式为9位 (market.code)"""
    # 确保市场代码是小写
    market = market.strip().lower()
    
    # 处理代码部分
    code_str = str(code).strip()
    
    # 如果是数字，补零到6位
    if code_str.isdigit():
        code_str = code_str.zfill(6)
    
    return f"{market}.{code_str}"

def parse_args() -> Optional[int]:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='分布式处理code_map.csv')
    parser.add_argument(
        '--shard',
        type=int,
        choices=[0, 1, 2],
        help='分片参数: 0, 1, 2 分别对应索引%3的余数'
    )

    args = parser.parse_args()
    return args.shard

def main():
    shard = parse_args()
    # 读取数据
    df = pd.read_csv(CODE_MAP_CSV)

    # 只处理flag为False的数据
    pending_df = df[(df['flag'] == False) & (df['market'].isin(['sh', 'sz', 'SH', 'SZ']))].copy()

    if shard is not None:
        # 应用分片过滤
        pending_df = pending_df[pending_df.index % 3 == shard]
        print(f"使用分片参数: {shard} (索引%3={shard})")

    if pending_df.empty:
        print("没有待处理的任务")
        return

    total = len(pending_df)
    total_all = len(df[df['flag'] == False])

    if shard is not None:
        print(f"分片任务: {total} 条 (总待处理: {total_all} 条)")
    else:
        print(f"待处理任务: {total} 条")

    # 处理每个任务
    success_count = 0
    for index, (_, row) in enumerate(pending_df.iterrows(), 1):
        code = row['code']
        market = str(row['market']).strip().lower()
        
        # 标准化股票代码格式
        normalized_code = normalize_stock_code(market, code)
        
        cmd = f"python bao.py {normalized_code}"
        percentage = (index / total) * 100
        print(f"[{index:>{len(str(total))}}/{total}] ({percentage:5.1f}%) {cmd}")
        
        # 执行命令
        success = subprocess.run(cmd, shell=True).returncode == 0

        if success:
            # 更新flag为True
            df = pd.read_csv(CODE_MAP_CSV)
            mask = (df['code'] == code) & (df['market'].str.lower() == market.lower())
            df.loc[mask, 'flag'] = True
            df.to_csv(CODE_MAP_CSV, index=False)
            success_count += 1
            print(f"    ✓ {normalized_code} 成功并更新flag")
        else:
            print(f"    ✗ {normalized_code} 执行失败")

    # 显示总结
    print(f"\n任务完成: {success_count}/{total} ({(success_count / total) * 100:.1f}%)")

    # 显示剩余任务
    remaining = len(df[df['flag'] == False])
    print(f"剩余待处理任务: {remaining} 条")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
