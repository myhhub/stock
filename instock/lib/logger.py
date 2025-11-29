#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)


def setup_logger(name, log_file=None, level=logging.INFO):
    """
    设置日志记录器
    :param name: 日志记录器名称
    :param log_file: 日志文件路径（可选）
    :param level: 日志级别（默认：logging.INFO）
    :return: 日志记录器
    """
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加处理器
    if not logger.handlers:
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # 定义日志格式
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        console_handler.setFormatter(formatter)
        
        # 添加控制台处理器到logger
        logger.addHandler(console_handler)
        
        # 如果提供了日志文件路径，则创建文件处理器
        if log_file:
            # 确保日志文件所在的目录存在
            log_dir = os.path.dirname(log_file)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # 创建文件处理器
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            
            # 添加文件处理器到logger
            logger.addHandler(file_handler)
    
    return logger


# 测试日志配置
if __name__ == '__main__':
    # 测试控制台日志
    logger1 = setup_logger('test1')
    logger1.info('这是一条控制台日志')
    
    # 测试文件日志
    log_file = os.path.join(cpath_current, 'log', 'test.log')
    logger2 = setup_logger('test2', log_file)
    logger2.info('这是一条文件日志')
    
    # 测试重复创建日志记录器
    logger3 = setup_logger('test1')
    logger3.info('这是一条重复创建的日志')