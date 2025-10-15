#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys
import subprocess
import json
import time
import threading
from abc import ABC
import tornado.web

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.lib.torndb as torndb
import instock.lib.database as mdb
from instock.lib.database_factory import get_database, db_config, DatabaseType
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '

# 全局变量，用于跟踪任务状态和防止频繁点击
_update_tasks = {}
_task_lock = threading.Lock()

class DataUpdateHandler(webBase.BaseHandler, ABC):
    @tornado.web.authenticated
    async def post(self):
        """处理数据更新请求"""
        try:
            data = json.loads(self.request.body)
            update_type = data.get('type', 'full')  # full: 全部数据, basic: 基础数据
            
            # 检查是否已有任务在执行
            with _task_lock:
                if _update_tasks.get('running', False):
                    self.write(json.dumps({
                        'success': False,
                        'message': '已有数据更新任务在执行中，请稍后再试',
                        'task_id': None
                    }))
                    self.finish()
                    return
                
                # 标记任务开始
                _update_tasks['running'] = True
                _update_tasks['start_time'] = time.time()
                _update_tasks['type'] = update_type
                _update_tasks['progress'] = 0
                _update_tasks['message'] = '任务开始执行'
            
            # 异步执行更新任务
            def run_update():
                try:
                    # 根据更新类型选择不同的脚本
                    if update_type == 'basic':
                        script_path = os.path.join(cpath, 'instock', 'job', 'basic_data_daily_job.py')
                        _update_tasks['message'] = '正在更新基础数据...'
                    elif update_type == 'indicators':
                        script_path = os.path.join(cpath, 'instock', 'job', 'indicators_data_daily_job.py')
                        _update_tasks['message'] = '正在计算技术指标...'
                    elif update_type == 'full':
                        script_path = os.path.join(cpath, 'instock', 'job', 'execute_daily_job.py')
                        _update_tasks['message'] = '正在执行完整数据更新...'
                    else:
                        script_path = os.path.join(cpath, 'instock', 'job', 'execute_daily_job.py')
                    
                    # 执行更新脚本
                    result = subprocess.run([
                        sys.executable, script_path
                    ], capture_output=True, text=True, cwd=cpath)
                    
                    with _task_lock:
                        if result.returncode == 0:
                            _update_tasks['progress'] = 100
                            _update_tasks['message'] = f'数据更新完成: {result.stdout[-200:]}'
                            _update_tasks['success'] = True
                        else:
                            _update_tasks['progress'] = 100
                            _update_tasks['message'] = f'更新失败: {result.stderr[-200:]}'
                            _update_tasks['success'] = False
                        
                        # 标记任务完成
                        _update_tasks['running'] = False
                        _update_tasks['end_time'] = time.time()
                        
                except Exception as e:
                    with _task_lock:
                        _update_tasks['progress'] = 100
                        _update_tasks['message'] = f'执行异常: {str(e)}'
                        _update_tasks['success'] = False
                        _update_tasks['running'] = False
                        _update_tasks['end_time'] = time.time()
            
            # 启动后台线程执行更新任务
            thread = threading.Thread(target=run_update)
            thread.daemon = True
            thread.start()
            
            self.write(json.dumps({
                'success': True,
                'message': '数据更新任务已开始执行',
                'task_id': int(time.time())
            }))
            self.finish()
            
        except Exception as e:
            with _task_lock:
                _update_tasks['running'] = False
            
            self.write(json.dumps({
                'success': False,
                'message': f'请求处理失败: {str(e)}',
                'task_id': None
            }))
            self.finish()

class DataUpdateStatusHandler(webBase.BaseHandler, ABC):
    @tornado.web.authenticated
    def get(self):
        """获取数据更新任务状态"""
        try:
            with _task_lock:
                task_info = {
                    'running': _update_tasks.get('running', False),
                    'progress': _update_tasks.get('progress', 0),
                    'message': _update_tasks.get('message', '无任务执行'),
                    'success': _update_tasks.get('success', None),
                    'start_time': _update_tasks.get('start_time', None),
                    'end_time': _update_tasks.get('end_time', None),
                    'type': _update_tasks.get('type', 'full')
                }
            
            self.write(json.dumps({
                'success': True,
                'data': task_info
            }))
            
        except Exception as e:
            self.write(json.dumps({
                'success': False,
                'message': f'获取状态失败: {str(e)}'
            }))

class DataCheckHandler(webBase.BaseHandler, ABC):
    @tornado.web.authenticated
    def get(self):
        """检查数据是否存在"""
        try:
            table_name = self.get_argument('table', '')
            date = self.get_argument('date', '')
            
            if not table_name:
                self.write(json.dumps({
                    'success': False,
                    'message': '缺少表名参数',
                    'has_data': False
                }))
                return
            
            # 检查指定表是否有数据
            sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
            if date:
                sql += f" WHERE `date` = '{date}'"
            
            result = self.db.query(sql)
            count = result[0]['count'] if result else 0
            
            self.write(json.dumps({
                'success': True,
                'has_data': count > 0,
                'count': count
            }))
            
        except Exception as e:
            self.write(json.dumps({
                'success': False,
                'message': f'数据检查失败: {str(e)}',
                'has_data': False
            }))