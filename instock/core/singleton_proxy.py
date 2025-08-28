#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import sys
import random
from instock.lib.singleton_type import singleton_type

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
proxy_filename = os.path.join(cpath_current, 'config', 'proxy.txt')

__author__ = 'myh '
__date__ = '2025/1/6 '


# 读取代理
class proxys(metaclass=singleton_type):
    def __init__(self):
        try:
            with open(proxy_filename, "r") as file:
                self.data = list(set(line.strip() for line in file.readlines() if line.strip()))
        except Exception:
           pass

    def get_data(self):
        return self.data

    def get_proxies(self):
        if self.data is None or len(self.data)==0:
            return None

        proxy = random.choice(self.data)
        return {"http": proxy, "https": proxy}

"""
    def get_proxies(self):
        if self.data is None:
            return None

        while len(self.data) > 0:
            proxy = random.choice(self.data)
            if https_validator(proxy):
                return {"http": proxy, "https": proxy}
            self.data.remove(proxy)

        return None


from requests import head
def https_validator(proxy):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               'Accept-Language': 'zh-CN,zh;q=0.8'}
    proxies = {"http": f"{proxy}", "https": f"{proxy}"}
    try:
        r = head("https://data.eastmoney.com", headers=headers, proxies=proxies, timeout=3, verify=False)
        return True if r.status_code == 200 else False
    except Exception as e:
        return False
"""