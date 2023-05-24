#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
import tornado.web
import instock.core.singleton_stock_web_module_data as sswmd

__author__ = 'myh '
__date__ = '2023/3/10 '


# 基础handler，主要负责检查mysql的数据库链接。
class BaseHandler(tornado.web.RequestHandler, ABC):
    @property
    def db(self):
        try:
            # check every time。
            self.application.db.query("SELECT 1 ")
        except Exception as e:
            print(e)
            self.application.db.reconnect()
        return self.application.db


class LeftMenu:
    def __init__(self, url):
        self.leftMenuList = sswmd.stock_web_module_data().get_data_list()
        self.current_url = url


# 获得左菜单。
def GetLeftMenu(url):
    return LeftMenu(url)
