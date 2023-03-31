#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
import numpy as np
from tornado import gen
import logging
import pandas as pd
# 首映 bokeh 画图。
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.palettes import Spectral11
from bokeh.layouts import column, row
from bokeh.models import DatetimeTickFormatter, ColumnDataSource, HoverTool, CheckboxGroup, LabelSet, Button, CustomJS, \
    CDSView, BooleanFilter, TabPanel, Tabs
import libs.stockfetch as stf
import libs.common as common
import libs.tablestructure as tbs
import indicator.stockstats_data as ssd
import kline.pattern_recognitions as kpr
import web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '

# 全部统计数据汇总
stats_dic = [
    {
        "title": "1、交易量delta",
        "desc": "The Volume Delta (Vol ∆) 与前一天交易量的增量。",
        "dic": [("volume", "volume_delta")]
    }, {
        "title": "2、计算n天价差",
        "desc": "可以计算，向前n天，和向后n天的价差。",
        "dic": [("close",), ("close_1_d", "close_2_d", "close_-1_d", "close_-2_d")]
    }, {
        "title": "3、n天涨跌百分百计算",
        "desc": "可以看到，-n天数据和今天数据的百分比。",
        "dic": [("close",), ("close_-1_r", "close_-2_r")]
    }, {
        "title": "4、最大值，最小值",
        "desc": """
                计算区间最大值
            """,
        "dic": [("volume", "volume_-2~2_max", "volume_-2~2_min")]
    }]

# 全部指标数据汇总
indicators_dic = [
    {
        "title": "MACD",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/MACD" rel="nofollow" target="_blank">平滑异同移动平均线(Moving Average Convergence Divergence，简称MACD指标)</a>
    """,
        "dic": ("macd", "macds", "macdh")
    }, {
        "title": "KDJ",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/%E9%9A%8F%E6%9C%BA%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">随机指标(KDJ)</a>
    """,
        "dic": ("kdjk", "kdjd", "kdjj")
    }, {
        "title": "BOLL",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/BOLL" rel="nofollow" target="_blank">布林线指标(Bollinger Bands)</a>
    """,
        "dic": ("close", "boll_ub", "boll", "boll_lb")
    }, {
        "title": "TRIX",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/TRIX" rel="nofollow" target="_blank">TRIX指标又叫三重指数平滑移动平均指标（Triple Exponentially Smoothed Average）</a>
    """,
        "dic": ("trix", "trix_20_sma")
    }, {
        "title": "CR",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/CR%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">价格动量指标(CR)</a>
    """,
        "dic": ("cr", "cr-ma1", "cr-ma2", "cr-ma3")
    }, {
        "title": "RSI",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/RSI" rel="nofollow" target="_blank">相对强弱指标（Relative Strength Index，简称RSI）</a> 
        """,
        "dic": ("rsi_6", "rsi_12", "rsi", "rsi_24")
    }, {
        "title": "VR",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/%E6%88%90%E4%BA%A4%E9%87%8F%E6%AF%94%E7%8E%87" rel="nofollow" target="_blank">成交量比率（Volumn Ratio，VR）（简称VR）</a>
    """,
        "dic": ("vr", "vr_6_sma")
    }, {
        "title": "ROC",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/ROC" target="_blank">ROC指标</a>
        """,
        "dic": ("roc", "rocma", "rocema")
    }, {
        "title": "DMI",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/DMI" rel="nofollow" target="_blank">动向指数Directional Movement Index,DMI）</a>
        <a href="http://wiki.mbalib.com/wiki/ADX" rel="nofollow" target="_blank">平均趋向指标（Average Directional Indicator，简称ADX）</a>
        <a href="http://wiki.mbalib.com/wiki/%E5%B9%B3%E5%9D%87%E6%96%B9%E5%90%91%E6%8C%87%E6%95%B0%E8%AF%84%E4%BC%B0" rel="nofollow" target="_blank">平均方向指数评估（ADXR）</a>
    """,
        "dic": ("pdi", "mdi", "dx", "adx", "adxr")
    }, {
        "title": "W&R",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/%E5%A8%81%E5%BB%89%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">威廉指数（Williams%Rate）</a>
        """,
        "dic": ("wr_6", "wr_10", "wr_14")
    }, {
        "title": "CCI",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/%E9%A1%BA%E5%8A%BF%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">顺势指标(CCI)</a>
        """,
        "dic": ("cci", "cci_84")
    }, {
        "title": "ATR",
        "desc": """
           <a href="http://wiki.mbalib.com/wiki/%E5%9D%87%E5%B9%85%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">均幅指标（Average True Ranger,ATR）</a>
        """,
        "dic": ("tr", "atr")
    }, {
        "title": "DMA",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/DMA" rel="nofollow" target="_blank">DMA指标（Different of Moving Average）又叫平行线差指标</a> 
        """,
        "dic": ("dma", "dma_10_sma")
    }, {
        "title": "OBV",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/OBV" rel="nofollow" target="_blank">OBV指标</a>
        """,
        "dic": ("obv",)
    }, {
        "title": "SAR",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/SAR%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">SAR指标</a>
        """,
        "dic": ("close", "sar")
    }, {
        "title": "PSY",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/PSY" rel="nofollow" target="_blank">PSY指标</a>
        """,
        "dic": ("psy",)
    }, {
        "title": "BRAR",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/BRAR%E6%8C%87%E6%A0%87" target="_blank">BRAR指标</a>
        """,
        "dic": ("br", "ar")
    }, {
        "title": "EMV",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/%E7%AE%80%E6%98%93%E6%B3%A2%E5%8A%A8%E6%8C%87%E6%A0%87" target="_blank">EMV指标</a>
        """,
        "dic": ("emv", "emva")
    }, {
        "title": "BIAS",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/BIAS%E6%8C%87%E6%A0%87" target="_blank">BIAS指标</a>
        """,
        "dic": ("bias", "bias_12", "bias_24")
    }
]


# 获得页面数据。
class GetDataIndicatorsHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        code = self.get_argument("code", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        # self.uri_ = ("self.request.url:", self.request.uri)
        # print self.uri_
        threshold = 120
        comp_list = []
        try:
            stock = stf.fetch_stock_hist((date, code))
            if stock is None:
                return

            data = ssd.get_indicators(stock, date, threshold=threshold)
            if data is None:
                return
            data['index'] = list(np.arange(len(data)))

            comp_list.append(add_indicators(data))

            r_k = add_kline(data, date, threshold)
            if r_k is not None:
                comp_list.insert(0, r_k)

        except Exception as e:
            logging.debug("{}处理异常：{}".format('dataIndicatorsHandler.GetDataIndicatorsHandler', e))

        self.render("stock_indicators.html", comp_list=comp_list,
                    stockVersion=common.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# 批量添加数据。
def add_indicators(data):
    length = len(data)
    tabs = []
    for conf in indicators_dic:
        p = figure(width=1000, height=180, x_range=(0, length + 1), toolbar_location=None)
        for name, color in zip(conf["dic"], Spectral11):
            if name == 'macdh':
                up = [True if val > 0 else False for val in data[name]]
                down = [True if val < 0 else False for val in data[name]]
                view_upper = CDSView(filter=BooleanFilter(up))
                view_lower = CDSView(filter=BooleanFilter(down))
                p.vbar('index', 0.1, 0, name, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                       color='green', source=data, view=view_lower)
                p.vbar('index', 0.1, name, 0, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                       color='red', source=data, view=view_upper)
            else:
                p.line(x=data['index'], y=data[name], legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                       color=color, line_width=1.5, alpha=0.8)
        p.legend.location = "top_left"
        p.legend.click_policy = "hide"
        p.xaxis.visible = False
        p.min_border_bottom = 0

        tabs.append(TabPanel(child=p, title=conf["title"]))
        layouts = column(row(Tabs(tabs=tabs, tabs_location='below')))

    script, div = components(layouts)
    return {
        "script": script,
        "div": div
    }


def add_kline(data, date, threshold):
    try:
        length = len(data)
        p = figure(width=1000, height=320, x_range=(0, length + 1), toolbar_location=None)
        hover = HoverTool(tooltips=[('日期', '@date'), ('开盘', '@open'),
                                    ('最高', '@high'), ('最低', '@low'),
                                    ('收盘', '@close')])
        # 均线图
        average_labels = ("close", "close_10_sma", "close_20_sma", "close_50_sma", "close_200_sma")
        for name, color in zip(average_labels, Spectral11):
            p.line(x=data['index'], y=data[name], legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA), color=color, line_width=1.5, alpha=0.8)
        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        stock_column = tbs.STOCK_KLINE_PATTERN_DATA['columns']
        data = kpr.get_pattern_recognitions(data, stock_column, threshold=threshold)
        if data is None:
            return None

        inc = data['close'] >= data['open']
        dec = data['open'] > data['close']

        inc_source = data.loc[inc]
        dec_source = data.loc[dec]

        p.segment(x0='index', y0='high', x1='index', y1='low', color='red', source=inc_source)
        p.segment(x0='index', y0='high', x1='index', y1='low', color='green', source=dec_source)
        p.vbar('index', 0.5, 'open', 'close', fill_color='red', line_color='red', source=inc_source,
               hover_fill_alpha=0.5)
        p.vbar('index', 0.5, 'open', 'close', fill_color='green', line_color='green', source=dec_source,
               hover_fill_alpha=0.5)
        # 提示
        p.add_tools(hover)
        # 注释
        args = {}
        code = """var acts = cb_obj.active;"""
        pattern_labels = []
        i = 0
        for k, v in stock_column.items():
            label_mask_u = (data[k] > 0)
            label_data_u = data.loc[label_mask_u].copy()
            isHas = False
            if len(label_data_u.index) > 0:
                label_data_u.loc[:, 'label_cn'] = v['cn']
                label_source_u = ColumnDataSource(label_data_u)
                locals()['pattern_labels_u_' + str(i)] = LabelSet(x='index', y='high', text="label_cn",
                                                                  source=label_source_u, x_offset=7, y_offset=5,
                                                                  angle=90, angle_units='deg', text_color='red',
                                                                  text_font_style='bold', text_font_size="9pt")
                p.add_layout(locals()['pattern_labels_u_' + str(i)])
                args['lsu' + str(i)] = locals()['pattern_labels_u_' + str(i)]
                code += "lsu{}.visible = acts.includes({});".format(i, i)
                pattern_labels.append(v['cn'])
                isHas = True

            label_mask_d = (data[k] < 0)
            label_data_d = data.loc[label_mask_d].copy()
            if len(label_data_d.index) > 0:
                label_data_d.loc[:, 'label_cn'] = v['cn']
                label_source_d = ColumnDataSource(label_data_d)
                locals()['pattern_labels_d_' + str(i)] = LabelSet(x='index', y='low', text='label_cn',
                                                                  source=label_source_d, x_offset=-7, y_offset=-5,
                                                                  angle=270, angle_units='deg',
                                                                  text_color='green',
                                                                  text_font_style='bold', text_font_size="9pt")
                p.add_layout(locals()['pattern_labels_d_' + str(i)])
                args['lsd' + str(i)] = locals()['pattern_labels_d_' + str(i)]
                code += "lsd{}.visible = acts.includes({});".format(i, i)
                if not isHas:
                    pattern_labels.append(v['cn'])
                    isHas = True
            if isHas:
                i += 1
        p.xaxis.visible = False
        p.min_border_bottom = 0

        p1 = figure(width=p.width, height=150, x_range=p.x_range, toolbar_location=None)
        p1.vbar('index', 0.5, 0, 'volume', color='red', source=inc_source)
        p1.vbar('index', 0.5, 0, 'volume', color='green', source=dec_source)
        p1.xaxis.major_label_overrides = {i: date for i, date in enumerate(data.index.values)}
        # p1.xaxis.major_label_orientation = pi / 4

        pattern_checkboxes = CheckboxGroup(labels=pattern_labels, active=list(range(len(pattern_labels))))
        # pattern_selection.inline = True
        pattern_checkboxes.height = p.height + p1.height
        if args:
            pattern_checkboxes.js_on_change('active', CustomJS(args=args, code=code))
        ck = column(row(pattern_checkboxes))

        select_all = Button(label="全选(形态)")
        select_none = Button(label='全不选(形态)')
        select_all.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes, 'pls': pattern_labels},
                                                        code="pcs.active = Array.from(pls, (x, i) => i);"))
        select_none.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes},
                                                         code="pcs.active = [];"))

        layouts = row(column(row(select_all, select_none), p, p1), ck)

        script, div = components(layouts)
        return {
            "script": script,
            "div": div
        }
    except Exception as e:
        logging.debug("{}处理异常：{}".format('dataIndicatorsHandler.add_kline', e))
