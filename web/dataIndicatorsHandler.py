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
from bokeh.layouts import column, row, layout
from bokeh.models import DatetimeTickFormatter, ColumnDataSource, HoverTool, CheckboxGroup, LabelSet, Button, CustomJS, \
    CDSView, BooleanFilter, TabPanel, Tabs, Div
import libs.stockfetch as stf
import libs.version as version
import libs.tablestructure as tbs
import indicator.stockstats_data as ssd
import kline.pattern_recognitions as kpr
import web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '

# 全部指标数据汇总
indicators_dic = [
    {
        "title": "MACD",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/MACD" rel="nofollow" target="_blank">平滑异同移动平均指标(MACD)</a>
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
        <a href="http://wiki.mbalib.com/wiki/BOLL" rel="nofollow" target="_blank">布林线指标(BOLL)</a>
    """,
        "dic": ("close", "boll_ub", "boll", "boll_lb")
    }, {
        "title": "TRIX",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/TRIX" rel="nofollow" target="_blank">三重指数平滑移动平均指标(TRIX)</a>
    """,
        "dic": ("trix", "trix_20_sma")
    }, {
        "title": "TEMA",
        "desc": """
            <a href="https://www.forextraders.com/forex-education/forex-technical-analysis/triple-exponential-moving-average-the-tema-indicator/" target="_blank">三重指数移动平均指标(TEMA)</a>
        """,
        "dic": ("tema",)
    }, {
        "title": "CR",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/CR%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">价格动量指标(CR)</a>
    """,
        "dic": ("cr", "cr-ma1", "cr-ma2", "cr-ma3")
    }, {
        "title": "RSI",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/RSI" rel="nofollow" target="_blank">相对强弱指标(RSI)</a> 
        """,
        "dic": ("rsi_6", "rsi_12", "rsi", "rsi_24")
    }, {
        "title": "VR",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/%E6%88%90%E4%BA%A4%E9%87%8F%E6%AF%94%E7%8E%87" rel="nofollow" target="_blank">成交量比率(VR)</a>
    """,
        "dic": ("vr", "vr_6_sma")
    }, {
        "title": "ROC",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/ROC" target="_blank">变动率(ROC)</a>
        """,
        "dic": ("roc", "rocma", "rocema")
    }, {
        "title": "DMI",
        "desc": """
        <a href="http://wiki.mbalib.com/wiki/DMI" rel="nofollow" target="_blank">动向指数(DMI)</a>
        <a href="http://wiki.mbalib.com/wiki/ADX" rel="nofollow" target="_blank">平均趋向指标(ADX)</a>
        <a href="http://wiki.mbalib.com/wiki/%E5%B9%B3%E5%9D%87%E6%96%B9%E5%90%91%E6%8C%87%E6%95%B0%E8%AF%84%E4%BC%B0" rel="nofollow" target="_blank">平均方向指数评估(DXR)</a>
    """,
        "dic": ("pdi", "mdi", "dx", "adx", "adxr")
    }, {
        "title": "W&R",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/%E5%A8%81%E5%BB%89%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">威廉指数(W&R)</a>
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
           <a href="http://wiki.mbalib.com/wiki/%E5%9D%87%E5%B9%85%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">均幅指标(ATR)</a>
        """,
        "dic": ("tr", "atr")
    }, {
        "title": "DMA",
        "desc": """
            <a href="http://wiki.mbalib.com/wiki/DMA" rel="nofollow" target="_blank">平行线差指标(DMA)</a> 
        """,
        "dic": ("dma", "dma_10_sma")
    }, {
        "title": "OBV",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/OBV" rel="nofollow" target="_blank">能量潮指标(OBV)</a>
        """,
        "dic": ("obv",)
    }, {
        "title": "SAR",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/SAR%E6%8C%87%E6%A0%87" rel="nofollow" target="_blank">抛物线转向指标(SAR)</a>
        """,
        "dic": ("close", "sar")
    }, {
        "title": "PSY",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/PSY" rel="nofollow" target="_blank">心理线指标(PSY)</a>
        """,
        "dic": ("psy",)
    }, {
        "title": "BRAR",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/BRAR%E6%8C%87%E6%A0%87" target="_blank">人气意愿指标(BRAR)</a>
        """,
        "dic": ("br", "ar")
    }, {
        "title": "EMV",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/%E7%AE%80%E6%98%93%E6%B3%A2%E5%8A%A8%E6%8C%87%E6%A0%87" target="_blank">简易波动指标(EMV)</a>
        """,
        "dic": ("emv", "emva")
    }, {
        "title": "BIAS",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/BIAS%E6%8C%87%E6%A0%87" target="_blank">乖离率指标(BIAS)</a>
        """,
        "dic": ("bias", "bias_12", "bias_24")
    }, {
        "title": "MFI",
        "desc": """
            <a href="https://wiki.mbalib.com/wiki/MFI" target="_blank">资金流量指标(MFI)</a>
        """,
        "dic": ("mfi",)
    }, {
        "title": "VWMA",
        "desc": """
            <a href="https://www.investopedia.com/articles/trading/11/trading-with-vwap-mvwap.asp" target="_blank">成交量加权平均指标(VWMA)</a>
        """,
        "dic": ("vwma",)
    }, {
        "title": "PPO",
        "desc": """
            <a href="https://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:price_oscillators_ppo" target="_blank">价格震荡百分比指标(PPO)</a>
        """,
        "dic": ("ppo", "ppos", "ppoh")
    }, {
        "title": "WT",
        "desc": """
            <a href="https://medium.com/@samuel.mcculloch/lets-take-a-look-at-wavetrend-with-crosses-lazybear-s-indicator-2ece1737f72f" target="_blank">LazyBear's Wave Trend指标(WT)</a>
        """,
        "dic": ("wt1", "wt2")
    }, {
        "title": "SUPERTREND",
        "desc": """
            <a href="https://view.inews.qq.com/a/20221030A05PCH00" target="_blank">超级趋势指标(SUPERTREND)</a>
        """,
        "dic": ("supertrend_ub", "supertrend", "supertrend_lb")
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
        comp_list = []
        try:
            stock = stf.fetch_stock_hist((date, code))
            if stock is None:
                return

            pk = get_plot_kline(stock, date)
            if pk is None:
                return

            comp_list.append(pk)
        except Exception as e:
            logging.debug("{}处理异常：{}".format('dataIndicatorsHandler.GetDataIndicatorsHandler', e))

        self.render("stock_indicators.html", comp_list=comp_list,
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


def get_plot_kline(stock, date):
    plot_list = []
    threshold = 120
    try:
        data = ssd.get_indicators(stock, date, threshold=threshold)
        if data is None:
            return None

        stock_column = tbs.STOCK_KLINE_PATTERN_DATA['columns']
        data = kpr.get_pattern_recognitions(data, stock_column)
        if data is None:
            return None

        length = len(data.index)
        data['index'] = list(np.arange(length))
        # K线
        p_kline = figure(width=1000, height=300, x_range=(0, length + 1), min_border_left=80, toolbar_location=None)
        hover = HoverTool(tooltips=[('日期', '@date'), ('开盘', '@open'),
                                    ('最高', '@high'), ('最低', '@low'),
                                    ('收盘', '@close')])
        source = ColumnDataSource(data)
        # 均线
        sam_labels = ("close", "ma10", "ma20", "ma50", "ma200")
        for name, color in zip(sam_labels, Spectral11):
            p_kline.line(x='index', y=name, source=source, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                         color=color, line_width=1.5, alpha=0.8)
        p_kline.legend.location = "top_left"
        p_kline.legend.click_policy = "hide"

        inc = data['close'] >= data['open']
        dec = data['open'] > data['close']
        inc_source = data.loc[inc]
        dec_source = data.loc[dec]
        # 股价柱
        p_kline.segment(x0='index', y0='high', x1='index', y1='low', color='red', source=inc_source)
        p_kline.segment(x0='index', y0='high', x1='index', y1='low', color='green', source=dec_source)
        p_kline.vbar('index', 0.5, 'open', 'close', fill_color='red', line_color='red', source=inc_source,
                     hover_fill_alpha=0.5)
        p_kline.vbar('index', 0.5, 'open', 'close', fill_color='green', line_color='green', source=dec_source,
                     hover_fill_alpha=0.5)
        # 提示信息
        p_kline.add_tools(hover)
        # 形态信息
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
                p_kline.add_layout(locals()['pattern_labels_u_' + str(i)])
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
                p_kline.add_layout(locals()['pattern_labels_d_' + str(i)])
                args['lsd' + str(i)] = locals()['pattern_labels_d_' + str(i)]
                code += "lsd{}.visible = acts.includes({});".format(i, i)
                if not isHas:
                    pattern_labels.append(v['cn'])
                    isHas = True
            if isHas:
                i += 1
        p_kline.xaxis.visible = False
        p_kline.min_border_bottom = 0

        # 交易量柱
        p_volume = figure(width=p_kline.width, height=120, x_range=p_kline.x_range,
                          min_border_left=p_kline.min_border_left, toolbar_location=None)
        vol_labels = ("vol_5", "vol_10")
        for name, color in zip(vol_labels, Spectral11):
            p_volume.line(x=data['index'], y=data[name], legend_label=name, color=color, line_width=1.5, alpha=0.8)
        p_volume.legend.location = "top_left"
        p_volume.legend.click_policy = "hide"
        p_volume.vbar('index', 0.5, 0, 'volume', color='red', source=inc_source)
        p_volume.vbar('index', 0.5, 0, 'volume', color='green', source=dec_source)
        p_volume.xaxis.major_label_overrides = {i: date for i, date in enumerate(data['date'])}
        # p_volume.xaxis.major_label_orientation = pi / 4

        # 形态复选框
        pattern_checkboxes = CheckboxGroup(labels=pattern_labels, active=list(range(len(pattern_labels))))
        # pattern_checkboxes.inline = True
        pattern_checkboxes.height = p_kline.height + p_volume.height
        if args:
            pattern_checkboxes.js_on_change('active', CustomJS(args=args, code=code))
        ck = column(row(pattern_checkboxes))

        # 按钮
        select_all = Button(label="全选(形态)")
        select_none = Button(label='全不选(形态)')
        select_all.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes, 'pls': pattern_labels},
                                                        code="pcs.active = Array.from(pls, (x, i) => i);"))
        select_none.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes},
                                                         code="pcs.active = [];"))

        # 指标
        tabs = []
        for conf in indicators_dic:
            p_indicator = figure(width=p_kline.width, height=150, x_range=p_kline.x_range,
                                 min_border_left=p_kline.min_border_left, toolbar_location=None)
            for name, color in zip(conf["dic"], Spectral11):
                if name == 'macdh':
                    up = [True if val > 0 else False for val in source.data[name]]
                    down = [True if val < 0 else False for val in source.data[name]]
                    view_upper = CDSView(filter=BooleanFilter(up))
                    view_lower = CDSView(filter=BooleanFilter(down))
                    p_indicator.vbar('index', 0.1, 0, name, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                                     color='green', source=source, view=view_lower)
                    p_indicator.vbar('index', 0.1, name, 0, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                                     color='red', source=source, view=view_upper)
                else:
                    p_indicator.line(x='index', y=name, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                                     color=color, source=source, line_width=1.5, alpha=0.8)
            p_indicator.legend.location = "top_left"
            p_indicator.legend.click_policy = "hide"
            p_indicator.xaxis.visible = False
            p_indicator.min_border_bottom = 0
            div = Div(text=conf["desc"], width=p_kline.width)
            tabs.append(TabPanel(child=column(p_indicator, div), title=conf["title"]))
        tabs_indicators = Tabs(tabs=tabs, tabs_location='below', width=p_kline.width, sizing_mode='fixed')

        # 组合图
        layouts = layout(row(column(row(select_all, select_none), p_kline, p_volume, tabs_indicators), ck))
        script, div = components(layouts)

        return {"script": script, "div": div}
    except Exception as e:
        logging.debug("{}处理异常：{}".format('dataIndicatorsHandler.get_plot_kline', e))
    return None
