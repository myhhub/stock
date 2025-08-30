#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
import json
import logging
import os.path
import datetime
# 首映 bokeh 画图。
from bokeh import events
from bokeh.io import curdoc
from bokeh.transform import factor_cmap
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.palettes import Spectral11
from bokeh.layouts import column, row, layout
from bokeh.models import ColumnDataSource, HoverTool, CheckboxGroup, LabelSet, Button, CustomJS, \
    CDSView, BooleanFilter, TabPanel, Tabs, Div, Styles, CrosshairTool, Span, BoxSelectTool, WheelZoomTool, PanTool, \
    BoxZoomTool, ZoomInTool, ZoomOutTool, RedoTool, ResetTool, SaveTool, UndoTool, Text
import instock.core.tablestructure as tbs
import instock.core.indicator.calculate_indicator as idr
import instock.core.pattern.pattern_recognitions as kpr
import instock.core.kline.indicator_web_dic as iwd

__author__ = 'myh '
__date__ = '2023/4/6 '


def get_plot_kline(code, stock, date, stock_name):
    plot_list = []
    try:
        # 确保所有数值列为float类型，避免Decimal序列化问题
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'p_change', 'turnover', 'preclose']
        for col in numeric_columns:
            if col in stock.columns:
                try:
                    stock[col] = stock[col].astype(float)
                except:
                    pass

        # 计算涨跌额，用于显示
        if 'ups_downs' not in stock.columns and 'close' in stock.columns and 'preclose' in stock.columns:
            stock['ups_downs'] = stock['close'] - stock['preclose']
            
        # 计算涨跌幅百分比显示格式
        if 'quote_change' not in stock.columns and 'p_change' in stock.columns:
            stock['quote_change'] = stock['p_change']
        # 确保原始数据按日期升序排列
        stock = stock.sort_values('date').reset_index(drop=True)

        data = idr.get_indicators(stock, date, threshold=360)
        if data is None:
            return None

        # 确保数据按日期升序排列（K线图需要时间序列正确排序）
        data = data.sort_values('date').reset_index(drop=True)

        # 确保日期列为字符串格式，便于显示，处理各种日期格式
        if 'date' in data.columns:
            def format_date(x):
                if pd.isna(x) or x is None:
                    return ''
                if isinstance(x, (datetime.date, datetime.datetime)):
                    return x.strftime('%Y-%m-%d')
                # 处理时间戳（可能是整数或浮点数）
                if isinstance(x, (int, float)) and x > 0:
                    try:
                        # 尝试作为时间戳解析（秒级）
                        if x > 1e9:  # 秒级时间戳
                            return datetime.fromtimestamp(x).strftime('%Y-%m-%d')
                        # 尝试作为Excel日期序列号
                        elif x > 1:
                            excel_epoch = datetime(1900, 1, 1)
                            return (excel_epoch + pd.Timedelta(days=x-2)).strftime('%Y-%m-%d')
                    except:
                        pass
                # 尝试作为字符串解析
                try:
                    return pd.to_datetime(str(x)).strftime('%Y-%m-%d')
                except:
                    return str(x)
            
            data['date'] = data['date'].apply(format_date)

        threshold = 120
        stock_column = tbs.STOCK_KLINE_PATTERN_DATA['columns']
        data = kpr.get_pattern_recognitions(data, stock_column, threshold=threshold)
        if data is None:
            return None

        cyq_days = 210
        cyq_stock = stock.tail(n=threshold + cyq_days).copy()

        min_price = data['low'].min()*0.98
        max_price = data['high'].max()*1.02
        k_length = len(data.index)
        data['index'] = list(np.arange(k_length))
        data['is_red'] = data.apply(lambda row: "1" if row['close'] > row['open'] else "0", axis=1)

        # 颜色，红盘或绿盘
        c_cmap = factor_cmap("is_red", ["red", "green"], ["1", "0"])
        # K线图数据源
        source = ColumnDataSource(data)
        # 工具条
        tools = pan, box_select, box_zoom, wheel_zoom, zoom_in, zoom_out, undo, redo, reset, save = \
            PanTool(description="平移"), BoxSelectTool(description="方框选取"), BoxZoomTool(description="方框缩放"), \
                WheelZoomTool(description="滚轮缩放"), ZoomInTool(description="放大"), ZoomOutTool(description="缩小"), \
                UndoTool(description="撤销"), RedoTool(description="重做"), ResetTool(description="重置"), \
                SaveTool(description="保存", filename=f"InStock_{code}({date})")

        # K线图
        p_kline = figure(width=1000, height=300, x_range=(0, k_length + 1), y_range=(min_price, max_price), min_border_left=80,
                         tools=tools, toolbar_location='above')
        # 均线
        sam_labels = ("close", "ma10", "ma20", "ma50", "ma200")
        for name, color in zip(sam_labels, Spectral11):
            p_kline.line(x='index', y=name, source=source, legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                         color=color, line_width=1.5, alpha=0.8)
        p_kline.legend.location = "top_left"
        p_kline.legend.click_policy = "hide"

        # 股价柱
        c_segment =p_kline.segment(x0='index', y0='high', x1='index', y1='low', color=c_cmap, source=source)
        p_kline.vbar('index', 0.5, 'open', 'close', fill_color=c_cmap, line_color=c_cmap, source=source,
                     hover_fill_alpha=0.5)

        # 悬停
        tooltips = [('日期', '@date'), ('开盘', '@open'),
                    ('最高', '@high'), ('最低', '@low'),
                    ('收盘', '@close'), ('涨跌幅', '@p_change%'),
                    ('金额', '@amount{¥0}'), ('换手', '@turnover%')]

        hover = HoverTool(tooltips=tooltips, description="悬停", renderers=[c_segment])

        # 十字瞄准线
        crosshair = CrosshairTool(overlay=[Span(dimension="width", line_dash="dashed", line_width=2),
                                           Span(dimension="height", line_dash="dotted", line_width=2)],
                                  description="十字瞄准线")
        # 筹码分布
        div_cyq = Div()
        p_cyq = figure(width=160, height=p_kline.height, y_range=p_kline.y_range, min_border_left=0,
                       toolbar_location=None, y_axis_location="right")
        p_cyq.xgrid.grid_line_color = None
        p_cyq.xaxis.visible = False
        
        # 创建筹码分布的初始数据源
        cyq_line_source = ColumnDataSource(dict(x=[], y=[]))
        cyq_text_source = ColumnDataSource(dict(x=[], y=[], text=[]))
        cyq_down_source = ColumnDataSource(dict(x=[], y1=[]))
        cyq_up_source = ColumnDataSource(dict(x=[], y1=[]))
        
        cyq_avgcost_line = p_cyq.line(x="x", y="y", source=cyq_line_source, color="red", line_width=2, line_dash="dotted")
        cyq_avgcost_text = p_cyq.add_glyph(cyq_text_source, glyph=Text(x="x", y="y", text="text", text_align="center"))
        cyq_down_varea = p_cyq.varea(x="x", y1="y1", y2=0, source=cyq_down_source, fill_alpha=0.3, fill_color="red")
        cyq_up_varea = p_cyq.varea(x="x", y1="y1", y2=0, source=cyq_up_source, fill_alpha=0.3, fill_color="blue")
        json_str_stock = cyq_stock.to_json(orient="records")
        js_array_str_stock = json.dumps(json.loads(json_str_stock), indent=2)
        cqy_callback =  CustomJS.from_file(os.path.join(os.path.dirname(__file__), "cyq.js"), isinit=False, div_cyq=div_cyq, cyq_avgcost_line=cyq_line_source, cyq_avgcost_text=cyq_text_source, cyq_down_varea=cyq_down_source, cyq_up_varea=cyq_up_source, kline_data=js_array_str_stock, k_range=k_length, cyq_days=cyq_days)
        cqy_hover = HoverTool(tooltips=None, callback=cqy_callback, renderers=[c_segment])
        cqy_callback_isinit =  CustomJS.from_file(os.path.join(os.path.dirname(__file__), "cyq.js"), isinit=True, div_cyq=div_cyq, cyq_avgcost_line=cyq_line_source, cyq_avgcost_text=cyq_text_source, cyq_down_varea=cyq_down_source, cyq_up_varea=cyq_up_source, kline_data=js_array_str_stock, k_range=k_length, cyq_days=cyq_days)
        curdoc().on_event(events.DocumentReady, cqy_callback_isinit)

        # K线图添加工具
        p_kline.add_tools(hover, cqy_hover, crosshair)

        # 形态信息
        pattern_is_show = True  # 形态缺省是否显示
        checkboxes_args = {}
        checkboxes_code = """let acts = cb_obj.active;"""
        pattern_labels = []
        i = 0
        for k in stock_column:
            label_cn = stock_column[k]['cn']
            label_mask_u = (data[k] > 0)
            label_data_u = data.loc[label_mask_u].copy()
            isHas = False
            # 处理向上的形态标签
            if len(label_data_u.index) > 0:
                label_data_u.loc[:, 'label_cn'] = label_cn
                label_source_u = ColumnDataSource(label_data_u)
                pattern_label_u = LabelSet(x='index', y='high', text="label_cn",
                                          source=label_source_u, x_offset=7, y_offset=5,
                                          angle=90, angle_units='deg', text_color='red',
                                          text_font_style='bold', text_font_size="9pt",
                                          visible=pattern_is_show)
                p_kline.add_layout(pattern_label_u)
                checkboxes_args[f'lsu{str(i)}'] = pattern_label_u
                checkboxes_code = f"{checkboxes_code}lsu{i}.visible = acts.includes({i});"
                pattern_labels.append(label_cn)
                isHas = True

            # 处理向下的形态标签
            label_mask_d = (data[k] < 0)
            label_data_d = data.loc[label_mask_d].copy()
            if len(label_data_d.index) > 0:
                label_data_d.loc[:, 'label_cn'] = label_cn
                label_source_d = ColumnDataSource(label_data_d)
                pattern_label_d = LabelSet(x='index', y='low', text='label_cn',
                                          source=label_source_d, x_offset=-7, y_offset=-5,
                                          angle=270, angle_units='deg',
                                          text_color='green',
                                          text_font_style='bold', text_font_size="9pt",
                                          visible=pattern_is_show)
                p_kline.add_layout(pattern_label_d)
                checkboxes_args[f'lsd{str(i)}'] = pattern_label_d
                checkboxes_code = f"{checkboxes_code}lsd{i}.visible = acts.includes({i});"
                if not isHas:
                    pattern_labels.append(label_cn)
                    isHas = True
            
            # 只有当有标签时才增加计数器
            if isHas:
                i += 1
        p_kline.xaxis.visible = False
        p_kline.min_border_bottom = 0

        # 交易量柱
        p_volume = figure(width=p_kline.width, height=120, x_range=p_kline.x_range,
                          min_border_left=p_kline.min_border_left, tools=tools, toolbar_location=None)
        vol_labels = ("vol_5", "vol_10")
        for name, color in zip(vol_labels, Spectral11):
            p_volume.line(x=data['index'], y=data[name], legend_label=name, color=color, line_width=1.5, alpha=0.8)
        p_volume.legend.location = "top_left"
        p_volume.legend.click_policy = "hide"
        p_volume.vbar('index', 0.5, 0, 'volume', color=c_cmap, source=source)
        p_volume.add_tools(crosshair)
        
        # 设置日期标签，使用已格式化的日期字符串
        try:
            date_labels = {i: str(date_val) for i, date_val in enumerate(data['date'])}
            p_volume.xaxis.major_label_overrides = date_labels
        except Exception as e:
            logging.exception(f"visualization.get_plot_kline设置日期标签异常：{code}代码{e}")
        # p_volume.xaxis.major_label_orientation = pi / 4

        # 形态复选框
        pattern_checkboxes = CheckboxGroup(labels=pattern_labels,
                                           active=list(range(len(pattern_labels))) if pattern_is_show else [])
        # pattern_checkboxes.inline = True
        pattern_checkboxes.height = p_kline.height + p_volume.height
        if checkboxes_args:
            pattern_checkboxes.js_on_change('active', CustomJS(args=checkboxes_args, code=checkboxes_code))
        ck = column(row(pattern_checkboxes))

        # 按钮
        select_all = Button(label="全选")
        select_none = Button(label='全弃')
        select_all.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes, 'pls': pattern_labels},
                                                        code="pcs.active = Array.from(pls, (x, i) => i);"))
        select_none.js_on_event("button_click", CustomJS(args={'pcs': pattern_checkboxes},
                                                         code="pcs.active = [];"))

        # 指标
        tabs = []
        for conf in iwd.indicators_dic:
            p_indicator = figure(width=p_kline.width, height=150, x_range=p_kline.x_range,
                                 min_border_left=p_kline.min_border_left, tools=tools, toolbar_location=None)
            for name, color in zip(conf["dic"], Spectral11):
                if name == 'macdh' or name == 'ppoh':
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
            p_indicator.add_tools(crosshair)
            p_indicator.xaxis.visible = False
            p_indicator.min_border_bottom = 0
            div_indicator = Div(text=f"""★★★★★指标详细解读：{conf["desc"]}""", width=p_kline.width)
            tabs.append(TabPanel(child=column(p_indicator, row(div_indicator)), title=conf["title"]))
        tabs_indicators = Tabs(tabs=tabs, tabs_location='below', width=p_kline.width, stylesheets=[
            {'.bk-tab': Styles(padding='1px 1.4px', font_size='xx-small'),
             '.bk-tab.bk-active': Styles(background_color='yellow', color='red')}])

        # 关注
        if code.startswith(('1', '5')):
            div_attention = Div()
        else:
            import instock.lib.database as mdb
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            _sql = f"SELECT EXISTS(SELECT 1 FROM `{table_name}` WHERE `code` = '{code}')"
            try:
                rc = mdb.executeSqlCount(_sql)
            except Exception as e:
                rc = 0
            if rc == 0:
                cvalue = "0"
                cname = "关注"
            else:
                cvalue = "1"
                cname = "取关"
            div_attention = Div(
                text=f"""<button id="attentionId" value="{cvalue}" onclick="attention('{code}',this);return false;">{cname}</button>""",
                width=47)

        # 东方财富股票页面
        if code.startswith("6"):
            code_name = f"SH{code}"
        else:
            code_name = f"SZ{code}"
        div_dfcf_hq = Div(
            text=f"""<a href="https://quote.eastmoney.com/{code_name}.html" target="_blank">{code}{stock_name}行情</a>""",
            width=150)
        if code.startswith(('1', '5')):
            div_dfcf_zl = Div()
        else:
            div_dfcf_zl = Div(
                text=f"""<a href="https://emweb.eastmoney.com/PC_HSF10/OperationsRequired/Index?code={code_name}" target="_blank">资料</a>""",
                width=40)
        if code.startswith(('1', '5')):
            div_dfcf_pj = Div()
        else:
            div_dfcf_pj = Div(
                text=f"""<a href="http://page1.tdx.com.cn:7615/site/pcwebcall_static/bxb/bxb.html?code={code}&color=0" target="_blank">扫雷评级</a>""",
                width=80)
        div_dfcf_pr = Div(
            text=f"""<a href="https://www.ljjyy.com/archives/2023/04/100718.html" target="_blank">形态</a>""",
            width=40)

        # 组合图
        layouts = layout(row(
            column(
                row(children=[div_attention, div_dfcf_hq, div_dfcf_zl, div_dfcf_pj, div_dfcf_pr, select_all, select_none],align='end'),
                row(children=[p_kline, p_cyq]),
                row(children=[column(p_volume, tabs_indicators),div_cyq])),
                ck))
        script, div = components(layouts)

        return {"script": script, "div": div}
    except Exception as e:
        logging.exception(f"visualization.get_plot_kline处理异常：{e}")
    return None
