#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'myh '
__date__ = '2023/4/6 '


# 全部指标数据汇总
indicators_dic = [
    {
        "title": "MACD",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100686.html" rel="nofollow" target="_blank">平滑异同移动平均指标(MACD)</a>
    """,
        "dic": ("macd", "macds", "macdh")
    }, {
        "title": "PPO",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100711.html" target="_blank">价格震荡百分比指标(PPO)</a>
        """,
        "dic": ("ppo", "ppos", "ppoh")
    }, {
        "title": "KDJ",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100687.html" rel="nofollow" target="_blank">随机指标(KDJ)</a>
    """,
        "dic": ("kdjk", "kdjd", "kdjj")
    }, {
        "title": "W&R",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100699.html" rel="nofollow" target="_blank">威廉指数(W&R)</a>
        """,
        "dic": ("wr_6", "wr_10", "wr_14")
    }, {
        "title": "BOLL",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100688.html" rel="nofollow" target="_blank">布林线指标(BOLL)</a>
    """,
        "dic": ("close", "boll_ub", "boll", "boll_lb")
    }, {
        "title": "ENE",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100689.html" target="_blank">轨道线(ENE)</a>
        """,
        "dic": ("close", "ene_ue", "ene", "ene_le")
    }, {
        "title": "TRIX",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100690.html" rel="nofollow" target="_blank">三重指数平滑移动平均指标(TRIX)</a>
    """,
        "dic": ("trix", "trix_20_sma")
    }, {
        "title": "TEMA",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100691.html" target="_blank">三重指数移动平均指标(TEMA)</a>
        """,
        "dic": ("close", "tema")
    }, {
        "title": "CR",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100692.html" rel="nofollow" target="_blank">价格动量指标(CR)</a>
    """,
        "dic": ("cr", "cr-ma1", "cr-ma2", "cr-ma3")
    }, {
        "title": "RSI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100693.html" rel="nofollow" target="_blank">相对强弱指标(RSI)</a> 
        """,
        "dic": ("rsi_6", "rsi_12", "rsi", "rsi_24")
    }, {
        "title": "STOCHRSI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100719.html" rel="nofollow" target="_blank">随机相对强弱指标(STOCHRSI)</a> 
        """,
        "dic": ("stochrsi_k", "stochrsi_d")
    }, {
        "title": "RVI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100716.html" target="_blank">相对离散指数(RVI)</a>
        """,
        "dic": ("rvi", "rvis")
    }, {
        "title": "WT",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100712.html" target="_blank">LazyBear's Wave Trend指标(WT)</a>
        """,
        "dic": ("wt1", "wt2")
    }, {
        "title": "VR",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100694.html" rel="nofollow" target="_blank">成交量比率(VR)</a>
    """,
        "dic": ("vr", "vr_6_sma")
    }, {
        "title": "ROC",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100695.html" target="_blank">变动率(ROC)</a>
        """,
        "dic": ("roc", "rocma", "rocema")
    }, {
        "title": "DMI",
        "desc": """
        <a href="https://www.ljjyy.com/archives/2023/04/100696.html" rel="nofollow" target="_blank">动向指数(DMI)</a>
        <a href="https://www.ljjyy.com/archives/2023/04/100697.html" rel="nofollow" target="_blank">平均趋向指标(ADX)</a>
        <a href="https://www.ljjyy.com/archives/2023/04/100698.html" rel="nofollow" target="_blank">平均方向指数评估(DXR)</a>
    """,
        "dic": ("pdi", "mdi", "dx", "adx", "adxr")
    }, {
        "title": "VHF",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100715.html" target="_blank">十字过滤线(VHF)</a>
        """,
        "dic": ("vhf",)
    }, {
        "title": "CCI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100700.html" rel="nofollow" target="_blank">顺势指标(CCI)</a>
        """,
        "dic": ("cci", "cci_84")
    }, {
        "title": "ATR",
        "desc": """
           <a href="https://www.ljjyy.com/archives/2023/04/100701.html" rel="nofollow" target="_blank">均幅指标(ATR)</a>
        """,
        "dic": ("tr", "atr")
    }, {
        "title": "DMA",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100702.html" rel="nofollow" target="_blank">平行线差指标(DMA)</a> 
        """,
        "dic": ("dma", "dma_10_sma")
    }, {
        "title": "OBV",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100703.html" rel="nofollow" target="_blank">能量潮指标(OBV)</a>
        """,
        "dic": ("obv",)
    }, {
        "title": "SAR",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100704.html" rel="nofollow" target="_blank">抛物线转向指标(SAR)</a>
        """,
        "dic": ("close", "sar")
    }, {
        "title": "PSY",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100705.html" rel="nofollow" target="_blank">心理线指标(PSY)</a>
        """,
        "dic": ("psy", "psyma")
    }, {
        "title": "BRAR",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100706.html" target="_blank">人气意愿指标(BRAR)</a>
        """,
        "dic": ("br", "ar")
    }, {
        "title": "EMV",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100707.html" target="_blank">简易波动指标(EMV)</a>
        """,
        "dic": ("emv", "emva")
    }, {
        "title": "BIAS",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100708.html" target="_blank">乖离率指标(BIAS)</a>
        """,
        "dic": ("bias", "bias_12", "bias_24")
    }, {
        "title": "MFI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100709.html" target="_blank">资金流量指标(MFI)</a>
        """,
        "dic": ("mfi", "mfisma")
    }, {
        "title": "VWMA",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100710.html" target="_blank">成交量加权平均指标(VWMA)</a>
        """,
        "dic": ("close", "vwma", "mvwma")
    }, {
        "title": "SUPERTREND",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100713.html" target="_blank">超级趋势指标(SUPERTREND)</a>
        """,
        "dic": ("close", "supertrend_ub", "supertrend", "supertrend_lb")
    }, {
        "title": "DPO",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100714.html" target="_blank">区间震荡线(DPO)</a>
        """,
        "dic": ("dpo", "madpo")
    }, {
        "title": "FI",
        "desc": """
            <a href="https://www.ljjyy.com/archives/2023/04/100717.html" target="_blank">劲道指数(FI)</a>
        """,
        "dic": ("fi", "force_2", "force_13")
    }
]
