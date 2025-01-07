#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'myh '
__date__ = '2025/1/6 '


#  * 筹码分布算法
#  * @param {Array.<Array.<string>>} kdata K图数据 [date,open,close,high,low,volume,amount,amplitude,turnover]
#  * @param {number} [accuracyFactor=500] 精度因子
#  * @param {number} [range] 计算范围
#  * @param {number} [cyq_days] 计算交易天数
class CYQCalculator:
    def __init__(self, kdata, accuracy_factor=150, crange=120, cyq_days=210):
        # K图数据
        self.klinedata = kdata
        # 精度因子(纵轴刻度数)
        self.fator = accuracy_factor
        # 计算K线条数
        self.range = crange
        # 计算筹码分布的交易天数
        self.tradingdays = cyq_days

    # *计算分布及相关指标
    # * @ param {number} index 当前选中的K线的索引
    # * @ return {{x: Array. < number >, y: Array. < number >}}
    def calc(self, index):
        maxprice = 0
        minprice = 1000000
        factor = self.fator
        end = index - self.range + 1
        start = end - self.tradingdays

        if end == 0:
            kdata = self.klinedata.tail(self.tradingdays)
        else:
            kdata = self.klinedata[start:end]

        for _high, _low in zip(kdata['high'].values, kdata['low'].values):
            maxprice = max(maxprice, _high)
            minprice = min(minprice, _low)

        #  精度不小于0.01产品逻辑
        accuracy = max(0.01, (maxprice - minprice) / (factor - 1))
        currentprice = kdata.iloc[-1]['close']
        boundary = -1

        # *值域 @ type  {Array. < number >}
        yrange = []

        for i in range(factor):
            _price = float(f"{minprice + accuracy * i:.2f}")
            yrange.append(_price)
            if boundary == -1 and _price >= currentprice:
                boundary = i

        # *横轴数据
        xdata = [0] * factor

        for open_price, close, high, low, turnover in zip(kdata['open'].values, kdata['close'].values, kdata['high'].values, kdata['low'].values, kdata['turnover'].values):
            avg = (open_price + close + high + low) / 4
            turnover_rate = min(1, turnover / 100)

            H = int((high - minprice) / accuracy)
            L = int((low - minprice) / accuracy + 0.99)
            #  G点坐标, 一字板时, X为进度因子
            GPoint = [factor - 1 if high == low else 2 / (high - low),
                      int((avg - minprice) / accuracy)]

            for n in range(len(xdata)):
                xdata[n] *= (1 - turnover_rate)

            if high == low:
                #  一字板时，画矩形面积是三角形的2倍
                xdata[GPoint[1]] += GPoint[0] * turnover_rate / 2
            else:
                for j in range(L, H + 1):
                    curprice = minprice + accuracy * j
                    if curprice <= avg:
                        #  上半三角叠加分布分布
                        if abs(avg - low) < 1e-8:
                            xdata[j] += GPoint[0] * turnover_rate
                        else:
                            xdata[j] += (curprice - low) / (avg - low) * GPoint[0] * turnover_rate
                    else:
                        #  下半三角叠加分布分布
                        if abs(high - avg) < 1e-8:
                            xdata[j] += GPoint[0] * turnover_rate
                        else:
                            xdata[j] += (high - curprice) / (high - avg) * GPoint[0] * turnover_rate

        total_chips = sum(float(f"{x:.12g}") for x in xdata)

        # *获取指定筹码处的成本
        # * @ param {number} chip 堆叠筹码
        def get_cost_by_chip(chip):
            result = 0
            sum_chips = 0
            for i in range(factor):
                x = float(f"{xdata[i]:.12g}")
                if sum_chips + x > chip:
                    result = minprice + i * accuracy
                    break
                sum_chips += x
            return result

        # *筹码分布数据
        class CYQData:
            def __init__(self):
                # 筹码堆叠
                self.x = None
                # 价格分布
                self.y = None
                # 获利比例
                self.benefit_part = None
                # 平均成本
                self.avg_cost = None
                # 百分比筹码
                self.percent_chips = None
                # 筹码堆叠亏盈分界下标
                self.b = None
                # 交易日期
                self.d = None
                # 交易天数
                self.t = None

            # *计算指定百分比的筹码
            # * @ param {number} percent 百分比大于0，小于1
            @staticmethod
            def compute_percent_chips(percent):
                if percent > 1 or percent < 0:
                    raise ValueError('argument "percent" out of range')
                ps = [(1 - percent) / 2, (1 + percent) / 2]
                pr = [get_cost_by_chip(total_chips * ps[0]),
                      get_cost_by_chip(total_chips * ps[1])]
                return {
                    'priceRange': [f"{pr[0]:.2f}", f"{pr[1]:.2f}"],
                    'concentration': 0 if pr[0] + pr[1] == 0 else (pr[1] - pr[0]) / (pr[0] + pr[1])
                }

            # *获取指定价格的获利比例
            # * @ param {number} price 价格
            @staticmethod
            def get_benefit_part(price):
                below = 0
                for i in range(factor):
                    x = float(f"{xdata[i]:.12g}")
                    if price >= minprice + i * accuracy:
                        below += x
                return 0 if total_chips == 0 else below / total_chips

        result = CYQData()
        result.x = xdata
        result.y = yrange
        result.b = boundary + 1
        result.d = kdata.iloc[-1]['date']
        result.t = self.tradingdays
        result.benefit_part = result.get_benefit_part(currentprice)
        result.avg_cost = f"{get_cost_by_chip(total_chips * 0.5):.2f}"
        result.percent_chips = {
            '90': result.compute_percent_chips(0.9),
            '70': result.compute_percent_chips(0.7)
        }

        return result

# if __name__ == "__main__":
#     # import instock.core.kline.cyq as cyq
#     # c = cyq.CYQCalculator(cyq_stock)
#     # r = c.calc(119)