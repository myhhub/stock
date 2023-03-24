<font size=8>**InStock股票系统**</font>

InStock股票系统，抓取股票每日关键数据，计算股票各种指标，识别各种K线形态，内置多种选股策略，支持选股验证回测，支持批量时间，运行高效，是量化投资的好帮手。

# 功能介绍

##  一：股票每日数据
抓取A股票每日数据，主要为一些关键数据，同时封装抓取方法，方便扩展系统获取个人关注的数据。基于免费开源的akshare抓取，更多数据获取参见 [akshare官网文档](https://www.akshare.xyz/data/stock/stock.html)。

![](img/00.jpg)

## 二：计算股票指标
计算如下指标：
```
1、交易量delta指标分析

The Volume Delta (Vol ∆) 与前一天交易量的增量。

2、计算n天价差

可以计算，向前n天，和向后n天的价差。

3、n天涨跌百分百计算

可以看到，-n天数据和今天数据的百分比。

4、CR指标

[价格动量指标(CR)](http://wiki.mbalib.com/wiki/CR指标) 跌穿a、b、c、d四条线，再由低点向上爬升160时，为短线获利的一个良机，应适当卖出股票。 CR跌至40以下时，是建仓良机。而CR高于300~400时，应注意适当减仓。

5、最大值，最小值

计算区间最大值 volume max of three days ago, yesterday and two days later stock["volume_-3,2,-1_max"] volume min between 3 days ago and tomorrow stock["volume_-3~1_min"] 实际使用的时候使用 -2~2 可计算出5天的最大，最小值。

6、KDJ指标

[随机指标(KDJ)](http://wiki.mbalib.com/wiki/随机指标) 一般是根据统计学的原理，通过一个特定的周期（常为9日、9周等）内出现过的最高价、 最低价及最后一个计算周期的收盘价及这三者之间的比例关系，来计算最后一个计算周期的未成熟随机值RSV， 然后根据平滑移动平均线的方法来计算K值、D值与J值，并绘成曲线图来研判股票走势。 （3）在使用中，常有J线的指标，即3乘以K值减2乘以D值（3K－2D＝J），其目的是求出K值与D值的最大乖离程度， 以领先KD值找出底部和头部。J大于100时为超买，小于10时为超卖。

7、SMA指标

[简单移动平均线（Simple Moving Average，SMA）](http://wiki.mbalib.com/wiki/Sma) 可以动态输入参数，获得几天的移动平均。

8、MACD指标

[平滑异同移动平均线(Moving Average Convergence Divergence，简称MACD指标)](http://wiki.mbalib.com/wiki/MACD) ，也称移动平均聚散指标 MACD 则可发挥其应有的功能，但当市场呈牛皮盘整格局，股价不上不下时，MACD买卖讯号较不明显。 当用MACD作分析时，亦可运用其他的技术分析指标如短期 K，D图形作为辅助工具，而且也可对买卖讯号作双重的确认。

9、BOLL指标

[布林线指标(Bollinger Bands)](http://wiki.mbalib.com/wiki/BOLL) bolling, including upper band and lower band stock["boll"] stock["boll_ub"] stock["boll_lb"] 1、当布林线开口向上后，只要股价K线始终运行在布林线的中轨上方的时候，说明股价一直处在一个中长期上升轨道之中，这是BOLL指标发出的持股待涨信号，如果TRIX指标也是发出持股信号时，这种信号更加准确。此时，投资者应坚决持股待涨。 2、当布林线开口向下后，只要股价K线始终运行在布林线的中轨下方的时候，说明股价一直处在一个中长期下降轨道之中，这是BOLL指标发出的持币观望信号，如果TRIX指标也是发出持币信号时，这种信号更加准确。此时，投资者应坚决持币观望。

10、RSI指标

[相对强弱指标（Relative Strength Index，简称RSI）](http://wiki.mbalib.com/wiki/RSI) ，也称相对强弱指数、相对力度指数 2）强弱指标保持高于50表示为强势市场，反之低于50表示为弱势市场。 （3）强弱指标多在70与30之间波动。当六日指标上升到达80时，表示股市已有超买现象，如果一旦继续上升，超过90以上时，则表示已到严重超买的警戒区，股价已形成头部，极可能在短期内反转回转。

11、W%R指标

[威廉指数（Williams%Rate）](http://wiki.mbalib.com/wiki/威廉指标) 该指数是利用摆动点来度量市场的超买超卖现象。 10 days WR stock["wr_10"] 6 days WR stock["wr_6"]

12、CCI指标

[顺势指标(CCI)](http://wiki.mbalib.com/wiki/顺势指标) ，其英文全称为“Commodity Channel Index”， 是由美国股市分析家唐纳德·蓝伯特（Donald Lambert）所创造的，是一种重点研判股价偏离度的股市分析工具。 1、当CCI指标从下向上突破﹢100线而进入非常态区间时，表明股价脱离常态而进入异常波动阶段， 中短线应及时买入，如果有比较大的成交量配合，买入信号则更为可靠。 2、当CCI指标从上向下突破﹣100线而进入另一个非常态区间时，表明股价的盘整阶段已经结束， 将进入一个比较长的寻底过程，投资者应以持币观望为主。 CCI, default to 14 days

13、TR、ATR指标

[均幅指标（Average True Ranger,ATR）](http://wiki.mbalib.com/wiki/均幅指标) 是取一定时间周期内的股价波动幅度的移动平均值，主要用于研判买卖时机。TR (true range) stock["tr"] ATR (Average True Range) stock["atr"] 均幅指标无论是从下向上穿越移动平均线，还是从上向下穿越移动平均线时，都是一种研判信号。

14、DMA指标

[DMA指标（Different of Moving Average）又叫平行线差指标](http://wiki.mbalib.com/wiki/DMA) ，是目前股市分析技术指标中的一种中短期指标，它常用于大盘指数和个股的研判。 DMA, difference of 10 and 50 moving average stock[‘dma’]

15、DMI，+DI，-DI，DX，ADX，ADXR指标

[动向指数Directional Movement Index,DMI）](http://wiki.mbalib.com/wiki/DMI) [平均趋向指标（Average Directional Indicator，简称ADX）](http://wiki.mbalib.com/wiki/ADX) [平均方向指数评估（ADXR）](http://wiki.mbalib.com/wiki/平均方向指数评估) 实际是今日ADX与前面某一日的ADX的平均值。ADXR在高位与ADX同步下滑，可以增加对ADX已经调头的尽早确认。 ADXR是ADX的附属产品，只能发出一种辅助和肯定的讯号，并非入市的指标，而只需同时配合动向指标(DMI)的趋势才可作出买卖策略。 在应用时，应以ADX为主，ADXR为辅。

16、TRIX，MATRIX指标

[TRIX指标又叫三重指数平滑移动平均指标（Triple Exponentially Smoothed Average）](http://wiki.mbalib.com/wiki/TRIX)

17、VR，MAVR指标

[成交量比率（Volumn Ratio，VR）（简称VR）](http://wiki.mbalib.com/wiki/成交量比率) ，是一项通过分析股价上升日成交额（或成交量，下同）与股价下降日成交额比值， 从而掌握市场买卖气势的中期技术指标。 VR, default to 26 days stock["vr"] MAVR is the simple moving average of VR stock["vr_6_sma"]
```

![](img/01.jpg)

![](img/02.jpg)

## 三：判断买入卖出的股票

根据指标判定可能买入卖出的股票，具体筛选条件如下：


```
KDJ:
1、超买区：K值在80以上，D值在70以上，J值大于90时为超买。一般情况下，股价有可能下跌。投资者应谨慎行事，局外人不应再追涨，局内人应适时卖出。
2、超卖区：K值在20以下，D值在30以下为超卖区。一般情况下，股价有可能上涨，反弹的可能性增大。局内人不应轻易抛出股票，局外人可寻机入场。
RSI:
1、当六日指标上升到达80时，表示股市已有超买现象，如果一旦继续上升，超过90以上时，则表示已到严重超买的警戒区，股价已形成头部，极可能在短期内反转回转。
2、当六日强弱指标下降至20时，表示股市有超卖现象，如果一旦继续下降至10以下时则表示已到严重超卖区域，股价极可能有止跌回升的机会。
CCI:
1、当CCI＞﹢100时，表明股价已经进入非常态区间——超买区间，股价的异动现象应多加关注。
2、当CCI＜﹣100时，表明股价已经进入另一个非常态区间——超卖区间，投资者可以逢低吸纳股票。
CR:
1、跌穿a、b、c、d四条线，再由低点向上爬升160时，为短线获利的一个良机，应适当卖出股票。
2、CR跌至40以下时，是建仓良机。
WR:
1、当％R线达到20时，市场处于超买状况，走势可能即将见顶。
2、当％R线达到80时，市场处于超卖状况，股价走势随时可能见底。
VR:
1、获利区域160－450根据情况获利了结。
2、低价区域40－70可以买进。
```

![](img/05.jpg)

## 四：K线形态识别

正在开发中............

**K线形态识别返回的结果有三种：**
**-100：出现卖出信号**
**0：没有出现该形态**
**100：出现买入信号**

### 两只乌鸦

> 函数名：CDL2CROWS
> 名称：Two Crows 两只乌鸦
> 简介：三日K线模式，第一天长阳，第二天高开收阴，第三天再次高开继续收阴， 收盘比前一日收盘价低，预示股价下跌。

```
integer = CDL2CROWS(open, high, low, close)
```

### 三只乌鸦

> 函数名：CDL3BLACKCROWS
> 名称：Three Black Crows 三只乌鸦
> 简介：三日K线模式，连续三根阴线，每日收盘价都下跌且接近最低价， 每日开盘价都在上根K线实体内，预示股价下跌。

```
integer = CDL3BLACKCROWS(open, high, low, close)
```

### 三内部上涨和下跌

> 函数名：CDL3INSIDE
> 名称： Three Inside Up/Down 三内部上涨和下跌
> 简介：三日K线模式，母子信号+长K线，以三内部上涨为例，K线为阴阳阳， 第三天收盘价高于第一天开盘价，第二天K线在第一天K线内部，预示着股价上涨。

```
integer = CDL3INSIDE(open, high, low, close)
```

### 三线打击

> 函数名：CDL3LINESTRIKE
> 名称： Three-Line Strike 三线打击
> 简介：四日K线模式，前三根阳线，每日收盘价都比前一日高， 开盘价在前一日实体内，第四日市场高开，收盘价低于第一日开盘价，预示股价下跌。

```
integer = CDL3LINESTRIKE(open, high, low, close)
```

###  三外部上涨和下跌

> 函数名：CDL3OUTSIDE
> 名称：Three Outside Up/Down 三外部上涨和下跌
> 简介：三日K线模式，与三内部上涨和下跌类似，K线为阴阳阳，但第一日与第二日的K线形态相反， 以三外部上涨为例，第一日K线在第二日K线内部，预示着股价上涨。

```
integer = CDL3OUTSIDE(open, high, low, close)
```

### 南方三星

> 函数名：CDL3STARSINSOUTH
> 名称：Three Stars In The South 南方三星
> 简介：三日K线模式，与大敌当前相反，三日K线皆阴，第一日有长下影线， 第二日与第一日类似，K线整体小于第一日，第三日无下影线实体信号， 成交价格都在第一日振幅之内，预示下跌趋势反转，股价上升。

```
integer = CDL3STARSINSOUTH(open, high, low, close)
```

### 三个白兵

> 函数名：CDL3WHITESOLDIERS
> 名称：Three Advancing White Soldiers 三个白兵
> 简介：三日K线模式，三日K线皆阳， 每日收盘价变高且接近最高价，开盘价在前一日实体上半部，预示股价上升。

```
integer = CDL3WHITESOLDIERS(open, high, low, close)
```

### 弃婴

> 函数名：CDLABANDONEDBABY
> 名称：Abandoned Baby 弃婴
> 简介：三日K线模式，第二日价格跳空且收十字星（开盘价与收盘价接近， 最高价最低价相差不大），预示趋势反转，发生在顶部下跌，底部上涨。

```
integer = CDLABANDONEDBABY(open, high, low, close, penetration=0)
```

### 大敌当前

> 函数名：CDLADVANCEBLOCK
> 名称：Advance Block 大敌当前
> 简介：三日K线模式，三日都收阳，每日收盘价都比前一日高， 开盘价都在前一日实体以内，实体变短，上影线变长。

```
integer = CDLADVANCEBLOCK(open, high, low, close)
```

### 捉腰带线

> 函数名：CDLBELTHOLD
> 名称：Belt-hold 捉腰带线
> 简介：两日K线模式，下跌趋势中，第一日阴线， 第二日开盘价为最低价，阳线，收盘价接近最高价，预示价格上涨。

```
integer = CDLBELTHOLD(open, high, low, close)
```

### 脱离

> 函数名：CDLBREAKAWAY
> 名称：Breakaway 脱离
> 简介：五日K线模式，以看涨脱离为例，下跌趋势中，第一日长阴线，第二日跳空阴线，延续趋势开始震荡， 第五日长阳线，收盘价在第一天收盘价与第二天开盘价之间，预示价格上涨。

```
integer = CDLBREAKAWAY(open, high, low, close)
```

### 收盘缺影线

> 函数名：CDLCLOSINGMARUBOZU
> 名称：Closing Marubozu 收盘缺影线
> 简介：一日K线模式，以阳线为例，最低价低于开盘价，收盘价等于最高价， 预示着趋势持续。

```
integer = CDLCLOSINGMARUBOZU(open, high, low, close)
```

### 藏婴吞没

> 函数名：CDLCONCEALBABYSWALL
> 名称： Concealing Baby Swallow 藏婴吞没
> 简介：四日K线模式，下跌趋势中，前两日阴线无影线 ，第二日开盘、收盘价皆低于第二日，第三日倒锤头， 第四日开盘价高于前一日最高价，收盘价低于前一日最低价，预示着底部反转。

```
integer = CDLCONCEALBABYSWALL(open, high, low, close)
```

### 反击线

> 函数名：CDLCOUNTERATTACK
> 名称：Counterattack 反击线
> 简介：二日K线模式，与分离线类似。

```
integer = CDLCOUNTERATTACK(open, high, low, close)
```

### 乌云压顶

> 函数名：CDLDARKCLOUDCOVER
> 名称：Dark Cloud Cover 乌云压顶
> 简介：二日K线模式，第一日长阳，第二日开盘价高于前一日最高价， 收盘价处于前一日实体中部以下，预示着股价下跌。

```
integer = CDLDARKCLOUDCOVER(open, high, low, close, penetration=0)
```

### 十字

> 函数名：CDLDOJI
> 名称：Doji 十字
> 简介：一日K线模式，开盘价与收盘价基本相同。

```
integer = CDLDOJI(open, high, low, close)
```

### 十字星

> 函数名：CDLDOJISTAR
> 名称：Doji Star 十字星
> 简介：一日K线模式，开盘价与收盘价基本相同，上下影线不会很长，预示着当前趋势反转。

```
integer = CDLDOJISTAR(open, high, low, close)
```

### 蜻蜓十字/T形十字

> 函数名：CDLDRAGONFLYDOJI
> 名称：Dragonfly Doji 蜻蜓十字/T形十字
> 简介：一日K线模式，开盘后价格一路走低， 之后收复，收盘价与开盘价相同，预示趋势反转。

```
integer = CDLDRAGONFLYDOJI(open, high, low, close)
```

### 吞噬模式

> 函数名：CDLENGULFING
> 名称：Engulfing Pattern 吞噬模式
> 简介：两日K线模式，分多头吞噬和空头吞噬，以多头吞噬为例，第一日为阴线， 第二日阳线，第一日的开盘价和收盘价在第二日开盘价收盘价之内，但不能完全相同。

```
integer = CDLENGULFING(open, high, low, close)
```

### 十字暮星

> 函数名：CDLEVENINGDOJISTAR
> 名称：Evening Doji Star 十字暮星
> 简介：三日K线模式，基本模式为暮星，第二日收盘价和开盘价相同，预示顶部反转。

```
integer = CDLEVENINGDOJISTAR(open, high, low, close, penetration=0)
```

### 暮星

> 函数名：CDLEVENINGSTAR
> 名称：Evening Star 暮星
> 简介：三日K线模式，与晨星相反，上升趋势中, 第一日阳线，第二日价格振幅较小，第三日阴线，预示顶部反转。

```
integer = CDLEVENINGSTAR(open, high, low, close, penetration=0)
```

### 向上/下跳空并列阳线

> 函数名：CDLGAPSIDESIDEWHITE
> 名称：Up/Down-gap side-by-side white lines 向上/下跳空并列阳线
> 简介：二日K线模式，上升趋势向上跳空，下跌趋势向下跳空, 第一日与第二日有相同开盘价，实体长度差不多，则趋势持续。

```
integer = CDLGAPSIDESIDEWHITE(open, high, low, close)
```

### 墓碑十字/倒T十字

> 函数名：CDLGRAVESTONEDOJI
> 名称：Gravestone Doji 墓碑十字/倒T十字
> 简介：一日K线模式，开盘价与收盘价相同，上影线长，无下影线，预示底部反转。

```
integer = CDLGRAVESTONEDOJI(open, high, low, close)
```

### 锤头

> 函数名：CDLHAMMER
> 名称：Hammer 锤头
> 简介：一日K线模式，实体较短，无上影线， 下影线大于实体长度两倍，处于下跌趋势底部，预示反转。

```
integer = CDLHAMMER(open, high, low, close)
```

### 上吊线

> 函数名：CDLHANGINGMAN
> 名称：Hanging Man 上吊线
> 简介：一日K线模式，形状与锤子类似，处于上升趋势的顶部，预示着趋势反转。

```
integer = CDLHANGINGMAN(open, high, low, close)
```

### 母子线

> 函数名：CDLHARAMI
> 名称：Harami Pattern 母子线
> 简介：二日K线模式，分多头母子与空头母子，两者相反，以多头母子为例，在下跌趋势中，第一日K线长阴， 第二日开盘价收盘价在第一日价格振幅之内，为阳线，预示趋势反转，股价上升。

```
integer = CDLHARAMI(open, high, low, close)
```

### 十字孕线

> 函数名：CDLHARAMICROSS
> 名称：Harami Cross Pattern 十字孕线
> 简介：二日K线模式，与母子县类似，若第二日K线是十字线， 便称为十字孕线，预示着趋势反转。

```
integer = CDLHARAMICROSS(open, high, low, close)
```

### 风高浪大线

> 函数名：CDLHIGHWAVE
> 名称：High-Wave Candle 风高浪大线
> 简介：三日K线模式，具有极长的上/下影线与短的实体，预示着趋势反转。

```
integer = CDLHIGHWAVE(open, high, low, close)
```

### 陷阱

> 函数名：CDLHIKKAKE
> 名称：Hikkake Pattern 陷阱
> 简介：三日K线模式，与母子类似，第二日价格在前一日实体范围内, 第三日收盘价高于前两日，反转失败，趋势继续。

```
integer = CDLHIKKAKE(open, high, low, close)
```

### 修正陷阱

> 函数名：CDLHIKKAKEMOD
> 名称：Modified Hikkake Pattern 修正陷阱
> 简介：三日K线模式，与陷阱类似，上升趋势中，第三日跳空高开； 下跌趋势中，第三日跳空低开，反转失败，趋势继续。

```
integer = CDLHIKKAKEMOD(open, high, low, close)
```

### 家鸽

> 函数名：CDLHOMINGPIGEON
> 名称：Homing Pigeon 家鸽
> 简介：二日K线模式，与母子线类似，不同的的是二日K线颜色相同， 第二日最高价、最低价都在第一日实体之内，预示着趋势反转。

```
integer = CDLHOMINGPIGEON(open, high, low, close)
```

### 三胞胎乌鸦

> 函数名：CDLIDENTICAL3CROWS
> 名称：Identical Three Crows 三胞胎乌鸦
> 简介：三日K线模式，上涨趋势中，三日都为阴线，长度大致相等， 每日开盘价等于前一日收盘价，收盘价接近当日最低价，预示价格下跌。

```
integer = CDLIDENTICAL3CROWS(open, high, low, close)
```

### 颈内线

> 函数名：CDLINNECK
> 名称：In-Neck Pattern 颈内线
> 简介：二日K线模式，下跌趋势中，第一日长阴线， 第二日开盘价较低，收盘价略高于第一日收盘价，阳线，实体较短，预示着下跌继续。

```
integer = CDLINNECK(open, high, low, close)
```

### 倒锤头

> 函数名：CDLINVERTEDHAMMER
> 名称：Inverted Hammer 倒锤头
> 简介：一日K线模式，上影线较长，长度为实体2倍以上， 无下影线，在下跌趋势底部，预示着趋势反转。

```
integer = CDLINVERTEDHAMMER(open, high, low, close)
```

### 反冲形态

> 函数名：CDLKICKING
> 名称：Kicking 反冲形态
> 简介：二日K线模式，与分离线类似，两日K线为秃线，颜色相反，存在跳空缺口。

```
integer = CDLKICKING(open, high, low, close)
```

### 由较长缺影线决定的反冲形态

> 函数名：CDLKICKINGBYLENGTH
> 名称：Kicking - bull/bear determined by the longer marubozu 由较长缺影线决定的反冲形态
> 简介：二日K线模式，与反冲形态类似，较长缺影线决定价格的涨跌。

```
integer = CDLKICKINGBYLENGTH(open, high, low, close)
```

### 梯底

> 函数名：CDLLADDERBOTTOM
> 名称：Ladder Bottom 梯底
> 简介：五日K线模式，下跌趋势中，前三日阴线， 开盘价与收盘价皆低于前一日开盘、收盘价，第四日倒锤头，第五日开盘价高于前一日开盘价， 阳线，收盘价高于前几日价格振幅，预示着底部反转。

```
integer = CDLLADDERBOTTOM(open, high, low, close)
```

### 长脚十字

> 函数名：CDLLONGLEGGEDDOJI
> 名称：Long Legged Doji 长脚十字
> 简介：一日K线模式，开盘价与收盘价相同居当日价格中部，上下影线长， 表达市场不确定性。

```
integer = CDLLONGLEGGEDDOJI(open, high, low, close)
```

### 长蜡烛

> 函数名：CDLLONGLINE
> 名称：Long Line Candle 长蜡烛
> 简介：一日K线模式，K线实体长，无上下影线。

```
integer = CDLLONGLINE(open, high, low, close)
```

### 光头光脚/缺影线

函数名：CDLMARUBOZU
名称：Marubozu 光头光脚/缺影线
简介：一日K线模式，上下两头都没有影线的实体， 阴线预示着熊市持续或者牛市反转，阳线相反。

```
integer = CDLMARUBOZU(open, high, low, close)
```

### 相同低价

> 函数名：CDLMATCHINGLOW
> 名称：Matching Low 相同低价
> 简介：二日K线模式，下跌趋势中，第一日长阴线， 第二日阴线，收盘价与前一日相同，预示底部确认，该价格为支撑位。

```
integer = CDLMATCHINGLOW(open, high, low, close)
```

### 铺垫

> 函数名：CDLMATHOLD
> 名称：Mat Hold 铺垫
> 简介：五日K线模式，上涨趋势中，第一日阳线，第二日跳空高开影线， 第三、四日短实体影线，第五日阳线，收盘价高于前四日，预示趋势持续。

```
integer = CDLMATHOLD(open, high, low, close, penetration=0)
```

### 十字晨星

> 函数名：CDLMORNINGDOJISTAR
> 名称：Morning Doji Star 十字晨星
> 简介：三日K线模式， 基本模式为晨星，第二日K线为十字星，预示底部反转。

```
integer = CDLMORNINGDOJISTAR(open, high, low, close, penetration=0)
```

### 晨星

> 函数名：CDLMORNINGSTAR
> 名称：Morning Star 晨星
> 简介：三日K线模式，下跌趋势，第一日阴线， 第二日价格振幅较小，第三天阳线，预示底部反转。

```
integer = CDLMORNINGSTAR(open, high, low, close, penetration=0)
```

### 颈上线

> 函数名：CDLONNECK
> 名称：On-Neck Pattern 颈上线
> 简介：二日K线模式，下跌趋势中，第一日长阴线，第二日开盘价较低， 收盘价与前一日最低价相同，阳线，实体较短，预示着延续下跌趋势。

```
integer = CDLONNECK(open, high, low, close)
```

### 刺透形态

> 函数名：CDLPIERCING
> 名称：Piercing Pattern 刺透形态
> 简介：两日K线模式，下跌趋势中，第一日阴线，第二日收盘价低于前一日最低价， 收盘价处在第一日实体上部，预示着底部反转。

```
integer = CDLPIERCING(open, high, low, close)
```

### 黄包车夫

> 函数名：CDLRICKSHAWMAN
> 名称：Rickshaw Man 黄包车夫
> 简介：一日K线模式，与长腿十字线类似， 若实体正好处于价格振幅中点，称为黄包车夫。

```
integer = CDLRICKSHAWMAN(open, high, low, close)
```

### 上升/下降三法

> 函数名：CDLRISEFALL3METHODS 
> 名称：Rising/Falling Three Methods 上升/下降三法
> 简介： 五日K线模式，以上升三法为例，上涨趋势中， 第一日长阳线，中间三日价格在第一日范围内小幅震荡， 第五日长阳线，收盘价高于第一日收盘价，预示股价上升。

```
integer = CDLRISEFALL3METHODS(open, high, low, close)
```

### 分离线

> 函数名：CDLSEPARATINGLINES
> 名称：Separating Lines 分离线
> 简介：二日K线模式，上涨趋势中，第一日阴线，第二日阳线， 第二日开盘价与第一日相同且为最低价，预示着趋势继续。

```
integer = CDLSEPARATINGLINES(open, high, low, close)
```

### 射击之星

> 函数名：CDLSHOOTINGSTAR
> 名称：Shooting Star 射击之星
> 简介：一日K线模式，上影线至少为实体长度两倍， 没有下影线，预示着股价下跌

```
integer = CDLSHOOTINGSTAR(open, high, low, close)
```

### 短蜡烛

> 函数名：CDLSHORTLINE
> 名称：Short Line Candle 短蜡烛
> 简介：一日K线模式，实体短，无上下影线

```
integer = CDLSHORTLINE(open, high, low, close)
```

### 纺锤

> 函数名：CDLSPINNINGTOP
> 名称：Spinning Top 纺锤
> 简介：一日K线，实体小。

```
integer = CDLSPINNINGTOP(open, high, low, close)
```

### 停顿形态

> 函数名：CDLSTALLEDPATTERN
> 名称：Stalled Pattern 停顿形态
> 简介：三日K线模式，上涨趋势中，第二日长阳线， 第三日开盘于前一日收盘价附近，短阳线，预示着上涨结束

```
integer = CDLSTALLEDPATTERN(open, high, low, close)
```

### 条形三明治

> 函数名：CDLSTICKSANDWICH
> 名称：Stick Sandwich 条形三明治
> 简介：三日K线模式，第一日长阴线，第二日阳线，开盘价高于前一日收盘价， 第三日开盘价高于前两日最高价，收盘价于第一日收盘价相同。

```
integer = CDLSTICKSANDWICH(open, high, low, close)
```

### 探水竿

> 函数名：CDLTAKURI
> 名称：Takuri (Dragonfly Doji with very long lower shadow) 探水竿
> 简介：一日K线模式，大致与蜻蜓十字相同，下影线长度长。

```
integer = CDLTAKURI(open, high, low, close)
```

### 跳空并列阴阳线

> 函数名：CDLTASUKIGAP
> 名称：Tasuki Gap 跳空并列阴阳线
> 简介：三日K线模式，分上涨和下跌，以上升为例， 前两日阳线，第二日跳空，第三日阴线，收盘价于缺口中，上升趋势持续。

```
integer = CDLTASUKIGAP(open, high, low, close)
```

### 插入

> 函数名：CDLTHRUSTING
> 名称：Thrusting Pattern 插入
> 简介：二日K线模式，与颈上线类似，下跌趋势中，第一日长阴线，第二日开盘价跳空， 收盘价略低于前一日实体中部，与颈上线相比实体较长，预示着趋势持续。

```
integer = CDLTHRUSTING(open, high, low, close)
```

### 三星

> 函数名：CDLTRISTAR
> 名称：Tristar Pattern 三星
> 简介：三日K线模式，由三个十字组成， 第二日十字必须高于或者低于第一日和第三日，预示着反转。

```
integer = CDLTRISTAR(open, high, low, close)
```

### 奇特三河床

> 函数名：CDLUNIQUE3RIVER
> 名称：Unique 3 River 奇特三河床
> 简介：三日K线模式，下跌趋势中，第一日长阴线，第二日为锤头，最低价创新低，第三日开盘价低于第二日收盘价，收阳线， 收盘价不高于第二日收盘价，预示着反转，第二日下影线越长可能性越大。

```
integer = CDLUNIQUE3RIVER(open, high, low, close)
```

### 向上跳空的两只乌鸦

> 函数名：CDLUPSIDEGAP2CROWS
> 名称：Upside Gap Two Crows 向上跳空的两只乌鸦
> 简介：三日K线模式，第一日阳线，第二日跳空以高于第一日最高价开盘， 收阴线，第三日开盘价高于第二日，收阴线，与第一日比仍有缺口。

```
integer = CDLUPSIDEGAP2CROWS(open, high, low, close)
```

### 上升/下降跳空三法

> 函数名：CDLXSIDEGAP3METHODS
> 名称：Upside/Downside Gap Three Methods 上升/下降跳空三法
> 简介：五日K线模式，以上升跳空三法为例，上涨趋势中，第一日长阳线，第二日短阳线，第三日跳空阳线，第四日阴线，开盘价与收盘价于前两日实体内， 第五日长阳线，收盘价高于第一日收盘价，预示股价上升。

```
integer = CDLXSIDEGAP3METHODS(open, high, low, close)
```

## 五：策略选股

内置放量上涨、停机坪、回踩年线、突破平台、放量跌停等多种选股策略，同时封装了策略模板，方便扩展实现自己的策略。


```
1、放量上涨
2、均线多头
3、停机坪
4、回踩年线
5、突破平台
6、无大幅回撤
7、海龟交易法则
8、高而窄的旗形
9、放量跌停
10、低ATR成长
```

![](img/04.jpg)

## 六：选股验证



对指标、策略等选出的股票进行回测，验证策略的成功率，是否可用。


![](img/05.jpg)

![](img/06.jpg)

## 七：支持批量


可以通过时间段、枚举时间、当前时间进行指标计算、策略选股及回测等。同时支持智能识别交易日，可以输入任意日期。

具体执行设置如下：
```
------整体作业，支持批量作业------
当前时间作业 python execute_daily_job.py
单个时间作业 python execute_daily_job.py 2022-03-01
枚举时间作业 python execute_daily_job.py 2022-01-01,2021-02-08,2022-03-12
区间时间作业 python execute_daily_job.py 2022-01-01 2022-03-01

------单功能作业，支持批量作业，回测数据自动填补到当前
基础数据作业 python basic_data_daily_job.py
指标数据作业 python indicators_data_daily_job.py
策略数据作业 python strategy_data_daily_job.py
回测数据 python backtest_data_daily_job.py
```

## 八：存储采用数据库设计

数据存储采用数据库设计，能保存历史数据，以及对数据进行扩展分析、统计、挖掘。系统实现自动创建数据库、数据表，封装了批量更新、插入数据，方便业务扩展。

![](img/07.jpg)

## 九：展示采用web设计

采用web设计，可视化展示结果。对展示进行封装，添加新的业务表单，只需要配置视图字典就可自动出现业务可视化界面，方便业务功能扩展。

## 十：运行高效


采用多线程、单例共享资源有效提高运算效率。1天数据的抓取、计算、策略选股、回测运行时间大概3分钟，计算天数越多效率越高。


## 十一：方便调试

系统运行的重要日志记录在stock_execute_job.log(数据抓取、处理、分析)、stock_web.log(web服务)，方便调试发现问题。

![](img/08.jpg)


# 安装说明

建议windows下安装，方便操作及使用系统，同时安装也非常简单。以下安装及运行以windows为例进行介绍。

1.安装最新的 python

```
（1）在官网 https://www.python.org/downloads/ 下载安装包，一键安装即可，安装切记勾选自动设置环境变量。
（2）配置永久全局国内镜像库（因为有墙，无法正常安装库文件），执行如下dos命令：
python pip config --global set  global.index-url https://mirrors.aliyun.com/pypi/simple/
# 如果你只想为当前用户设置，你也可以去掉下面的"--global"选项
```
2.安装最新的 mysql

```
在官网 https://dev.mysql.com/downloads/mysql/ 下载安装包，一键安装即可。
```
3.安装库文件，库都是目前最新版本

```
dos切换到本系统的根目录，执行下面命令：
python pip install -r requirements.txt

也可以通过下面命令生成自己的requirements.txt
python pip freeze > requirements.txt
```

4.安装 talib，安装见以下：

```
第一种方法. pip 下安装
（1）https://www.ta-lib.org/下载并解压ta-lib-0.4.0-msvc.zip
（2）解压并将ta_lib放在C盘根目录
（3）https://visualstudio.microsoft.com/zh-hans/downloads/下载并安装Visual Studio Community，安装切记勾选Visual C++功能
（4）Build TA-Lib Library # 构建 TA-Lib 库
    ①在开始菜单中搜索并打开[Native Tools Command Prompt](根据操作系统选择32位或64位)
    ②输入 cd C:\ta-lib\c\make\cdr\win32\msvc
    ③构建库，输入 nmake
（5）安装完成。
第二种方法. Anaconda 下安装
（1）打开Anaconda Prompt终端。
（2）在终端输入命令行conda install -c conda-forge ta-lib 。
（3）此处确认是否继续安装？输入y 继续安装，直到完成
（4）安装完成。
```
5.安装 Navicat（可选）

Navicat可以方便管理数据库，以及可以手工对数据进行查看、处理、分析、挖掘。

Navicat是一套可创建多个连接的数据库管理工具，用以方便管理 MySQL、Oracle、PostgreSQL、SQLite、SQL Server、MariaDB 和 MongoDB 等不同类型的数据库

```
（1）在官网 https://www.navicat.com.cn/download/navicat-premium 下载安装包，一键安装即可。

（2）然后下载破解补丁: https://pan.baidu.com/s/18XpTHrm9OiLEl3u6z_uxnw 提取码: 8888 ，破解即可。
```
6.配置数据库

一般可能会修改的信息是”数据库访问密码“。

修改database.py相关信息:

```
db_host = "localhost"  # 数据库服务主机
db_user = "root"  # 数据库访问用户
db_password = "root"  # 数据库访问密码
db_port = 3306  # 数据库服务端口
db_charset = "utf8mb4"  # 数据库字符集
```

# 运行说明

1.执行数据抓取、处理、分析

支持批量作业，具体参见_run_job.bat中的注释说明。

建议将其加入到任务计划中，工作日的每天17：00执行。

```

运行 _run_job.bat
```
2.启动web服务

```
运行 _run_web.bat
```

# 特别声明
本系统参考了pythonstock、sngyai。

股市有风险投资需谨慎，本系统只能用于学习、股票分析，投资盈亏概不负责。
