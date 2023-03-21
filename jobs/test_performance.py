#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import timeit
import numpy as np
from numpy import mean
import pandas as pd

__author__ = 'myh '
__date__ = '2023/3/10 '


# 测效率
def main():
    print('-----------------')
    print(mean(timeit.repeat('bool(a)', 'a = [5, 6, 7]', repeat=10)))
    print(mean(timeit.repeat('bool(a)', 'a = []', repeat=10)))
    print(mean(timeit.repeat('not a', 'a = [5, 6, 7]', repeat=10)))
    print(mean(timeit.repeat('not a', 'a = []', repeat=10)))
    print(mean(timeit.repeat('len(a) == 0', 'a = [5, 6, 7]', repeat=10)))
    print(mean(timeit.repeat('len(a) == 0', 'a = []', repeat=10)))
    # print('-----------------')
    # print(mean(timeit.repeat('bool(a)', 'a = {5:3, 6:33, 7:66}', repeat=10)))
    # print(mean(timeit.repeat('bool(a)', 'a = {}', repeat=10)))
    # print(mean(timeit.repeat('not a', 'a = {5:3, 6:33, 7:66}', repeat=10)))
    # print(mean(timeit.repeat('not a', 'a = {}', repeat=10)))
    # print(mean(timeit.repeat('len(a) == 0', 'a = {5:3, 6:33, 7:66}', repeat=10)))
    # print(mean(timeit.repeat('len(a) == 0', 'a = {}', repeat=10)))
    # print('-----------------')
    # print(mean(timeit.repeat('bool(a)', 'a = set({5:3, 6:33, 7:66})', repeat=10)))
    # print(mean(timeit.repeat('bool(a)', 'a = set({})', repeat=10)))
    # print(mean(timeit.repeat('not a', 'a = set({5:3, 6:33, 7:66})', repeat=10)))
    # print(mean(timeit.repeat('not a', 'a = set({})', repeat=10)))
    # print(mean(timeit.repeat('len(a) == 0', 'a = set({5:3, 6:33, 7:66})', repeat=10)))
    # print(mean(timeit.repeat('len(a) == 0', 'a = set({})', repeat=10)))
    # print('-----------------')
    # print(timeit.timeit('stupid1()', setup='from __main__ import stupid1', number=10000))
    # print(timeit.timeit('stupid2()', setup='from __main__ import stupid2', number=10000))
    # print(timeit.timeit('stupid3()', setup='from __main__ import stupid3', number=10000))
    # print(timeit.timeit('stupid4()', setup='from __main__ import stupid4', number=10000))


def stupid1():
    a = pd.DataFrame(np.random.randint(low=0, high=10, size=(5, 5)), columns=['a', 'b', 'c', 'd', 'e'])
    len(a) == 0


def stupid2():
    a = pd.DataFrame()
    len(a.index) == 0


def stupid3():
    a = None
    a is None


def stupid4():
    a = pd.DataFrame(np.random.randint(low=0, high=10, size=(5, 5)), columns=['a', 'b', 'c', 'd', 'e'])
    a.empty


# main函数入口
if __name__ == '__main__':
    main()
