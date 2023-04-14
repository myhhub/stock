#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import multiprocessing as mp
from threading import Thread

__author__ = 'myh '
__date__ = '2023/4/10 '


class ProcessWrapper(object):
    def __init__(self, strategy):
        self.__strategy = strategy
        # 时钟队列
        self.__clock_queue = mp.Queue(10000)
        # 包装进程
        self.__proc = mp.Process(target=self._process)
        self.__proc.start()

    def stop(self):
        self.__clock_queue.put(0)
        self.__proc.join()

    def on_clock(self, event):
        self.__clock_queue.put(event)

    def _process_clock(self):
        while True:

            try:
                event = self.__clock_queue.get(block=True)
                # 退出
                if event == 0:
                    break
                self.__strategy.clock(event)
            except:
                pass

    def _process(self):
        clock_thread = Thread(target=self._process_clock, name="ProcessWrapper._process_clock")
        clock_thread.start()

        clock_thread.join()
