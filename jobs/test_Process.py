import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


# import nb_log

def f(x):
    pass
    if x % 1000 == 0:
        print(x)


if __name__ == '__main__':
    # pool = ProcessPoolExecutor(10)  # 这个分别测试进程池和线程池运行10万次无io 无cpu函数的耗时
    pool = ThreadPoolExecutor(10)
    t1 = time.time()
    for i in range(100000):
        pool.submit(f, i)
    pool.shutdown()
    print(time.time() - t1)
