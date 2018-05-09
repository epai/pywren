import pywren

from pprint import pprint
import time

def timeit(message):
    print(message)
    start = time.time()
    def timer(_):
        print('{}s elapsed'.format(time.time() - start))
        return _
    return timer

# ex = pywren.warm_executor(10)

# foo = lambda x: x + 5
# bar = lambda x: x * 2

def foo(x):
    return 1

import numpy as np

loopcnt = 1000

def big_flops(std_dev):
    running_sum = 0
    for i in range(loopcnt):
        A = np.random.normal(0, std_dev, (96, 96))
        B = np.random.normal(0, std_dev, (96, 96))
        c = np.dot(A, B)
        running_sum += np.sum(c)
    return running_sum

wrenexec = pywren.default_executor()

N = 1000
std_devs = range(N)

begin_ts = time.time()
futures = wrenexec.map(big_flops, std_devs, chunk_size=4)
print(timeit('big flops')(pywren.get_all_results(futures)))

import matplotlib as mpl
mpl.use('TkAgg')

import matplotlib.pyplot as plt

all_points = []
for ft in futures:
    points = [(ts -begin_ts, int(ft.call_id) +1) for ts in ft.timestamps]
    all_points.append(points)

submits, starts, setups, dones = zip(*all_points)

plt.ylim(0, N+1)
plt.scatter(*zip(*submits), marker='o', c='b', s=2)
plt.scatter(*zip(*starts), marker='o', c='g', s=2)
plt.scatter(*zip(*setups), marker='o', c='k', s=2)
plt.scatter(*zip(*dones), marker='o', c='r', s=2)
plt.show()

# futures = ex.map(foo, range(100))
