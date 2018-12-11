# coding:utf8

import time

def f1():
    for i in range(10):
        #time.sleep(1)
        yield i

def f2():
    yield from f1()

for i in f2():
    print(i)

from urllib.parse import urlencode


r = urlencode({"a": 100, "b":300})
print(r)
