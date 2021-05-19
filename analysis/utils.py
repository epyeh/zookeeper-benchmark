import numpy as np
import matplotlib.pyplot as plt
import os
import re

def get_config(path = './benchmark.conf'):
    config = {}
    with open(path) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if len(line) == 0 or line[0] == '#':
                continue
            print(line.strip().split('='))
            k, v = line.strip().split('=')
            config[k] = v
    return config


def get_latency_data(op):
    data = {}
    files = os.listdir('./')
    for fn in files:
        match = re.search('-{}_timings.dat'.format(op), fn)
        if match:
            with open(fn) as f:
                lines = f.readlines()
                ts = [[float(l.strip().split(' ')[0]), float(l.strip().split(' ')[1])] for l in lines]
                id = int(fn[:match.start()])
                data[id] = ts
    return data


def avg_latency_per_client(data, filter=False):
    avg = {}
    for k, v in data.items():
        a = np.array(v)
        d = a[:, 1] - a[:, 0]
        if filter:
            mean = np.mean(d)
            std = np.std(d)
            mask = np.logical_and(d > mean-3*std, d < mean+3*std)
            # print(mask)
            d = d[mask]
        avg[k] = np.mean(d)
    return avg


def avg_latency(data):
    avg = 0
    cnt = 0
    for _, v in data.items():
        avg += v
        cnt += 1
    return avg / cnt


def get_rate_data(op):
    ts = None
    files = os.listdir('./')
    for fn in files:
        match = re.search('{}.dat'.format(op), fn)
        if match:
            with open(fn) as f:
                lines = f.readlines()
                ts = [(float(l.strip().split(' ')[0]), float(l.strip().split(' ')[1])) for l in lines] # (current time, rate)
    return ts


def avg_rate(data, filter=-1):
    # filter < 0, no filter at all
    # ToDo, filter = 0, 3 sigma filtering
    # filter > 0, throw away the first filter intervals for warm up

    last_timestamp = 0
    total_time = 0
    total_ops = 0
    
    _data = np.array(data)
    if filter > 0:
        last_timestamp = _data[filter-1, 0]
        _data = _data[filter:, :]
        
    print(_data.shape, last_timestamp)
    n, _ = _data.shape
    for i in range(n):
        c = _data[i][0] # current timestamp
        r = _data[i][1] # rate
        interval = c - last_timestamp
        total_time += interval
        total_ops += interval * r
        last_timestamp = c
        print(total_time, total_ops)
    return total_ops / total_time