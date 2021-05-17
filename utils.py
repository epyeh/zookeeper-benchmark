import numpy as np
import matplotlib.pyplot as plt
import os
import re

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


def avg_latency_per_client(data):
    avg = {}
    for k, v in data.items():
        a = np.array(v)
        d = a[:, 1] - a[:, 0]
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


def avg_rate(data):
    last_timestamp = 0
    total_time = 0
    total_ops = 0
    for c, r in data:
        interval = c - last_timestamp
        total_time += interval
        total_ops += interval * r
        last_timestamp = c
    return total_ops / total_time