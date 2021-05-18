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

'''

path: Path to the latency file or rate file to be read

'''
def general_read(path):
    with open(path) as f:
        lines = f.readlines()
        ts = [[float(l.strip().split(' ')[0]), float(l.strip().split(' ')[1])] for l in lines]
    return ts

'''

path: Path to the read-write.dat file to be read to determine the percentage of read and write

Return: (read percentage, write percentage)

'''
def get_num_reads(path):
    with open(path) as f:
        numReads = 0
        count = 0
        lines = f.readlines()
        for l in lines:
            l = l.strip()
            if l == "read":
                numReads += 1
            count += 1
#         readPercent = numReads/count
    return numReads, count


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
                print(ts)
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