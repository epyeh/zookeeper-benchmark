import numpy as np
import matplotlib.pyplot as plt
import os
import re

def get_config(path = '../benchmark.conf'):
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


def get_data(path):
    with open(path) as f:
        lines = f.readlines()
        data = []
        stat = {}
        for line in lines:
            line = line.strip()
            if line:
                if line[0] == '#':
                    stat[line.split(' ')[0]] = float(line.split(' ')[1])
                else:
                    data.append([float(line.split(' ')[0]), float(line.split(' ')[1])])
        return data, stat


def get_latency_files(op, prefix='../results/'):

    files = os.listdir(prefix)
    pattern = '-{}_timings.dat'.format(op)
    
    fs = []
    for fn in files:
        match = re.search(pattern, fn)
        if match:
            fs.append(fn)
    return fs


def avg_latency(data, filter=0):
    # calculate the average latency
    # for certain read write percentage
    # among all clients

    _data = []
    
    for k in data.keys():
        _data.append(np.array(data[k])[filter:, :])
    
    _data = np.vstack(_data)
    _d = _data[:, 1] - _data[:, 0]
    return np.mean(_d), np.std(_d)


def get_mixrw_rate(prefix, step):

    data = {}
    print('files:')

    for i in range(0, 101, step):
        ratio = i / 100
        path = os.path.join(prefix, 'MIXREADWRITE-{:.1f}.dat'.format(ratio))
        print(path)
        with open(path) as f:
            lines = f.readlines()
            ts = [(float(l.strip().split(' ')[0]), float(l.strip().split(' ')[1])) for l in lines] # (current time, rate)
            data[ratio] = ts
    
    return data


def get_mixrw_latency(prefix, step):

    files = os.listdir(prefix)
    data = {}

    for i in range(0, 101, step):
        ratio = i / 100
        data[ratio] = {}
        pattern = '-MIXREADWRITE-{:.1f}_timings.dat'.format(ratio)
        for fn in files:
            match = re.search(pattern, fn)
            if match:
                print('fn', fn)
                idx = int(fn[:match.start()])
                # print('idx', idx)
                data[ratio][idx], _ = get_data(os.path.join(prefix, fn))
    
    return data
    

def avg_rate(data, filter=-1):
    # filter <=0 0, no filter at all
    # filter > 0, throw away the first filter intervals for warm up
        
    
    
    _data = np.array(data)[filter:, :]

    return np.mean(_data[:, 1]), np.std(_data[:, 1])