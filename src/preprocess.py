#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import h5py
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from tqdm import tqdm
import os
import glob

def verify_data(X, y):
    print(np.shape(X), np.shape(y))
    for i in range(len(X)-60):
        print("cur:%d, fut:%d, lab:%d" % (X[i,-1,0], X[i+60,-1,0], y[i]))
    unique, counts = np.unique(y, return_counts=True)
    print(dict(zip(unique, counts)))

def make_x_y(df):
    X = []
    y = []
    print("make x, y")
    for i in tqdm(range(120, len(df)-340000), ncols=80):
        cur_time = df.loc[i, '시간'][11:16]
        if cur_time > "15:20": continue
        if cur_time < "09:02": continue
        X.append(np.array(df.iloc[i-120:i+1,2:]))
        pred = 1 if df.iloc[i][2] < df.iloc[i+60][2] else -1
        y.append(pred)
        for r in range(len(X[0])):
            for c in range(len(X[0][r])):
                try:
                    int(X[0][r][c])
                except:
                    print(r, c, X[0][r][c])
                    raise
    return np.array(X), np.array(y)


def main():
    tmp_df = pd.read_hdf('../hdf/A005930.h5', 'table').reset_index()
    X, y = make_x_y(tmp_df)
    verify_data(X, y)

if __name__ == '__main__':
    main()
