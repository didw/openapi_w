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

columns = [['종목코드', '시간', '수신시간', '현재가', '전일대비', '매도호가', '매수호가', '체결량'],
            ['종목코드', '시간', '수신시간', '누적거래량', '10차매도호가', '10차매도잔량', '9차매도호가', '9차매도잔량', '8차매도호가', '8차매도잔량', '7차매도호가', '7차매도잔량', '6차매도호가', '6차매도잔량', '5차매도호가', '5차매도잔량', '4차매도호가', '4차매도잔량', '3차매도호가', '3차매도잔량', '2차매도호가', '2차매도잔량', '1차매도호가', '1차매도잔량', 
        '1차매수호가', '1차매수잔량', '2차매수호가', '2차매수잔량', '3차매수호가', '3차매수잔량', '4차매수호가', '4차매수잔량', '5차매수호가', '5차매수잔량', '6차매수호가', '6차매수잔량', '7차매수호가', '7차매수잔량', '8차매수호가', '8차매수잔량', '9차매수호가', '9차매수잔량', '10차매수호가', '10차매수잔량'],
            ['종목코드', '시간', '수신시간', '매도1위거래원코드', '매도1위거래원매도수량', '매수1위거래원코드', '매수1위거래원매수수량', '매도2위거래원코드', '매도2위거래원매도수량', '매수2위거래원코드', '매수2위거래원매수수량', '매도3위거래원코드', '매도3위거래원매도수량', '매수3위거래원코드', '매수3위거래원매수수량', '매도4위거래원코드', '매도4위거래원매도수량', '매수4위거래원코드', '매수4위거래원매수수량', '매도5위거래원코드', '매도5위거래원매도수량', '매수5위거래원코드', '매수5위거래원매수수량']
            ]

new_columns = ['시간', '현재가', '전일대비', '매도호가', '매수호가', '체결량', '체결수', 
        '누적거래량', '10차매도호가', '10차매도잔량', '9차매도호가', '9차매도잔량', '8차매도호가', '8차매도잔량', '7차매도호가', '7차매도잔량', '6차매도호가', '6차매도잔량', '5차매도호가', '5차매도잔량', '4차매도호가', '4차매도잔량', '3차매도호가', '3차매도잔량', '2차매도호가', '2차매도잔량', '1차매도호가', '1차매도잔량', 
        '1차매수호가', '1차매수잔량', '2차매수호가', '2차매수잔량', '3차매수호가', '3차매수잔량', '4차매수호가', '4차매수잔량', '5차매수호가', '5차매수잔량', '6차매수호가', '6차매수잔량', '7차매수호가', '7차매수잔량', '8차매수호가', '8차매수잔량', '9차매수호가', '9차매수잔량', '10차매수호가', '10차매수잔량',
        '매도1위거래원코드', '매도1위거래원매도수량', '매수1위거래원코드', '매수1위거래원매수수량', '매도2위거래원코드', '매도2위거래원매도수량', '매수2위거래원코드', '매수2위거래원매수수량', '매도3위거래원코드', '매도3위거래원매도수량', '매수3위거래원코드', '매수3위거래원매수수량', '매도4위거래원코드', '매도4위거래원매도수량', '매수4위거래원코드', '매수4위거래원매수수량', '매도5위거래원코드', '매도5위거래원매도수량', '매수5위거래원코드', '매수5위거래원매수수량'
        ]
def load_data(fname):
    """ 
    0: cur, 1: jpbid, 2: member
    """
    idx = 0
    if "cur" in fname: idx = 0
    elif "Jpbid" in fname: idx = 1
    elif "Member" in fname: idx = 2
    df = pd.read_csv(fname, header=0, names=columns[idx])
    return df

def merge_data(df_cur_sel, df_bid_sel, df_mem_sel, code):
    begin_time = df_bid_sel.loc[0,'시간']
    cur_time = datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
    
    if os.path.exists('../hdf/%s.h5'%code):
        try:
            old_df = pd.read_hdf('../hdf/%s.h5'%code, 'table').reset_index()
        except KeyError:
            print("%s is not porperly stored" % code)
            raise
        #hdf = pd.HDFStore('../hdf/%s.h5'%code)
        #old_df = hdf.select('table')
        last_time = old_df.loc[len(old_df)-1,'시간']
        if begin_time < last_time: 
            print("last_time:%s, begin_time:%s" % (last_time, begin_time))
            return

    idx = [0,0,0]
    new_df = []
    while True:
        cur_time += timedelta(seconds=1)
        cur_time_str = cur_time.strftime('%Y-%m-%d %H:%M:%S')

        cnt_tick = 0
        while idx[0] < len(df_cur_sel)-1:
            # 동일 시간에 거래한 체결량 누적. 값 변경은 무시.
            next_time = df_cur_sel.loc[idx[0]+1, '시간']
            next_time = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
            if next_time > cur_time: break
            if int(df_cur_sel.loc[idx[0]+1, '체결량']) * int(df_cur_sel.loc[idx[0]+1, '현재가']) > 100000:
                cnt_tick += 1
            df_cur_sel.loc[idx[0]+1, '체결량'] += df_cur_sel.loc[idx[0], '체결량']
            idx[0] += 1

        if idx[0] == len(df_cur_sel) or idx[1] == len(df_bid_sel) or idx[2] == len(df_mem_sel): break
        next_time = df_cur_sel.loc[idx[0], '시간']
        next_time = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
        if next_time == cur_time:
            cnt_tick += 1
            new_df.append([cur_time_str] + list(df_cur_sel.iloc[idx[0],4:]) + [cnt_tick] + list(df_bid_sel.iloc[idx[1],4:]) + list(df_mem_sel.iloc[idx[2],4:]))
        else:
            new_df.append([cur_time_str] + list(df_cur_sel.iloc[idx[0],4:8]) + [0, cnt_tick] + list(df_bid_sel.iloc[idx[1],4:]) + list(df_mem_sel.iloc[idx[2],4:]))
        while idx[0] < len(df_cur_sel):
            next_time = df_cur_sel.loc[idx[0], '시간']
            next_time = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
            if next_time > cur_time: break
            idx[0] += 1
        while idx[1] < len(df_bid_sel):
            next_time = df_bid_sel.loc[idx[1], '시간']
            next_time = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
            if next_time > cur_time: break
            idx[1] += 1
        while idx[2] < len(df_mem_sel):
            next_time = df_mem_sel.loc[idx[2], '시간']
            next_time = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
            if next_time > cur_time: break
            idx[2] += 1
    if len(new_df) < 10:
        return
    new_df = pd.DataFrame(new_df, columns=new_columns)
    new_df = new_df.sort_values(by=['시간'])
    new_df.to_hdf('../hdf/%s.h5'%code, 'table', append=True)


def load_hdf(dirname):
    data_dict = {}
    filelist = glob.glob("%s/*h5"%dirname)
    for afile in filelist:
        code = afile.replace('.h5', '')
        #data_dict[code] = pd.read_hdf(afile, 'table')
        hdf = pd.HDFStore(afile)
        data_dict[code] = hdf.select('table')
    return data_dict


def main():
    tmp_df = pd.read_hdf('../hdf/A005930.h5', 'table').reset_index()
    saved_time = tmp_df.loc[len(tmp_df)-1, '시간'][:10]
    flist = glob.glob('../StockCur/*.csv')
    for afile in sorted(flist):
        fname = afile[-14:]
        new_time = fname[:10]
        if saved_time > new_time:
            print("saved time: %s, new_time: %s, pass" % (saved_time, new_time))
            continue

        df_cur = load_data('../StockCur/%s'%fname)
        df_bid = load_data('../StockJpbid/%s'%fname)
        df_mem = load_data('../StockMember/%s'%fname)
        print(len(df_cur), len(df_bid), len(df_mem))
        code_list = sorted(df_cur['종목코드'].unique())
        print("code_list", len(code_list))

        new_df = {}
        q = Queue()
        p = []
        for code in tqdm(code_list, ncols=80):
            #print("%s Data Preprocessing.."%code)
            df_cur_sel = df_cur.loc[df_cur['종목코드']==code].reset_index()
            df_bid_sel = df_bid.loc[df_bid['종목코드']==code].reset_index()
            df_mem_sel = df_mem.loc[df_mem['종목코드']==code].reset_index()

            p.append(Process(target=merge_data, args=(df_cur_sel, df_bid_sel, df_mem_sel, code)))
            p[-1].start()
        for pr in p:
            pr.join()
            #merge_data(df_cur_sel, df_bid_sel, df_mem_sel, code)

def load_test():
    new_df = load_hdf('../hdf')
    for k,v in new_df.items():
        print(k, len(v))

if __name__ == '__main__':
    main()
    load_test()
