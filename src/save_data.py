import sys
from PyQt5.QtWidgets import QApplication
from pykiwoom.kiwoom import *
from pykiwoom.wrapper import *
import numpy as np
import pandas as pd
import sqlite3
import datetime
import os

MARKET_KOSPI   = 0
MARKET_KOSDAK  = 10

class DailyData:
    def __init__(self):
        self.kiwoom = Kiwoom()
        self.kiwoom.comm_connect()
        print("success to connect")
        self.wrapper = KiwoomWrapper(self.kiwoom)
        print("success to init kiwoomwrapper")
        self.get_item_list()
        print("item_list", self.item_list)
        self.code_list = ['ESZ17']

    def get_item_list(self):
        """ 해외선물 상품리스트를 반환한다.
        """
        self.item_list = self.kiwoom.get_global_future_item_list()

    def get_code_list(self):
        """ 해외상품별 해외옵션 종목코드 리스트를 반환
        실행시 파이썬 dead
        """
        self.code_list = {}
        for item in self.item_list:
            time.sleep(1)
            self.code_list[item] = self.kiwoom.get_global_option_code_list(item)
            print("code_list[%s]" % item, self.code_list[item])

    def check_recent_file(self, code):
        import os
        from time import strftime, gmtime, time
        fname = '../data/hdf/%s.hdf'%code
        try:
            print(time() - os.path.getmtime(fname))
            if (time() - os.path.getmtime(fname)) < 40000:
                return True
        except FileNotFoundError:
            return False
        return False

    def save_all_data(self):
        today = datetime.date.today().strftime("%Y%m%d")
        #today = datetime.date(2011,9,1).strftime("%Y%m%d")
        print(today, len(self.code_list))

        # save data
        for code in self.code_list:
            if code == '':
                continue
            print("get data of %s" % code)
            self.save_table(code)

    def save_table(self, code):
        TR_REQ_TIME_INTERVAL = 4
        time.sleep(TR_REQ_TIME_INTERVAL)
        data_01 = self.wrapper.get_data_opc10001(code)
        print(data_01)


if __name__ == '__main__':
    app = QApplication(sys.argv)

    daily_data = DailyData()
    
    daily_data.save_all_data()
