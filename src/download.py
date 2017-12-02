import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import pandas as pd
import time, datetime
import os

TR_REQ_TIME_INTERVAL = 0.2

class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self._create_kiwoom_instance()
        self._set_signal_slots()
        self.received_data = []

    def _create_kiwoom_instance(self):
        self.setControl("KFOPENAPI.KFOpenAPICtrl.1")

    def _set_signal_slots(self):
        self.OnEventConnect.connect(self._event_connect)
        self.OnReceiveTrData.connect(self._receive_tr_data)

    def comm_connect(self):
        self.dynamicCall("CommConnect(1)")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def _event_connect(self, err_code):
        if err_code == 0:
            print("connected")
        else:
            print("disconnected")

        self.login_event_loop.exit()

    def get_code_list_by_market(self, market):
        code_list = self.dynamicCall("GetCodeListByMarket(QString)", market)
        code_list = code_list.split(';')
        return code_list[:-1]

    def get_master_code_name(self, code):
        code_name = self.dynamicCall("GetMasterCodeName(QString)", code)
        return code_name

    def set_input_value(self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):
        self.dynamicCall("CommRqData(QString, QString, int, QString", rqname, trcode, next, screen_no)
        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()

    def _get_comm_data(self, code, record_name, index, item_name):
        ret = self.dynamicCall("GetCommData(QString, QString, int, QString", code,
                               record_name, index, item_name)
        return ret.strip()

    def _get_repeat_cnt(self, trcode, rqname):
        ret = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next):
        if next == '01':
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == "opc10001_req":
            self._opc10001(rqname, trcode, record_name)

        try:
            self.tr_event_loop.exit()
        except AttributeError:
            pass

    def _opc10001(self, rqname, trcode, record_name):
        data_cnt = self._get_repeat_cnt(trcode, rqname)

        for i in range(data_cnt):
            datetime = self._get_comm_data(trcode, record_name, i, "체결시간")
            close = self._get_comm_data(trcode, record_name, i, "현재가")
            volume = self._get_comm_data(trcode, record_name, i, "거래량")
            workingday = self._get_comm_data(trcode, record_name, i, "영업일자")
            try:
                self.received_data.append([int(datetime), float(close), float(volume), int(workingday)])
            except ValueError:
                print(datetime, close, volume, workingday)
                raise


def concatenate_data(code, df):
    fname = 'Data/%s.csv'%code
    if not os.path.exists(fname):
        df.to_csv(fname, encoding='UTF-8', index=False)
        return

    df_old = pd.read_csv(fname, encoding='UTF-8')
    df_old = df_old.sort_values(by=['체결시간'])
    last_date = int(df_old.loc[len(df_old)-1,'체결시간'])
    df_old = df_old.loc[df_old['체결시간']<last_date]

    df = df.loc[df['체결시간']>=last_date]

    df_new = pd.concat([df_old, df])
    df_new = df_new.sort_values(by=['체결시간'])
    df_new.to_csv(fname, encoding='UTF-8', index=False)


def download_add(code, kiwoom):
    # opt10081 TR 요청
    print("download %s" % code)
    kiwoom.received_data = []
    time.sleep(TR_REQ_TIME_INTERVAL)
    kiwoom.set_input_value("종목코드", code)
    kiwoom.set_input_value("시간단위", 1)
    kiwoom.comm_rq_data("opc10001_req", "opc10001", 0, "0101")

    while kiwoom.remained_data == True:
        time.sleep(TR_REQ_TIME_INTERVAL)
        kiwoom.set_input_value("종목코드", code)
        kiwoom.set_input_value("시간단위", 1)
        kiwoom.comm_rq_data("opc10001_req", "opc10001", 2, "0101")
    df = pd.DataFrame(kiwoom.received_data, columns=["체결시간", "현재가", "거래량", "영업일자"])
    concatenate_data(code, df)


def main():
    app = QApplication(sys.argv)
    kiwoom = Kiwoom()
    kiwoom.comm_connect()

    while True:        
        # British Pound, Canadian Dollar, Euro Fx, Japanese Yen, Brazilian Real
        # Mexican Pesos, New Zealand Dollars, Swiss Franc, Brent Crude Oil, Live Cattle
        # Light/Sweet Crude Oil, FTSE China A50, RMB Currency, E-mini Euro FX, E-mini S&P MidCap 400
        # E-mini S&P 500, COMEX Gold, Micro AUD, Micro GBP, Micro Euro
        # Eurodollar, Feeder Cattle, Lean Hogs, 'COMEX Copper', H-shares Index
        # Heating Oil, 'Hang Seng Index', CNX Nifty, SGX INR USD FX Futures, Micro CAD
        code_list = ['6BZ17', '6CZ17', '6EZ17', '6JZ17', '6LF18', 
            '6MZ17', '6NZ17', '6SZ17', 'BZG18', 'LEG18',
            'CLF18', 'CNZ17', 'CUSH18', 'E7Z17', 'EMDZ17',
            'ESZ17', 'GCG18', 'M6AZ17', 'M6BZ17', 'M6EZ17',
            'GEZ18', 'GFF18', 'HEG18', 'HGH18', 'HHIZ17',
            'HOF18', 'HSIZ17', 'INZ17', 'IUZ17', 'MCDZ17',
            '6AZ17', 'MCHZ17', 'ZWH18', 'MGCG18', 'MHIZ17',
            'MJYZ17', 'NGF18', 'NKZ17', 'NKDZ17',
            'NQZ17', 'PAH18', 'PLF18', 'QGF18',
            'QMF18', 'QOG18', 'RBF18', 
            'RS1Z17', 'RSGZ17', 'RSVZ17', 'SIH18', 'TWZ17',
            'UCZ17', 'YMZ17', 'ZBH18', 'ZCH18', 'ZFH18',
            'ZLF18', 'ZMF18', 'ZNH18', 'ZOH18', 'ZRF18',
            'ZSF18', 'ZTH18',
            ]
        # Australian Dollar, Mini H-shares Index, Wheat, COMEX Micro Gold, Mini-Hang Seng Index
        # Micro JPY, Henry Hub Natural Gas, SGX Nikkei225 Yen, Nikkei 225 Dollar
        # E-mini NASDAQ 100, Palladium, Platinum, miNY Natural Gas
        # Mini Russel 1000 Growth, Mini Russel 1000 Value, COMEX Silver, MSCI Taiwan
        # COMEX Miny Silver, miNY Crude Oil, COMEX Miny Gold, RBOB Gasoline
        # SGX USD CNH FX Futures, E-mini Dow($5), 30Yr U.S. Treasury Bond, Corn, 5 Yr U.S. Treasury Note
        # Soybean Oil, Soybean Meal, 10 Yr U.S. Treasury Note, Oats, Rough Rice,
        # Soybeans, 2 Yr U.S. Treasury Note

        for code in code_list:
            download_add(code, kiwoom)

        today = datetime.datetime.now()
        print(today)
        time.sleep(10)



if __name__ == "__main__":
    main()
