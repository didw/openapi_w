import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import pandas as pd
import time, datetime
import os
import pyautogui
from tqdm import tqdm

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
        with open("account.txt", 'r') as f:
            account = f.readlines()
        self.dynamicCall("CommConnect(1)")
        time.sleep(5)
        pyautogui.typewrite("%s\n"%account[1], interval=1)
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
                break


def concatenate_data(code, df):
    if len(code)<8:
        fname = '../Data/future/%s.csv'%code
    else:
        fname = '../Data/option/%s.csv'%code
    if not os.path.exists(fname):
        df = df.sort_values(by=['체결시간'])
        df.to_csv(fname, encoding='UTF-8', index=False)

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
    #print("download %s" % code)
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
        future_code_list = ['6AH18','6AZ17','6BH18','6BZ17','6CH18','6CZ17','6EF18','6EG18','6EH18','6EZ17','6JH18','6JZ17','6LF18',
            '6MZ17','6NH18','6NZ17','6SZ17','BZG18','BZH18','CLF18','CLG18','CLH18','CLJ18','CLK18','CLM18','CLM19','CLN18',
            'CLQ18','CLU18','CLV18','CLX18','CLZ18','CLZ19','CLZ20','CNF18','CNZ17','CUSF18','CUSH18','CUSM18','CUSU18','CUSZ17',
            'E7H18','E7Z17','EMDZ17','ESH18','ESM18','ESZ17','GCF18','GCG18','GCJ18','GCM18','GCZ17','GCZ18','GEH18','GEH19','GEH20',
            'GEH21','GEH22','GEM18','GEM19','GEM20','GEM21','GEM22','GEU18','GEU19','GEU20','GEU21','GEU22','GEZ17','GEZ18','GEZ19',
            'GEZ20','GEZ21','GEZ22','GFF18','GFH18','GFJ18','GFK18','GFQ18','HEG18','HEJ18','HEM18','HEN18','HEQ18','HEV18','HEZ17',
            'HGF18','HGH18','HGK18','HGN18','HGZ17','HHIH18','HHIM18','HHIZ17','HOF18','HOG18','HOH18','HOJ18','HOK18','HOM18','HOZ18',
            'HSIH18','HSIM18','HSIZ17','INZ17','IUF18','IUZ17','LEG18','LEJ18','LEM18','LEQ18','LEV18','LEZ17','LEZ18','M6AZ17','M6BZ17',
            'M6EH18','M6EZ17','MCDZ17','MCHH18','MCHZ17','MGCG18','MGCZ17','MGCZ18','MHIF18','MHIH18','MHIM18','MHIZ17','MJYZ17','NGF18',
            'NGF19','NGG18','NGH18','NGH19','NGJ18','NGJ19','NGK18','NGM18','NGN18','NGQ18','NGU18','NGV18','NGX18','NGZ18','NKDH18','NKDZ17',
            'NKH18','NKZ17','NQH18','NQZ17','PAH18','PAZ17','PLF18','PLJ18','QGF18','QMF18','QMG18','QOG18','RBF18','RBG18','RBH18','RBJ18',
            'RBK18','RBM18','RBN18','RBZ18','RS1Z17','RSGZ17','RSVZ17','SIF18','SIH18','SIK18','SIZ17','TWZ17','UCF18','UCH18','UCM18',
            'UCZ17','YMH18','YMZ17','ZBH18','ZBZ17','ZCH18','ZCH19','ZCK18','ZCN18','ZCU18','ZCZ17','ZCZ18','ZFH18','ZFZ17','ZLF18','ZLH18',
            'ZLK18','ZLN18','ZLQ18','ZLV18','ZLZ17','ZLZ18','ZMF18','ZMH18','ZMK18','ZMN18','ZMQ18','ZMV18','ZMZ17','ZMZ18','ZNH18','ZNZ17',
            'ZOH18','ZOZ17','ZRF18','ZSF18','ZSH18','ZSK18','ZSN18','ZSQ18','ZSX18','ZTH18','ZTZ17','ZWH18','ZWK18','ZWN18','ZWU18','ZWZ17','ZWZ18']

        option_code_list = ['O6BZ17C1335','O6BZ17C1340','O6BZ17C1345','O6BZ17C1350','O6BZ17P1330','O6EF18C1200','O6EF18C1205','O6EF18C1210','O6EF18C1215',
            'O6EZ17C1180','O6EZ17C1185','O6EZ17C1190','O6EZ17C1195','O6EZ17C1200','O6EZ17C1205','O6EZ17P1170','O6EZ17P1175','O6EZ17P1180','O6EZ17P1185',
            'O6EZ17P1190','O6JZ17C0900','O6JZ17P0885','OCLF18C5700','OCLF18C5750','OCLF18C5800','OCLF18C5850','OCLF18C5900','OCLF18C5950','OCLF18C6000',
            'OCLF18C6050','OCLF18C6100','OCLF18C6150','OCLF18C6200','OCLF18C6250','OCLF18C6300','OCLF18C6350','OCLF18C6400','OCLF18P4850','OCLF18P4900',
            'OCLF18P4950','OCLF18P5000','OCLF18P5050','OCLF18P5100','OCLF18P5150','OCLF18P5200','OCLF18P5250','OCLF18P5300','OCLF18P5350','OCLF18P5400',
            'OCLF18P5450','OCLF18P5500','OCLF18P5550','OCLF18P5600','OCLF18P5650','OCLF18P5700','OCLF18P5750','OCLF18P5800','OCLG18C5800','OCLG18C5850',
            'OCLG18C5900','OCLG18C5950','OCLG18C6000','OCLG18C6100','OCLG18C6150','OCLG18C6200','OCLG18C6250','OCLG18C6300','OCLG18C6400','OCLG18C6500',
            'OCLG18P4900','OCLG18P5000','OCLG18P5100','OCLG18P5150','OCLG18P5200','OCLG18P5250','OCLG18P5300','OCLG18P5350','OCLG18P5400','OCLG18P5450',
            'OCLG18P5500','OCLG18P5600','OCLG18P5700','OCLH18C6000','OCLH18C6500','OCLH18P4600','OCLH18P5200','OESZ17C2580','OESZ17C2600','OESZ17C2605',
            'OESZ17C2610','OESZ17C2615','OESZ17C2620','OESZ17C2625','OESZ17C2630','OESZ17C2635','OESZ17C2640','OESZ17C2645','OESZ17C2650','OESZ17C2655',
            'OESZ17C2660','OESZ17C2665','OESZ17C2670','OESZ17C2675','OESZ17C2680','OESZ17C2685','OESZ17C2690','OESZ17C2695','OESZ17C2700','OESZ17C2710',
            'OESZ17P2300','OESZ17P2400','OESZ17P2450','OESZ17P2475','OESZ17P2490','OESZ17P2500','OESZ17P2520','OESZ17P2525','OESZ17P2530','OESZ17P2535',
            'OESZ17P2540','OESZ17P2545','OESZ17P2550','OESZ17P2555','OESZ17P2560','OESZ17P2565','OESZ17P2570','OESZ17P2575','OESZ17P2580','OESZ17P2585',
            'OESZ17P2590','OESZ17P2595','OESZ17P2600','OESZ17P2605','OESZ17P2610','OESZ17P2615','OESZ17P2620','OESZ17P2625','OESZ17P2630','OESZ17P2640',
            'OESZ17P2650','OEW2Z17C2625','OEW2Z17C2630','OEW2Z17C2640','OEW2Z17C2650','OEW2Z17P2600','OEW3F18C2675','OEW3F18C2700','OEW3F18P2500','OZCF18C0360',
            'OZCH18C0350','OZCH18C0360','OZCH18C0370','OZCH18P0340','OZCH18P0350','OZCN18C0370']

        df_summary = []
        progress = "."
        for code in tqdm(future_code_list):
            download_add(code, kiwoom)
        for code in tqdm(option_code_list):
            download_add(code, kiwoom)

        today = datetime.datetime.now()
        print(today)
        time.sleep(10)
        now = time.localtime()
        cur_time = int("%02d%02d" % (now.tm_hour, now.tm_min))
        if 705 < cur_time < 730:
            break



if __name__ == "__main__":
    main()
