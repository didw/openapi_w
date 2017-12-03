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
        self.OnReceiveRealData.connect(self._receive_real_data)
        self.OnReceiveMsg.connect(self._receive_msg)
        self.OnReceiveChejanData.connect(self._receive_chejan_data)

    def comm_connect(self):
        state = self.GetConnectState()
        print("state: ", state)
        if state == 1:
            return
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

    def _receive_real_data(self, jongmok_code, real_type, real_data):
        print(jongmok_code, real_type, real_data)

    def _receive_msg(self, screen_no, rqname, tr_code, msg):
        print(screen_no, rqname, tr_code, msg)

    def _receive_chejan_data(self, gubun, item_cnt, fid_list):
        print(gubun, item_cnt, fid_list)

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next):
        if next == '01':
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == "opc10001_req":
            self._opc10001(rqname, trcode, record_name)

        if rqname == "opt10006_req":
            self._opt10006(rqname, trcode, record_name)

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

    def _opt10006(self, rqname, trcode, record_name):
        data_cnt = self._get_repeat_cnt(trcode, rqname)

        for i in range(data_cnt):
            datetime = self._get_comm_data(trcode, record_name, i, "종목코드")
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
    kiwoom.comm_rq_data("opt10006_req", "opt10006", 0, "0101")

    while kiwoom.remained_data == True:
        time.sleep(TR_REQ_TIME_INTERVAL)
        kiwoom.set_input_value("종목코드", code)
        kiwoom.set_input_value("시간단위", 1)
        kiwoom.comm_rq_data("opt10006_req", "opt10006", 2, "0101")
    df = pd.DataFrame(kiwoom.received_data, columns=["종목코드", "현재가", "누적거래량", "거래소코드"])
    return df


def GetGlobalFutureItemlist(kiwoom):
    time.sleep(TR_REQ_TIME_INTERVAL)
    itemlist = kiwoom.GetGlobalFutureItemlist()
    print(itemlist)
    return itemlist.split(';')

def GetGlobalFutureCodelist(kiwoom, item):
    time.sleep(TR_REQ_TIME_INTERVAL)
    codelist = kiwoom.GetGlobalFutureCodelist(item)
    print(item, len(codelist.split(';')))
    return codelist.split(';')

def GetGlobalOptionItemlist(kiwoom):
    time.sleep(TR_REQ_TIME_INTERVAL)
    itemlist = kiwoom.GetGlobalOptionItemlist()
    print(itemlist)
    return itemlist.split(';')

def GetGlobalOptionCodelist(kiwoom, item):
    time.sleep(TR_REQ_TIME_INTERVAL)
    codelist = kiwoom.GetGlobalOptionCodelist(item)
    print(item, len(codelist.split(';')))
    return codelist.split(';')

def main():
    app = QApplication(sys.argv)
    kiwoom = Kiwoom()
    kiwoom.comm_connect()
    itemlist = GetGlobalOptionItemlist(kiwoom)
    df = {}
    df_summary = []
    codelist = []
    for item in itemlist:
        time.sleep(TR_REQ_TIME_INTERVAL)
        codelist.extend(GetGlobalOptionCodelist(kiwoom, item))
    #    for code in codelist:
    #        if len(code) > 6:
    #            print("code:%s is weired.."%code)
    #        df[code] = download_add(code, kiwoom)
    #        df_summary.append([len(df[code]), code])
    #df_summary.sort()
    #print(df_summary[:1000])
    print(codelist)

if __name__ == "__main__":
    main()
