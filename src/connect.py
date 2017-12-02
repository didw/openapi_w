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

class Connect:
    def __init__(self):
        self.kiwoom = Kiwoom()
        self.kiwoom.comm_connect()


if __name__ == '__main__':
    app = QApplication(sys.argv)

    connect = Connect()
