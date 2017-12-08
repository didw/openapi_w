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

def build_model():
    """
    120x67 input
    60x67 1d-conv(10), max-pooling(2)
    30x67 1d-conv(10), max-pooling(2)
    15x67 1d-conv(10), max-pooling(2)
    7x67 1d-conv(10), max-pooling(2)
    """

def main():
    pass

if __name__ == '__main__':
    main()
