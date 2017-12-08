#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import os
from glob import glob
import numpy as np
from scipy.io import wavfile
from tensorflow.contrib.learn.python.learn.learn_io.generator_io import generator_input_fn
from sklearn.preprocessing import StandardScaler
import pandas as pd


POSSIBLE_LABELS = 'yes no up down left right on off stop go silence unknown'.split()
id2name = {i: name for i, name in enumerate(POSSIBLE_LABELS)}
name2id = {name: i for i, name in id2name.items()}
 

def load_data(data_dir, sanity_check=False):
    """ Return 2 lists of tuples:
    [(class_id, user_id, path), ...] for train
    [(class_id, user_id, path), ...] for validation
    """
    # Just a simple regexp for paths with three groups:
    # prefix, label, user_id
    pattern = re.compile("(.+\/)?(\w+)\/([^_]+)_.+wav")
    all_files = glob(os.path.join(data_dir, 'train/audio/*/*wav'))
    if sanity_check == True:
        all_files = all_files[:200]

    with open(os.path.join(data_dir, 'train/validation_list.txt'), 'r') as fin:
        validation_files = fin.readlines()
        if sanity_check == True:
            validation_files = validation_files[:200]
    valset = set()
    for entry in validation_files:
        r = re.match(pattern, entry)
        if r:
            valset.add(r.group(3))

    possible = set(POSSIBLE_LABELS)
    train, noise, val = [], [], []
    for entry in all_files:
        r = re.match(pattern, entry)
        if r:
            label, uid = r.group(2), r.group(3)
            if label == '_background_noise_':
                label = 'silence'
            if label not in possible:
                label = 'unknown'

            label_id = name2id[label]

            sample = (label_id, uid, entry)
            if uid in valset:
                val.append(sample)
            else:
                train.append(sample)
                if label == 'silence':
                    noise.append(sample)

    print('There are {} train ({} background_noise included) and {} val samples'.format(len(train), len(noise), len(val)))
    return train, noise, val




def data_generator_train(code):
    def generator():
        scaler = StandardScaler()
        print("loading train data set")
        df = pd.read_hdf('../Data/hdf_201709/%s.h5'%code, 'table').reset_index()
        scaler.fit(df.iloc[0:1000,2:])
        split_train_val = int(len(df)*0.8)
        prev_label = -1
        for _ in range(120, split_train_val):
            i = np.random.randint(120, split_train_val)
            cur_time = df.loc[i, '시간'][11:16]
            if cur_time > "15:20": continue
            if cur_time < "09:02": continue
            if df.iloc[i][2] < df.iloc[i+60][2]:
                pred = 1 
            elif df.iloc[i][2] > df.iloc[i+60][2]:
                pred = -1
            else:
                pred = 0
            if pred == prev_label: continue
            if pred in [-1, 1]:
                prev_label = pred
            yield dict(
                target=np.int32(pred),
                data=scaler.transform(np.array(df.iloc[i-120:i,2:])).reshape(120,67,1).astype(np.float32)
            )

    return generator


def data_generator_val(code):
    def generator():
        scaler = StandardScaler()
        print("loading val data set")
        df = pd.read_hdf('../hdf_201709/%s.h5'%code, 'table').reset_index()
        scaler.fit(df.iloc[0:1000,2:])
        split_train_val = int(len(df)*0.8)
        prev_label = -1
        for _ in range(120, split_train_val):
            i = np.random.randint(split_train_val, len(df)-60)
            cur_time = df.loc[i, '시간'][11:16]
            if cur_time > "15:20": continue
            if cur_time < "09:02": continue
            if df.iloc[i][2] < df.iloc[i+60][2]:
                pred = 1 
            elif df.iloc[i][2] > df.iloc[i+60][2]:
                pred = -1
            else:
                pred = 0
            if pred == prev_label: continue
            if pred in [-1, 1]:
                prev_label = pred
            yield dict(
                target=np.int32(pred),
                data=scaler.transform(np.array(df.iloc[i-120:i,2:])).reshape(120,67,1).astype(np.float32)
            )

    return generator


def get_data_generator(code, hparams):
    # it's a magic function :)
    
    train_input_fn = generator_input_fn(
        x=data_generator_train(code),
        target_key='target',  # you could leave target_key in features, so labels in model_handler will be empty
        batch_size=hparams.batch_size, shuffle=True, num_epochs=None,
        queue_capacity=3 * hparams.batch_size + 10, num_threads=10,
    )

    val_input_fn = generator_input_fn(
        x=data_generator_val(code),
        target_key='target',
        batch_size=hparams.batch_size, shuffle=True, num_epochs=None,
        queue_capacity=3 * hparams.batch_size + 10, num_threads=1,
    )
    return train_input_fn, val_input_fn


def get_test_data_generator(code, hparams):
    def test_data_generator(code):
        def generator():
            scaler = StandardScaler()
            print("loading val data set")
            df = pd.read_hdf('../hdf_201709/%s.h5'%code, 'table').reset_index()
            scaler.fit(df.iloc[0:1000,2:])
            df = pd.read_hdf('../hdf_201710/%s.h5'%code, 'table').reset_index()
            for i in range(120, 100000):
                cur_time = df.loc[i, '시간'][11:16]
                if cur_time > "15:20": continue
                if cur_time < "09:02": continue
                if df.iloc[i][2] < df.iloc[i+60][2]:
                    pred = 1 
                elif df.iloc[i][2] > df.iloc[i+60][2]:
                    pred = -1
                else:
                    pred = 0
                yield dict(
                    target=np.int32(pred),
                    data=scaler.transform(np.array(df.iloc[i-120:i,2:])).reshape(120,67,1).astype(np.float32),
                    cur=df.iloc[i,2],
                    future=df.iloc[i+60,2],
                    buy=df.iloc[i,27],
                    sell=df.iloc[i,29]
                )

        return generator

    test_input_fn = generator_input_fn(
        x=test_data_generator(code),
        batch_size=hparams.batch_size, 
        shuffle=False, 
        num_epochs=1,
        queue_capacity= 10 * hparams.batch_size, 
        num_threads=1)
    return test_input_fn
