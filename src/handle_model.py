
import os

import numpy as np
import tensorflow as tf
from tensorflow.contrib import layers
from tensorflow.contrib import rnn

from feature import get_features
from model import get_model


def eval_confusion_matrix(labels, predictions):
    with tf.variable_scope("eval_confusion_matrix"):
        con_matrix = tf.confusion_matrix(labels=labels, predictions=predictions, num_classes=12)

        con_matrix_sum = tf.Variable(tf.zeros(shape=(12,12), dtype=tf.int32),
                                            trainable=False,
                                            name="confusion_matrix_result",
                                            collections=[tf.GraphKeys.LOCAL_VARIABLES])

        update_op = tf.assign_add(con_matrix_sum, con_matrix)
        return tf.convert_to_tensor(con_matrix_sum), update_op


def model_handler(features, labels, mode, params, config):
    extractor = tf.make_template(
        'extractor', get_model(params),
        create_scope_now_=True,
    )

    feature_fn = get_features(params)
    model_input = features['data']

    predictions = extractor(model_input, params, mode == tf.estimator.ModeKeys.TRAIN)

    if mode == tf.estimator.ModeKeys.TRAIN:
        loss = tf.losses.absolute_difference(labels=labels, predictions=predictions)
        # some lr tuner, you could use move interesting functions
        def learning_rate_decay_fn(learning_rate, global_step):
            return tf.train.exponential_decay(
                learning_rate, global_step, decay_steps=5000, decay_rate=0.97)
        train_op = tf.contrib.layers.optimize_loss(
            loss=loss,
            global_step=tf.contrib.framework.get_global_step(),
            learning_rate=params.learning_rate,
            #optimizer=lambda lr: tf.train.MomentumOptimizer(lr, 0.9, use_nesterov=True),
            optimizer='Adam',
            #learning_rate_decay_fn=learning_rate_decay_fn,
            clip_gradients=params.clip_gradients,
            variables=tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES))

        specs = dict(
            mode=mode,
            loss=loss,
            train_op=train_op,
        )

    if mode == tf.estimator.ModeKeys.EVAL:
        loss = tf.losses.absolute_difference(labels=labels, predictions=predictions)
        specs = dict(
            mode=mode,
            loss=loss,
        )

    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'pred': predictions,
            'target': features['target'],
            'cur': features['cur'],
            'future': features['future'],
            'buy': features['buy'],
            'sell': features['sell'],
        }
        specs = dict(
            mode=mode,
            predictions=predictions,
        )
    return tf.estimator.EstimatorSpec(**specs)


def create_model(config=None, hparams=None):
    return tf.estimator.Estimator(
        model_fn=model_handler,
        config=config,
        params=hparams)

