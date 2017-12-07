
from tensorflow.contrib import signal
import tensorflow as tf
import numpy as np

def get_spectrogram(wav):
    specgram = signal.stft(
        wav,
        256,  # 16000 [samples per second] * 0.025 [s] -- default stft window frame
        128,  # 16000 * 0.010 -- default stride
    )
    # log(1 + abs) is a default transformation for energy units
    amp = tf.log1p(tf.abs(specgram))
    # specgram is a complex tensor, so split it into abs and phase parts:
    phase = tf.angle(specgram) / np.pi

    x = tf.stack([amp, phase], axis=3) # shape is [bs, time, freq_bins, 2]
    x = tf.to_float(x)  # we want to have float32, not float64

    return x


def get_mfcc(wav):
    log_mel_spectrograms = get_mel_spectrogram(wav)
    mfccs = tf.contrib.signal.mfccs_from_log_mel_spectrograms(log_mel_spectrograms)[:, :, :64, :]

    return mfccs


def get_mel_spectrogram(wav):
    specgram = signal.stft(
        wav,
        256,  # 16000 [samples per second] * 0.025 [s] -- default stft window frame
        128,  # 16000 * 0.010 -- default stride
    )
    spectrograms = tf.abs(specgram)
    sample_rate = 16000
    num_spectrogram_bins = specgram.shape[-1].value
    lower_edge_hertz, upper_edge_hertz, num_mel_bins = 80.0, 7600.0, 80
    linear_to_mel_weight_matrix = tf.contrib.signal.linear_to_mel_weight_matrix(num_mel_bins, num_spectrogram_bins,
                                                                                sample_rate, lower_edge_hertz,
                                                                                upper_edge_hertz)
    mel_spectrograms = tf.tensordot(spectrograms, linear_to_mel_weight_matrix, 1)
    mel_spectrograms.set_shape(spectrograms.shape[:-1].concatenate(linear_to_mel_weight_matrix.shape[-1:]))
    log_mel_spectrograms = tf.log(mel_spectrograms + 1e-6)
    log_mel_spectrograms = tf.expand_dims(log_mel_spectrograms, axis=3)

    return log_mel_spectrograms


def get_log_spectrogram(wav):
    specgram = signal.stft(
        wav,
        256,  # 16000 [samples per second] * 0.025 [s] -- default stft window frame
        128,  # 16000 * 0.010 -- default stride
    )
    spectrograms = tf.abs(specgram)
    log_spectrograms = tf.log(spectrograms + 1e-6)
    log_spectrograms = tf.expand_dims(log_spectrograms, axis=3)

    return log_spectrograms



def get_features(params):
    if params.features == 'mfcc':
        return get_mfcc
    elif params.features == 'mel_spectrogram':
        return get_mel_spectrogram
    elif params.features == 'log_spectrogram':
        return get_log_spectrogram
    return get_spectrogram
