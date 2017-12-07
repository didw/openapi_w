import tensorflow as tf
from tensorflow.contrib import layers
import inception
from preprocessing import image_normalize

slim = tf.contrib.slim


class InceptionV4(object):
  """Model class for CleverHans library."""

  def __init__(self, num_classes):
    self.num_classes = num_classes
    self.built = False

  def __call__(self, x_input):
    """Constructs model and return probabilities for given input."""
    reuse = True if self.built else None
    x_input = image_normalize(x_input, 'default')
    with slim.arg_scope(inception.inception_v4_arg_scope()):
      logit, end_points = inception.inception_v4(
          x_input, num_classes=self.num_classes, is_training=True,
          reuse=reuse)
    self.built = True
    output = end_points['Predictions']
    # Strip off the extra reshape op at the output
    probs = output.op.inputs[0]
    return logit

def baseline_inception(x, params, is_training):
    model = InceptionV4(params.num_classes)
    return model(x)

def set_input_shape(input_t, params):
    if params.features == 'mfcc':
        input_t.set_shape((128, 124, 64, 1))
    elif params.features == 'mel_spectrogram':
        input_t.set_shape((128, 124, 80, 1))
    elif params.features == 'log_spectrogram':
        input_t.set_shape((128, 124, 129, 1))
    else:
        input_t.set_shape((128, 124, 129, 2))


def baseline_rnn(x, params, is_training):
    def dropout(cell, keep_prob):
        return tf.nn.rnn_cell.DropoutWrapper(cell=cell, output_keep_prob=keep_prob)

    def rnn(x, num_hidden=128, layers=4, keep_prob=0.5, bidirectional=True):
        lstm_cell_fw = [dropout(tf.nn.rnn_cell.LSTMCell(size, state_is_tuple=True), keep_prob) for size in [num_hidden] * layers]
        lstm_cell_fw = tf.nn.rnn_cell.MultiRNNCell(cells=lstm_cell_fw, state_is_tuple=True)

        lstm_cell_bw = [dropout(tf.nn.rnn_cell.LSTMCell(size, state_is_tuple=True), keep_prob) for size in [num_hidden] * layers]
        lstm_cell_bw = tf.nn.rnn_cell.MultiRNNCell(cells=lstm_cell_bw, state_is_tuple=True)

        
        if bidirectional == True:
            outputs, states = tf.nn.bidirectional_dynamic_rnn(lstm_cell_fw, lstm_cell_bw, x, dtype=tf.float32, time_major=False)
            outputs = tf.concat((outputs[0], outputs[1]), axis=2)
        else:
            outputs, states = tf.nn.dynamic_rnn(lstm_cell_fw, x, dtype=tf.float32, time_major=False)

        return outputs

    print(x.shape)
    set_input_shape(x, params)
    x = layers.batch_norm(x, is_training=is_training)
    x = layers.conv2d(
        x, 16, 3, 1,
        activation_fn=tf.nn.elu,
        normalizer_fn=layers.batch_norm if params.use_batch_norm else None,
        normalizer_params={'is_training': is_training}
    )
    x = layers.conv2d(
        x, 1, 1, 1,
        activation_fn=tf.nn.elu,
        normalizer_fn=layers.batch_norm if params.use_batch_norm else None,
        normalizer_params={'is_training': is_training}
    )

    x = tf.squeeze(x)
    x = rnn(x, 256, params.lstm_layer, params.keep_prob, bidirectional=params.bidirectional)
    x = tf.reduce_sum(x, axis=1)
    # we can use conv2d 1x1 instead of dense
    logits = layers.fully_connected(x, params.num_classes, activation_fn=None)
    return logits


def baseline(x, params, is_training):
    x = layers.batch_norm(x, is_training=is_training)
    for i in range(5):
        x = layers.conv2d(
            x, 1 * (2 ** i), 3, 1,
            activation_fn=tf.tanh,
            normalizer_fn=layers.batch_norm if params.use_batch_norm else None,
            normalizer_params={'is_training': is_training}
        )
        x = layers.conv2d(
            x, 1 * (2 ** i), 3, 1,
            activation_fn=tf.tanh,
            normalizer_fn=layers.batch_norm if params.use_batch_norm else None,
            normalizer_params={'is_training': is_training}
        )
        x = layers.max_pool2d(x, 2, 2)

    x = tf.reduce_max(x, axis=[1, 2], keep_dims=True)

    # we can use conv2d 1x1 instead of dense
    x = layers.conv2d(x, 128, 1, 1, activation_fn=tf.tanh)
    x = tf.nn.dropout(x, keep_prob=params.keep_prob if is_training else 1.0)

    # again conv2d 1x1 instead of dense layer
    logits = layers.conv2d(x, 1, 1, 1, activation_fn=tf.tanh)
    return tf.squeeze(logits, [1, 2, 3])


def get_model(params):
    if params.model == 'cnn':
        return baseline
    elif params.model == 'lstm':
        return baseline_rnn
    elif params.model == 'inception':
        return baseline_inception

    return baseline
