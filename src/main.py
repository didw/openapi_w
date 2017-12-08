import os
import argparse

DATADIR = '../data' # unzipped train and test data


from glob import glob
import tensorflow as tf
from handle_model import create_model
from data import load_data, get_data_generator, get_test_data_generator, POSSIBLE_LABELS, id2name

sanity = False
tf.logging.set_verbosity(tf.logging.DEBUG)

def get_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', action='store',
                        dest='outdir',
                        default=None,
                        help='model_output_directory',
                        )
    parser.add_argument('--gpu', action='store',
                        dest='gpu',
                        default='0',
                        help='assigned GPU',
                        )
    parser.add_argument('--predict_only', action='store_true',
                        dest='predict_only',
                        help='training/eval skipped',
                        )
    parser.add_argument('--bidirectional', action='store_true',
                        dest='bidirectional',
                        help='bidirectional or not',
                        )
    parser.add_argument('--feature', action='store',
                        dest='feature',
                        default='mel_spectrogram',
                        help='[mel_spectrogram, mfcc, spectrogram, log_spectrogram]',
                        )
    parser.add_argument('--model', action='store',
                        dest='model',
                        default='cnn',
                        help='[cnn, lstm, cnn_lstm]',
                        )
    parser.add_argument('--lstm_layer', action='store',
                        dest='lstm_layer',
                        type=int,
                        default=6,
                        help='the number of LSTM layers',
                        )

    return parser.parse_args()


def get_hparam_config(model_dir, parser):
    params=dict(
        seed=2018,
        batch_size=128,
        keep_prob=0.8,
        learning_rate=1e-4,
        clip_gradients=15.0,
        use_batch_norm=True,
        num_classes=len(POSSIBLE_LABELS),
        features=parser.feature,
        lstm_layer=parser.lstm_layer,
        bidirectional=parser.bidirectional,
        model=parser.model,

    )

    hparams = tf.contrib.training.HParams(**params)

    if not os.path.exists(os.path.join(model_dir, 'eval')):
        os.makedirs(os.path.join(model_dir, 'eval'))

    run_config = tf.contrib.learn.RunConfig(model_dir=model_dir)

    return hparams, run_config


def model_predict(model, model_dir, hparams, output_filename):
    from tqdm import tqdm
    #paths = glob(os.path.join(DATADIR, 'test/audio/*wav'))
    test_input_fn = get_test_data_generator('A005930', hparams)
    it = model.predict(input_fn=test_input_fn)

    print("done for Prediction")
    simulation = list()
    balance = 0
    for t in tqdm(it):
        pred, tlabel, cur, future, buy, sell = t['pred'], t['target'], t['cur'], t['future'], t['buy'], t['sell']
        simulation.append((pred,tlabel,cur,future,buy,sell))

    nstock = 0
    with open(os.path.join(model_dir, output_filename), 'w') as fout:
        fout.write('pred,tlabel,cur,future,buy,sell,balance,total\n')
        for pred, tlabel, cur, future, buy, sell in simulation:
            if nstock > 0 and pred < 0:
                balance += sell
                nstock -= 1
            if pred > 0 and nstock == 0:
                balance -= buy
                nstock += 1
            fout.write('{},{},{},{},{},{},{},{}({})\n'.format(pred, tlabel, cur, future, buy, sell, balance, balance+nstock*cur, nstock))



def main():

    # parsing the argument
    parser = get_arg()
    if parser.outdir == None:
        print('outdir should be set')
        exit(-1)
    out_dir = parser.outdir
    os.environ["CUDA_VISIBLE_DEVICES"] = parser.gpu

    # load data
    print("load data")
    #trainset, noise, valset = load_data(DATADIR, sanity_check=sanity)

    # config
    print("config")
    hparams, run_config = get_hparam_config(out_dir, parser)

    # data generator
    print("data_generator")
    train_input_fn, val_input_fn = get_data_generator('A005930', hparams)

    # build experiment
    print("build experiment")
    def _create_my_experiment(run_config, hparams):
        exp = tf.contrib.learn.Experiment(
            estimator=create_model(config=run_config, hparams=hparams),
            train_input_fn=train_input_fn,
            eval_input_fn=val_input_fn,
            train_steps=200000, # just randomly selected params
            eval_steps=3000,  # read source code for steps-epochs ariphmetics
            train_steps_per_iteration=10000,
        )
        return exp


    # run
    if parser.predict_only == False:
        print("run model!!!!!")
        tf.contrib.learn.learn_runner.run(
            experiment_fn=_create_my_experiment,
            run_config=run_config,
            schedule="continuous_train_and_eval",
            hparams=hparams)

    # prediction
    print("prediction")
    model = create_model(config=run_config, hparams=hparams)
    model_predict(model, out_dir, hparams, "simulation.csv")


if __name__ == "__main__":
    main()
