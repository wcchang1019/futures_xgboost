#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from xgboost import XGBClassifier
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import matplotlib as mpl
from matplotlib.ticker import MultipleLocator, FuncFormatter
from sklearn.metrics import accuracy_score


def plot_performance(df, y_pred, model_name):
    ans = list(df['target_points']*y_pred-2)
    trades_count = sum(y_pred != 0)
    win_count = len([x for x in ans if x > 0])
    total = list(np.cumsum(ans))
    dd = []
    for x in ans:
        if len(dd) == 0:
            if x > 0:
                dd.append(0)
            else:
                dd.append(x)
        elif dd[-1] + x > 0:
            dd.append(0)
        else:
            dd.append(dd[-1] + x)
    dates = list(df['date'])
    xs = [datetime.strptime(d, '%Y/%m/%d').date() for d in dates]
    highest_x = []
    highest_dt = []
    for i in range(len(total)):
        if total[i] == max(total[:i+1]) and total[i] > 0:
            highest_x.append(total[i])
            highest_dt.append(i)
    mpl.style.use('seaborn')
    f, axarr = plt.subplots(2, sharex=True, figsize=(20, 12), gridspec_kw = {'height_ratios':[3, 1]})
    axarr[0].plot(np.arange(len(xs)), total, color='b', zorder=1)
    axarr[0].scatter(highest_dt, highest_x, color='lime', marker='o', s=40, zorder=2)
    axarr[0].set_title(str(model_name) + ' Equity Curve', fontsize=20)
    axarr[1].bar(np.arange(len(xs)), dd, color='red')
    date_tickers = df.date.values
    def format_date(x,pos=None):
        if x < 0 or x > len(date_tickers)-1:
            return ''
        return date_tickers[int(x)]
    axarr[0].xaxis.set_major_locator(MultipleLocator(80))
    axarr[0].xaxis.set_major_formatter(FuncFormatter(format_date))
    axarr[0].grid(True)
    shift = (max(total)-min(total))/20
    text_loc = max(total)-shift
    axarr[0].text(np.arange(len(xs))[5], text_loc, 'Total trades: %d'%trades_count, fontsize=15)
    axarr[0].text(np.arange(len(xs))[5], text_loc-shift, 'Win ratio: %.2f'%(win_count/trades_count), fontsize=15)
    axarr[0].text(np.arange(len(xs))[5], text_loc-shift*2, 'Total profit points: %d'%total[-1], fontsize=15)
    axarr[0].text(np.arange(len(xs))[5], text_loc-shift*3, 'Average profit points: %.2f'%(total[-1]/trades_count), fontsize=15)
    axarr[0].text(np.arange(len(xs))[5], text_loc-shift*4, 'Max drawdown: %d'%min(dd), fontsize=15)
    plt.show()


def train_model(plot=False):
    df = pd.read_csv('data.csv')
    tmp = list(df)
    df = df.drop(columns=tmp[11:14] + tmp[16:20] + tmp[23:26] + tmp[28:32])
    ratio = 0.8
    thres = int(len(df)*ratio)
    x_train = df.iloc[:thres, 1:-2]
    x_test = df.iloc[thres:, 1:-2]
    y_train = df.iloc[:thres, -1]
    y_test = df.iloc[thres:, -1]
    model = XGBClassifier(learning_rate=0.005, n_estimators=500, max_depth=13, min_child_weight=1, gamma=0.9,
                          subsample=0.5, objective='multi:softprob', random_state=27, n_jobs=-1, silent=1)
    model.fit(x_train, y_train, eval_set=[(x_test, y_test)], eval_metric="merror", early_stopping_rounds=50, verbose=True)
    print('training set score: %.3f' % accuracy_score(model.predict(x_train), y_train))
    print('testing set score: %.3f' % accuracy_score(model.predict(x_test), y_test))
    y_pred = model.predict(x_test)
    if plot:
        plot_performance(df.loc[x_test.index], y_pred, 'XGboost')
    return y_pred[-1]


if __name__ == '__main__':
    print(train_model())
