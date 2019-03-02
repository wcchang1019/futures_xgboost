import pandas as pd
import math
from crawler.taifex_crawler import daily_tw_futures_reload
from crawler.twse_crawler import daily_tw_stock_reload
from crawler.alphavantage_crawler import daily_us_stock_reload


def reload():
    daily_us_stock_reload()
    daily_tw_stock_reload()
    daily_tw_futures_reload()


def us_tw_ratio_merge(tw, us):
        us = us.iloc[us[us['date'] == list(tw['date'])[0]].index[0]:, :]
        df = pd.merge(tw, us, on='date', how='outer')
        df.sort_values(by='date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        tmp = 0
        pos = 0
        for i in range(0, len(df)):
            if math.isnan(df.iloc[i, -1]):
                df.iloc[i, -1] = 0
            elif math.isnan(df.iloc[i, 1]):
                tmp += df.iloc[i, -1]
                if pos == 0:
                    pos = i-1
            else:
                df.iloc[pos, -1] += tmp
                tmp = 0
                pos = 0
        return df


def preprocess():
    txf = pd.read_csv('csv/daily_TXF.csv')
    txf = txf.iloc[:, [0, 1, 2, 3, 4, 11]]
    txf['TXF'] = (txf['close'] - txf['open']) / txf['open']

    twse = pd.read_csv('csv/daily_TWSE.csv')
    twse["open"] = twse["open"].str.replace(",", "").astype(float)
    twse["high"] = twse["high"].str.replace(",", "").astype(float)
    twse["low"] = twse["low"].str.replace(",", "").astype(float)
    twse["close"] = twse["close"].str.replace(",", "").astype(float)
    twse['TWSE'] = (twse['close'] - twse['open'])/twse['open']
    twse = twse.iloc[:, [0, 4, 8]]
    twse.columns = ['date', 'TWSE_close', 'TWSE']
    txf = pd.merge(txf, twse, how='left', on=['date'])
    txf['diff'] = txf['close'] - txf['TWSE_close']
    txf.drop(columns='TWSE_close', inplace=True)

    twse_big3 = pd.read_csv('csv/daily_TWSE_big3.csv')
    twse_big3.columns = ['date', 'TWSE_dealer_diff', 'TWSE_trust_diff', 'TWSE_foreign_diff']
    txf = pd.merge(txf, twse_big3, how='left', on=['date'])

    txf_big3 = pd.read_csv('csv/daily_TXF_big3.csv')
    txf_big3['TXF_dealer_diff'] = txf_big3.iloc[:, 1] - txf_big3.iloc[:, 2]
    txf_big3['TXF_trust_diff'] = txf_big3.iloc[:, 3] - txf_big3.iloc[:, 4]
    txf_big3['TXF_foreign_diff'] = txf_big3.iloc[:, 5] - txf_big3.iloc[:, 6]
    txf_big3 = txf_big3.iloc[:, [0, 7, 8, 9]]
    txf = pd.merge(txf, txf_big3, how='left', on=['date'])

    mxf_big3 = pd.read_csv('csv/daily_MXF_big3.csv')
    mxf_big3['MXF_dealer_diff'] = mxf_big3.iloc[:, 1] - mxf_big3.iloc[:, 2]
    mxf_big3['MXF_trust_diff'] = mxf_big3.iloc[:, 3] - mxf_big3.iloc[:, 4]
    mxf_big3['MXF_foreign_diff'] = mxf_big3.iloc[:, 5] - mxf_big3.iloc[:, 6]
    mxf_big3 = mxf_big3.iloc[:, [0, 7, 8, 9]]
    txf = pd.merge(txf, mxf_big3, how='left', on=['date'])

    txf_large_trader = pd.read_csv('csv/daily_TXF_large_trader.csv')
    txf_large_trader['TXF_top5_diff'] = txf_large_trader.iloc[:, 1] -  txf_large_trader.iloc[:, 5]
    txf_large_trader['TXF_top10_diff'] = txf_large_trader.iloc[:, 3] -  txf_large_trader.iloc[:, 7]
    txf_large_trader = txf_large_trader.iloc[:, [0, 10, 11]]
    txf = pd.merge(txf, txf_large_trader, how='left', on=['date'])

    txo_large_trader = pd.read_csv('csv/daily_TXO_large_trader.csv')
    txo_large_trader['TXO_call_top5_diff'] = txo_large_trader.iloc[:, 1] - txo_large_trader.iloc[:, 5]
    txo_large_trader['TXO_call_top10_diff'] = txo_large_trader.iloc[:, 3] - txo_large_trader.iloc[:, 7]
    txo_large_trader['TXO_put_top5_diff'] = txo_large_trader.iloc[:, 9] - txo_large_trader.iloc[:, 13]
    txo_large_trader['TXO_put_top10_diff'] = txo_large_trader.iloc[:, 11] - txo_large_trader.iloc[:, 15]
    txo_large_trader = txo_large_trader.iloc[:, [0, 17, 18, 19, 20]]
    txf = pd.merge(txf, txo_large_trader, how='left', on=['date'])

    txf['TXF_foreign_diff_diff'] = txf['TXF_foreign_diff'] - txf['TXF_foreign_diff'].shift(1)
    txf['TXF_trust_diff_diff'] = txf['TXF_trust_diff'] - txf['TXF_trust_diff'].shift(1)
    txf['TXF_dealer_diff_diff'] = txf['TXF_dealer_diff'] - txf['TXF_dealer_diff'].shift(1)

    txf['MXF_foreign_diff_diff'] = txf['MXF_foreign_diff'] - txf['MXF_foreign_diff'].shift(1)
    txf['MXF_trust_diff_diff'] = txf['MXF_trust_diff'] - txf['MXF_trust_diff'].shift(1)
    txf['MXF_dealer_diff_diff'] = txf['MXF_dealer_diff'] - txf['MXF_dealer_diff'].shift(1)

    txf['TXF_top5_diff_diff'] = txf['TXF_top5_diff'] - txf['TXF_top5_diff'].shift(1)
    txf['TXF_top10_diff_diff'] = txf['TXF_top10_diff'] - txf['TXF_top10_diff'].shift(1)

    txf['TXO_call_top5_diff_diff'] = txf['TXO_call_top5_diff'] - txf['TXO_call_top5_diff'].shift(1)
    txf['TXO_call_top10_diff_diff'] = txf['TXO_call_top10_diff'] - txf['TXO_call_top10_diff'].shift(1)
    txf['TXO_put_top5_diff_diff'] = txf['TXO_put_top5_diff'] - txf['TXO_put_top5_diff'].shift(1)
    txf['TXO_put_top10_diff_diff'] = txf['TXO_put_top10_diff'] - txf['TXO_put_top10_diff'].shift(1)

    txf['TWSE_foreign_diff_diff'] = txf['TWSE_foreign_diff'] - txf['TWSE_foreign_diff'].shift(1)
    txf['TWSE_trust_diff_diff'] = txf['TWSE_trust_diff'] - txf['TWSE_trust_diff'].shift(1)
    txf['TWSE_dealer_diff_diff'] = txf['TWSE_dealer_diff'] - txf['TWSE_dealer_diff'].shift(1)

    dji = pd.read_csv('csv/daily_DJI.csv')
    spx = pd.read_csv('csv/daily_SPX.csv')
    ndx = pd.read_csv('csv/daily_NDX.csv')
    for df in [dji, spx, ndx]:
        df['target'] = (df['close'] - df['open']) / df['open']

    dji = dji.iloc[:, [0, 6]]
    spx = spx.iloc[:, [0, 6]]
    ndx = ndx.iloc[:, [0, 6]]
    dji.columns = ['date', 'DJI']
    spx.columns = ['date', 'SPX']
    ndx.columns = ['date', 'NDX']
    txf = us_tw_ratio_merge(txf, dji)
    txf = us_tw_ratio_merge(txf, spx)
    txf = us_tw_ratio_merge(txf, ndx)
    drop_rows = list(txf[txf.isnull().any(axis=1)].index)
    txf.drop(txf.index[drop_rows], inplace=True)

    txf['target_points'] = (txf['close'] - txf['open'])
    txf.target_points = txf.target_points.shift(-1)
    txf = txf.drop(columns=['open', 'high', 'low', 'close'])
    txf.fillna(0, inplace=True)
    txf['label'] = txf['target_points'].apply(lambda x: 0 if (x > -5 and x < 5) else int(x/abs(x)))
    txf.to_csv('data.csv', index=False)


if __name__ == '__main__':
    reload()
    preprocess()
