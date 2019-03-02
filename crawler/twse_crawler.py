# -*- coding: utf-8 -*-
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import datetime
import json
from datetime import timedelta
import os
abspath = os.path.split(os.path.split(__file__)[0])[0]


def twse_crawler(start_year, start_month):
    df = pd.DataFrame()
    df2 = pd.DataFrame()
    now = datetime.datetime.now()
    x = start_year
    y = start_month
    while x != now.year or y != now.month+1:
        if y > 12:
            x = x + 1
            y = 1
        print(x, y)
        r = requests.get('http://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date='+str(x)+str(y).zfill(2)
                         + '01')
        j = json.loads(r.text)
        if j['stat'] == 'OK':
            df = pd.concat([df, pd.DataFrame(columns=j['fields'], data=j['data'])])
        try:
            tmp = pd.read_html('http://www.twse.com.tw/exchangeReport/FMTQIK?response=html&date='
                                + str(x) + str(y).zfill(2) + '01')
            df2 = pd.concat([df2, tmp[0].iloc[:, 1:4]])
        except:
            print('can not crawl twse report')
        y = y + 1
        time.sleep(5)

    if df.empty and df2.empty:
        return pd.DataFrame()
    df.rename(columns={'日期': 'date', '開盤指數': 'open', '最高指數': 'high', '最低指數': 'low', '收盤指數': 'close'}, inplace=True)
    df2.columns = ['volume', 'price', 'records']
    df['date'] = [str(int(x[:x.find('/')]) + 1911) + x[x.find('/'):] for x in df['date']]
    df.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    result = pd.concat([df, df2], axis=1, sort=False)
    return result


def daily_twse_reload():
    df = pd.read_csv(abspath + '/csv/daily_TWSE.csv')
    file_end_date = str(list(df['date'])[-1])
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    df2 = twse_crawler(start_dt.year, start_dt.month)
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.drop_duplicates(inplace=True)
    result.to_csv(abspath + '/csv/daily_TWSE.csv', index=False)


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def get_big3_trading_value(target_date, debug=True):
    """ Get big3 trading value from the website.

    http://www.twse.com.tw/zh/page/trading/fund/BFI82U.html
    http://www.twse.com.tw/fund/BFI82U?response=html&dayDate=20180920
    (三大法人買賣金額)

    Args:
        target_date: date in datetime.date() format

    Return:
        True/False:
            True: Parse the website successfully
            False: Fail to parse the website
        table: a list contains all values
    """
    year = str(target_date.year)
    month = str(target_date.month).zfill(2)
    day = str(target_date.day).zfill(2)

    try:
        url = 'http://www.twse.com.tw/fund/BFI82U?response=html&dayDate={}{}{}'.format(year, month, day)
        r = requests.post(url)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all('table')
        table_values = [item.text for item in tables[0].find_all('td')]
        table_values = [item.replace(',', '') for item in table_values]
        if len(table_values) == 24:
            return True, [int(table_values[7]) + int(table_values[11]), int(table_values[15]), int(table_values[19])]
        elif len(table_values) == 20:
            return True, [int(table_values[7]), int(table_values[11]), int(table_values[15])]
    except Exception as e:
        if debug:
            print('Parse big3 trading value table: ' + str(e))
        return False, list()


def daily_twse_big3_reload():
    all_big3 = list()
    df = pd.read_csv(abspath + '/csv/daily_TWSE_big3.csv')
    file_end_date = list(df['date'])[-1]
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    end_dt = datetime.date.today()
    for dt in date_range(start_dt, end_dt):
        a, b = get_big3_trading_value(dt)
        if a:
            big3 = [dt.strftime('%Y/%m/%d')] + b
            all_big3.append(big3)
            print(dt)
        time.sleep(5)
    col = ['date', 'dealer_diff', 'trust_diff', 'foreign_diff']
    df2 = pd.DataFrame(all_big3, columns=col)
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.to_csv(abspath + '/csv/daily_TWSE_big3.csv', index=False)


def daily_tw_stock_reload():
    print('twse price is crawling ...')
    daily_twse_reload()
    print('twse big3 is crawling ...')
    daily_twse_big3_reload()


if __name__ == '__main__':
    twse_crawler(2019, 3)
