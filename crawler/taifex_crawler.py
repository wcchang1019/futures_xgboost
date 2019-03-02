# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
import time
import pandas as pd
import os
abspath = os.path.split(os.path.split(__file__)[0])[0]


def get_big3_futures(date, contract, debug=True):
    """ Get big3 futures table from the website.

    http://www.taifex.com.tw/cht/3/futContractsDate
    (三大法人區分各期貨契約)

    Args:
        date: date in datetime.date() format
        contract: contract in string format
    Return:
        target_table: an array with all the values
    """
    available_contract = ['TXF', 'EXF', 'FXF', 'MXF', 'T5F', 'STF',
                          'ETF', 'GTF', 'XIF', 'TJF', 'I5F', 'SPF', 'UDF']
    if contract not in available_contract:
        print('Parse taifex big3 futures: Invalid contract type')
        return False, list()

    url = 'http://www.taifex.com.tw/cht/3/futContractsDate'
    data = {'queryType': 1,
            'doQuery': 1,
            'commodityId': contract,
            'queryDate': str(date.year) + '/' + str(date.month).zfill(2) + '/' + str(date.day).zfill(2)}

    try:
        r = requests.post(url, data=data)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all('table')
        # return tables
        target_table = tables[2]
        target_table = [item.text for item in target_table.find_all('td', {'bgcolor': '#FFFFF0'})]
        if len(target_table) == 0:
            if debug:
                print('Parse taifex big3 futures: Target table is empty')
            return False, list()
        ignore_list = ['\r', '\n', '\t', ',', ' ']
        for ignore in ignore_list:
            target_table = [item.replace(ignore, '') for item in target_table]
        return True, target_table
    except Exception as e:
        if debug:
            print('Parse taifex big3 futures: ' + str(e))
        return False, list()


def get_big3_options(date, contract, debug=True):
    """ Get big3 options table from the website.

    http://www.taifex.com.tw/cht/3/callsAndPutsDate
    (三大法人選擇權買賣權分計)

    Args:
        date: date in datetime.date() format
        contract: contract in string format
    Return:
        target_table: an array with all the values
    """
    available_contract = ['TXO', 'TEO', 'TFO', 'STO', 'ETC', 'GTO', 'XIO']
    if contract not in available_contract:
        print('Parse taifex big3 options: Invalid contract type')
        return False, list()

    url = 'http://www.taifex.com.tw/cht/3/callsAndPutsDate'
    data = {'queryType': 2,
            'marketCode': 1,
            'commodity_id': contract,
            'MarketCode': 1,
            'commodity_idt': contract,
            'queryDate': str(date.year) + '/' + str(date.month).zfill(2) + '/' + str(date.day).zfill(2)}
    try:
        r = requests.post(url, data)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all('table')
        target_table = tables[2]
        target_table = [item.text for item in target_table.find_all('td', {'bgcolor': '#FFFFF0'})]
        if len(target_table) == 0:
            if debug:
                print('Parse taifex big3 options: Target table is empty')
            return False, list()
        ignore_list = ['\r', '\n', '\t', ',', ' ']
        for ignore in ignore_list:
            target_table = [item.replace(ignore, '') for item in target_table]
        return True, target_table
    except Exception as e:
        if debug:
            print('Parse taifex big3 options: ' + str(e))
        return False, list()


def get_large_trader_futures(date, contract, debug=True):
    """ Get large trader futures table from the website.

    http://www.taifex.com.tw/cht/3/largeTraderFutQry
    (期貨大額交易人未沖銷部位結構表)

    Args:
        date: date in datetime.date() format
        contract: contract in string format
    Return:
        True/False:
            True: Parse the website successfully
            False: Fail to parse the website
        target_table: an array with all the values
    """
    url = 'http://www.taifex.com.tw/cht/3/largeTraderFutQry'
    data = {'queryDate': str(date.year) + '/' + str(date.month).zfill(2) + '/' + str(date.day).zfill(2),
            'contractId': contract}

    try:
        r = requests.post(url, data)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all('table')
        target_table = tables[2]
        target_table = [item.text for item in target_table.find_all('td', {'bgcolor': 'ivory', 'class': '11b'})]
        if len(target_table) == 0:
            if debug:
                print('Parse taifex large trader futures: Target table is empty')
            return False, list()

        ignore_list = ['\r', '\n', '\t', ',', ' ', '%']
        for ignore in ignore_list:
            target_table = [item.replace(ignore, '') for item in target_table]

        # 除去每格下方的括號內容
        for idx, item in enumerate(target_table):
            if '(' in item:
                target_table[idx] = item[:item.find('(')]
        return True, target_table
    except Exception as e:
        if debug:
            print('Parse taifex large trader futures: ' + str(e))
        return False, list()


def get_large_trader_options(date, contract, debug=True):
    """ Get large trader options table from the website.

    http://www.taifex.com.tw/cht/3/largeTraderOptQry
    (選擇權大額交易人未沖銷部位結構)

    Args:
        date: date in datetime.date() format
        contract: contract in string format
    Return:
        True/False:
            True: Parse the website successfully
            False: Fail to parse the website
        target_table: an array with all the values (前半買權，後半賣權)
    """
    url = 'http://www.taifex.com.tw/cht/3/largeTraderOptQry'
    data = {'queryDate': str(date.year) + '/' + str(date.month).zfill(2) + '/' + str(date.day).zfill(2),
            'contractId': contract}
    try:
        r = requests.post(url, data)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all("table")

        target_table = tables[2]
        target_table_buy = [item.text for item in target_table.find_all('td', {'bgcolor': 'ivory', 'class': '11b'})]
        target_table_sell = [item.text for item in target_table.find_all('td', {'bgcolor': '#CFDFEF', 'class': '11b'})]

        if len(target_table_buy) == 0 or len(target_table_sell) == 0:
            if debug:
                print('Parse taifex large trader options: Target table is empty')
            return False, list()

        ignore_list = ['\r', '\n', '\t', ',', ' ', '%']
        for ignore in ignore_list:
            target_table_buy = [item.replace(ignore, '') for item in target_table_buy]
            target_table_sell = [item.replace(ignore, '') for item in target_table_sell]

            # 除去每格下方的括號內容
        for idx, item in enumerate(target_table_buy):
            if '(' in item:
                target_table_buy[idx] = item[:item.find('(')]
        for idx, item in enumerate(target_table_sell):
            if '(' in item:
                target_table_sell[idx] = item[:item.find('(')]

        target_table = target_table_buy + target_table_sell
        return True, target_table
    except Exception as e:
        if debug:
            print('Parse taifex large trader options: ' + str(e))
        return False, list()


def get_futures_price(date, contract, session, debug=True):
    """ Get futures price table from the website.

    http://www.taifex.com.tw/cht/3/futDailyMarketReport
    (期貨每日交易行情)

    Args:
        date: date in datetime.date() format
        contract: contract in string format
        session: trading session
            regular: 一般交易時段
            ah: 盤後交易時段(after hours)

    Return:
        True/False:
            True: Parse the website successfully
            False: Fail to parse the website
        target_table: an array with all the values
    """

    url = 'http://www.taifex.com.tw/cht/3/futDailyMarketReport'

    if session == 'regular':
        market_code = 0
    elif session == 'ah':
        market_code = 1
    else:
        print('Parse taifex futures price: Invalid session type')
        return False, list()

    data = {'queryType': 2,
            'marketCode': market_code,
            'commodity_id': contract,
            'MarketCode': market_code,
            'commodity_idt': contract,
            'queryDate': str(date.year) + '/' + str(date.month).zfill(2) + '/' + str(date.day).zfill(2)}

    try:
        r = requests.post(url, data)
        r.encoding = 'utf8'
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all("table")
        target_table = tables[4]
        target_table = [item.text for item in target_table.find_all('td', {'class': ['12bk', '12green']})]
        if len(target_table) == 0:
            if debug:
                print('Parse taifex futures price: Target table is empty')
            return False, list()

        ignore_list = ['\r', '\n', '\t', ',', ' ', '▼', '▲', '%']
        for ignore in ignore_list:
            target_table = [item.replace(ignore, '') for item in target_table]

        # 部分表格為'-'
        for i, v in enumerate(target_table):
            if v == '-':
                target_table[i] = None

        return True, target_table
    except Exception as e:
        if debug:
            print('Parse taifex futures price: ' + str(e))
        return False, list()


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def daily_futures_large_trader_reload():
    df = pd.read_csv(abspath + '/csv/daily_TXF_large_trader.csv')
    file_end_date = list(df['date'])[-1]
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    end_dt = datetime.date.today()
    all_large_trader = list()
    for dt in date_range(start_dt, end_dt):
        a, b = get_large_trader_futures(dt, 'TX')
        if a:
            begin = b.index('所有契約')
            daily_large_trader = [dt.strftime('%Y/%m/%d')] + b[begin+1:]
            all_large_trader.append(daily_large_trader)
            print(dt)
        time.sleep(2)
    col = ['date', 'top5_b_v', 'top5_b_ratio', 'top10_b_v', 'top10_b_ratio',
           'top5_s_v', 'top5_s_ratio', 'top10_s_v', 'top10_s_ratio',
           'total_unfilled'
           ]
    df2 = pd.DataFrame(all_large_trader, columns=col)
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.to_csv(abspath + '/csv/daily_TXF_large_trader.csv', index=False)


def daily_txf_reload():
    df = pd.read_csv(abspath + '/csv/daily_TXF.csv')
    file_end_date = list(df['date'])[-1]
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    end_dt = datetime.date.today()
    all_daily_price = list()
    for dt in date_range(start_dt, end_dt):
        a, b = get_futures_price(dt, 'TX', 'regular')
        if a:
            daily_price = [dt.strftime('%Y/%m/%d')] + b[2:13]
            all_daily_price.append(daily_price)
            print(dt)
        time.sleep(2)
    col = ['date', 'open', 'high', 'low', 'close', 'up_down', 'up_down_ratio',
           'ah_v', 'r_v', 'total_v', 'final_price', 'unfilled_v']
    df2 = pd.DataFrame(all_daily_price, columns=col)
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.to_csv(abspath + '/csv/daily_TXF.csv', index=False)


def daily_futures_big3_reload(symbol_name):
    df = pd.read_csv(abspath + '/csv/daily_' + symbol_name + '_big3.csv')
    file_end_date = list(df['date'])[-1]
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    end_dt = datetime.date.today()
    all_daily_big3 = list()
    for dt in date_range(start_dt, end_dt):
        a, b = get_big3_futures(dt, symbol_name)
        if a:
            x = b.index('自營商')
            y = b.index('投信')
            z = b.index('外資')
            end = b.index('期貨合計')
            daily_big3 = [dt.strftime('%Y/%m/%d')] + b[x+1:y] + b[y+1:z] + b[z+1:end]
            all_daily_big3.append(daily_big3)
            print(dt)
        time.sleep(2)
    col = ['date',
           'd_total_b_v', 'd_total_b_m', 'd_total_s_v', 'd_total_s_m', 'd_total_bs_v', 'd_total_bs_m',
           'd_unfilled_b_v', 'd_unfilled_b_m', 'd_unfilled_s_v', 'd_unfilled_s_m', 'd_unfilled_bs_v', 'd_unfilled_bs_m',
           't_total_b_v', 't_total_b_m', 't_total_s_v', 't_total_s_m', 't_total_bs_v', 't_total_bs_m',
           't_unfilled_b_v', 't_unfilled_b_m', 't_unfilled_s_v', 't_unfilled_s_m', 't_unfilled_bs_v', 't_unfilled_bs_m',
           'f_total_b_v', 'f_total_b_m', 'f_total_s_v', 'f_total_s_m', 'f_total_bs_v', 'f_total_bs_m',
           'f_unfilled_b_v', 'f_unfilled_b_m', 'f_unfilled_s_v', 'f_unfilled_s_m', 'f_unfilled_bs_v', 'f_unfilled_bs_m'
           ]
    df2 = pd.DataFrame(all_daily_big3, columns=col)
    df2 = df2[['date', 'd_unfilled_b_v', 'd_unfilled_s_v', 't_unfilled_b_v',
               't_unfilled_s_v', 'f_unfilled_b_v', 'f_unfilled_s_v']]
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.to_csv(abspath + '/csv/daily_' + symbol_name + '_big3.csv', index=False)


def daily_option_large_trader_reload():
    df = pd.read_csv(abspath + '/csv/daily_TXO_large_trader.csv')
    file_end_date = list(df['date'])[-1]
    start_dt = datetime.datetime.strptime(file_end_date, "%Y/%m/%d").date() + timedelta(days=1)
    end_dt = datetime.date.today()
    all_large_trader = list()
    for dt in date_range(start_dt, end_dt):
        a, b = get_large_trader_options(dt, 'TXO')
        if a:
            begin = b.index('所有契約')
            daily_large_trader = [dt.strftime('%Y/%m/%d')] + b[begin+1:begin+9]
            b[begin] = 0
            begin = b.index('所有契約')
            daily_large_trader += b[begin+1:begin+9]
            all_large_trader.append(daily_large_trader)
            print(dt)
        time.sleep(2)
    col = ['date', 'call_top5_b_v', 'call_top5_b_ratio', 'call_top10_b_v', 'call_top10_b_ratio',
           'call_top5_s_v', 'call_top5_s_ratio', 'call_top10_s_v', 'call_top10_s_ratio',
           'put_top5_b_v', 'put_top5_b_ratio', 'put_top10_b_v', 'put_top10_b_ratio',
           'put_top5_s_v', 'put_top5_s_ratio', 'put_top10_s_v', 'put_top10_s_ratio'
           ]
    df2 = pd.DataFrame(all_large_trader, columns=col)
    result = pd.concat([df, df2])
    result.reset_index(drop=True, inplace=True)
    result.to_csv(abspath + '/csv/daily_TXO_large_trader.csv', index=False)


def daily_tw_futures_reload():
    print('txf price is crawling ...')
    daily_txf_reload()
    print('txf big3 is crawling ...')
    daily_futures_big3_reload('TXF')
    print('mxf big3 is crawling ...')
    daily_futures_big3_reload('MXF')
    print('txf large trader is crawling ...')
    daily_futures_large_trader_reload()
    print('option large trader is crawling ...')
    daily_option_large_trader_reload()

