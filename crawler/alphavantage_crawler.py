import requests
import pandas as pd
import os


def get_us_stock_price(symbol_name):
    abspath = os.path.split(os.path.split(__file__)[0])[0]
    api_key = 'Q1UMSAG8895UV7NP'
    r = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + symbol_name +
                     '&outputsize=full&apikey=' + api_key)
    df = pd.DataFrame.from_dict(r.json()['Time Series (Daily)'], orient='index')
    col_name = []
    df.index.name = 'date'
    for x in list(df.columns):
        try:
            col_name.append(x.split(' ')[1])
        except IndexError:
            pass
    df.columns = col_name
    df.reset_index(inplace=True)
    df['date'] = [x.replace('-', '/') for x in df['date']]
    df.to_csv(abspath + '/csv/daily_' + symbol_name + '.csv', index=False)
    print(df)


def daily_us_stock_reload():
    print('DJI price is crawling ...')
    get_us_stock_price('DJI')
    print('NDX price is crawling ...')
    get_us_stock_price('NDX')
    print('SPX price is crawling ...')
    get_us_stock_price('SPX')


if __name__ == '__main__':
    get_us_stock_price('DJI')
