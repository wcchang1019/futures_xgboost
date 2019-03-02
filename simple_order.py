from apscheduler.schedulers.blocking import BlockingScheduler
from preprocess import reload
from XGboost import train_model
import os
import comtypes.client
import datetime
comtypes.client.GetModule('dll/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
skC = comtypes.client.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
skO = comtypes.client.CreateObject(sk.SKOrderLib, interface=sk.ISKOrderLib)


def login():
    try:
        skC.SKCenterLib_SetLogPath(os.path.split(os.path.realpath(__file__))[0] + "\\CapitalLog_Order")
        with open('confidential.txt') as f:
            confidential = f.readlines()
        confidential = [x.strip() for x in confidential]
        global global_id
        global_id = confidential[0]
        stat = skC.SKCenterLib_login(confidential[0], confidential[1])
        if stat == 0:
            print('【 Login successful 】')
        else:
            print("Login", stat, "Login")
    except Exception as e:
        print("error！", e)


def order_initialize():
    try:
        stat = skO.SKOrderLib_Initialize()
        if stat == 0:
            print("【 Order initialize successful 】")
        else:
            print("Order", stat, "SKOrderLib_Initialize")
    except Exception as e:
        print("error！", e)


def read_certificate():
    try:
        stat = skO.ReadCertByID(global_id)
        if stat == 0:
            print('【 Read certificate successful 】')
        else:
            print("Order", stat, "ReadCertByID")
    except Exception as e:
        print("error！", e)


def get_account():
    try:
        stat = skO.GetUserAccount()
        print("Order", stat, "GetUserAccount")
    except Exception as e:
        print("error！", e)


class SKOrderLibEvent:
    __account_list = dict(
        stock=[],
        future=[],
        sea_future=[],
        foreign_stock=[],
    )

    def OnAccount(self, bstrLogInID, bstrAccountData):
        strValues = bstrAccountData.split(',')
        strAccount = strValues[1] + strValues[3]
        if strValues[0] == 'TS':
            SKOrderLibEvent.__account_list['stock'].append(strAccount)
        elif strValues[0] == 'TF':
            SKOrderLibEvent.__account_list['future'].append(strAccount)
        elif strValues[0] == 'OF':
            SKOrderLibEvent.__account_list['sea_future'].append(strAccount)
        elif strValues[0] == 'OS':
            SKOrderLibEvent.__account_list['foreign_stock'].append(strAccount)
    global global_account_list
    global_account_list = __account_list


def send_order(buy_sell, b_async_order=False):
    print(datetime.datetime.now(), ' is sending order ......')
    try:
        '''
        Buy: sBuySell = 0 , Sell: sBuySell = 1
        ROD: sTradeType = 0, IOC: sTradeType = 1, FOK: sTradeType = 2
        非當沖: sDayTrade = 0, 當沖: sDayTrade = 1
        新倉: sNewClose = 0, 平倉: sNewClose = 1, 自動: sNewClose = 2
        盤中: sReserved = 0, T盤預約: sReserved = 1
        '''
        order = sk.FUTUREORDER()
        order.bstrFullAccount = global_account_list['future'][0]
        order.bstrStockNo = 'MTX00'
        order.sBuySell = buy_sell
        order.sTradeType = 1
        order.sDayTrade = 0
        order.bstrPrice = 'M'
        order.nQty = 1
        order.sNewClose = 2
        order.sReserved = 0
        message, stat = skO.SendFutureOrderCLR(global_id, b_async_order, order)
        print("Order", stat, "SendFutureOrderCLR")
    except Exception as e:
        print("error！", e)


def daily_open_position():
    position = train_model()
    print('daily position: ', position)
    if position == 1:
        send_order(0)
    elif position == -1:
        send_order(1)


def daily_close_position():
    print('closing position ...')
    position = train_model()
    if position == 1:
        send_order(1)
    elif position == -1:
        send_order(0)


if __name__ == '__main__':
    login()
    order_initialize()
    read_certificate()
    SKOrderEvent = SKOrderLibEvent()
    SKOrderLibEventHandler = comtypes.client.GetEvents(skO, SKOrderEvent)
    get_account()
    scheduler = BlockingScheduler()
    scheduler.add_job(reload, 'cron', hour=7, minute=40, second=0)
    scheduler.add_job(daily_open_position, 'cron', hour=8, minute=40, second=0)
    scheduler.add_job(daily_close_position, 'cron', hour=13, minute=44, second=58)
    scheduler.start()

