from apscheduler.schedulers.background import BackgroundScheduler
from preprocess import reload
from XGboost import train_model
import os
import comtypes.client
import datetime
import pythoncom
import time

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
        if stat == 0:
            print('【 GetUserAccount successful 】')
        else:
            print('【 GetUserAccount error code:' + str(stat) + '】')
    except Exception as e:
        print("error！", e)


def get_open_position():
    try:
        global_now_open_position = list()
        stat = skO.GetOpenInterest(global_id, global_account[0])
        if stat == 0:
            print('【 GetOpenPosition successful 】')
        else:
            print('【 GetOpenPosition error code:' + str(stat) + '】')
    except Exception as e:
        print("error!", e)


class SKOrderLibEvent:
    def OnAccount(self, bstrLogInID, bstrAccountData):
        strValues = bstrAccountData.split(',')
        strAccount = strValues[1] + strValues[3]
        if strValues[0] == 'TF':
            global_account.append(strAccount)

    def OnOpenInterest(self, bstrData):
        tmp = bstrData.split(',')
        if tmp[0] != '##':
            global_now_open_position.append(tmp)

    def OnFutureRights(self, bstrData):
        print(bstrData.split(','))


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
        order.bstrFullAccount = global_account[0]
        order.bstrStockNo = 'MTX00'
        order.sBuySell = buy_sell
        order.sTradeType = 1
        order.sDayTrade = 0
        order.bstrPrice = 'M'
        order.nQty = 1
        order.sNewClose = 2
        order.sReserved = 0
        message, stat = skO.SendFutureOrderCLR(global_id, b_async_order, order)
        if stat == 0:
            print('【 SendingOrder successful 】')
        else:
            print('【 SendingOrder error code:' + str(stat) + '】')
    except Exception as e:
        print("error！", e)


def daily_open_position():
    print(datetime.datetime.now(), ' is opening position ...')
    print('daily position: ', global_daily_position)
    if global_daily_position == 1:
        send_order(0)
    elif global_daily_position == -1:
        send_order(1)


def daily_close_position():
    print(datetime.datetime.now(), ' is closing position ...')
    get_open_position()
    while True:
        pythoncom.PumpWaitingMessages()
        if global_now_open_position:
            break
        time.sleep(0.5)
    if 'NO DATA' in global_now_open_position[0][0]:
        return
    elif global_now_open_position[0][3] == 'B':
        send_order(1)
    elif global_now_open_position[0][3] == 'S':
        send_order(0)


def daily_stop_loss_order(stop_loss_point=85):
    print('sending stop loss order ...')
    try:
        get_open_position()
        while True:
            pythoncom.PumpWaitingMessages()
            if global_now_open_position:
                break
            time.sleep(0.5)
        order = sk.FUTUREORDER()
        order.bstrFullAccount = global_account[0]
        order.bstrStockNo = 'MTX00'
        order.sTradeType = 1
        order.sDayTrade = 0
        order.bstrPrice = 'M'
        order.nQty = 1
        order.sNewClose = 2
        order.sReserved = 0
        if 'NO DATA' in global_now_open_position[0][0]:
            return
        elif global_now_open_position[0][3] == 'B':
            stop_loss_price = str(int(global_now_open_position[0][6])/1000-stop_loss_point)
            print('Stop loss point: ', stop_loss_price)
            order.bstrTrigger = stop_loss_price
            order.sBuySell = 1
        elif global_now_open_position[0][3] == 'S':
            stop_loss_price = str(int(global_now_open_position[0][6])/1000+stop_loss_point)
            print('Stop loss point: ', stop_loss_price)
            order.bstrTrigger = stop_loss_price
            order.sBuySell = 0

        message, stat = skO.SendFutureStopLossOrder(global_id, False, order)
        if stat == 0:
            print('【 SendFutureStopLossOrder successful 】')
        else:
            print('【 SendFutureStopLossOrder error code:' + str(stat) + '】')
    except Exception as e:
        print("error！", e)


def daily_reload_train_model():
    # reload()
    global global_daily_position
    global_daily_position = train_model()
    global_daily_position = -1
    print('daily prediction:', global_daily_position)


if __name__ == '__main__':
    global global_account, global_now_open_position
    global_account = []
    global_now_open_position = []
    SKOrderEvent = SKOrderLibEvent()
    SKOrderLibEventHandler = comtypes.client.GetEvents(skO, SKOrderEvent)
    skC.SKCenterLib_ResetServer("morder1.capital.com.tw")  # Simulation platform
    login()
    order_initialize()
    read_certificate()
    get_account()
    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_reload_train_model, 'cron', hour=7, minute=20, second=0)
    scheduler.add_job(daily_open_position, 'cron', hour=8, minute=40, second=0)
    scheduler.add_job(daily_stop_loss_order, 'cron', hour=8, minute=46, second=0)
    scheduler.add_job(daily_close_position, 'cron', hour=13, minute=44, second=58)
    scheduler.start()
    pythoncom.PumpMessages()

