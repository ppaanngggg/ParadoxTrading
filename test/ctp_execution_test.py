import logging
from time import sleep

from ParadoxTrading.Engine import DirectionType, ActionType
from ParadoxTrading.Utils.CTPTraderSpi import CTPTraderSpi

logging.basicConfig(level=logging.INFO)

trader = CTPTraderSpi(
    b'./con/',
    b'tcp://180.168.146.187:10030',
    # b'tcp://180.168.146.187:10000',
    b'9999', b'079375', b'1994225123'
)

trader.Connect()
trader.ReqUserLogin()
askprice = trader.ReqQryDepthMarketData(b'rb1801')['AskPrice'][0]
print(askprice)
# trader.ReqSettlementInfoConfirm()
trader.ReqOrderInsert(
    b'rb1801', DirectionType.BUY, ActionType.OPEN, 1, askprice
)
sleep(5)
