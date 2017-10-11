import logging
import time

from ParadoxTrading.Utils.CTP import CTPMarketSpi

logging.basicConfig(level=logging.INFO)


def print_market(_market):
    print(_market.InstrumentID)


spi = CTPMarketSpi(
    b'./con/', b'tcp://115.238.108.184:41213',
    b'66666', b'', b'', print_market
)
spi.Connect()
spi.ReqUserLogin()
spi.SubscribeMarketData([b'MA801', b'rb1801', b'ag1712'])
time.sleep(3)
