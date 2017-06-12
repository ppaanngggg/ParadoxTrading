import logging
from datetime import datetime

from ParadoxTrading.Engine import OrderEvent, OrderType, ActionType, DirectionType
from ParadoxTrading.EngineExt import CTPExecution
from time import sleep

logging.basicConfig(level=logging.INFO)

execution = CTPExecution(
    b'./con/',
    b'tcp://180.168.146.187:10030',
    # b'tcp://180.168.146.187:10000',
    b'9999', b'079375', b'1994225123'
)

execution.dealOrderEvent(OrderEvent(
    _index=0, _symbol='rb1709',
    _tradingday='20170609', _datetime=datetime.now(),
    _order_type=OrderType.MARKET, _action=ActionType.OPEN,
    _direction=DirectionType.BUY
))

