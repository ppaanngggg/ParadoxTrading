import threading
import time
import typing

from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent
from ParadoxTrading.Utils import DataStruct
from ParadoxTrading.Utils.CTPTraderSpi import CTPTraderSpi


class CTPExecution(ExecutionAbstract):
    def __init__(
            self, _con_path: typing.ByteString,
            _front_addr: typing.ByteString,
            _broker_id: typing.ByteString,
            _user_id: typing.ByteString,
            _passwd: typing.ByteString,
    ):
        super().__init__()

        self.con_path = _con_path
        self.front_addr = _front_addr
        self.broker_id = _broker_id
        self.user_id = _user_id
        self.passwd = _passwd

        self.trader: CTPTraderSpi = None
        self.trader_thread = threading.Thread(
            target=self.runTrader
        )
        self.trader_thread.start()

    def runTrader(self):
        self.trader: CTPTraderSpi = CTPTraderSpi(
            self.con_path, self.front_addr,
            self.broker_id, self.user_id, self.passwd
        )
        self.trader.Connect()
        self.trader.ReqUserLogin()
        self.trader.ReqQrySettlementInfo()
        self.trader.ReqSettlementInfoConfirm()
        # self.trader.ReqQryInvestorPosition()

        for k, v in self.order_dict.items():
            self.trader.ReqOrderInsert(
                v.symbol, v.direction,
                v.action, v.quantity
            )
        time.sleep(5)

    def join(self):
        self.trader_thread.join()

    def dealOrderEvent(self, _order_event: OrderEvent):
        self.order_dict[_order_event.index] = _order_event

    def matchMarket(self, _symbol: str, _data: DataStruct):
        pass
