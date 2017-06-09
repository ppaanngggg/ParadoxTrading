import logging
import threading

import PyCTP

from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent
from ParadoxTrading.Utils import DataStruct


class TraderSpi(PyCTP.CThostFtdcTraderSpi):
    TIME_OUT = 5

    def __init__(
            self, _con_path: str, _front_addr: str,
            _broker_id: str, _user_id: str, _passwd: str,
    ):
        super().__init__()

        self.con_path = _con_path
        self.front_addr = _front_addr
        self.broker_id = _broker_id
        self.user_id = _user_id
        self.passwd = _passwd

        self.event = threading.Event()

        self.setApi(
            PyCTP.CThostFtdcTraderApi.CreateFtdcTraderApi(
                self.con_path
            )
        )

    def eventClear(self):
        self.event.clear()

    def eventWait(self, _sec: int):
        if self.event.wait(_sec):
            return True
        else:
            logging.warning('{} sec TIMEOUT!'.format(_sec))
            return False

    def eventSet(self):
        self.event.set()

    def setApi(self, _api: PyCTP.CThostFtdcTraderApi):
        self.api = _api
        self.api.RegisterSpi(self)

    def Connect(self) -> bool:
        self.eventClear()

        logging.info('connect front TRY!')
        self.api.RegisterFront(self.front_addr)
        self.api.Init()
        self.api.SubscribePrivateTopic(PyCTP.THOST_TERT_RESUME)
        self.api.SubscribePublicTopic(PyCTP.THOST_TERT_RESUME)

        return self.eventWait(self.TIME_OUT)

    def OnFrontConnected(self):
        self.eventSet()
        logging.info('connect front DONE!')


class CTPExecution(ExecutionAbstract):
    def __init__(
            self, _con_path: str, _front_addr: str,
            _broker_id: str, _user_id: str, _passwd: str,
    ):
        super().__init__()

        self.trader: TraderSpi = TraderSpi(
            _con_path, _front_addr,
            _broker_id, _user_id, _passwd
        )
        print(self.trader.Connect())

    def dealOrderEvent(self, _order_event: OrderEvent):
        self.order_dict[_order_event.index] = _order_event

    def matchMarket(self, _symbol: str, _data: DataStruct):
        pass
