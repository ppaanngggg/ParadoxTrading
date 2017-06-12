import logging
import threading
import typing
import time

import PyCTP

from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, DirectionType, ActionType
from ParadoxTrading.Utils import DataStruct


class TraderSpi(PyCTP.CThostFtdcTraderSpi):
    TIME_OUT = 5

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

        self.request_id = 1

        self.event = threading.Event()

        self.api: PyCTP.CThostFtdcTraderApi = \
            PyCTP.CThostFtdcTraderApi.CreateFtdcTraderApi(
                self.con_path
            )
        self.api.RegisterSpi(self)

    def getRequestID(self):
        tmp = self.request_id
        self.request_id += 1
        return tmp

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

    def ReqUserLogin(self) -> bool:
        self.eventClear()

        logging.info('login TRY!')
        req = PyCTP.CThostFtdcReqUserLoginField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.Password = self.passwd
        self.api.ReqUserLogin(req, self.getRequestID())

        return self.eventWait(self.TIME_OUT)

    def OnRspUserLogin(
            self, _user_login: PyCTP.CThostFtdcRspUserLoginField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        if _is_last:
            self.eventSet()
            logging.info('login {} DONE!'.format(
                _rsp_info.ErrorMsg.decode('gb2312')
            ))

    def ReqQrySettlementInfo(self) -> bool:
        self.eventClear()

        logging.info('qry settlement info TRY!')
        req = PyCTP.CThostFtdcQrySettlementInfoField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self.api.ReqQrySettlementInfo(req, self.getRequestID())

        return self.eventWait(self.TIME_OUT)

    def OnRspQrySettlementInfo(
            self, _settlement_info: PyCTP.CThostFtdcSettlementInfoField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        if _is_last:
            self.eventSet()
            logging.info('qry settlement info DONE!')

    def ReqSettlementInfoConfirm(self) -> bool:
        self.eventClear()

        logging.info('qry settlement info confirm TRY!')
        req = PyCTP.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self.api.ReqSettlementInfoConfirm(req, self.getRequestID())

        return self.eventWait(self.TIME_OUT)

    def OnRspSettlementInfoConfirm(
            self,
            _settlement_info_confirm: PyCTP.CThostFtdcSettlementInfoConfirmField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        if _is_last:
            self.eventSet()
            logging.info('qry settlement info confirm DONE!')

    def ReqQryInvestorPosition(self):
        self.eventClear()

        logging.info('qry investor position TRY!')
        req = PyCTP.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self.api.ReqQryInvestorPosition(req, self.getRequestID())

        return self.eventWait(self.TIME_OUT)

    def OnRspQryInvestorPosition(
            self,
            _investor_position: PyCTP.CThostFtdcInvestorPositionField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        print(_investor_position.InstrumentID)
        print(_investor_position.Position)
        print(_investor_position.PosiDirection)

        if _is_last:
            self.eventSet()
            logging.info('qry investor position DONE!')

    def ReqOrderInsert(
            self, _instrument: str, _direction: int,
            _action: int, _volume: int
    ):
        # self.eventClear()

        logging.info('order insert TRY!')
        req = PyCTP.CThostFtdcInputOrderField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        req.InstrumentID = _instrument.encode()
        req.OrderRef = b'2'
        if _direction == DirectionType.BUY:
            req.Direction = PyCTP.THOST_FTDC_D_Buy
        elif _direction == DirectionType.SELL:
            req.Direction = PyCTP.THOST_FTDC_D_Sell
        else:
            logging.error('unknown direction!')
            return False
        if _action == ActionType.OPEN:
            req.CombOffsetFlag = PyCTP.THOST_FTDC_OF_Open
        elif _action == ActionType.CLOSE:
            req.CombOffsetFlag = PyCTP.THOST_FTDC_OF_Close
        else:
            logging.error('unknown action!')
            return False
        req.CombHedgeFlag = PyCTP.THOST_FTDC_HF_Speculation
        req.VolumeTotalOriginal = _volume
        req.ContingentCondition = PyCTP.THOST_FTDC_CC_Immediately
        req.VolumeCondition = PyCTP.THOST_FTDC_VC_AV
        req.ForceCloseReason = PyCTP.THOST_FTDC_FCC_NotForceClose
        req.IsAutoSuspend = 0
        req.UserForceClose = 0

        req.OrderPriceType = PyCTP.THOST_FTDC_OPT_AnyPrice
        req.LimitPrice = 0
        req.TimeCondition = PyCTP.THOST_FTDC_TC_IOC

        self.api.ReqOrderInsert(req, self.getRequestID())

        # return self.eventWait(self.TIME_OUT)

    def OnRspOrderInsert(
            self,
            _input_order: PyCTP.CThostFtdcInputOrderField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        logging.info(_rsp_info.ErrorID)

    def OnErrRtnOrderInsert(
            self,
            _input_order: PyCTP.CThostFtdcInputOrderField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
    ):
        logging.info(_rsp_info.ErrorID)

    def OnRtnOrder(self, _order: PyCTP.CThostFtdcOrderField):
        print(_order.OrderSubmitStatus)
        print(_order.StatusMsg.decode('gb2312'))

    def OnRtnTrade(self, _trade: PyCTP.CThostFtdcTradeField):
        print(_trade.OrderRef)


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

        self.trader: TraderSpi = None
        self.trader_thread = threading.Thread(
            target=self.runTrader
        )
        self.trader_thread.start()

    def runTrader(self):
        self.trader: TraderSpi = TraderSpi(
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
