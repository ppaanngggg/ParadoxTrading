import logging
import threading
import typing

import PyCTP

from ParadoxTrading.Engine.Event import DirectionType, ActionType
from ParadoxTrading.Utils.DataStruct import DataStruct


class CTPTraderSpi(PyCTP.CThostFtdcTraderSpi):
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
        self.ret_data: DataStruct = None

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
        logging.info('connect front DONE!')
        self.eventSet()

    def ReqUserLogin(self) -> bool:
        self.eventClear()

        req = PyCTP.CThostFtdcReqUserLoginField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.Password = self.passwd
        logging.info('login TRY!')
        if self.api.ReqUserLogin(req, self.getRequestID()):
            logging.error('login FAILED!')
            return False

        return self.eventWait(self.TIME_OUT)

    def OnRspUserLogin(
            self, _user_login: PyCTP.CThostFtdcRspUserLoginField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        if _is_last:
            logging.info('login {} DONE!'.format(
                _rsp_info.ErrorMsg.decode('gb2312')
            ))
            self.eventSet()

    def ReqQryInstrument(self, _instrument_id=None) -> typing.Union[bool, DataStruct]:
        qry = PyCTP.CThostFtdcQryInstrumentField()
        qry.ExchangeID = b''
        qry.InstrumentID = _instrument_id if _instrument_id is not None else b''

        self.eventClear()
        self.ret_data = DataStruct([
            'InstrumentID', 'ProductID', 'VolumeMultiple', 'PriceTick',
            'DeliveryYear', 'DeliveryMonth'
        ], 'InstrumentID')
        logging.info('instrument TRY!')
        if self.api.ReqQryInstrument(qry, self.getRequestID()):
            logging.error('instrument FAILED!')
            return False

        ret = self.eventWait(self.TIME_OUT * 10)
        if ret is False:
            return False
        return self.ret_data

    def OnRspQryInstrument(
            self,
            _instrument: PyCTP.CThostFtdcInstrumentField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        symbol: str = _instrument.InstrumentID.decode()
        if len(symbol) <= 6 and not symbol.endswith('efp'):
            self.ret_data.addDict({
                'InstrumentID': symbol,
                'ProductID': _instrument.ProductID.decode(),
                'VolumeMultiple': _instrument.VolumeMultiple,
                'PriceTick': _instrument.PriceTick,
                'DeliveryYear': _instrument.DeliveryYear.decode(),
                'DeliveryMonth': _instrument.DeliveryMonth.decode(),
            })
        if _is_last:
            logging.info('instrument DONE! (total: {})'.format(len(self.ret_data)))
            self.eventSet()

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
            logging.info('qry settlement info DONE!')
            self.eventSet()

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
            logging.info('qry settlement info confirm DONE!')
            self.eventSet()

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
            logging.info('qry investor position DONE!')
            self.eventSet()

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
