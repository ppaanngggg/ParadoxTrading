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
        self.order_id = 1

        self.event = threading.Event()  # lock for threading
        self.ret_data: typing.Any = None  # buf for return data

        self.api: PyCTP.CThostFtdcTraderApi = \
            PyCTP.CThostFtdcTraderApi.CreateFtdcTraderApi(
                self.con_path
            )
        self.api.RegisterSpi(self)

    def getRequestID(self):
        tmp = self.request_id
        self.request_id += 1
        return tmp

    def getOrderID(self):
        tmp = self.order_id
        self.order_id += 1
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
                'DeliveryYear': _instrument.DeliveryYear,
                'DeliveryMonth': _instrument.DeliveryMonth,
            })
        if _is_last:
            logging.info('instrument DONE! (total: {})'.format(
                len(self.ret_data)
            ))
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

        req = PyCTP.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self.eventClear()
        logging.info('qry settlement info confirm TRY!')
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

    def ReqQryInvestorPosition(self) -> typing.Union[bool, DataStruct]:
        req = PyCTP.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id

        self.eventClear()
        self.ret_data = DataStruct([
            'InstrumentID'
        ], 'InstrumentID')
        logging.info('qry investor position TRY!')
        if self.api.ReqQryInvestorPosition(req, self.getRequestID()):
            logging.error('qry investor position FAILED!')
            return False

        ret = self.eventWait(self.TIME_OUT)
        if ret is False:
            return False
        return self.ret_data

    def OnRspQryInvestorPosition(
            self,
            _investor_position: PyCTP.CThostFtdcInvestorPositionField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        print(
            _investor_position.InstrumentID,
            _investor_position.PosiDirection,
            _investor_position.PositionDate,
            _investor_position.Position,
            _investor_position.YdPosition,
            _investor_position.TodayPosition,
        )

        if _is_last:
            logging.info('qry investor position DONE! (total: {})'.format(
                len(self.ret_data)
            ))
            self.eventSet()

    def ReqQryDepthMarketData(self, _instrument_id):
        req = PyCTP.CThostFtdcQryDepthMarketDataField()
        req.InstrumentID = _instrument_id

        self.eventClear()
        self.ret_data = DataStruct([
            'InstrumentID', 'TradingDay', 'UpdateTime', 'UpdateMillisec',
            'LastPrice', 'HighestPrice', 'LowestPrice',
            'Volume', 'Turnover', 'OpenInterest',
            'AskPrice', 'AskVolume', 'BidPrice', 'BidVolume'
        ], 'InstrumentID')
        logging.info('qry {} market TRY!'.format(_instrument_id))
        if self.api.ReqQryDepthMarketData(req, self.getRequestID()):
            logging.error('qry {} market FAILED!'.format(_instrument_id))
            return False

        ret = self.eventWait(self.TIME_OUT)
        if ret is False:
            return False
        return self.ret_data

    def OnRspQryDepthMarketData(
            self,
            _depth_market_data: PyCTP.CThostFtdcDepthMarketDataField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        self.ret_data.addDict({
            'InstrumentID': _depth_market_data.InstrumentID.decode('gb2312'),
            'TradingDay': _depth_market_data.TradingDay.decode('gb2312'),
            'UpdateTime': _depth_market_data.UpdateTime.decode('gb2312'),
            'UpdateMillisec': _depth_market_data.UpdateMillisec,
            'LastPrice': _depth_market_data.LastPrice,
            'HighestPrice': _depth_market_data.HighestPrice,
            'LowestPrice': _depth_market_data.LowestPrice,
            'Volume': _depth_market_data.Volume,
            'Turnover': _depth_market_data.Turnover,
            'OpenInterest': _depth_market_data.OpenInterest,
            'AskPrice': _depth_market_data.AskPrice1,
            'AskVolume': _depth_market_data.AskVolume1,
            'BidPrice': _depth_market_data.BidPrice1,
            'BidVolume': _depth_market_data.BidVolume1,
        })

        if _is_last:
            logging.info('qry {} market DONE!'.format(
                _depth_market_data.InstrumentID
            ))
            self.eventSet()

    def ReqOrderInsert(
            self, _instrument: bytes, _direction: int,
            _action: int, _volume: int, _price: float
    ):

        req = PyCTP.CThostFtdcInputOrderField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        req.InstrumentID = _instrument
        req.OrderRef = '{}'.format(self.order_id).encode()
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
        req.ForceCloseReason = PyCTP.THOST_FTDC_FCC_NotForceClose
        req.IsAutoSuspend = 0
        req.UserForceClose = 0

        req.OrderPriceType = PyCTP.THOST_FTDC_OPT_LimitPrice
        req.LimitPrice = _price
        req.TimeCondition = PyCTP.THOST_FTDC_TC_IOC
        req.VolumeCondition = PyCTP.THOST_FTDC_VC_CV

        self.api.ReqOrderInsert(req, self.getRequestID())

        # self.eventClear()
        logging.info('order insert TRY!')
        # return self.eventWait(self.TIME_OUT)

    def OnRspOrderInsert(
            self,
            _input_order: PyCTP.CThostFtdcInputOrderField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
            _request_id: int, _is_last: bool
    ):
        logging.info(_rsp_info.ErrorID)
        logging.info(_rsp_info.ErrorMsg.decode('gb2312'))

    def OnErrRtnOrderInsert(
            self,
            _input_order: PyCTP.CThostFtdcInputOrderField,
            _rsp_info: PyCTP.CThostFtdcRspInfoField,
    ):
        logging.info(_rsp_info.ErrorID)
        logging.info(_rsp_info.ErrorMsg.decode())

    def OnRtnOrder(self, _order: PyCTP.CThostFtdcOrderField):
        print(
            'order ref: {}\n'
            'order status: {}\n'
            'order submit status: {}\n'
            'volume total: {}\n'
            'msg: {}'.format(
                _order.OrderRef, _order.OrderStatus, _order.OrderSubmitStatus,
                _order.VolumeTotal, _order.StatusMsg.decode('gb2312')
            )
        )

    def OnRtnTrade(self, _trade: PyCTP.CThostFtdcTradeField):
        print(_trade.OrderRef)
