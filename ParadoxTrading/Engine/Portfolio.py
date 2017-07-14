import logging
import typing

import pymongo
import tabulate
from pymongo import MongoClient
from pymongo.collection import Collection

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import SignalType, OrderType, ActionType, \
    DirectionType, FillEvent, OrderEvent, SignalEvent, EventType
from ParadoxTrading.Utils import DataStruct


class PositionMgr:
    def __init__(
            self, _symbol: str,
            _init_long: int = 0, _init_short: int = 0
    ):
        self.symbol: str = _symbol
        self.long: int = _init_long
        self.short: int = _init_short

    def getPosition(self, _type: int) -> int:
        if _type == SignalType.LONG:
            return self.long
        elif _type == SignalType.SHORT:
            return self.short
        else:
            raise Exception('unavailable type')

    def incPosition(self, _type: int, _quantity: int):
        if _type == SignalType.LONG:
            self.long += _quantity
        elif _type == SignalType.SHORT:
            self.short += _quantity
        else:
            raise Exception('unavailable type')

    def decPosition(self, _type: int, _quantity: int):
        assert self.getPosition(_type) >= _quantity
        if _type == SignalType.LONG:
            self.long -= _quantity
        elif _type == SignalType.SHORT:
            self.short -= _quantity
        else:
            raise Exception('unavailable type')


class FundMgr:
    def __init__(self, _init_fund: float):
        self.fund: float = _init_fund

    def dealFill(self, _fill_event: FillEvent):
        self.fund -= _fill_event.commission
        if _fill_event.direction == DirectionType.BUY:
            self.fund -= _fill_event.price * _fill_event.quantity
        elif _fill_event.direction == DirectionType.SELL:
            self.fund += _fill_event.price * _fill_event.quantity
        else:
            raise Exception('unknown direction')

    @staticmethod
    def getUnfilledFund(
            _position_mgr: typing.Dict[str, PositionMgr],
            _symbol_price_dict: typing.Dict[str, float],
    ) -> float:
        """
        compute unfilled fund according current positions and
        current prices

        :param _position_mgr:
        :param _symbol_price_dict:
        :return:
        """
        unfilled_fund = 0.0
        for p in _position_mgr.values():
            if p.long:
                unfilled_fund += _symbol_price_dict[p.symbol] * p.long
            if p.short:
                unfilled_fund -= _symbol_price_dict[p.symbol] * p.short
        return unfilled_fund

    def getFund(self) -> float:
        return self.fund


class PortfolioMgr:
    def __init__(self, _init_fund: float = 0.0):
        # records for signal, order and fill
        self.signal_record: typing.List[typing.Dict] = []
        self.order_record: typing.List[typing.Dict] = []
        self.fill_record: typing.List[typing.Dict] = []
        self.settlement_record: typing.List[typing.Dict] = []

        # map order index to unfilled orders
        self.unfilled_order: typing.Dict[int, OrderEvent] = {}

        # position mgr and fund mgr
        # position_mgr will map symbols to a PositionMgr
        # fund_mgr manager total fund
        self.position_mgr: typing.Dict[str, PositionMgr] = {}
        self.fund_mgr: FundMgr = FundMgr(_init_fund)

    def getSymbolList(self) -> typing.List[str]:
        """
        get symbols which have long or short positions

        :return: return list of symbols
        """
        return [
            p.symbol for p in self.position_mgr.values()
            if p.long or p.short
        ]

    def getFund(self) -> float:
        return self.fund_mgr.getFund()

    def getUnfilledFund(
            self, _symbol_price_dict: typing.Dict[str, float]
    ) -> float:
        """
        compute current fund according to prices

        :param _symbol_price_dict:
        :return:
        """
        return self.fund_mgr.getUnfilledFund(
            self.position_mgr, _symbol_price_dict
        )

    def incPosition(self, _symbol: str, _type: int, _quantity: int = 1):
        """
        inc position of symbol

        :param _symbol: symbol to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        assert _quantity > 0
        try:
            # try to add directly
            self.position_mgr[_symbol].incPosition(_type, _quantity)
        except KeyError:
            # create if failed
            self.position_mgr[_symbol] = PositionMgr(_symbol)
            self.position_mgr[_symbol].incPosition(_type, _quantity)

    def decPosition(self, _symbol: str, _type: int, _quantity: int = 1):
        """
        dec position of symbol

        :param _symbol: symbol to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        assert _quantity > 0
        assert _symbol in self.position_mgr.keys()
        self.position_mgr[_symbol].decPosition(_type, _quantity)

    def getPosition(self, _symbol: str, _type: int) -> int:
        """
        get _type position of symbol

        :param _symbol: which symbol
        :param _type: long or short
        :return: number of position
        """
        if _symbol in self.position_mgr.keys():
            return self.position_mgr[_symbol].getPosition(_type)
        return 0

    def getLongPosition(self, _symbol: str) -> int:
        """
        get long position of symbol

        :param _symbol: which symbol
        :return: number of position
        """
        return self.getPosition(_symbol, SignalType.LONG)

    def getShortPosition(self, _symbol: str) -> int:
        """
        get short position of symbol

        :param _symbol: which symbol
        :return: number of position
        """
        return self.getPosition(_symbol, SignalType.SHORT)

    def getUnfilledOrder(
            self, _symbol: str,
            _action: int, _direction: int
    ) -> int:
        """
        get number of unfilled orders for _instrument

        :param _symbol: which symbol
        :param _action: open or close
        :param _direction: buy or sell
        :return: number of unfilled order
        """
        num = 0
        for order in self.unfilled_order.values():
            if order.symbol == _symbol and \
                            order.action == _action and \
                            order.direction == _direction:
                num += order.quantity

        return num

    def getOpenBuyUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is OPEN and BUY

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(
            _symbol, ActionType.OPEN, DirectionType.BUY
        )

    def getOpenSellUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is OPEN and SELL

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(
            _symbol, ActionType.OPEN, DirectionType.SELL
        )

    def getCloseBuyUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is CLOSE and BUY

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(
            _symbol, ActionType.CLOSE, DirectionType.BUY
        )

    def getCloseSellUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is CLOSE and SELL

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(
            _symbol, ActionType.CLOSE, DirectionType.SELL
        )

    def dealSignalEvent(
            self, _signal_event: SignalEvent
    ):
        """
        deal signal event to set inner state

        :param _signal_event:
        :return:
        """
        self.signal_record.append(
            _signal_event.toDict()
        )

    def dealOrderEvent(
            self, _strategy: str,
            _order_event: OrderEvent
    ):
        """
        deal order event to set inner state

        :param _strategy:
        :param _order_event: order event generated by global portfolio
        :return:
        """
        assert _order_event.index not in self.unfilled_order.keys()
        # store record
        order_dict = _order_event.toDict()
        order_dict['strategy'] = _strategy
        self.order_record.append(order_dict)
        self.unfilled_order[_order_event.index] = _order_event

    def dealFillEvent(
            self, _strategy: str,
            _fill_event: FillEvent
    ):
        """
        deal fill event to set inner state

        :param _strategy:
        :param _fill_event:
        :return:
        """
        assert _fill_event.index in self.unfilled_order.keys()
        # store record
        fill_dict = _fill_event.toDict()
        fill_dict['strategy'] = _strategy
        self.fill_record.append(fill_dict)
        # delete unfilled order record
        del self.unfilled_order[_fill_event.index]
        # update position
        if _fill_event.action == ActionType.OPEN:
            if _fill_event.direction == DirectionType.BUY:
                self.incPosition(
                    _fill_event.symbol,
                    SignalType.LONG,
                    _fill_event.quantity
                )
            elif _fill_event.direction == DirectionType.SELL:
                self.incPosition(
                    _fill_event.symbol,
                    SignalType.SHORT,
                    _fill_event.quantity
                )
            else:
                raise Exception('unknown direction')
        elif _fill_event.action == ActionType.CLOSE:
            if _fill_event.direction == DirectionType.BUY:
                self.decPosition(
                    _fill_event.symbol,
                    SignalType.SHORT,
                    _fill_event.quantity
                )
            elif _fill_event.direction == DirectionType.SELL:
                self.decPosition(
                    _fill_event.symbol,
                    SignalType.LONG,
                    _fill_event.quantity
                )
            else:
                raise Exception('unknown direction')
        else:
            raise Exception('unknown action')
        # update fund
        self.fund_mgr.dealFill(_fill_event)

    def dealSettlement(
            self,
            _tradingday: str, _next_tradingday: str,
            _symbol_price_dict: typing.Dict[str, float]
    ):
        """
        do settlement, and store settlement information

        :param _tradingday:
        :param _next_tradingday:
        :param _symbol_price_dict:
        :return:
        """
        unfilled_fund = self.getUnfilledFund(_symbol_price_dict)
        self.settlement_record.append({
            'tradingday': _tradingday,
            'next_tradingday': _next_tradingday,
            'type': EventType.SETTLEMENT,
            'fund': self.getFund(),
            'unfilled_fund': unfilled_fund,
            'total_fund': self.getFund() + unfilled_fund,
        })

    def storeRecords(self, _coll: Collection):
        """
        store records into mongodb

        :param _coll:
        :return:
        """
        logging.info('Portfolio store records...')
        _coll.insert_many(
            self.signal_record +
            self.order_record +
            self.fill_record +
            self.settlement_record
        )

    def __repr__(self) -> str:
        ret = '@@@ POSITION @@@\n'

        table = []
        for k, v in self.position_mgr.items():
            table.append([k, v.long, v.short])
        ret += tabulate.tabulate(table, ['symbol', 'LONG', 'SHORT'])

        ret += '\n@@@ UNFILLED ORDER @@@\n'

        table = []
        for k, v in self.unfilled_order.items():
            table.append([
                k, v.symbol,
                ActionType.toStr(v.action),
                DirectionType.toStr(v.direction),
                v.quantity
            ])
        ret += tabulate.tabulate(
            table, ['index', 'symbol', 'ACTION', 'DIRECTION', 'QUANTITY']
        )

        ret += '\n@@@ RECORD @@@\n'
        ret += ' - Signal: {}\n'.format(len(self.signal_record))
        ret += ' - Order: {}\n'.format(len(self.order_record))
        ret += ' - Fill: {}\n'.format(len(self.fill_record))
        ret += ' - Settlement: {}\n'.format(len(self.settlement_record))

        ret += '\n@@@ FUND @@@\n'
        ret += ' - Fund: {}\n'.format(self.getFund())

        return ret


class PortfolioAbstract:
    def __init__(self, _init_fund: float = 0.0):
        self.engine: ParadoxTrading.Engine.EngineAbstract = None

        # init order index, and create a map from order to strategy
        self.order_index: int = 0  # cur unused order index

        # the global portfolio,
        self.portfolio: PortfolioMgr = PortfolioMgr(_init_fund)

    def setEngine(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        """
        used by engine

        :param _engine: ref to engine
        :return:
        """
        self.engine = _engine

    def incOrderIndex(self) -> int:
        """
        return cur index and inc order index

        :return: cur unused order index
        """
        tmp = self.order_index
        self.order_index += 1
        return tmp

    def addEvent(self, _order_event: OrderEvent):
        """
        add order event into engine's engine

        :param _order_event: order event object to be added
        :return:
        """

        # check if it is valid
        assert _order_event.order_type is not None
        assert _order_event.action is not None
        assert _order_event.direction is not None
        assert _order_event.quantity > 0
        if _order_event.order_type == OrderType.LIMIT:
            assert _order_event.price is not None

        # add it into event queue
        self.engine.addEvent(_order_event)
        logging.info('Portfolio send: {} {} {} {} at {} when {}'.format(
            ActionType.toStr(_order_event.action),
            DirectionType.toStr(_order_event.direction),
            _order_event.quantity,
            _order_event.symbol,
            _order_event.price,
            _order_event.datetime
        ))

    def storeRecords(
            self,
            _backtest_key: str,
            _mongo_host: str = 'localhost',
            _mongo_database: str = 'Backtest',
    ):
        """
        !!! This func will delete the old coll of _backtest_key !!!
        store all strategies' records into mongodb

        :param _backtest_key:
        :param _mongo_host:
        :param _mongo_database:
        :return:
        """
        client = MongoClient(host=_mongo_host)
        db = client[_mongo_database]
        # clear old backtest records
        if _backtest_key in db.collection_names():
            db.drop_collection(_backtest_key)

        coll = db[_backtest_key]
        coll.create_index([
            ('type', pymongo.ASCENDING),
            ('strategy', pymongo.ASCENDING),
            ('tradingday', pymongo.ASCENDING),
            ('datetime', pymongo.ASCENDING),
        ])
        self.portfolio.storeRecords(coll)

        client.close()

    def dealSignal(self, _event: SignalEvent):
        """
        deal signal event from strategy

        :param _event: signal event to deal
        :return:
        """
        raise NotImplementedError('dealSignal not implemented')

    def dealFill(self, _event: FillEvent):
        """
        deal fill event from execute

        :param _event: fill event to deal
        :return:
        """
        raise NotImplementedError('dealFill not implemented')

    def dealSettlement(self, _tradingday: str, _next_tradingday: str):
        raise NotImplementedError('dealSettlement not implemented')

    def dealMarket(self, _symbol: str, _data: DataStruct):
        raise NotImplementedError('dealMarket not implemented')
