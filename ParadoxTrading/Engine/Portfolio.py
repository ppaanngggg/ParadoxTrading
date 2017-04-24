import logging
import typing

import pymongo
import tabulate
from pymongo import MongoClient
from pymongo.collection import Collection

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import SignalType, OrderType, ActionType, \
    DirectionType, FillEvent, OrderEvent, SignalEvent, EventAbstract, EventType
from ParadoxTrading.Engine.Strategy import StrategyAbstract


class PortfolioPerStrategy:
    def __init__(self):
        # records for signal, order and fill
        self.signal_record: typing.List[SignalEvent] = []
        self.order_record: typing.List[OrderEvent] = []
        self.fill_record: typing.List[FillEvent] = []
        self.settlement_record: typing.List[typing.Dict] = []

        # cur order and position state
        self.position: typing.Dict[str, typing.Dict[str, int]] = {}
        self.unfilled_order: typing.Dict[int, OrderEvent] = {}

    def incPosition(self, _symbol: str, _type: str, _quantity: int = 1):
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
            self.position[_symbol][_type] += _quantity
        except KeyError:
            # create if failed
            self.position[_symbol] = {
                SignalType.LONG: 0,
                SignalType.SHORT: 0,
            }
            self.position[_symbol][_type] = _quantity

    def decPosition(self, _symbol: str, _type: str, _quantity: int = 1):
        """
        dec position of symbol

        :param _symbol: symbol to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        assert _quantity > 0
        assert _symbol in self.position.keys()
        assert self.position[_symbol][_type] >= _quantity
        self.position[_symbol][_type] -= _quantity
        assert self.position[_symbol][_type] >= 0

    def getPosition(self, _symbol: str, _type: str) -> int:
        """
        get _type position of symbol

        :param _symbol: which symbol
        :param _type: long or short
        :return: number of position
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        if _symbol in self.position.keys():
            return self.position[_symbol][_type]
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

    def getUnfilledOrder(self, _symbol: str, _action: int,
                         _direction: int) -> int:
        """
        get number of unfilled orders for _insturment

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
        return self.getUnfilledOrder(_symbol, ActionType.OPEN,
                                     DirectionType.BUY)

    def getOpenSellUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is OPEN and SELL

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(_symbol, ActionType.OPEN,
                                     DirectionType.SELL)

    def getCloseBuyUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is CLOSE and BUY

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(_symbol, ActionType.CLOSE,
                                     DirectionType.BUY)

    def getCloseSellUnfilledOrder(self, _symbol: str) -> int:
        """
        number of unfilled order which is CLOSE and SELL

        :param _symbol:
        :return:
        """
        return self.getUnfilledOrder(_symbol, ActionType.CLOSE,
                                     DirectionType.SELL)

    def dealSignalEvent(self, _signal_event: SignalEvent):
        """
        deal signal event to set inner state

        :param _signal_event:
        :return:
        """
        self.signal_record.append(_signal_event)

    def dealOrderEvent(self, _order_event: OrderEvent):
        """
        deal order event to set inner state

        :param _order_event: order event generated by global portfolio
        :return:
        """
        assert _order_event.index not in self.unfilled_order.keys()
        self.order_record.append(_order_event)
        self.unfilled_order[_order_event.index] = _order_event

    def dealFillEvent(self, _fill_event: FillEvent):
        """
        deal fill event to set inner state

        :param _fill_event:
        :return:
        """
        assert _fill_event.index in self.unfilled_order.keys()
        self.fill_record.append(_fill_event)
        del self.unfilled_order[_fill_event.index]
        if _fill_event.action == ActionType.OPEN:
            if _fill_event.direction == DirectionType.BUY:
                self.incPosition(_fill_event.symbol, SignalType.LONG,
                                 _fill_event.quantity)
            elif _fill_event.direction == DirectionType.SELL:
                self.incPosition(_fill_event.symbol,
                                 SignalType.SHORT, _fill_event.quantity)
            else:
                raise Exception('unknown direction')
        elif _fill_event.action == ActionType.CLOSE:
            if _fill_event.direction == DirectionType.BUY:
                self.decPosition(_fill_event.symbol,
                                 SignalType.SHORT, _fill_event.quantity)
            elif _fill_event.direction == DirectionType.SELL:
                self.decPosition(_fill_event.symbol, SignalType.LONG,
                                 _fill_event.quantity)
            else:
                raise Exception('unknown direction')
        else:
            raise Exception('unknown action')

    def dealSettlement(
            self, _tradingday, _next_tradingday,
            _symbol_price_dict
    ):
        unfilled_fund = 0.0
        for k, v in self.position.items():
            if v[SignalType.LONG]:
                unfilled_fund += _symbol_price_dict[k] * v[SignalType.LONG]
            if v[SignalType.SHORT]:
                unfilled_fund -= _symbol_price_dict[k] * v[SignalType.SHORT]
        self.settlement_record.append({
            'tradingday': _tradingday,
            'type': EventType.SETTLEMENT,
            'unfilled_fund': unfilled_fund,
        })

    def storeRecords(self, _name: str, _coll: Collection):
        """
        store records into mongodb

        :param _name:
        :param _coll:
        :return:
        """
        logging.info('Portfolio({}) store records...'.format(_name))
        tmp_list: typing.List[EventAbstract] = []
        tmp_list += self.signal_record
        tmp_list += self.order_record
        tmp_list += self.fill_record
        for d in tmp_list:
            d = d.toDict()
            d['strategy_name'] = _name
            _coll.insert_one(d)
        for d in self.settlement_record:
            d['strategy_name'] = _name
            _coll.insert_one(d)

    def __repr__(self) -> str:
        def action2str(_action: int) -> str:
            if _action == ActionType.OPEN:
                return 'open'
            elif _action == ActionType.CLOSE:
                return 'close'
            else:
                raise Exception()

        def direction2str(_direction: int) -> str:
            if _direction == DirectionType.BUY:
                return 'buy'
            elif _direction == DirectionType.SELL:
                return 'sell'
            else:
                raise Exception()

        ret = '@@@ POSITION @@@\n'

        table = []
        for k, v in self.position.items():
            table.append(
                [k, v[SignalType.LONG], v[SignalType.SHORT]])
        ret += tabulate.tabulate(table, ['symbol', 'LONG', 'SHORT'])

        ret += '\n@@@ UNFILLED ORDER @@@\n'

        table = []
        for k, v in self.unfilled_order.items():
            table.append([
                k, v.symbol, action2str(v.action),
                direction2str(v.direction), v.quantity
            ])
        ret += tabulate.tabulate(
            table, ['index', 'symbol', 'ACTION', 'DIRECTION', 'QUANTITY'])

        ret += '\n@@@ UNFILLED ORDER @@@\n'
        ret += str(sorted(self.unfilled_order.keys()))

        ret += '\n@@@ RECORD @@@\n'
        ret += ' - Signal: ' + str(len(self.signal_record)) + '\n'
        ret += ' - Order: ' + str(len(self.order_record)) + '\n'
        ret += ' - Fill: ' + str(len(self.fill_record)) + '\n'

        return ret


class PortfolioAbstract:
    def __init__(self):
        self.engine: ParadoxTrading.Engine.EngineAbstract = None

        # init order index, and create a map from order to strategy
        self.order_index: int = 0  # cur unused order index
        self.order_strategy_dict: typing.Dict[int, str] = {}

        # map from strategy key to its portfolio
        # !!! maybe this is the virtual portfolio
        self.strategy_portfolio_dict: typing.Dict[
            str, PortfolioPerStrategy] = {}
        # the global portfolio,
        # !!! usually this is the true portfolio,
        # !!! however how to manage it is up to implement
        self.global_portfolio: PortfolioPerStrategy = PortfolioPerStrategy()

    def addStrategy(self, _strategy: StrategyAbstract):
        """
        add strategy in to portfolio

        :param _strategy:
        :return:
        """

        # check unique
        assert _strategy.name not in self.strategy_portfolio_dict.keys()
        # create a portfolio for this strategy
        self.strategy_portfolio_dict[_strategy.name] = PortfolioPerStrategy()

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

    def addEvent(self, _order_event: OrderEvent, _strategy: str):
        """
        add order event into engine's engine

        :param _order_event: order event object to be added
        :param _strategy:
        :return:
        """

        # check if it is valid
        assert _order_event.order_type is not None
        assert _order_event.action is not None
        assert _order_event.direction is not None
        assert _order_event.quantity > 0
        if _order_event.order_type == OrderType.LIMIT:
            assert _order_event.price is not None

        # map index to strategy
        self.order_strategy_dict[_order_event.index] = _strategy
        # add it into event queue
        self.engine.addEvent(_order_event)
        logging.info('Portfolio {} {} at {} when {}'.format(
            DirectionType.toStr(_order_event.direction),
            _order_event.symbol, _order_event.price, _order_event.datetime
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
            ('strategy_name', pymongo.ASCENDING),
            ('type', pymongo.ASCENDING),
            ('tradingday', pymongo.ASCENDING),
            ('datetime', pymongo.ASCENDING),
        ])
        for k, v in self.strategy_portfolio_dict.items():
            v.storeRecords(k, coll)

        client.close()

    def dealSignal(self, _event: SignalEvent):
        """
        deal signal event from stategy

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

    def dealSettlement(self, _tradingday, _next_tradingday):
        raise NotImplementedError('dealSettlement not implemented')

    def getPortfolioByStrategy(
            self,
            _strategy_name: str
    ) -> PortfolioPerStrategy:
        """
        get the individual portfolio manager of strategy

        :param _strategy_name: key
        :return:
        """
        return self.strategy_portfolio_dict[_strategy_name]

    def getPortfolioByIndex(self, _index: int) -> PortfolioPerStrategy:
        """
        get the portfolio by order's index

        :param _index:
        :return:
        """
        return self.getPortfolioByStrategy(self.order_strategy_dict[_index])
