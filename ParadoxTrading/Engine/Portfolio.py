import logging
import typing

import pymongo
import tabulate
from pymongo import MongoClient
from pymongo.collection import Collection

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import ActionType, DirectionType, EventType, \
    FillEvent, OrderEvent, OrderType, SignalEvent, SignalType
from ParadoxTrading.Utils import DataStruct, Serializable


class PositionMgr:
    def __init__(self, _symbol: str):
        self.symbol: str = _symbol

        self.long: int = 0
        self.long_price: float = 0.0
        self.short: int = 0
        self.short_price: float = 0.0

        self.margin_count: int = 0
        self.margin: float = 0.0

    def getPosition(self, _type: int) -> int:
        if _type == SignalType.LONG:
            return self.long
        elif _type == SignalType.SHORT:
            return self.short
        else:
            raise Exception('unavailable type')

    def updateMargin(self, _price: float, _margin_rate: float):
        # margin count is the max of long or short
        cur_margin_count = max(self.long, self.short)
        if cur_margin_count > self.margin_count:
            # if margin inc, add margin according to price and margin rate
            self.margin += _margin_rate * _price * (
                cur_margin_count - self.margin_count)
        elif cur_margin_count < self.margin_count:
            # if margin dec, sub margin as dec rate
            self.margin *= cur_margin_count / self.margin_count
        else:
            pass

        self.margin_count = cur_margin_count

    def incPosition(
            self,
            _type: int,
            _quantity: int,
            _price: float,
            _margin_rate: float
    ):
        """
        :param _type: long or short
        :param _quantity: how many
        :param _price: how much money
        :param _margin_rate:
        :return:
        """
        if _type == SignalType.LONG:
            tmp = self.long * self.long_price + _quantity * _price
            self.long += _quantity
            self.long_price = tmp / self.long
        elif _type == SignalType.SHORT:
            tmp = self.short * self.short_price + _quantity * _price
            self.short += _quantity
            self.short_price = tmp / self.short
        else:
            raise Exception('unavailable type')

        self.updateMargin(_price, _margin_rate)

    def decPosition(
            self,
            _type: int,
            _quantity: int,
            _price: float,
            _margin_rate: float
    ) -> float:
        """
        :param _type: long or short
        :param _quantity: how many
        :param _price: how much money
        :param _margin_rate:
        :return: profit or loss
        """
        assert self.getPosition(_type) >= _quantity
        if _type == SignalType.LONG:
            self.long -= _quantity
            profit_loss = (_price - self.long_price) * _quantity
        elif _type == SignalType.SHORT:
            self.short -= _quantity
            profit_loss = (self.short_price - _price) * _quantity
        else:
            raise Exception('unavailable type')

        self.updateMargin(_price, _margin_rate)

        return profit_loss

    def dealSettlement(self, _price: float, _margin_rate: float):
        self.long_price = _price
        self.short_price = _price
        self.margin_count = max(self.long, self.short)
        self.margin = _margin_rate * _price * self.margin_count


class FundMgr:
    def __init__(
            self, _init_fund: float,
    ):
        # update each tradingday
        self.static_fund: float = _init_fund
        # reset each tradingday
        self.commission: float = 0.0

    def setStaticFund(self, _fund: float):
        self.static_fund = _fund

    def getStaticFund(self) -> float:
        return self.static_fund

    def getDynamicFund(self, _profit_and_loss: float) -> float:
        return self.static_fund - self.commission + _profit_and_loss

    def getCommission(self) -> float:
        return self.commission

    def incCommission(self, _commission: float):
        self.commission += _commission

    def dealSettlement(self, _profit_and_loss: float):
        self.static_fund += -self.commission + _profit_and_loss
        self.commission = 0.0


class PortfolioMgr:
    def __init__(
            self,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
    ):
        super().__init__()

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
        self.margin_rate = _margin_rate
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

    def getMargin(self) -> float:
        """
        get current total margin
        """
        return sum([p.margin for p in self.position_mgr.values()])

    def setStaticFund(self, _fund: float):
        """
        set static fund to pointed value

        :param _fund:
        :return:
        """
        assert isinstance(_fund, float)
        self.fund_mgr.setStaticFund(_fund)

    def getStaticFund(self) -> float:
        return self.fund_mgr.getStaticFund()

    def getDynamicFund(self, _profit_and_loss: float) -> float:
        return self.fund_mgr.getDynamicFund(_profit_and_loss)

    def getCommission(self) -> float:
        return self.fund_mgr.getCommission()

    def getProfitAndLoss(
            self, _symbol_price_dict: typing.Dict[str, float]
    ) -> float:
        ret = 0
        for k, v in self.position_mgr.items():
            price = _symbol_price_dict[k]
            long_part = v.long * (price - v.long_price)
            short_part = v.short * (v.short_price - price)
            ret += long_part + short_part
        return ret

    def incPosition(
            self, _symbol: str, _type: int,
            _quantity: int, _price: float
    ):
        """
        inc position of symbol

        :param _symbol: symbol to inc
        :param _type: long or short
        :param _quantity: how many
        :param _price: how much money
        :return:
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        assert _quantity > 0
        try:
            # try to add directly
            self.position_mgr[_symbol].incPosition(
                _type, _quantity, _price, self.margin_rate
            )
        except KeyError:
            # create if failed
            self.position_mgr[_symbol] = PositionMgr(_symbol)
            self.position_mgr[_symbol].incPosition(
                _type, _quantity, _price, self.margin_rate
            )

    def decPosition(
            self, _symbol: str, _type: int,
            _quantity: int, _price: float
    ) -> float:
        """
        dec position of symbol

        :param _symbol: symbol to inc
        :param _type: long or short
        :param _quantity: how many
        :param _price: how much money
        :return:
        """
        assert _type == SignalType.LONG or _type == SignalType.SHORT
        assert _quantity > 0
        assert _symbol in self.position_mgr.keys()
        tmp = self.position_mgr[_symbol]
        profit_loss = tmp.decPosition(
            _type, _quantity, _price, self.margin_rate
        )
        # delete if empty, speed up and reduce memory
        if tmp.long == 0 and tmp.short == 0:
            del self.position_mgr[_symbol]

        return profit_loss

    def dealSignal(self, _signal_event: SignalEvent):
        """
        deal signal event to set inner state

        :param _signal_event:
        :return:
        """
        self.signal_record.append(_signal_event.toDict())

    def dealOrder(self, _strategy: str, _order_event: OrderEvent):
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
        # add to unfilled table
        self.unfilled_order[_order_event.index] = _order_event

    def dealFill(self, _strategy: str, _fill_event: FillEvent):
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
                    _fill_event.quantity,
                    _fill_event.price,
                )
            elif _fill_event.direction == DirectionType.SELL:
                self.incPosition(
                    _fill_event.symbol,
                    SignalType.SHORT,
                    _fill_event.quantity,
                    _fill_event.price,
                )
            else:
                raise Exception('unknown direction')
        elif _fill_event.action == ActionType.CLOSE:
            if _fill_event.direction == DirectionType.BUY:
                profit_loss = self.decPosition(
                    _fill_event.symbol,
                    SignalType.SHORT,
                    _fill_event.quantity,
                    _fill_event.price,
                )
            elif _fill_event.direction == DirectionType.SELL:
                profit_loss = self.decPosition(
                    _fill_event.symbol,
                    SignalType.LONG,
                    _fill_event.quantity,
                    _fill_event.price,
                )
            else:
                raise Exception('unknown direction')
            # update static fund according to close profit
            self.fund_mgr.setStaticFund(
                self.fund_mgr.getStaticFund() + profit_loss
            )
        else:
            raise Exception('unknown action')

        # add commission
        self.fund_mgr.incCommission(_fill_event.commission)

    def dealSettlement(
            self, _tradingday: str,
            _symbol_price_dict: typing.Dict[str, float]
    ):
        """
        do settlement, and store settlement information

        :param _tradingday:
        :param _symbol_price_dict:
        :return:
        """
        profit_loss = self.getProfitAndLoss(_symbol_price_dict)

        self.settlement_record.append({
            'tradingday': _tradingday,
            'type': EventType.SETTLEMENT,
            'fund': self.getDynamicFund(profit_loss),
            'commission': self.getCommission(),
            'margin': self.getMargin(),
        })

        self.fund_mgr.dealSettlement(profit_loss)
        for k, v in self.position_mgr.items():
            v.dealSettlement(_symbol_price_dict[k], self.margin_rate)

    def storeRecords(self, _coll: Collection):
        """
        store records into mongodb

        :param _coll:
        :return:
        """
        logging.info('Portfolio store records...')
        _coll.insert_many(
            self.signal_record + self.order_record +
            self.fill_record + self.settlement_record
        )

    def getPositionTable(self):
        table = []
        for k, v in self.position_mgr.items():
            table.append([
                k, v.long, v.long_price, v.short, v.short_price
            ])
        return tabulate.tabulate(
            table, ['SYMBOL', 'LONG', 'LONG PRICE', 'SHORT', 'SHORT PRICE']
        )

    def getUnfilledOrderTable(self):
        table = []
        for k, v in self.unfilled_order.items():
            table.append([
                k, v.symbol,
                ActionType.toStr(v.action),
                DirectionType.toStr(v.direction), v.quantity,
                v.tradingday, v.datetime
            ])
        return tabulate.tabulate(
            table,
            ['INDEX', 'SYMBOL', 'ACTION', 'DIRECTION', 'QUANTITY',
             'TRADINGDAY', 'DATETIME']
        )

    def __repr__(self) -> str:
        ret = '@@@ POSITION @@@\n{}' \
              '\n@@@ UNFILLED ORDER @@@\n{}' \
              '\n@@@ RECORD @@@\n' \
              ' - Signal - NUM: {}, LAST: {}\n' \
              ' - Order - NUM: {}, LAST: {}\n' \
              ' - Fill - NUM: {}, LAST: {}\n' \
              ' - Settlement - NUM: {}, LAST: {}\n' \
              '@@@ FUND @@@\n' \
              ' - Static Fund: {}' \
              ' - Commission: {}'

        return ret.format(
            self.getPositionTable(), self.getUnfilledOrderTable(),
            len(self.signal_record),
            self.signal_record[-1] if self.signal_record else None,
            len(self.order_record),
            self.order_record[-1] if self.order_record else None,
            len(self.fill_record),
            self.fill_record[-1] if self.fill_record else None,
            len(self.settlement_record),
            self.settlement_record[-1] if self.settlement_record else None,
            self.getStaticFund(), self.getCommission()
        )


class PortfolioAbstract(Serializable):
    def __init__(
            self,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
    ):
        """
        :param _init_fund: init fund for portfolio mgr
        :param _margin_rate: margin rate for portfolio mgr
        """
        super().__init__()

        self.engine: ParadoxTrading.Engine.EngineAbstract = None

        # init order index, and create a map from order to strategy
        self.order_index: int = 0  # cur unused order index
        # the global portfolio,
        self.portfolio_mgr: PortfolioMgr = PortfolioMgr(_init_fund, _margin_rate)

        self.addPickleKey('order_index', 'portfolio_mgr')

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
            DirectionType.toStr(_order_event.direction), _order_event.quantity,
            _order_event.symbol, _order_event.price, _order_event.datetime
        ))

    def storeRecords(
            self,
            _backtest_key: str,
            _mongo_host: str = 'localhost',
            _mongo_database: str = 'Backtest',
            _clear: bool = True, ):
        """
        !!! This func will delete the old coll of _backtest_key !!!
        store all strategies' records into mongodb

        :param _backtest_key:
        :param _mongo_host:
        :param _mongo_database:
        :param _clear:
        :return:
        """
        client = MongoClient(host=_mongo_host)
        db = client[_mongo_database]
        # clear old backtest records
        if _backtest_key in db.collection_names() and _clear:
            db.drop_collection(_backtest_key)

        coll = db[_backtest_key]
        coll.create_index([
            ('type', pymongo.ASCENDING),
            ('strategy', pymongo.ASCENDING),
            ('tradingday', pymongo.ASCENDING),
            ('datetime', pymongo.ASCENDING),
        ])
        self.portfolio_mgr.storeRecords(coll)

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

    def dealSettlement(self, _tradingday: str):
        raise NotImplementedError('dealSettlement not implemented')

    def dealMarket(self, _symbol: str, _data: DataStruct):
        raise NotImplementedError('dealMarket not implemented')

    def __repr__(self) -> str:
        return '@@@ ORDER INDEX @@@\n{}\n{}'.format(
            self.order_index, self.portfolio_mgr
        )
