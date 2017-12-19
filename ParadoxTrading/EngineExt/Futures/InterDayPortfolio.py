import logging
import sys
import typing

import tabulate

from ParadoxTrading.Engine import ActionType, DirectionType, FillEvent, \
    OrderEvent, OrderType, PortfolioAbstract, SignalEvent
from ParadoxTrading.EngineExt.Futures.PointValue import POINT_VALUE
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Utils import DataStruct


class InstrumentMgr:
    """
    Manage the product's instrument status
    """

    def __init__(self, _product):
        # pointed to its key
        self.product: str = _product
        # strength management
        self.prev_strength: float = 0.0
        self.strength: float = 0.0

        self.cur_instrument_dict: typing.Dict[str, int] = {}
        self.next_instrument_dict: typing.Dict[str, int] = {}

    def reset(self):
        self.prev_strength = self.strength
        self.next_instrument_dict: typing.Dict[str, int] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        update strength
        """
        self.strength = _event.strength

    def dealFill(self, _event: FillEvent):
        """
        store quantity into detail
        """
        if _event.direction == DirectionType.BUY:
            try:
                self.cur_instrument_dict[_event.symbol] += _event.quantity
            except KeyError:
                self.cur_instrument_dict[_event.symbol] = _event.quantity
        elif _event.direction == DirectionType.SELL:
            try:
                self.cur_instrument_dict[_event.symbol] -= _event.quantity
            except KeyError:
                self.cur_instrument_dict[_event.symbol] = -_event.quantity
        else:
            raise Exception('unknown direction')

        # if empty, then clear from dict
        if self.cur_instrument_dict[_event.symbol] == 0:
            del self.cur_instrument_dict[_event.symbol]

    @staticmethod
    def _open_buy_order_dict(_instrument, _quantity):
        assert _quantity > 0
        return {
            'Instrument': _instrument,
            'Action': ActionType.OPEN,
            'Direction': DirectionType.BUY,
            'Quantity': _quantity,
        }

    @staticmethod
    def _open_sell_order_dict(_instrument, _quantity):
        assert _quantity > 0
        return {
            'Instrument': _instrument,
            'Action': ActionType.OPEN,
            'Direction': DirectionType.SELL,
            'Quantity': _quantity,
        }

    @staticmethod
    def _close_buy_order_dict(_instrument, _quantity):
        assert _quantity > 0
        return {
            'Instrument': _instrument,
            'Action': ActionType.CLOSE,
            'Direction': DirectionType.BUY,
            'Quantity': _quantity,
        }

    @staticmethod
    def _close_sell_order_dict(_instrument, _quantity):
        assert _quantity > 0
        return {
            'Instrument': _instrument,
            'Action': ActionType.CLOSE,
            'Direction': DirectionType.SELL,
            'Quantity': _quantity,
        }

    @staticmethod
    def _adjust_order_dicts(
            _instrument, _cur_quantity, _next_quantity
    ) -> typing.List[dict]:
        ret = []
        if _next_quantity > 0:
            if _cur_quantity > 0:
                if _next_quantity > _cur_quantity:
                    ret.append(InstrumentMgr._open_buy_order_dict(
                        _instrument, _next_quantity - _cur_quantity
                    ))
                elif _next_quantity < _cur_quantity:
                    ret.append(InstrumentMgr._close_sell_order_dict(
                        _instrument, _cur_quantity - _next_quantity
                    ))
                else:  # nothing changes
                    pass
            elif _cur_quantity < 0:
                ret.append(InstrumentMgr._close_buy_order_dict(
                    _instrument, -_cur_quantity
                ))
                ret.append(InstrumentMgr._open_buy_order_dict(
                    _instrument, _next_quantity
                ))
            else:
                raise Exception('why _cur_quantity == 0 ???')
        elif _next_quantity < 0:
            if _cur_quantity < 0:
                if _next_quantity < _cur_quantity:
                    ret.append(InstrumentMgr._open_sell_order_dict(
                        _instrument, _cur_quantity - _next_quantity
                    ))
                elif _next_quantity > _cur_quantity:
                    ret.append(InstrumentMgr._close_buy_order_dict(
                        _instrument, _next_quantity - _cur_quantity
                    ))
                else:  # nothing changes
                    pass
            elif _cur_quantity > 0:
                ret.append(InstrumentMgr._close_sell_order_dict(
                    _instrument, _cur_quantity
                ))
                ret.append(InstrumentMgr._open_sell_order_dict(
                    _instrument, -_next_quantity
                ))
            else:
                raise Exception('why _cur_quantity == 0 ???')
        else:
            raise Exception('why _next_quantity == 0 ???')

        return ret

    def getOrderDicts(self) -> typing.List[dict]:
        # check quantity is not zero
        for v in self.cur_instrument_dict.values():
            assert v != 0
        for v in self.next_instrument_dict.values():
            assert v != 0

        ret = []

        for k in self.cur_instrument_dict:
            if k not in self.next_instrument_dict:  # close now
                if self.cur_instrument_dict[k] > 0:
                    ret.append(InstrumentMgr._close_sell_order_dict(
                        k, self.cur_instrument_dict[k]
                    ))
                elif self.cur_instrument_dict[k] < 0:
                    ret.append(InstrumentMgr._close_buy_order_dict(
                        k, -self.cur_instrument_dict[k]
                    ))
                else:
                    raise Exception(
                        'why cur_instrument_dict[k] == 0 ???'
                    )

        for k in self.next_instrument_dict:
            if k in self.cur_instrument_dict:  # adjust
                ret += self._adjust_order_dicts(
                    k, self.cur_instrument_dict[k], self.next_instrument_dict[k]
                )
            else:  # new open
                if self.next_instrument_dict[k] > 0:
                    ret.append(InstrumentMgr._open_buy_order_dict(
                        k, self.next_instrument_dict[k]
                    ))
                elif self.next_instrument_dict[k] < 0:
                    ret.append(InstrumentMgr._open_sell_order_dict(
                        k, -self.next_instrument_dict[k]
                    ))
                else:
                    raise Exception(
                        'why next_instrument_dict[k] == 0 ???'
                    )

        return ret

    def __repr__(self):
        ret = '### InstrumentMgr ###\n' \
              'PRODUCT: {}, PREV_STRENGTH: {}, STRENGTH:{}\n' \
              'CUR_INSTRUMENT_DICT: {}\n' \
              'NEXT_INSTRUMENT_DICT: {}'
        return ret.format(
            self.product, self.prev_strength, self.strength,
            self.cur_instrument_dict, self.next_instrument_dict
        )


class ProductMgr:
    """
    Manage the strategy's products
    """

    def __init__(self, _strategy):
        # pointed to self key
        self.strategy = _strategy
        # map product to instrument status
        self.product_table: typing.Dict[str, InstrumentMgr] = {}
        # map order to product
        self.order_table: typing.Dict[int, str] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        find the correct product to deal with it

        :param _event: the signal event
        :return: the correct product
        """
        product = _event.symbol
        try:
            i_mgr = self.product_table[product]
        except KeyError:
            self.product_table[product] = InstrumentMgr(product)
            i_mgr = self.product_table[product]

        i_mgr.dealSignal(_event)
        return product

    def dealFill(self, _event: FillEvent):
        """
        find the correct product to deal with it

        :param _event: the fill event
        :return: the correct product
        """
        try:
            product = self.order_table.pop(_event.index)
        except KeyError as e:
            logging.error('order index({}) not found in order_table'.format(e))
            ret = input('Continue?(y/n): ')
            if ret != 'y':
                sys.exit(1)
            product = input('Please input product name: ')
        self.product_table[product].dealFill(_event)

        return product

    def dealOrder(self, _event: OrderEvent, _product: str):
        assert _event.index not in self.order_table
        self.order_table[_event.index] = _product

    def __iter__(self):
        """
        iter all product position of this strategy

        :return:
        """
        for p in self.product_table.values():
            yield p


class StrategyMgr:
    """
    Manage the portfolio's strategies
    """

    def __init__(self):
        # map strategy to product mgr
        self.strategy_table: typing.Dict[str, ProductMgr] = {}
        # map order to strategy
        self.order_table: typing.Dict[int, str] = {}

    def dealSignal(self, _event: SignalEvent) -> str:
        """
        find the correct strategy to deal with it

        :param _event: the signal event
        :return: the correct strategy
        """
        strategy = _event.strategy
        try:
            p_mgr = self.strategy_table[strategy]
        except KeyError:
            self.strategy_table[strategy] = ProductMgr(strategy)
            p_mgr = self.strategy_table[strategy]

        p_mgr.dealSignal(_event)

        return strategy

    def dealFill(self, _event: FillEvent):
        """
        find the correct strategy to deal with it

        :param _event: the fill event
        :return: the correct strategy
        """
        try:
            strategy = self.order_table.pop(_event.index)
        except KeyError as e:
            logging.error('order index({}) not found in order_table'.format(e))
            ret = input('Continue?(y/n): ')
            if ret != 'y':
                sys.exit(1)
            strategy = input('Please input strategy name: ')
        self.strategy_table[strategy].dealFill(_event)

        return strategy

    def dealOrder(
            self, _event: OrderEvent,
            _strategy: str, _product: str
    ):
        assert _event.index not in self.order_table
        self.order_table[_event.index] = _strategy
        self.strategy_table[_strategy].dealOrder(
            _event, _product
        )

    def __iter__(self):
        for s in self.strategy_table.values():
            yield s

    def getCurInstrumentTable(self):
        table = []
        for p_mgr in self:
            for i_mgr in p_mgr:
                for k, v in i_mgr.cur_instrument_dict.items():
                    table.append([
                        p_mgr.strategy, i_mgr.product, i_mgr.strength,
                        k, v
                    ])
        return tabulate.tabulate(table, [
            'strategy', 'product', 'strength',
            'cur instrument', 'cur quantity',
        ])

    def getNextInstrumentTable(self):
        table = []
        for p_mgr in self:
            for i_mgr in p_mgr:
                for k, v in i_mgr.next_instrument_dict.items():
                    table.append([
                        p_mgr.strategy, i_mgr.product, i_mgr.strength,
                        k, v
                    ])
        return tabulate.tabulate(table, [
            'strategy', 'product', 'strength',
            'next instrument', 'next quantity',
        ])

    def __repr__(self):
        return '@@@ STRATEGY STATUS @@@\n{}\n{}'.format(
            self.getCurInstrumentTable(),
            self.getNextInstrumentTable(),
        )


class InterDayPortfolio(PortfolioAbstract):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _simulate_product_index: bool = False,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(_init_fund, _margin_rate)

        self.fetcher = _fetcher
        self.simulate_product_index = _simulate_product_index
        self.settlement_price_index = _settlement_price_index

        self.strategy_mgr = StrategyMgr()

        self.addPickleKey('strategy_mgr')

        # map symbol to latest price, need to be reset after settlement
        self.symbol_price_dict: typing.Dict[str, float] = {}

    def dealSignal(self, _event: SignalEvent):
        self.strategy_mgr.dealSignal(_event)
        self.portfolio_mgr.dealSignal(_event)

    def dealFill(self, _event: FillEvent):
        strategy = self.strategy_mgr.dealFill(_event)
        self.portfolio_mgr.dealFill(strategy, _event)

    def _update_symbol_price_dict(
            self, _tradingday
    ):
        # get price dict for each portfolio
        keys = self.symbol_price_dict.keys()
        for symbol in self.portfolio_mgr.getSymbolList():
            if symbol in keys:
                continue
            try:
                self.symbol_price_dict[symbol] = self.fetcher.fetchData(
                    _tradingday, symbol
                )[self.settlement_price_index][0]
            except TypeError as e:
                # if not available, use the last tradingday's price
                logging.warning('Tradingday: {}, Symbol: {}, e: {}'.format(
                    _tradingday, symbol, e
                ))
                if input('Continue?(y/n): ') != 'y':
                    sys.exit(1)
                self.symbol_price_dict[symbol] = self.fetcher.fetchData(
                    self.fetcher.instrumentLastTradingDay(
                        symbol, _tradingday), symbol
                )[self.settlement_price_index][0]

    def _iter_update_next_status(self, _tradingday):
        """
        :param _tradingday:
        :return:
        """
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength == 0:
                    continue
                next_instrument = self.fetcher.fetchSymbol(
                    _tradingday, _product=i_mgr.product
                )
                if i_mgr.strength > 0:
                    quantity = 1
                elif i_mgr.strength < 0:
                    quantity = -1
                else:
                    quantity = 0
                i_mgr.next_instrument_dict[next_instrument] = \
                    POINT_VALUE[i_mgr.product] * quantity

    def _iter_send_order(self):
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                for order_dict in i_mgr.getOrderDicts():
                    o = OrderEvent(
                        _index=self.incOrderIndex(),
                        _symbol=order_dict['Instrument'],
                        _tradingday=self.engine.getTradingDay(),
                        _datetime=self.engine.getDatetime(),
                        _order_type=OrderType.MARKET,
                        _action=order_dict['Action'],
                        _direction=order_dict['Direction'],
                        _quantity=order_dict['Quantity'],
                    )
                    self.addEvent(o)
                    self.strategy_mgr.dealOrder(
                        o, p_mgr.strategy, i_mgr.product
                    )
                    self.portfolio_mgr.dealOrder(p_mgr.strategy, o)

    def _iter_reset_status(self):
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                i_mgr.reset()

    def dealSettlement(self, _tradingday: str):
        # check it's the end of prev tradingday
        assert _tradingday

        # settle current portfolio's positions
        self._update_symbol_price_dict(_tradingday)
        self.portfolio_mgr.dealSettlement(
            _tradingday, self.symbol_price_dict
        )

        # update next portfolio status and send orders
        self._iter_update_next_status(_tradingday)
        self._iter_send_order()
        self._iter_reset_status()

        self.symbol_price_dict = {}

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass

    # utility functions
    def _detect_instrument_change(
            self, _tradingday
    ) -> bool:
        """
        Iter to update all products' current instruments.
        If strength is 0, remain None.
        If any product's instrument changes, return True, else False
        """
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength == 0:
                    continue
                instrument = self.fetcher.fetchSymbol(
                    _tradingday, _product=i_mgr.product
                )
                if instrument not in i_mgr.cur_instrument_dict:
                    return True
        return False

    def _detect_strength_change(self) -> bool:
        """
        detect any strength changes

        :return:
        """
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength != i_mgr.prev_strength:
                    return True
        return False

    def _detect_sign_change(self) -> bool:
        """
        detect sign of strength changes
        """
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength > 0 and i_mgr.prev_strength > 0:
                    continue
                if i_mgr.strength < 0 and i_mgr.prev_strength < 0:
                    continue
                return True
        return False

    def _calc_total_strength(self) -> float:
        """
        sum all the strength
        """
        total_strength = 0.0
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                total_strength += abs(i_mgr.strength)
        return total_strength

    def _calc_available_strategy(self) -> int:
        """
        count all available strategy
        """
        parts = 0
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength != 0:
                    parts += 1
                    break
        return parts

    def _calc_available_product(self) -> int:
        """
        count how many strategies open positions
        """
        parts = 0
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength != 0:
                    parts += 1
        return parts

    def _fetch_buf_price(self, _tradingday, _symbol) -> float:
        """
        fetch price and store into buf
        """
        try:
            price = self.symbol_price_dict[_symbol]
        except KeyError:
            price = self.fetcher.fetchData(
                _tradingday, _symbol
            )[self.settlement_price_index][0]
            self.symbol_price_dict[_symbol] = price

        return price

    def __repr__(self):
        return "{}\n{}".format(
            self.strategy_mgr,
            super().__repr__()
        )
