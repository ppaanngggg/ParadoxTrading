import typing
import logging
import sys
import tabulate

from ParadoxTrading.Engine import PortfolioAbstract, SignalEvent, \
    FillEvent, DirectionType, OrderEvent, OrderType, ActionType
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Utils import DataStruct

POINT_VALUE = {
    'if': 300, 'ic': 200, 'ih': 300, 't': 10000, 'tf': 10000,  # cffex
    'cu': 5, 'al': 5, 'zn': 5, 'pb': 5, 'ni': 1, 'sn': 1,  # shfe
    'au': 1000, 'ag': 15, 'rb': 10, 'wr': 10, 'hc': 10,  # shfe
    'fu': 50, 'bu': 10, 'ru': 10,  # shfe
    'c': 10, 'cs': 10, 'a': 10, 'b': 10, 'm': 10, 'y': 10, 'p': 10,  # dce
    'fb': 500, 'bb': 500, 'jd': 10,  # dce
    'l': 5, 'v': 5, 'pp': 5, 'j': 100, 'jm': 60, 'i': 100,  # dce
    'wh': 20, 'pm': 50, 'ri': 20, 'jr': 20, 'lr': 20,  # czce
    'cf': 5, 'cy': 5, 'sr': 10, 'rs': 10, 'rm': 10, 'ma': 10, 'oi': 10,  # czce
    'tc': 200, 'zc': 100, 'sm': 5, 'sf': 5, 'ta': 5, 'fg': 20,  # czce
}


class InstrumentMgr:
    """
    Manage the product's instrument status
    """

    def __init__(self, _product):
        # pointed to its key
        self.product = _product
        # strength management
        self.prev_strength = 0.0
        self.strength = 0.0
        # instrument and quantity management
        self.cur_instrument = None
        self.cur_quantity = 0
        self.next_instrument = None
        self.next_quantity = 0

        # map instrument to quantity changes
        self.chg_detail: typing.Dict[str, int] = {}

    def reset(self):
        self.prev_strength = self.strength
        self.next_instrument = None
        self.next_quantity = 0
        self.chg_detail: typing.Dict[str, int] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        update strength

        :param _event:
        :return:
        """
        self.strength = _event.strength

    def dealFill(self, _event: FillEvent):
        """
        store quantity into detail

        :param _event:
        :return:
        """
        if _event.direction == DirectionType.BUY:
            try:
                self.chg_detail[_event.symbol] += _event.quantity
            except KeyError:
                self.chg_detail[_event.symbol] = _event.quantity
        elif _event.direction == DirectionType.SELL:
            try:
                self.chg_detail[_event.symbol] -= _event.quantity
            except KeyError:
                self.chg_detail[_event.symbol] = -_event.quantity
        else:
            raise Exception('unknown direction')

    def updateCurStatus(self):
        tmp_dict = {}
        # get cur position
        if self.cur_instrument:
            tmp_dict[self.cur_instrument] = self.cur_quantity
        # update positions
        for k, v in self.chg_detail.items():
            try:
                tmp_dict[k] += v
            except KeyError:
                tmp_dict[k] = v
        # remove empty position
        new_dict = {}
        for k, v in tmp_dict.items():
            if v:
                new_dict[k] = v
        if new_dict:  # there is position
            assert len(new_dict) == 1
            for k, v in new_dict.items():
                self.cur_instrument = k
                self.cur_quantity = v
        else:  # no position
            self.cur_instrument = None
            self.cur_quantity = 0

    def _open_buy_order_dict(self, _quantity):
        return {
            'Instrument': self.next_instrument,
            'Action': ActionType.OPEN,
            'Direction': DirectionType.BUY,
            'Quantity': _quantity,
        }

    def _open_sell_order_dict(self, _quantity):
        return {
            'Instrument': self.next_instrument,
            'Action': ActionType.OPEN,
            'Direction': DirectionType.SELL,
            'Quantity': _quantity,
        }

    def _close_buy_order_dict(self, _quantity):
        return {
            'Instrument': self.cur_instrument,
            'Action': ActionType.CLOSE,
            'Direction': DirectionType.BUY,
            'Quantity': _quantity,
        }

    def _close_sell_order_dict(self, _quantity):
        return {
            'Instrument': self.cur_instrument,
            'Action': ActionType.CLOSE,
            'Direction': DirectionType.SELL,
            'Quantity': _quantity,
        }

    def _open_next_order_dict(self):
        if self.next_quantity > 0:  # open buy
            return self._open_buy_order_dict(self.next_quantity)
        elif self.next_quantity < 0:
            return self._open_sell_order_dict(-self.next_quantity)
        else:
            assert self.next_quantity != 0

    def _close_cur_order_dict(self):
        if self.cur_quantity > 0:
            return self._close_sell_order_dict(self.cur_quantity)
        elif self.cur_quantity < 0:
            return self._close_buy_order_dict(-self.cur_quantity)
        else:
            assert self.cur_quantity != 0

    def _adjust_order_dict(self):
        quantity_diff = self.next_quantity - self.cur_quantity
        assert quantity_diff
        if self.next_quantity > 0:
            if quantity_diff > 0:
                return self._open_buy_order_dict(quantity_diff)
            elif quantity_diff < 0:
                return self._close_sell_order_dict(-quantity_diff)
            else:
                assert quantity_diff
        elif self.next_quantity < 0:
            if quantity_diff < 0:
                return self._open_sell_order_dict(-quantity_diff)
            elif quantity_diff > 0:
                return self._close_buy_order_dict(quantity_diff)
            else:
                assert quantity_diff
        else:
            assert self.next_quantity

    def getOrderDicts(self) -> typing.List[dict]:
        ret = []

        if self.cur_quantity != 0:  # cur is not empty
            assert self.cur_instrument is not None
            if self.next_quantity != 0:  # next is not empty
                assert self.next_instrument is not None
                if self.next_instrument != self.cur_instrument \
                        or self.next_quantity * self.cur_quantity < 0:
                    # instrument changed, or strength direction changed
                    ret.append(self._close_cur_order_dict())  # close cur
                    ret.append(self._open_next_order_dict())  # open next
                else:
                    assert self.next_instrument == self.cur_instrument
                    if self.next_quantity != self.cur_quantity:  # adjust
                        ret.append(self._adjust_order_dict())
                    else:  # do nothing
                        pass
            else:  # next it empty, need to close position
                ret.append(self._close_cur_order_dict())
        else:  # cur is empty
            if self.next_quantity != 0:  # next is not empty
                assert self.next_instrument is not None
                ret.append(self._open_next_order_dict())
            else:  # next is empty too, do nothing
                pass

        return ret

    def __repr__(self) -> str:
        return \
            '-- PRODUCT: {} --\n' \
            'PREV_STRENGTH: {}, STRENGTH: {}\n' \
            'CUR_INSTRUMENT: {}, CUR_QUANTITY: {}\n' \
            'NEXT_INSTRUMENT: {}, NEXT_QUANTITY: {}\n' \
            'DETAIL: {}\n' \
            '---'.format(
                self.product,
                self.prev_strength, self.strength,
                self.cur_instrument, self.cur_quantity,
                self.next_instrument, self.next_quantity,
                self.chg_detail
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
        product = self.order_table.pop(_event.index)
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

    def getInstrumentTable(self):
        table = []
        for i_mgr in self:
            table.append([
                i_mgr.product, i_mgr.strength,
                i_mgr.cur_instrument, i_mgr.cur_quantity,
                i_mgr.next_instrument, i_mgr.next_quantity
            ])
        return tabulate.tabulate(table, [
            'product', 'strength',
            'cur instrument', 'cur quantity',
            'next instrument', 'next quantity',
        ])

    def __repr__(self) -> str:
        return '@@@ PRODUCT STATUS @@@\n{}'.format(
            self.getInstrumentTable()
        )


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
        strategy = self.order_table.pop(_event.index)
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

    def getInstrumentTable(self):
        table = []
        for p_mgr in self:
            for i_mgr in p_mgr:
                table.append([
                    p_mgr.strategy, i_mgr.product, i_mgr.strength,
                    i_mgr.cur_instrument, i_mgr.cur_quantity,
                    i_mgr.next_instrument, i_mgr.next_quantity
                ])
        return tabulate.tabulate(table, [
            'strategy', 'product', 'strength',
            'cur instrument', 'cur quantity',
            'next instrument', 'next quantity',
        ])

    def __repr__(self):
        return '@@@ STRATEGY STATUS @@@\n{}'.format(
            self.getInstrumentTable()
        )


class InterDayPortfolio(PortfolioAbstract):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(_init_fund, _margin_rate)

        self.fetcher = _fetcher
        self.settlement_price_index = _settlement_price_index

        self.strategy_mgr = StrategyMgr()

        self.addPickleKey('strategy_mgr')

        # map symbol to latest price, need to be reset after settlement
        self.symbol_price_dict: typing.Dict[str, float] = {}
        # tmp value inter functions
        self.total_fund: float = 0.0

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
            if symbol not in keys:
                try:
                    self.symbol_price_dict[symbol] = self.fetcher.fetchData(
                        _tradingday, symbol
                    )[self.settlement_price_index][0]
                except TypeError as e:
                    logging.error('Tradingday: {}, Symbol: {}, e: {}'.format(
                        _tradingday, symbol, e
                    ))
                    sys.exit(1)

    def _iter_update_cur_status(self):
        # iter each strategy's each product
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                i_mgr.updateCurStatus()

    def _iter_update_next_status(self, _tradingday):
        """
        :param _tradingday:
        :return:
        """
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                i_mgr.next_quantity = int(i_mgr.strength) \
                    * POINT_VALUE[i_mgr.product]
                if i_mgr.next_quantity != 0:
                    # next dominant instrument
                    i_mgr.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=i_mgr.product
                    )

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

        # 1. get the table map symbols to their price
        self._update_symbol_price_dict(_tradingday)
        # 2. portfolio settlement
        self.portfolio_mgr.dealSettlement(
            _tradingday, self.symbol_price_dict
        )
        # 3. set cur total fund
        self.total_fund = self.portfolio_mgr.getStaticFund()
        # 4. update each strategy's position to current status
        self._iter_update_cur_status()

        # 5. update next status
        self._iter_update_next_status(_tradingday)
        # 6. send new orders
        self._iter_send_order()
        # 7. reset all tmp info
        self._iter_reset_status()
        self.symbol_price_dict = {}
        self.total_fund = 0.0

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass

    # utility
    def _detect_update_instrument(
            self, _tradingday
    ) -> bool:
        flag = False
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength == 0:
                    pass
                else:
                    i_mgr.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=i_mgr.product
                    )
                    if i_mgr.next_instrument != i_mgr.cur_instrument:
                        flag = True

        return flag

    def _detect_change(self) -> bool:
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

        :return:
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

        :return:
        """
        total_strength = 0.0
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                total_strength += abs(i_mgr.strength)
        return total_strength

    def _calc_parts(self) -> int:
        """
        count how many strategies open positions

        :return:
        """
        parts = 0
        for p_mgr in self.strategy_mgr:
            for i_mgr in p_mgr:
                if i_mgr.strength != 0:
                    parts += 1
        return parts

    def __repr__(self):
        return "{}\n{}".format(
            self.strategy_mgr,
            super().__repr__()
        )
