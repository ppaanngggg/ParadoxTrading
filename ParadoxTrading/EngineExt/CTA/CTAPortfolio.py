import typing

import tabulate

from ParadoxTrading.Engine import ActionType, DirectionType, FillEvent, \
    OrderEvent, OrderType, PortfolioAbstract, SignalEvent
from ParadoxTrading.Fetch.FetchAbstract import FetchAbstract
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


class ProductPosition:
    def __init__(self, _product):
        self.product = _product
        self.prev_strength = 0.0
        self.strength = 0.0
        self.cur_instrument = None
        self.cur_quantity = 0
        self.next_instrument = None
        self.next_quantity = 0

        # map instrument to quantity changes
        self.detail: typing.Dict[str, int] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        update strength

        :param _event:
        :return:
        """
        self.strength = _event.strength

    def reset(self):
        self.prev_strength = self.strength
        self.next_instrument = None
        self.next_quantity = 0
        self.detail: typing.Dict[str, int] = {}

    def dealFill(self, _event: FillEvent):
        """
        store quantity into detail

        :param _event:
        :return:
        """
        if _event.direction == DirectionType.BUY:
            try:
                self.detail[_event.symbol] += _event.quantity
            except KeyError:
                self.detail[_event.symbol] = _event.quantity
        elif _event.direction == DirectionType.SELL:
            try:
                self.detail[_event.symbol] -= _event.quantity
            except KeyError:
                self.detail[_event.symbol] = -_event.quantity
        else:
            raise Exception('unknown direction')

    def __repr__(self) -> str:
        return \
            '\tPRODUCT: {}\n' \
            '\tPREV_STRENGTH: {}\n' \
            '\tSTRENGTH: {}\n' \
            '\tCUR_INSTRUMENT: {}\n' \
            '\tCUR_QUANTITY: {}\n' \
            '\tNEXT_INSTRUMENT: {}\n' \
            '\tNEXT_QUANTITY: {}\n' \
            '\tDETAIL: {}\n'.format(
                self.product,
                self.prev_strength, self.strength,
                self.cur_instrument, self.cur_quantity,
                self.next_instrument, self.next_quantity,
                self.detail
            )


class StrategyProduct:
    def __init__(self, _strategy):
        self.strategy = _strategy
        # map product to position
        self.product_table: typing.Dict[str, ProductPosition] = {}

    def dealSignal(self, _event: SignalEvent) -> ProductPosition:
        """
        deal signal of this strategy
        1. find the position of this product
        2. if not found, create one
        3. update position

        :param _event:
        :return:
        """
        product = _event.symbol
        try:
            product_position = self.product_table[product]
        except KeyError:
            self.product_table[product] = ProductPosition(product)
            product_position = self.product_table[product]

        product_position.dealSignal(_event)
        return product_position

    def __iter__(self):
        """
        iter all product position of this strategy

        :return:
        """
        for p in self.product_table.values():
            yield p

    def __repr__(self) -> str:
        ret = 'STRATEGY: {}\n'.format(self.strategy)
        for k, v in self.product_table.items():
            ret += '--- {} ---:\n{}\n'.format(k, v)

        return ret


class OrderStrategyProduct:
    def __init__(self, _order_index, _strategy, _product):
        self.order_index = _order_index
        self.strategy = _strategy
        self.product = _product

    def __repr__(self):
        return \
            'ORDER_INDEX: {}\n' \
            'STRATEGY: {}\n' \
            'PRODUCT: {}\n'.format(
                self.order_index,
                self.strategy,
                self.product
            )


class CTAPortfolio(PortfolioAbstract):
    """
    A base CTA portfolio, it will auto adjust instrument's positions
    for each strategies. It contains a very simple portfolio allocation,
    simply as int(strength).

    :param _fetcher:
    :param _settlement_price_index:
    """

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

        # map strategy to its position manager
        self.strategy_table: typing.Dict[str, StrategyProduct] = {}
        # map order to its order info
        self.order_table: typing.Dict[int, OrderStrategyProduct] = {}
        # map symbol to latest price, need to be reset after settlement
        self.symbol_price_dict: typing.Dict[str, float] = {}

        self.addPickleSet('strategy_table', 'order_table')

        # tmp value inter functions
        self.total_fund: float = 0.0

    def dealSignal(self, _event: SignalEvent):
        """
        deal the signal from a CTA strategy,
        1. find the strategy's mgr
        2. if not found, the create one
        3. deal this signal

        :param _event:
        :return:
        """

        # deal the strategy mgr
        try:
            strategy_product = self.strategy_table[_event.strategy]
        except KeyError:
            self.strategy_table[_event.strategy] = StrategyProduct(
                _event.strategy
            )
            strategy_product = self.strategy_table[_event.strategy]
        strategy_product.dealSignal(_event)

        # add signal to portfolio
        self.portfolio.dealSignalEvent(_event)

    def dealFill(self, _event: FillEvent):
        """
        deal fill event

        :param _event:
        :return:
        """
        # 1. store position change into position mgr
        order_sp: OrderStrategyProduct = self.order_table.pop(_event.index)
        strategy_p = self.strategy_table[order_sp.strategy]
        product_p = strategy_p.product_table[order_sp.product]
        product_p.dealFill(_event)

        # 2. store strategy portfolio mgr
        self.portfolio.dealFillEvent(order_sp.strategy, _event)

    @staticmethod
    def _update_cur_position(_p: ProductPosition):
        p_dict = {}
        # get cur position
        if _p.cur_instrument:
            p_dict[_p.cur_instrument] = _p.cur_quantity
        # update positions
        for k, v in _p.detail.items():
            try:
                p_dict[k] += v
            except KeyError:
                p_dict[k] = v
        # remove empty position
        new_p_dict = {}
        for k, v in p_dict.items():
            if v:
                new_p_dict[k] = v
        if new_p_dict:  # there is position
            assert len(new_p_dict) == 1
            for k, v in new_p_dict.items():
                _p.cur_instrument = k
                _p.cur_quantity = v
        else:  # no position
            _p.cur_instrument = None
            _p.cur_quantity = 0

    def _iter_update_cur_position(self):
        # iter each strategy's each product
        for s in self.strategy_table.values():
            for p in s:
                # update position according to fill event
                self._update_cur_position(p)

    def _update_symbol_price_dict(
            self, _tradingday
    ):
        # get price dict for each portfolio
        keys = self.symbol_price_dict.keys()
        for symbol in self.portfolio.getSymbolList():
            if symbol not in keys:
                self.symbol_price_dict[symbol] = self.fetcher.fetchData(
                    _tradingday, symbol
                )[self.settlement_price_index][0]

    def _portfolio_settlement(
            self, _tradingday,
            _symbol_price_dict: typing.Dict[str, float]
    ):
        # update each portfolio settlement
        self.portfolio.dealSettlement(
            _tradingday, _symbol_price_dict
        )

    def _iter_update_instrument(self, _tradingday):
        flag = False
        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:
                    pass
                else:
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    if p.next_instrument != p.cur_instrument:
                        flag = True

        return flag

    def _iter_update_next_position(self, _tradingday):
        """
        just for test

        :param _tradingday:
        :return:
        """
        for s in self.strategy_table.values():
            for p in s:
                p.next_quantity = int(p.strength) * POINT_VALUE[p.product]
                if p.next_quantity != 0:
                    # next dominant instrument
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )

    def _create_order(self, _inst, _action, _direction, _quantity) -> OrderEvent:
        assert _quantity > 0

        order = OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_inst,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.MARKET,
            _action=_action,
            _direction=_direction,
            _quantity=_quantity,
        )

        return order

    def _close_cur_open_next(self, _p) -> typing.List[OrderEvent]:
        ret_list: typing.List[OrderEvent] = []
        # close cur
        if _p.cur_quantity > 0:
            ret_list.append(self._create_order(
                _p.cur_instrument, ActionType.CLOSE,
                DirectionType.SELL, _p.cur_quantity
            ))
        elif _p.cur_quantity < 0:
            ret_list.append(self._create_order(
                _p.cur_instrument, ActionType.CLOSE,
                DirectionType.BUY, -_p.cur_quantity
            ))
        else:
            assert _p.cur_quantity != 0
        # open next
        if _p.next_quantity > 0:
            ret_list.append(self._create_order(
                _p.next_instrument, ActionType.OPEN,
                DirectionType.BUY, _p.next_quantity
            ))
        elif _p.next_quantity < 0:
            ret_list.append(self._create_order(
                _p.next_instrument, ActionType.OPEN,
                DirectionType.SELL, -_p.next_quantity
            ))
        else:
            assert _p.next_quantity != 0
        return ret_list

    def _adjust_cur(self, _p) -> typing.List[OrderEvent]:
        quantity_diff = _p.next_quantity - _p.cur_quantity
        if quantity_diff:  # quantity changes
            if _p.next_quantity > 0:  # keep long position
                if quantity_diff > 0:  # inc long position
                    return [self._create_order(
                        _p.next_instrument, ActionType.OPEN,
                        DirectionType.BUY, quantity_diff
                    )]
                elif quantity_diff < 0:  # dec long position
                    return [self._create_order(
                        _p.next_instrument, ActionType.CLOSE,
                        DirectionType.SELL, -quantity_diff
                    )]
                else:
                    assert quantity_diff != 0
            elif _p.next_quantity < 0:  # keep short position
                if quantity_diff < 0:  # inc short position
                    return [self._create_order(
                        _p.next_instrument, ActionType.OPEN,
                        DirectionType.SELL, -quantity_diff
                    )]
                elif quantity_diff > 0:  # dec short position
                    return [self._create_order(
                        _p.next_instrument, ActionType.CLOSE,
                        DirectionType.BUY, quantity_diff
                    )]
                else:
                    assert quantity_diff != 0
            else:
                assert _p.next_quantity != 0
        else:  # quantity not changes
            return []

    def _gen_orders(self, _p: ProductPosition) -> typing.List[OrderEvent]:
        if _p.cur_quantity != 0:  # cur is not empty
            if _p.next_quantity != 0:  # next is not empty, need to adjust position
                if _p.cur_instrument != _p.next_instrument \
                        or _p.cur_quantity * _p.next_quantity < 0:
                    # change instrument, or change direction,
                    # need to close cur and open next
                    return self._close_cur_open_next(_p)
                else:
                    # just adjust cur position
                    return self._adjust_cur(_p)
            else:  # next is empty, need to close position
                if _p.cur_quantity > 0:
                    return [self._create_order(
                        _p.cur_instrument, ActionType.CLOSE,
                        DirectionType.SELL, _p.cur_quantity
                    )]
                elif _p.cur_quantity < 0:
                    return [self._create_order(
                        _p.cur_instrument, ActionType.CLOSE,
                        DirectionType.BUY, -_p.cur_quantity
                    )]
                else:
                    assert _p.cur_quantity != 0
        else:  # cur is empty
            # check if next is not empty, to open position
            if _p.next_quantity > 0:  # open buy
                return [self._create_order(
                    _p.next_instrument, ActionType.OPEN,
                    DirectionType.BUY, _p.next_quantity
                )]
            elif _p.next_quantity < 0:  # open sell
                return [self._create_order(
                    _p.next_instrument, ActionType.OPEN,
                    DirectionType.SELL, -_p.next_quantity
                )]
            else:
                # next is empty, nothing to do
                return []

    def _iter_send_order(self):
        for s in self.strategy_table.values():
            for p in s:
                # send orders according to instrument and quantity chg
                orders = self._gen_orders(p)
                for o in orders:
                    self.addEvent(o)
                    self.order_table[o.index] = OrderStrategyProduct(
                        o.index, s.strategy, p.product
                    )
                    self.portfolio.dealOrderEvent(s.strategy, o)
                # reset next status
                p.reset()

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        self._update_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._portfolio_settlement(
            _tradingday, self.symbol_price_dict
        )

        # 3 compute current total fund
        self.total_fund = self.portfolio.getStaticFund()

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()

        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()
        # 7. reset price table
        self.symbol_price_dict = {}

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass

    # utility
    def _detect_change(self) -> bool:
        """
        detect any strength changes

        :return:
        """
        for s in self.strategy_table.values():
            for p in s:
                if p.strength != p.prev_strength:
                    return True
        return False

    def _detect_sign_change(self) -> bool:
        """
        detect sign of strength changes

        :return:
        """
        for s in self.strategy_table.values():
            for p in s:
                if p.strength > 0 and p.prev_strength > 0:
                    continue
                if p.strength < 0 and p.prev_strength < 0:
                    continue
                return True
        return False

    def _calc_total_strength(self) -> float:
        """
        sum all the strength

        :return:
        """
        total_strength = 0.0
        for s in self.strategy_table.values():
            for p in s:
                total_strength += abs(p.strength)
        return total_strength

    def _calc_parts(self) -> int:
        """
        count how many strategies open positions

        :return:
        """
        parts = 0
        for s in self.strategy_table.values():
            for p in s:
                if p.strength != 0:
                    parts += 1
        return parts

    def __repr__(self):
        table = []
        for s in self.strategy_table.values():
            for p in s:
                table.append([
                    p.product, p.strength,
                    p.cur_instrument, p.cur_quantity,
                    p.next_instrument, p.next_instrument
                ])
        return "@@@ PRODUCT STATUS @@@\n{}\n{}".format(
            tabulate.tabulate(table, [
                'product', 'strength',
                'cur instrument', 'cur quantity',
                'next instrument', 'next quantity',
            ]), super().__repr__()
        )


class CTAWeightedPortfolio(CTAPortfolio):
    """
    a bit more complex allocation.
    It will keep all the current positions weighted by strength.
    (strength / total_strength * total_fund * margin_rate)
    Because it always adjusts the positions, it's a little
    ugly and expensive in commissions.
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage: float = 1.0,
            _alloc_limit: float = 0.1,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.leverage = _leverage
        self.alloc_limit = _alloc_limit
        self.total_fund: float = 0.0

    def _iter_update_next_position(self, _tradingday):
        total_strength = self._calc_total_strength()
        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:  # no position at all
                    p.next_quantity = 0
                else:
                    # next dominant instrument
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    try:
                        price = self.symbol_price_dict[p.next_instrument]
                    except KeyError:
                        price = self.fetcher.fetchData(
                            _tradingday, p.next_instrument
                        )[self.settlement_price_index][0]
                        self.symbol_price_dict[p.next_instrument] = price
                    p.next_quantity = int(
                        min(self.alloc_limit, p.strength / total_strength) *
                        self.total_fund * self.leverage /
                        price / POINT_VALUE[p.product]
                    ) * POINT_VALUE[p.product]


class CTAWeightedStablePortfolio(CTAPortfolio):
    """
    it also equally alloc the fund, however only triggered when
    signal adjusted. if the strategies, their products or strengths
    changes, it will adjust fund allocation according to their
    strengths
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage: float = 1.0,
            _alloc_limit: float = 0.1,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.leverage = _leverage
        self.alloc_limit = _alloc_limit
        self.total_fund: float = 0.0

    def _iter_update_next_position(self, _tradingday):
        total_strength = self._calc_total_strength()
        flag = self._detect_change()

        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:
                    p.next_quantity = 0
                else:
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    if flag or p.next_instrument != p.cur_instrument:
                        # if strength status changes or instrument changes
                        try:
                            price = self.symbol_price_dict[p.next_instrument]
                        except KeyError:
                            price = self.fetcher.fetchData(
                                _tradingday, p.next_instrument
                            )[self.settlement_price_index][0]
                            self.symbol_price_dict[p.next_instrument] = price
                        p.next_quantity = int(
                            min(self.alloc_limit, p.strength / total_strength) *
                            self.total_fund * self.leverage /
                            price / POINT_VALUE[p.product]
                        ) * POINT_VALUE[p.product]
                    else:
                        p.next_quantity = p.cur_quantity
