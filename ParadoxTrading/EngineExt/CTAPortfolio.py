import typing

from ParadoxTrading.Engine import FillEvent, SignalEvent, OrderEvent, OrderType, ActionType, DirectionType
from ParadoxTrading.Engine import PortfolioAbstract
from ParadoxTrading.Fetch.FetchAbstract import FetchAbstract
from ParadoxTrading.Utils import DataStruct


class ProductPosition:
    def __init__(self, _product):
        self.product = _product
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

    def resetDetail(self):
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
            '\tSTRENGTH: {}\n' \
            '\tCUR_INSTRUMENT: {}\n' \
            '\tCUR_QUANTITY: {}\n' \
            '\tNEXT_INSTRUMENT: {}\n' \
            '\tNEXT_QUANTITY: {}\n' \
            '\tDETAIL: {}\n'.format(
                self.product, self.strength,
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
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(_init_fund)

        self.fetcher = _fetcher
        self.settlement_price_index = _settlement_price_index

        # map strategy to its position manager
        self.strategy_table: typing.Dict[str, StrategyProduct] = {}
        # map order to its order info
        self.order_table: typing.Dict[int, OrderStrategyProduct] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        deal the signal from a CTA strategy,
        1. find the strategy's mgr
        2. if not found, the create one
        3. deal this signal

        :param _event:
        :return:
        """

        # deal the position
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
        order_sp: OrderStrategyProduct = self.order_table[_event.index]
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

    def _get_symbol_price_dict(
            self, _tradingday
    ) -> typing.Dict[str, float]:
        # get price dict for each portfolio
        symbol_price_dict = {}
        for symbol in self.portfolio.getSymbolList():
            symbol_price_dict[symbol] = self.fetcher.fetchData(
                _tradingday, symbol
            )[self.settlement_price_index][0]
        return symbol_price_dict

    def _iter_portfolio_settlement(
            self, _tradingday,
            _symbol_price_dict: typing.Dict[str, float]
    ):
        # update each portfolio settlement
        self.portfolio.dealSettlement(
            _tradingday, _symbol_price_dict
        )

    def _iter_update_next_position(self, _tradingday):
        for s in self.strategy_table.values():
            for p in s:
                p.next_quantity = int(p.strength)
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
                _p.cur_instrument, ActionType.CLOSE, DirectionType.SELL, _p.cur_quantity
            ))
        elif _p.cur_quantity < 0:
            ret_list.append(self._create_order(
                _p.cur_instrument, ActionType.CLOSE, DirectionType.BUY, -_p.cur_quantity
            ))
        else:
            assert _p.cur_quantity != 0
        # open next
        if _p.next_quantity > 0:
            ret_list.append(self._create_order(
                _p.next_instrument, ActionType.OPEN, DirectionType.BUY, _p.next_quantity
            ))
        elif _p.next_quantity < 0:
            ret_list.append(self._create_order(
                _p.next_instrument, ActionType.OPEN, DirectionType.SELL, -_p.next_quantity
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
                        _p.next_instrument, ActionType.OPEN, DirectionType.BUY, quantity_diff
                    )]
                elif quantity_diff < 0:  # dec long position
                    return [self._create_order(
                        _p.next_instrument, ActionType.CLOSE, DirectionType.SELL, -quantity_diff
                    )]
                else:
                    assert quantity_diff != 0
            elif _p.next_quantity < 0:  # keep short position
                if quantity_diff < 0:  # inc short position
                    return [self._create_order(
                        _p.next_instrument, ActionType.OPEN, DirectionType.SELL, -quantity_diff
                    )]
                elif quantity_diff > 0:  # dec short position
                    return [self._create_order(
                        _p.next_instrument, ActionType.CLOSE, DirectionType.BUY, quantity_diff
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
                if _p.cur_instrument != _p.next_instrument or _p.cur_quantity * _p.next_quantity < 0:
                    # change instrument, or change direction,
                    # need to close cur and open next
                    return self._close_cur_open_next(_p)
                else:
                    # just adjust cur position
                    return self._adjust_cur(_p)
            else:  # next is empty, need to close position
                if _p.cur_quantity > 0:
                    return [self._create_order(
                        _p.cur_instrument, ActionType.CLOSE, DirectionType.SELL, _p.cur_quantity
                    )]
                elif _p.cur_quantity < 0:
                    return [self._create_order(
                        _p.cur_instrument, ActionType.CLOSE, DirectionType.BUY, -_p.cur_quantity
                    )]
                else:
                    assert _p.cur_quantity != 0
        else:  # cur is empty
            # check if next is not empty, to open position
            if _p.next_quantity > 0:  # open buy
                return [self._create_order(
                    _p.next_instrument, ActionType.OPEN, DirectionType.BUY, _p.next_quantity
                )]
            elif _p.next_quantity < 0:  # open sell
                return [self._create_order(
                    _p.next_instrument, ActionType.OPEN, DirectionType.SELL, -_p.next_quantity
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
                self._reset_position_status(p)

    @staticmethod
    def _reset_position_status(_p: ProductPosition):
        _p.next_instrument = None
        _p.next_quantity = 0
        _p.resetDetail()

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        symbol_price_dict = self._get_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._iter_portfolio_settlement(
            _tradingday, symbol_price_dict
        )

        # 3. update each strategy's positions to current status
        # according to fill results
        self._iter_update_cur_position()
        # 4. update each strategy's positions to next status
        # according to new strength and other information
        self._iter_update_next_position(_tradingday)
        # 5. send new orders
        self._iter_send_order()

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass


class CTAEqualFundPortfolio(CTAPortfolio):
    """
    a bit more complex allocation. It will keep all the current positions
    equally using total_fund * margin_rate. Because it always keep the
    positions, it will adjust positions when fund changes. It's a little
    ugly and expensive in commissions.
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _settlement_price_index
        )

        # used for allocQuantity()
        self.margin_rate = _margin_rate
        self.total_fund: float = 0.0
        self.parted_num: float = 0.0

    def _iter_update_next_position(self, _tradingday):
        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:  # no position at all
                    p.next_quantity = 0
                else:
                    # next dominant instrument
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    price = self.fetcher.fetchData(
                        _tradingday, p.next_instrument
                    )[self.settlement_price_index][0]
                    tmp = int(
                        self.margin_rate * self.total_fund
                        / self.parted_num / price
                    )
                    if p.strength > 0:
                        p.next_quantity = tmp
                    elif p.strength < 0:
                        p.next_quantity = -tmp
                    else:
                        raise Exception('p.strength == 0 ???')

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        symbol_price_dict = self._get_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._iter_portfolio_settlement(
            _tradingday, symbol_price_dict
        )

        # 3.1 compute current total fund
        self.total_fund = self.portfolio.getFund(
        ) + self.portfolio.getUnfilledFund(symbol_price_dict)
        # 3.2 compute how many parts to be divided
        self.parted_num = 0
        for strategy_p in self.strategy_table.values():
            for product_p in strategy_p:
                if product_p.strength != 0:
                    self.parted_num += 1

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()
        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()


class CTAEqualFundReducePortfolio(CTAPortfolio):
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
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _settlement_price_index
        )

        self.margin_rate = _margin_rate
        self.total_fund: float = 0.0

        # to check whether the signals change
        self.last_signal_status: typing.Set[str] = set()
        self.signal_status: typing.Set[str] = set()

    def _iter_signal_status(self):
        for s in self.strategy_table.values():
            for p in s:
                if p.strength != 0:
                    self.signal_status.add('{}_{}_{}'.format(
                        s.strategy, p.product, p.strength
                    ))

    def _iter_update_next_position(self, _tradingday):
        if self.signal_status == self.last_signal_status:
            # if signal status not changed
            for s in self.strategy_table.values():
                for p in s:
                    # just copy the last position status
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        p.next_instrument = self.fetcher.fetchSymbol(
                            _tradingday, _product=p.product
                        )
                        p.next_quantity = p.cur_quantity
        else:
            # if changed, then get the total num of strengths
            total_strength = 0.0
            for s in self.strategy_table.values():
                for p in s:
                    total_strength += abs(p.strength)
            # alloc according to strength
            for s in self.strategy_table.values():
                for p in s:
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        p.next_instrument = self.fetcher.fetchSymbol(
                            _tradingday, _product=p.product
                        )
                        price = self.fetcher.fetchData(
                            _tradingday, p.next_instrument
                        )[self.settlement_price_index][0]
                        p.next_quantity = int(
                            p.strength / total_strength *
                            self.total_fund * self.margin_rate / price
                        )

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        symbol_price_dict = self._get_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._iter_portfolio_settlement(
            _tradingday, symbol_price_dict
        )

        # 3 compute current total fund
        self.total_fund = self.portfolio.getFund(
        ) + self.portfolio.getUnfilledFund(symbol_price_dict)

        # deal with new signal status
        self._iter_signal_status()

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()

        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()

        # reset signal status
        self.last_signal_status = self.signal_status
        self.signal_status: typing.Set[str] = set()


class CTAConstAllocPortfolio(CTAPortfolio):
    """
    it will create a const position according to alloc_rate when
    receive a open signal. It will the position unchanged until
    receive a close signal.
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _alloc_rate: float = 0.05,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _settlement_price_index
        )

        self.alloc_rate: float = _alloc_rate
        self.total_fund: float = None

    def _calc_next_position(self, _tradingday, _symbol, _strength) -> int:
        price = self.fetcher.fetchData(
            _tradingday, _symbol
        )[self.settlement_price_index][0]
        return int(
            _strength * self.total_fund * self.alloc_rate / price
        )

    def _iter_update_next_position(self, _tradingday):
        for s in self.strategy_table.values():
            for p in s:
                if p.strength > 0:  # long position
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    if p.cur_quantity > 0:  # has long position now
                        p.next_quantity = p.cur_quantity
                    else:
                        p.next_quantity = self._calc_next_position(
                            _tradingday, p.next_instrument, p.strength
                        )
                elif p.strength < 0:  # short position
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    if p.cur_quantity < 0:  # has short position now
                        p.next_quantity = p.cur_quantity
                    else:
                        p.next_quantity = self._calc_next_position(
                            _tradingday, p.next_instrument, p.strength
                        )
                else:  # no position
                    p.next_quantity = 0

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        symbol_price_dict = self._get_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._iter_portfolio_settlement(
            _tradingday, symbol_price_dict
        )

        # 3 compute current total fund
        self.total_fund = self.portfolio.getFund(
        ) + self.portfolio.getUnfilledFund(symbol_price_dict)

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()
        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()
