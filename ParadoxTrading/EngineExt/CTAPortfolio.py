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
            symbol_price_dict[symbol] = 0.0
        for k in symbol_price_dict.keys():
            symbol_price_dict[k] = self.fetcher.fetchData(
                _tradingday, k
            )[self.settlement_price_index][0]
        return symbol_price_dict

    def _iter_portfolio_settlement(
            self, _tradingday, _next_tradingday,
            _symbol_price_dict: typing.Dict[str, float]
    ):
        # update each portfolio settlement
        self.portfolio.dealSettlement(
            _tradingday, _next_tradingday,
            _symbol_price_dict
        )

    def allocQuantity(
            self, **kwargs
    ) -> int:
        """
        the core part to alloc quantity

        :return:
        """
        return int(kwargs['_strength'])

    def _update_next_position(
            self, _tradingday: str,
            _product2position: ProductPosition
    ):
        """
        comp the next instrument and quantity according cur strength

        :param _tradingday:
        :return:
        """
        _product2position.next_instrument = self.fetcher.fetchSymbol(
            _tradingday, _product=_product2position.product
        )
        # !!! here is the portfolio !!! #
        _product2position.next_quantity = self.allocQuantity(
            _strength=_product2position.strength
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

    def _iter_send_order(self, _tradingday):
        # send orders
        for s in self.strategy_table.values():
            for p in s:
                # next dominant instrument
                self._update_next_position(
                    _tradingday, p
                )
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

    def dealSettlement(self, _tradingday, _next_tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        symbol_price_dict = self._get_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._iter_portfolio_settlement(
            _tradingday, _next_tradingday,
            symbol_price_dict
        )

        # 3. update each strategy's positions to current status
        self._iter_update_cur_position()
        # 4. send new orders
        self._iter_send_order(_tradingday)

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass
