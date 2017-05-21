import typing

from ParadoxTrading.Engine import FillEvent, SignalEvent, OrderEvent, OrderType, ActionType, DirectionType
from ParadoxTrading.Engine import PortfolioAbstract
from ParadoxTrading.Utils import DataStruct


class ProductPosition:
    def __init__(self, _product):
        self.product = _product
        self.cur_instrument = None
        self.cur_quantity = 0
        self.next_instrument = None
        self.next_quantity = 0

        self.detail: typing.Dict[str, int] = {}

    def dealSignal(self, _event: SignalEvent):
        self.product = _event.symbol
        self.next_instrument = _event.symbol
        self.next_quantity = int(_event.strength * 10.)

    def dealFill(self, _event: FillEvent):
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
            '\tCUR_INSTRUMENT: {}\n' \
            '\tCUR_QUANTITY: {}\n' \
            '\tNEXT_INSTRUMENT: {}\n' \
            '\tNEXT_QUANTITY: {}\n'.format(
                self.product,
                self.cur_instrument,
                self.cur_quantity,
                self.next_instrument,
                self.next_quantity,
            )


class StrategyProduct:
    def __init__(self, _strategy_name):
        self.strategy_name = _strategy_name
        self.product_table: typing.Dict[str, ProductPosition] = {}

    def dealSignal(self, _event: SignalEvent) -> ProductPosition:
        product = _event.symbol
        try:
            product_position = self.product_table[product]
        except KeyError:
            self.product_table[product] = ProductPosition(product)
            product_position = self.product_table[product]

        product_position.dealSignal(_event)
        return product_position

    def __iter__(self):
        for p in self.product_table.values():
            yield p

    def __repr__(self) -> str:
        ret = 'STRATEGY: {}\n'.format(self.strategy_name)
        for k, v in self.product_table.items():
            ret += '--- {} ---:\n{}\n'.format(k, v)

        return ret


class OrderStrategyProduct:
    def __init__(self, _order_index, _strategy_name, _product):
        self.order_index = _order_index
        self.strategy_name = _strategy_name
        self.product = _product

    def __repr__(self):
        return \
            'ORDER_INDEX: {}\n' \
            'STRATEGY_NAME: {}\n' \
            'PRODUCT: {}\n'.format(
                self.order_index,
                self.strategy_name,
                self.product
            )


class CTAPortfolio(PortfolioAbstract):
    """

    :param _fetcher:
    :param _settlement_price_index:
    """

    def __init__(
            self,
            _fetcher,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__()

        self.fetcher = _fetcher
        self.settlement_price_index = _settlement_price_index

        self.strategy_table: typing.Dict[str, StrategyProduct] = {}
        self.order_table: typing.Dict[str, OrderStrategyProduct] = {}

    def dealSignal(self, _event: SignalEvent):
        """
        
        :param _event: 
        :return: 
        """
        try:
            strategy_product = self.strategy_table[_event.strategy_name]
        except KeyError:
            self.strategy_table[_event.strategy_name] = StrategyProduct(
                _event.strategy_name
            )
            strategy_product = self.strategy_table[_event.strategy_name]

        # product_position
        strategy_product.dealSignal(_event)

    def dealFill(self, _event: FillEvent):
        """
        
        :param _event: 
        :return: 
        """
        order_strategy_product: OrderStrategyProduct = self.order_table[_event.index]
        strategy_product = self.strategy_table[order_strategy_product.strategy_name]
        product_position = strategy_product.product_table[order_strategy_product.product]
        product_position.dealFill(_event)

        self.getPortfolioByIndex(_event.index).dealFillEvent(_event)

    def _update_next_instrument(self, _p: ProductPosition, _tradingday: str):
        # p had been update, and the next_instrument will be set to dominant
        if _p.next_instrument:
            _p.next_instrument = self.fetcher.fetchSymbol(
                _tradingday, _p.product
            )
        else: # has no signal
            if _p.cur_quantity: # if cur has position, continue
                _p.next_instrument = self.fetcher.fetchSymbol(
                    _tradingday, _p.product
                )
                _p.next_quantity = _p.cur_quantity

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
            pass
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
            pass
        return ret_list

    def _gen_orders(self, _p: ProductPosition) -> typing.List[OrderEvent]:
        if _p.cur_quantity:  # cur is not empty
            if _p.next_quantity:  # next is not empty, need to adjust position
                if _p.cur_instrument != _p.next_instrument or _p.cur_quantity * _p.next_quantity < 0:
                    # change instrument, or change direction, need to close cur and open next
                    return self._close_cur_open_next(_p)
                else:
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
                                raise Exception('excuse me ??? quantity_diff == 0 ???')
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
                                raise Exception('excuse me ??? quantity_diff == 0 ???')
                        else:
                            raise Exception('excuse me ??? next_quantity == 0 ???')
                    else:  # quantity not changes
                        return []
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
                    raise Exception('excuse me ??? cur_quantity == 0 ???')
        else:  # cur is empty
            if _p.next_quantity:  # next is not empty, to open position
                if _p.next_quantity > 0:
                    return [self._create_order(
                        _p.next_instrument, ActionType.OPEN, DirectionType.BUY, _p.next_quantity
                    )]
                elif _p.next_quantity < 0:
                    return [self._create_order(
                        _p.next_instrument, ActionType.OPEN, DirectionType.SELL, -_p.next_quantity
                    )]
                raise Exception('excuse me ??? next_quantity == 0 ???')
            else:  # next is empty, nothing to do
                return []

    @staticmethod
    def _reset_pp(_p: ProductPosition):
        _p.next_instrument = None
        _p.next_quantity = 0
        _p.detail = {}

    @staticmethod
    def _update_position(_p: ProductPosition):
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

    def dealSettlement(self, _tradingday, _next_tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # iter each strategy
        for s in self.strategy_table.values():
            # iter each product
            for p in s:
                # update position according to fill event
                self._update_position(p)
                # check the next dominant instrument
                self._update_next_instrument(p, _tradingday)
                # send orders according to instrument and quantity chg
                orders = self._gen_orders(p)
                for o in orders:
                    self.order_table[o.index] = OrderStrategyProduct(
                        o.index, s.strategy_name, p.product
                    )
                    self.addEvent(o, s.strategy_name)
                    # input(p)
                    self.getPortfolioByStrategy(s.strategy_name).dealOrderEvent(o)
                # reset next status
                self._reset_pp(p)

        # get price dict for each portfolio
        symbol_price_dict = {}
        for s in self.strategy_table.values():
            for p in s:
                if p.cur_instrument:
                    symbol_price_dict[p.cur_instrument] = 0.0
        for k in symbol_price_dict.keys():
            symbol_price_dict[k] = self.fetcher.fetchData(
                _tradingday, k
            )[self.settlement_price_index][0]

        # update each portfolio settlement
        for v in self.strategy_portfolio_dict.values():
            v.dealSettlement(
                _tradingday, _next_tradingday,
                symbol_price_dict
            )

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass
