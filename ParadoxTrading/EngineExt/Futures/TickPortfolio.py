import logging
import re
import sys

import typing
from ParadoxTrading.Engine import ActionType, DirectionType, FillEvent, \
    OrderEvent, OrderType, PortfolioAbstract, SignalEvent, SignalType
from ParadoxTrading.EngineExt.Futures.PointValue import POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Utils import DataStruct


class TickPortfolio(PortfolioAbstract):
    """
    this simple portfolio is suitable for tick level backtest.
    """

    def __init__(
        self,
        _fetcher: FetchAbstract,
        _init_fund: float = 0.0,
        _margin_rate: float = 1.0,
        _settlement_price_index='lastprice'
    ):
        super().__init__(_init_fund, _margin_rate)

        self.index_strategy_table: typing.Dict[int, str] = {}

        self.fetcher = _fetcher
        self.settlement_price_index = _settlement_price_index

        self.addPickleKey('index_strategy_table')

    def _gen_order(
            self, _symbol: str,
            _action: int, _direction: int, _quantity: int
    ) -> OrderEvent:
        return OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.MARKET,
            _action=_action,
            _direction=_direction,
            _quantity=_quantity,
        )

    def dealSignal(self, _event: SignalEvent):
        # add signal event to portfolio record
        self.portfolio_mgr.dealSignal(_event)

        instrument = _event.symbol
        product = re.findall(r'[a-zA-Z]+', instrument)[0]

        order_list: typing.List[OrderEvent] = []
        short_quantity = self.portfolio_mgr.getPosition(
            instrument, SignalType.SHORT
        )
        long_quantity = self.portfolio_mgr.getPosition(
            instrument, SignalType.LONG
        )
        if _event.signal_type == SignalType.LONG:
            if short_quantity > 0:  # close short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.BUY,
                    short_quantity
                ))
            if long_quantity == 0:  # open long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.OPEN, DirectionType.BUY,
                    POINT_VALUE[product]
                ))
        elif _event.signal_type == SignalType.SHORT:
            if long_quantity > 0:  # close long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.SELL,
                    long_quantity
                ))
            if short_quantity == 0:  # open short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.OPEN, DirectionType.SELL,
                    POINT_VALUE[product]
                ))
        elif _event.signal_type == SignalType.EMPTY:
            if long_quantity > 0:  # close long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.SELL,
                    long_quantity
                ))
            if short_quantity > 0:  # close short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.BUY,
                    short_quantity
                ))
        else:
            raise Exception('unknown signal type')

        for o in order_list:
            # buf the strategy who send order
            self.index_strategy_table[o.index] = _event.strategy
            # send order event
            self.addEvent(o)
            # add order event to record
            self.portfolio_mgr.dealOrder(_event.strategy, o)

    def dealFill(self, _event: FillEvent):
        self.portfolio_mgr.dealFill(
            self.index_strategy_table[_event.index], _event
        )

    def dealSettlement(self, _tradingday):
        assert _tradingday

        # do settlement for cur positions
        symbol_price_dict = {}
        for symbol in self.portfolio_mgr.getSymbolList():
            try:
                symbol_price_dict[symbol] = self.fetcher.fetchData(
                    _tradingday, symbol
                )[self.settlement_price_index][0]
            except TypeError as e:
                logging.error('Tradingday: {}, Symbol: {}, e: {}'.format(
                    _tradingday, symbol, e
                ))
                sys.exit(1)
        self.portfolio_mgr.dealSettlement(
            _tradingday, symbol_price_dict
        )

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass
