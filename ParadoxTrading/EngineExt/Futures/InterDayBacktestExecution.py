import logging
import sys

from ParadoxTrading.Engine import ExecutionAbstract, FillEvent, OrderEvent, \
    OrderType
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Utils import DataStruct


class InterDayBacktestExecution(ExecutionAbstract):
    """
    This is a simple and !!dangerous!! execution.
    It only accept limit order with price.
    And it will send fill event according to the order's
    price directly.

    :param _commission_rate:
    """

    def __init__(
            self, _fetcher: FetchBase,
            _commission_rate: float = 0.0,
            _price_idx='openprice'
    ):
        super().__init__()

        self.fetcher: FetchBase = _fetcher
        self.commission_rate = _commission_rate
        self.price_idx = _price_idx

    def dealOrderEvent(
            self, _order_event: OrderEvent
    ):
        assert _order_event.order_type == OrderType.MARKET

        tradingday = self.engine.getTradingDay()
        symbol = _order_event.symbol
        try:
            price = self.fetcher.fetchData(
                self.engine.getTradingDay(), _order_event.symbol,
            )[self.price_idx][0]
        except TypeError as e:
            # if not available, use last tradingday's closeprice
            logging.warning('Tradingday: {}, Symbol: {}, e: {}'.format(
                tradingday, symbol, e
            ))
            if input('Continue:(y/n): ') != 'y':
                sys.exit(1)
            price = self.fetcher.fetchData(
                self.fetcher.instrumentLastTradingDay(
                    _order_event.symbol, self.engine.getTradingDay()
                ), _order_event.symbol
            )[self.price_idx][0]

        fill_event = FillEvent(
            _index=_order_event.index,
            _symbol=symbol,
            _tradingday=tradingday,
            _datetime=self.engine.getDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=price,
            _commission=self.commission_rate * _order_event.quantity * price
        )
        self.addEvent(fill_event)

    def matchMarket(self, _symbol: str, _data: DataStruct):
        pass
