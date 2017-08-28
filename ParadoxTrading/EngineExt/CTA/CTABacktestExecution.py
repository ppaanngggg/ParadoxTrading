from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, OrderType, \
    FillEvent
from ParadoxTrading.Utils import DataStruct


class CTABacktestExecution(ExecutionAbstract):
    """
    This is a simple and !!dangerous!! execution.
    It only accept limit order with price.
    And it will send fill event according to the order's
    price directly.

    :param _commission_rate:
    """

    def __init__(
            self, _fetcher,
            _commission_rate: float = 0.0,
            _price_idx='openprice'
    ):
        super().__init__()

        self.fetcher = _fetcher
        self.commission_rate = _commission_rate
        self.price_idx = _price_idx

    def dealOrderEvent(
            self, _order_event: OrderEvent
    ):
        assert _order_event.order_type == OrderType.MARKET

        fill_price = self.fetcher.fetchData(
            self.engine.getTradingDay(),
            _order_event.symbol,
        )[self.price_idx][0]

        fill_event = FillEvent(
            _index=_order_event.index,
            _symbol=_order_event.symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=fill_price,
            _commission=self.commission_rate * _order_event.quantity * fill_price,
        )
        self.addEvent(fill_event)

    def matchMarket(self, _symbol: str, _data: DataStruct):
        pass
