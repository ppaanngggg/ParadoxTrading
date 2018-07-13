from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, OrderType, \
    FillEvent
from ParadoxTrading.Utils import DataStruct


class TickerBacktestExecution(ExecutionAbstract):

    def __init__(
            self, _commission_rate: float = 0.0
    ):
        super().__init__()

        self.commission_rate = _commission_rate

    def dealOrderEvent(
            self, _order_event: OrderEvent
    ):
        assert _order_event.order_type == OrderType.MARKET
        assert _order_event.index not in self.order_dict.keys()
        self.order_dict[_order_event.index] = _order_event

    def _gen_event(
            self, _price: float, _order_event: OrderEvent
    ) -> FillEvent:
        return FillEvent(
            _index=_order_event.index,
            _symbol=_order_event.symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=_price,
            _commission=self.commission_rate * _order_event.quantity * _price
        )

    def matchMarket(self, _symbol: str, _data: DataStruct):
        if not self.order_dict:  # skip if no order
            return

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.symbol != _symbol:
                continue
            last_price = _data['price'][-1]
            self.addEvent(self._gen_event(last_price, order))
            del self.order_dict[index]
