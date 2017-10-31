from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, FillEvent
from ParadoxTrading.Utils import DataStruct


class BarBacktestExecution(ExecutionAbstract):
    """
    This is a simple and !!dangerous!! execution.
    It only accept limit order with price.
    And it will send fill event according to the order's
    price directly.

    :param _commission_rate:
    """

    def __init__(
            self, _commission_rate: float = 0.0,
            _price_idx='openprice'
    ):
        super().__init__()

        self.commission_rate = _commission_rate
        self.price_idx = _price_idx

    def dealOrderEvent(
            self, _order_event: OrderEvent
    ):
        assert _order_event.index not in self.order_dict.keys()
        self.order_dict[_order_event.index] = _order_event

    def matchMarket(self, _symbol: str, _data: DataStruct):
        assert len(_data) == 1

        time = _data.index()[0]

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.symbol == _symbol and time > order.datetime:
                exec_price: float = _data[self.price_idx][0]
                comm = self.commission_rate * order.quantity * exec_price
                self.addEvent(FillEvent(
                    _index=order.index,
                    _symbol=order.symbol,
                    _tradingday=self.engine.getTradingDay(),
                    _datetime=self.engine.getDatetime(),
                    _quantity=order.quantity,
                    _action=order.action,
                    _direction=order.direction,
                    _price=exec_price,
                    _commission=comm
                ))
                del self.order_dict[index]
