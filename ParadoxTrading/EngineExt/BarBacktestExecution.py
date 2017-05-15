from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, OrderType, \
    FillEvent
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
            _openprice_idx='openprice'
    ):
        super().__init__()

        self.commission_rate = _commission_rate
        self.openprice_idx = _openprice_idx

    def dealOrderEvent(self,
                       _order_event: OrderEvent):
        if _order_event.order_type == OrderType.LIMIT:
            fill_event = FillEvent(
                _index=_order_event.index,
                _symbol=_order_event.symbol,
                _tradingday=self.engine.getTradingDay(),
                _datetime=self.engine.getDatetime(),
                _quantity=_order_event.quantity,
                _action=_order_event.action,
                _direction=_order_event.direction,
                _price=_order_event.price,
                _commission=self.commission_rate * _order_event.quantity * _order_event.price,
            )
            self.addEvent(fill_event)
        elif _order_event.order_type == OrderType.MARKET:
            assert _order_event.index not in self.order_dict.keys()
            self.order_dict[_order_event.index] = _order_event
        else:
            raise Exception('unknown order type')

    def matchMarket(self, _symbol: str, _data: DataStruct):
        assert len(_data) == 1

        openprice: float = _data[self.openprice_idx][0]

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.symbol == _symbol:
                self.addEvent(FillEvent(
                    _index=order.index,
                    _symbol=order.symbol,
                    _tradingday=self.engine.getTradingDay(),
                    _datetime=self.engine.getDatetime(),
                    _quantity=order.quantity,
                    _action=order.action,
                    _direction=order.direction,
                    _price=openprice,
                    _commission=self.commission_rate * order.quantity * openprice
                ))
                del self.order_dict[index]
