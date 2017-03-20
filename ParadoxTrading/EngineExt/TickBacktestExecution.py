from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, FillEvent, \
    DirectionType, OrderType
from ParadoxTrading.Utils import DataStruct


class TickBacktestExecution(ExecutionAbstract):
    def __init__(self):
        super().__init__()

    def dealOrderEvent(self,
                       _order_event: OrderEvent):
        assert _order_event.index not in self.order_dict.keys()

        self.order_dict[_order_event.index] = _order_event

    def _genEvent(self,
                  _price: float,
                  _order_event: OrderEvent
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
            _commission=0.0)

    def matchMarket(self, _symbol: str, _data: DataStruct):
        assert len(_data) == 1

        askprice: float = _data.getColumn('askprice')[0]
        bidprice: float = _data.getColumn('bidprice')[0]

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.symbol == _symbol:
                if order.direction == DirectionType.BUY:
                    if (order.order_type == OrderType.MARKET or
                                askprice <= order.price) and askprice > 0:
                        self.addEvent(self._genEvent(askprice, order))
                        del self.order_dict[index]
                elif order.direction == DirectionType.SELL:
                    if (order.order_type == OrderType.MARKET or
                                bidprice >= order.price) and bidprice > 0:
                        self.addEvent(self._genEvent(bidprice, order))
                        del self.order_dict[index]
                else:
                    raise Exception('unknown direction type')
