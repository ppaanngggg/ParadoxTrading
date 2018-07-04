from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, OrderType, \
    DirectionType, FillEvent
from ParadoxTrading.Utils import DataStruct


class DepthBacktestExecution(ExecutionAbstract):

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

    def _get_mean_price(self, _data_dict, _amount, _direction):
        turnover = 0.0
        tmp_amount = _amount
        for i in range(10):
            price = _data_dict['{}price{}'.format(_direction, i)]
            amount = _data_dict['{}amount{}'.format(_direction, i)]
            if price is None or amount is None:
                break
            if tmp_amount > amount:  # not enough
                turnover += amount * price
                tmp_amount -= amount
            else:  # end
                turnover += tmp_amount * price
                return turnover / _amount

        turnover += tmp_amount * price
        return turnover / _amount

    def _gen_event(
            self, _price: float,
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
            _commission=self.commission_rate * _order_event.quantity * _price
        )

    def matchMarket(self, _symbol: str, _data: DataStruct):
        if not self.order_dict:  # skip if no order
            return

        assert len(_data) == 1
        data_dict = _data.toDict()

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.symbol != _symbol:
                continue
            if order.direction == DirectionType.BUY:
                avg_price = self._get_mean_price(
                    data_dict, order.quantity, 'ask'
                )
                self.addEvent(self._gen_event(avg_price, order))
                del self.order_dict[index]
            elif order.direction == DirectionType.SELL:
                avg_price = self._get_mean_price(
                    data_dict, order.quantity, 'bid'
                )
                self.addEvent(self._gen_event(avg_price, order))
                del self.order_dict[index]
            else:
                raise Exception('unknown direction type')
