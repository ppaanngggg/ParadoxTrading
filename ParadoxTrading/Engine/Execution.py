import typing

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import OrderEvent, FillEvent, OrderType, \
    DirectionType
from ParadoxTrading.Utils import DataStruct


class ExecutionAbstract:
    def __init__(self):
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

        self.order_dict: typing.Dict[
            int, ParadoxTrading.Engine.Event.OrderEvent] = {}

    def _setEngine(self,
                   _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        self.engine = _engine

    def dealOrderEvent(self,
                       _order_event: OrderEvent):
        raise NotImplementedError('deal not implemented')

    def matchMarket(self, _instrument: str, _data: DataStruct):
        raise NotImplementedError('matchMarket not implemented')

    def addEvent(self, _fill_event: FillEvent):
        self.engine.addEvent(_fill_event)


class SimpleTickBacktestExecution(ExecutionAbstract):
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
        return ParadoxTrading.Engine.Event.FillEvent(
            _index=_order_event.index,
            _instrument=_order_event.instrument,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getCurDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=_price,
            _commission=0.0)

    def matchMarket(self, _instrument: str, _data: DataStruct):
        assert len(_data) == 1

        askprice: float = _data.getColumn('askprice')[0]
        bidprice: float = _data.getColumn('bidprice')[0]

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.instrument == _instrument:
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


class SimpleBarBacktestExecution(ExecutionAbstract):
    def __init__(self):
        super().__init__()

    def dealOrderEvent(self,
                       _order_event: OrderEvent):
        assert _order_event.order_type == OrderType.LIMIT
        fill_event = FillEvent(
            _index=_order_event.index,
            _instrument=_order_event.instrument,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getCurDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=_order_event.price,
            _commission=0.0,
        )
        self.addEvent(fill_event)

    def matchMarket(self, _instrument: str, _data: DataStruct):
        pass
