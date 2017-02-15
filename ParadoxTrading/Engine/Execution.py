import typing

import ParadoxTrading.Engine
import ParadoxTrading.Engine.Event
import ParadoxTrading.Engine.Engine
from ParadoxTrading.Utils import DataStruct


class ExecutionAbstract:
    def __init__(self):
        self.SIGNAL_TYPE = ParadoxTrading.Engine.SignalType
        self.ORDER_TYPE = ParadoxTrading.Engine.OrderType
        self.ACTION_TYPE = ParadoxTrading.Engine.ActionType
        self.DIRECTION_TYPE = ParadoxTrading.Engine.DirectionType

        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

        self.order_dict: typing.Dict[int, ParadoxTrading.Engine.Event.OrderEvent] = {}

    def _setEngine(self, _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'):
        self.engine = _engine

    def dealOrderEvent(
            self,
            _order_event: 'ParadoxTrading.Engine.Event.OrderEvent'
    ):
        raise NotImplementedError('deal not implemented')

    def matchMarket(self, _instrument: str, _data: DataStruct):
        raise NotImplementedError('matchMarket not implemented')

    def addEvent(
            self,
            _fill_event: 'ParadoxTrading.Engine.Event.FillEvent'
    ):
        self.engine.addEvent(_fill_event)


class SimpleBacktestExecution(ExecutionAbstract):
    def __init__(self):
        super().__init__()

    def dealOrderEvent(
            self,
            _order_event: 'ParadoxTrading.Engine.Event.OrderEvent'
    ):
        assert _order_event.index not in self.order_dict.keys()

        self.order_dict[_order_event.index] = _order_event

    def _genEvent(
            self, _price: float,
            _order_event: 'ParadoxTrading.Engine.Event.OrderEvent'
    ) -> 'ParadoxTrading.Engine.Event.FillEvent':
        return ParadoxTrading.Engine.Event.FillEvent(
            _index=_order_event.index,
            _instrument=_order_event.instrument,
            _datetime=self.engine.getCurDatetime(),
            _quantity=_order_event.quantity,
            _action=_order_event.action,
            _direction=_order_event.direction,
            _price=_price,
            _commission=0.0
        )

    def matchMarket(self, _instrument: str, _data: DataStruct):
        assert len(_data) == 1

        askprice: float = _data.getColumn('askprice')[0]
        bidprice: float = _data.getColumn('bidprice')[0]

        for index in sorted(self.order_dict.keys()):
            order = self.order_dict[index]
            if order.instrument == _instrument:
                if order.direction == self.DIRECTION_TYPE.BUY:
                    if askprice > 0 and \
                            (order.order_type == self.ORDER_TYPE.MARKET or
                                     askprice <= order.price):
                        self.addEvent(self._genEvent(askprice, order))
                        del self.order_dict[index]
                elif order.direction == self.DIRECTION_TYPE.SELL:
                    if bidprice > 0 and \
                            (order.order_type == self.ORDER_TYPE.MARKET or
                                     bidprice >= order.price):
                        self.addEvent(self._genEvent(bidprice, order))
                        del self.order_dict[index]
                else:
                    raise Exception('unknown direction type')
