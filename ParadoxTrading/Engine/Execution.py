import typing

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import OrderEvent, FillEvent
from ParadoxTrading.Utils import DataStruct


class ExecutionAbstract:
    def __init__(self):
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

        self.order_dict: typing.Dict[
            int, ParadoxTrading.Engine.Event.OrderEvent] = {}

    def setEngine(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        self.engine = _engine

    def dealOrderEvent(self, _order_event: OrderEvent):
        raise NotImplementedError('deal not implemented')

    def matchMarket(self, _instrument: str, _data: DataStruct):
        raise NotImplementedError('matchMarket not implemented')

    def addEvent(self, _fill_event: FillEvent):
        self.engine.addEvent(_fill_event)
