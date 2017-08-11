import logging
import typing

import tabulate

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import OrderEvent, FillEvent, DirectionType, ActionType
from ParadoxTrading.Utils import DataStruct
from ParadoxTrading.Utils import Serializable


class ExecutionAbstract(Serializable):
    def __init__(self):
        super().__init__()

        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

        self.order_dict: typing.Dict[
            int, ParadoxTrading.Engine.Event.OrderEvent] = {}

        self.addPickleSet('order_dict')

    def setEngine(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        self.engine = _engine

    def dealOrderEvent(self, _order_event: OrderEvent):
        raise NotImplementedError('deal not implemented')

    def matchMarket(self, _symbol: str, _data: DataStruct):
        raise NotImplementedError('matchMarket not implemented')

    def addEvent(self, _fill_event: FillEvent):
        self.engine.addEvent(_fill_event)
        logging.info('Execution send {} {} {} {} at {} when {}'.format(
            ActionType.toStr(_fill_event.action),
            DirectionType.toStr(_fill_event.direction),
            _fill_event.quantity,
            _fill_event.symbol,
            _fill_event.price,
            _fill_event.datetime
        ))

    def save(self, _path: str, _filename: str = 'Execution'):
        super().save(_path, _filename)
        logging.info('Execution save to {}'.format(_path))

    def load(self, _path: str, _filename: str = 'Execution'):
        super().load(_path, _filename)
        logging.info('Execution load from {}'.format(_path))

    def __repr__(self) -> str:
        ret = '<<< ORDER DICT >>>\n'
        table = []
        for k, v in self.order_dict.items():
            table.append([
                k, v.symbol,
                ActionType.toStr(v.action),
                DirectionType.toStr(v.direction),
                v.quantity
            ])
        ret += tabulate.tabulate(
            table, ['INDEX', 'SYMBOL', 'ACTION', 'DIRECTION', 'QUANTITY']
        )
        return ret