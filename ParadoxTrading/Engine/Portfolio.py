import typing

import tabulate

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import (SignalEvent, OrderEvent, FillEvent)
from ParadoxTrading.Engine.Strategy import StrategyAbstract


class PortfolioPerStrategy:
    LONG = 'LONG'
    SHORT = 'SHORT'

    def __init__(self):
        # records for signal, order and fill
        self.signal_record: typing.List[SignalEvent] = []
        self.order_record: typing.List[OrderEvent] = []
        self.fill_record: typing.List[FillEvent] = []

        # cur order and position state
        self.position: typing.Dict[str, typing.Dict[str, int]] = {}
        self.unfilled_order: typing.Dict[int, OrderEvent]

    def incPosition(self, _instrument: str, _type: str, _quantity: int = 1):
        """
        inc position of instrument

        :param _instrument: instrument to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == self.LONG or _type == self.SHORT
        assert _quantity > 0
        try:
            # try to add directly
            self.position[_instrument][_type] += _quantity
        except KeyError:
            # create if failed
            self.position[_instrument] = {
                self.LONG: 0,
                self.SHORT: 0,
            }
            self.position[_instrument][_type] = _quantity

    def decPosition(self, _instrument: str, _type: str, _quantity: int = 1):
        """
        dec position of instrument

        :param _instrument: instrument to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == self.LONG or _type == self.SHORT
        assert _quantity > 0
        assert _instrument in self.position.keys()
        assert self.position[_instrument][_type] >= _quantity
        self.position[_instrument][_type] -= _quantity
        assert self.position[_instrument][_type] >= 0

    def getPosition(self, _instrument: str, _type: str) -> int:
        """
        get cur position of instrument

        :param _instrument: which instrument
        :param _type: long or short
        :return: number of position
        """
        assert _type == self.LONG or _type == self.SHORT
        if _instrument in self.position.keys():
            return self.position[_instrument][_type]
        return 0

    def __repr__(self) -> str:
        table = []
        for k, v in self.position.items():
            table.append([k, v[self.LONG], v[self.SHORT]])
        return tabulate.tabulate(table, ['instrument', self.LONG, self.SHORT])


class PortfolioAbstract:
    def __init__(self):
        self.SIGNAL_TYPE = ParadoxTrading.Engine.SignalType
        self.ORDER_TYPE = ParadoxTrading.Engine.OrderType
        self.ACTION_TYPE = ParadoxTrading.Engine.ActionType
        self.DIRECTION_TYPE = ParadoxTrading.Engine.DirectionType

        self.order_index: int = 0
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

    def _setEngine(self, _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'):
        self.engine = _engine

    def incOrderIndex(self):
        tmp = self.order_index
        self.order_index += 1
        return tmp

    def dealSignal(self, _event: SignalEvent):
        raise NotImplementedError('dealSignal not implemented')

    def dealFill(self, _event: FillEvent):
        raise NotImplementedError('dealFill not implemented')

    def addStrategy(self, _strategy: StrategyAbstract):
        raise NotImplementedError('addStrategy not implemented')


class SimplePortfolio(PortfolioAbstract):
    def __init__(self):
        super().__init__()

        self.global_portfolio: PortfolioPerStrategy = PortfolioPerStrategy()
        self.strategy_portfolio_dict: typing.Dict[str, PortfolioPerStrategy] = {}

    def dealSignal(self, _event: SignalEvent):
        assert self.engine is not None

        order_event = OrderEvent(
            _index=self.incOrderIndex(),
            _instrument=_event.instrument,
            _datetime=self.engine.getCurDatetime(),
        )

        print(self.strategy_portfolio_dict[_event.strategy_name])
        input()

    def dealFill(self, _event: FillEvent):
        pass

    def addStrategy(self, _strategy: StrategyAbstract):
        assert _strategy.name not in self.strategy_portfolio_dict.keys()
        self.strategy_portfolio_dict[_strategy.name] = PortfolioPerStrategy()
