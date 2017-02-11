import typing
from collections import deque

from ParadoxTrading.Engine.Event import EventAbstract, EventType
from ParadoxTrading.Engine.MarketSupply import BacktestMarketSupply
from ParadoxTrading.Engine.Strategy import StrategyAbstract


class EngineAbstract:

    def addEvent(self, _event: EventAbstract):
        raise NotImplementedError('addEvent not implemented')

    def addStrategy(self, _strategy: StrategyAbstract):
        raise NotImplementedError('addStrategy not implemented')

    def getCurDatetime(self):
        raise NotImplementedError('getCurDatetime not implemented')

    def run(self):
        raise NotImplementedError('run not implemented')


class BacktestEngine(EngineAbstract):

    def __init__(self, _begin_day: str, _end_day: str):

        self.event_queue = deque()  # typing.Sequence[EventAbstract]
        self.strategy_dict = {}  # typing.Dict[str, StrategyAbstract]

        self.begin_day = _begin_day
        self.end_day = _end_day

        self.market_supply = BacktestMarketSupply(
            self.begin_day, self.end_day, self.event_queue
        )

    def addEvent(self, _event: EventAbstract):
        assert isinstance(_event, EventAbstract)
        self.event_queue.append(_event)

    def addStrategy(self, _strategy: StrategyAbstract):
        assert _strategy.name not in self.strategy_dict.keys()
        _strategy._setEngine(self)
        self.strategy_dict[_strategy.name] = _strategy
        for key in _strategy.market_register_dict.keys():
            self.market_supply.registerStrategy(_strategy.name, key)
            _strategy.market_register_dict[key] = \
                self.market_supply.market_register_dict[key]

    def getCurDatetime(self):
        return self.market_supply.getCurDatetime()

    def run(self):
        while self.market_supply.updateData():
            while len(self.event_queue):
                event = self.event_queue.popleft()
                if event.type == EventType.MARKET:
                    self.strategy_dict[event.strategy_name].deal(
                        event.market_register_key)
                elif event.type == EventType.SIGNAL:
                    pass
                elif event.type == EventType.ORDER:
                    pass
                elif event.type == EventType.FILL:
                    pass
                else:
                    raise Exception('Unknow event type!')
