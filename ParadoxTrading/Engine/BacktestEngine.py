import typing
from collections import deque

from ParadoxTrading.Engine.Event import EventAbstract, EventType
from ParadoxTrading.Engine.MarketSupply import BacktestMarketSupply
from ParadoxTrading.Engine.Strategy import StrategyAbstract


class BacktestEngine:

    def __init__(self, _begin_day: str, _end_day: str):

        self.event_queue = deque()  # typing.Sequence[EventAbstract]
        self.strategy_dict = {}  # typing.Dict[str, StrategyAbstract]

        self.begin_day = _begin_day
        self.end_day = _end_day

        self.market_supply = BacktestMarketSupply(
            self.begin_day, self.end_day, self.event_queue
        )

    def addStrategy(self, _strategy: StrategyAbstract):
        assert _strategy.name not in self.strategy_dict.keys()
        self.strategy_dict[_strategy.name] = _strategy
        for key in _strategy.market_register_dict.keys():
            self.market_supply.registerStrategy(_strategy.name, key)
            _strategy.market_register_dict[key] = \
                self.market_supply.market_register_dict[key]

    def run(self):
        while self.market_supply.updateData() is not None:
            while not self.event_queue:
                event = self.event_queue.popleft()
                if event.type == EventType.MARKET:
                    pass
                elif event.type == EventType.SIGNAL:
                    pass
                elif event.type == EventType.ORDER:
                    pass
                elif event.type == EventType.FILL:
                    pass
                else:
                    raise Exception('Unknow event type!')
