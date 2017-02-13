import typing
from datetime import datetime
from collections import deque

from ParadoxTrading.Engine.Event import EventAbstract, EventType
from ParadoxTrading.Engine.MarketSupply import BacktestMarketSupply
from ParadoxTrading.Engine.Strategy import StrategyAbstract
from ParadoxTrading.Engine.Portfolio import PortfolioAbstract


class EngineAbstract:
    def addEvent(self, _event: EventAbstract):
        raise NotImplementedError('addEvent not implemented')

    def addPortfolio(self, _portfolio: PortfolioAbstract):
        raise NotImplementedError('addPortfolio not implemented')

    def addStrategy(self, _strategy: StrategyAbstract):
        raise NotImplementedError('addStrategy not implemented')

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        raise NotImplementedError('getCurDatetime not implemented')

    def run(self):
        raise NotImplementedError('run not implemented')


class BacktestEngine(EngineAbstract):
    def __init__(self, _begin_day: str, _end_day: str):
        """
        Engine used for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        """

        self.event_queue: deque = deque()  # store event
        self.strategy_dict: typing.Dict[str, StrategyAbstract] = {}  # map strategy's object to its name

        self.begin_day: str = _begin_day
        self.end_day: str = _end_day

        self.market_supply = BacktestMarketSupply(
            self.begin_day, self.end_day, self
        )

        self.portfolio: PortfolioAbstract = None  # portfolio manager

    def addEvent(self, _event: EventAbstract):
        """
        Add event into queue

        :param _event: MarketEvent, SignalEvent ...
        :return: None
        """
        assert isinstance(_event, EventAbstract)
        self.event_queue.append(_event)

    def addPortfolio(self, _portfolio: PortfolioAbstract):
        """
        set portfolio

        :param _portfolio: _portfolio for this backtest
        :return:
        """
        assert self.portfolio is None

        self.portfolio = _portfolio
        _portfolio._setEngine(self)

    def addStrategy(self, _strategy: StrategyAbstract):
        """
        Register strategy to engine

        :param _strategy: object
        :return: None
        """

        assert self.portfolio is not None
        assert _strategy.name not in self.strategy_dict.keys()

        self.strategy_dict[_strategy.name] = _strategy
        _strategy._setEngine(self)

        self.market_supply.addStrategy(_strategy)
        self.portfolio.addStrategy(_strategy)

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        """
        Return latest datetime of market

        :return: datetime
        """
        return self.market_supply.getCurDatetime()

    def run(self):
        """
        backtest until there is no market tick

        :return:
        """
        while self.market_supply.updateData():  # while there is data
            print('--- one tick ---', self.getCurDatetime())
            while len(self.event_queue):  # deal all event at that moment
                event = self.event_queue.popleft()
                if event.type == EventType.MARKET:
                    self.strategy_dict[event.strategy_name].deal(
                        event.market_register_key)
                elif event.type == EventType.SIGNAL:
                    self.portfolio.dealSignal(event)
                elif event.type == EventType.ORDER:
                    print(event)
                    # self.execution
                elif event.type == EventType.FILL:
                    self.portfolio.dealFill(event)
                else:
                    raise Exception('Unknow event type!')
