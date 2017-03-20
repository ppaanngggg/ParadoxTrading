import typing
from collections import deque
from datetime import datetime

from ParadoxTrading.Engine.Event import EventAbstract
from ParadoxTrading.Engine.Execution import ExecutionAbstract
from ParadoxTrading.Engine.MarketSupply import MarketSupplyAbstract
from ParadoxTrading.Engine.Portfolio import PortfolioAbstract
from ParadoxTrading.Engine.Strategy import StrategyAbstract
from ParadoxTrading.Utils import DataStruct


class EngineAbstract:
    def __init__(self):
        self.event_queue: deque = deque()  # store event
        self.strategy_dict: typing.Dict[str, StrategyAbstract] = {}

        self.market_supply: MarketSupplyAbstract = None
        self.execution: ExecutionAbstract = None
        self.portfolio: PortfolioAbstract = None

    def addEvent(self, _event: EventAbstract):
        """
        Add event into queue

        :param _event: MarketEvent, SignalEvent ...
        :return: None
        """
        assert isinstance(_event, EventAbstract)
        self.event_queue.append(_event)

    def addMarketSupply(self, _market_supply: MarketSupplyAbstract):
        assert self.market_supply is None
        self.market_supply = _market_supply
        _market_supply.setEngine(self)

    def addExecution(self, _execution: ExecutionAbstract):
        """
        set execution

        :param _execution:
        :return:
        """
        assert self.execution is None

        self.execution = _execution
        _execution.setEngine(self)

    def addPortfolio(self, _portfolio: PortfolioAbstract):
        """
        set portfolio

        :param _portfolio: _portfolio for this backtest
        :return:
        """
        assert self.portfolio is None

        self.portfolio = _portfolio
        _portfolio.setEngine(self)

    def addStrategy(self, _strategy: StrategyAbstract):
        """
        Register strategy to engine

        :param _strategy: object
        :return: None
        """
        assert self.market_supply is not None
        assert self.portfolio is not None
        assert _strategy.name not in self.strategy_dict.keys()

        self.strategy_dict[_strategy.name] = _strategy
        _strategy.setEngine(self)

        self.market_supply.addStrategy(_strategy)
        self.portfolio.addStrategy(_strategy)

    def getTradingDay(self) -> str:
        """
        Return cur tradingday of market

        :return: str
        """
        return self.market_supply.getTradingDay()

    def getDatetime(self) -> typing.Union[None, datetime]:
        """
        Return latest datetime of market

        :return: datetime
        """
        return self.market_supply.getDatetime()

    def getSymbolData(self, _symbol: str) -> DataStruct:
        """
        Return data

        :param _symbol:
        :return:
        """
        return self.market_supply.getSymbolData(_symbol)

    def run(self):
        raise NotImplementedError('run not implemented')
