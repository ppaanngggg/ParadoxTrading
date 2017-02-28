import logging
import typing
from collections import deque
from datetime import datetime

from ParadoxTrading.Engine.Event import EventAbstract, EventType
from ParadoxTrading.Engine.Execution import ExecutionAbstract
from ParadoxTrading.Engine.MarketSupply import MarketSupplyAbstract, \
    BacktestMarketSupply
from ParadoxTrading.Engine.Portfolio import PortfolioAbstract
from ParadoxTrading.Engine.Strategy import StrategyAbstract
from ParadoxTrading.Utils import DataStruct


class EngineAbstract:
    def __init__(self):
        self.market_supply: MarketSupplyAbstract = None
        self.execution: ExecutionAbstract = None
        self.portfolio: PortfolioAbstract = None

    def addEvent(self, _event: EventAbstract):
        raise NotImplementedError('addEvent not implemented')

    def addExecution(self, _execution: ExecutionAbstract):
        raise NotImplementedError('addExecution not implemented')

    def addPortfolio(self, _portfolio: PortfolioAbstract):
        raise NotImplementedError('addPortfolio not implemented')

    def addStrategy(self, _strategy: StrategyAbstract):
        raise NotImplementedError('addStrategy not implemented')

    def getTradingDay(self) -> str:
        raise NotImplementedError('getTradingDay not implemented')

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        raise NotImplementedError('getCurDatetime not implemented')

    def getInstrumentData(self, _instrument: str) -> DataStruct:
        raise NotImplementedError('getInstrumentData not implemented')

    def run(self):
        raise NotImplementedError('run not implemented')


class BacktestEngine(EngineAbstract):
    def __init__(self, _begin_day: str, _end_day: str, _backtest_type: str):
        """
        Engine used for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        """
        super().__init__()

        self.event_queue: deque = deque()  # store event
        self.strategy_dict: typing.Dict[str, StrategyAbstract] = {
        }  # map strategy's object to its name

        self.begin_day: str = _begin_day
        self.end_day: str = _end_day

        self.market_supply = BacktestMarketSupply(
            self.begin_day, self.end_day, self, _backtest_type)
        self.execution: ExecutionAbstract = None
        self.portfolio: PortfolioAbstract = None  # portfolio manager

    def addEvent(self, _event: EventAbstract):
        """
        Add event into queue

        :param _event: MarketEvent, SignalEvent ...
        :return: None
        """
        assert isinstance(_event, EventAbstract)
        self.event_queue.append(_event)

    def addExecution(self, _execution: ExecutionAbstract):
        """
        set execution

        :param _execution:
        :return:
        """
        assert self.execution is None

        self.execution = _execution
        _execution._setEngine(self)

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

    def getTradingDay(self) -> str:
        """
        Return cur tradingday of market

        :return: str
        """
        return self.market_supply.getTradingDay()

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        """
        Return latest datetime of market

        :return: datetime
        """
        return self.market_supply.getCurDatetime()

    def getInstrumentData(self, _instrument: str) -> DataStruct:
        """
        Return data

        :param _instrument:
        :return:
        """
        return self.market_supply.getInstrumentData(_instrument)

    def run(self):
        """
        backtest until there is no market tick

        :return:
        """
        logging.info('Begin RUN!')
        while True:
            data = self.market_supply.updateData()
            if data is None:
                # if data is None, it means end
                break
            while True:
                # match market for each tick, maybe there will be order to fill.
                # If filled, execution will add fill event into queue
                self.execution.matchMarket(*data)

                if len(self.event_queue):  # deal all event at that moment
                    event = self.event_queue.popleft()
                    if event.type == EventType.MARKET:
                        self.strategy_dict[event.strategy_name].deal(event)
                    elif event.type == EventType.SIGNAL:
                        self.portfolio.dealSignal(event)
                    elif event.type == EventType.ORDER:
                        self.execution.dealOrderEvent(event)
                    elif event.type == EventType.FILL:
                        self.portfolio.dealFill(event)
                    else:
                        raise Exception('Unknow event type!')
                else:
                    break
