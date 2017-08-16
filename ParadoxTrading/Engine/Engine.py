import logging
import typing
from collections import deque
from datetime import datetime

from ParadoxTrading.Engine.Event import EventAbstract
from ParadoxTrading.Engine.Execution import ExecutionAbstract
from ParadoxTrading.Engine.MarketSupply import MarketSupplyAbstract
from ParadoxTrading.Engine.Portfolio import PortfolioAbstract
from ParadoxTrading.Engine.Strategy import StrategyAbstract
from ParadoxTrading.Utils import Serializable


class EngineAbstract(Serializable):
    def __init__(
            self,
            _market_supply: MarketSupplyAbstract,
            _execution: ExecutionAbstract,
            _portfolio: PortfolioAbstract,
            _strategy: typing.Union[StrategyAbstract, typing.Iterable[StrategyAbstract]]
    ):
        super().__init__()

        self.event_queue: deque = deque()  # store event

        self.market_supply: MarketSupplyAbstract = None
        self.execution: ExecutionAbstract = None
        self.portfolio: PortfolioAbstract = None
        self.strategy_dict: typing.Dict[str, StrategyAbstract] = {}

        self._add_market_supply(_market_supply)
        self._add_execution(_execution)
        self._add_portfolio(_portfolio)
        if isinstance(_strategy, StrategyAbstract):
            self._add_strategy(_strategy)
        else:
            for s in _strategy:
                self._add_strategy(s)

        self.addPickleSet('event_queue')

    def addEvent(self, _event: EventAbstract):
        """
        Add event into queue

        :param _event: MarketEvent, SignalEvent ...
        :return: None
        """
        assert isinstance(_event, EventAbstract)
        self.event_queue.append(_event)

    def _add_market_supply(self, _market_supply: MarketSupplyAbstract):
        """
        set marketsupply
        
        :param _market_supply: 
        :return: 
        """
        assert self.market_supply is None

        self.market_supply = _market_supply
        _market_supply.setEngine(self)

    def _add_execution(self, _execution: ExecutionAbstract):
        """
        set execution

        :param _execution:
        :return:
        """
        assert self.execution is None

        self.execution = _execution
        _execution.setEngine(self)

    def _add_portfolio(self, _portfolio: PortfolioAbstract):
        """
        set portfolio

        :param _portfolio: _portfolio for this backtest
        :return:
        """
        assert self.portfolio is None

        self.portfolio = _portfolio
        _portfolio.setEngine(self)

    def _add_strategy(self, _strategy: StrategyAbstract):
        """
        Register strategy to engine

        :param _strategy: object
        :return: None
        """
        assert self.market_supply is not None
        assert self.execution is not None
        assert self.portfolio is not None
        assert _strategy.name not in self.strategy_dict.keys()

        self.strategy_dict[_strategy.name] = _strategy
        _strategy.setEngine(self)
        self.market_supply.addStrategy(_strategy)

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

    def run(self):
        raise NotImplementedError('run not implemented')

    def save(self, _path: str, _filename: str = 'Engine'):
        super().save(_path, _filename)
        logging.debug('Engine save to {}'.format(_path))

    def load(self, _path: str, _filename: str = 'Engine'):
        super().load(_path, _filename)
        logging.debug('Engine load from {}'.format(_path))

    def __repr__(self) -> str:
        ret = '[[[ EVENT QUEUE ]]]\n'
        for d in self.event_queue:
            ret += '{}\n'.format(d)
        ret += '[[[ STRATEGY ]]]\n{}'.format(self.strategy_dict.keys())
        return ret
