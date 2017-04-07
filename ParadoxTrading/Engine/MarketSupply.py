"""该模块

"""

from datetime import datetime

import typing

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import MarketEvent
from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class MarketSupplyAbstract:
    def __init__(self, _fetcher: FetchAbstract):
        """
        base class market supply

        """
        self.fetcher: FetchAbstract = _fetcher
        # map market register's key to its object
        self.register_dict: typing.Dict[str, RegisterAbstract] = {}
        # map symbol to set of market register
        self.symbol_dict: typing.Dict[str, typing.Set[str]] = {}
        self.data_dict: typing.Dict[str, DataStruct] = {}

        self.engine: ParadoxTrading.Engine.EngineAbstract = None

    def setEngine(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        self.engine = _engine

    def addStrategy(self, _strategy: 'ParadoxTrading.Engine.StrategyAbstract'):
        """
        Add strategy into market supply

        :param _strategy: strategy oject
        :return: None
        """

        # for each market register
        for key in _strategy.register_dict.keys():
            if key not in self.register_dict.keys():
                # if not existed, create it
                self.register_dict[key] = self.fetcher.register_type.fromJson(
                    key)
            # add strategy into market register
            self.register_dict[key].addStrategy(_strategy)
            _strategy.register_dict[key] = self.register_dict[key]

    def addEvent(self, _symbol: str, _data: DataStruct):
        """
        add new tick data into market register, and add event

        :param _symbol:
        :param _data:
        :return:
        """
        try:
            self.data_dict[_symbol].merge(_data)
        except KeyError:
            self.data_dict[_symbol] = _data.iloc[:]
        for k in self.symbol_dict[_symbol]:
            # add event for each strategy if necessary
            for strategy in self.register_dict[k].strategy_set:
                self.engine.addEvent(
                    MarketEvent(k, strategy, _symbol, _data))

    def getTradingDay(self) -> str:
        raise NotImplementedError('getTradingDay not implemented')

    def getDatetime(self) -> datetime:
        raise NotImplementedError('getDatetime not implemented')

    def getSymbolData(self, _symbol: str) -> DataStruct:
        """
        return data of one symbol
        
        :param _symbol: 
        :return: 
        """
        return self.data_dict[_symbol]

    def updateData(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        raise NotImplementedError('updateData not implemented')

    def __repr__(self):
        ret = '### MARKET REGISTER ###'
        for k, v in self.register_dict.items():
            ret += '\n' + k
        ret += '\n### SYMNOL ###'
        for k, v in self.symbol_dict.items():
            ret += '\n' + k + ': ' + str(v)
        ret += '\n### DATA ###'
        for k, v in self.data_dict.items():
            ret += '\n--- ' + k + ' ---\n' + str(v)

        return ret
