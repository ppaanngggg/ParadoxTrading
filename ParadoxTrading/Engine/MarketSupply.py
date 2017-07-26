import logging
import typing
from datetime import datetime

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import MarketEvent, SettlementEvent
from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class ReturnMarket:
    def __init__(self, _symbol: str, _data: DataStruct):
        self.symbol = _symbol
        self.data = _data

    def __repr__(self) -> str:
        return "ReturnMarket:\n\tSymbol {}".format(self.symbol)


class ReturnSettlement:
    def __init__(self, _tradingday: str):
        self.tradingday = _tradingday

    def __repr__(self) -> str:
        return "ReturnSettlement:\n\ttradingday {}".format(
            self.tradingday
        )


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
        # map symbol to datastruct
        self.data_dict: typing.Dict[str, DataStruct] = {}

        self.engine: ParadoxTrading.Engine.EngineAbstract = None

    def setEngine(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        self.engine = _engine

    def addStrategy(self, _strategy: 'ParadoxTrading.Engine.StrategyAbstract'):
        """
        Add strategy into market supply

        :param _strategy: strategy object
        :return: None
        """

        # for each market register
        for key in _strategy.register_dict.keys():
            if key not in self.register_dict.keys():
                # if not existed, create it
                self.register_dict[key] = \
                    self.fetcher.register_type.fromJson(key)
            # add strategy into market register
            self.register_dict[key].addStrategy(_strategy)
            _strategy.register_dict[key] = self.register_dict[key]

    def addSettlementEvent(self, _tradingday):
        self.engine.addEvent(SettlementEvent(_tradingday))
        logging.debug('Settlement - tradingday:{}'.format(
            _tradingday
        ))
        return ReturnSettlement(_tradingday)

    def addMarketEvent(self, _symbol: str, _data: DataStruct):
        """
        add new tick data into market register, and add event

        :param _symbol:
        :param _data:
        :return:
        """
        try:
            self.data_dict[_symbol].merge(_data)
        except KeyError:
            self.data_dict[_symbol] = _data.clone()
        for k in self.symbol_dict[_symbol]:
            # add event for each strategy if necessary
            for strategy in self.register_dict[k].strategy_set:
                self.engine.addEvent(MarketEvent(k, strategy, _symbol, _data))
        logging.debug('Data({}) {}'.format(_symbol, _data.toDict()))
        return ReturnMarket(_symbol, _data)

    def getTradingDay(self) -> str:
        raise NotImplementedError('getTradingDay not implemented')

    def getDatetime(self) -> datetime:
        raise NotImplementedError('getDatetime not implemented')

    def getSymbolList(self) -> typing.List[str]:
        return sorted(self.data_dict.keys())

    def getSymbolData(self, _symbol: str) -> DataStruct:
        """
        return data of one symbol

        :param _symbol:
        :return:
        """
        return self.data_dict[_symbol]

    def updateData(self) -> typing.Union[ReturnMarket, ReturnSettlement]:
        """
        update data tick by tick, or bar by bar

        :return: ReturnMarket - return market data
            ReturnSettlement - return settlement signal
        """
        raise NotImplementedError('updateData not implemented')

    def __repr__(self):
        ret = '### MARKET REGISTER ###'
        for k, v in self.register_dict.items():
            ret += '\n' + k
        ret += '\n### SYMBOL ###'
        for k, v in self.symbol_dict.items():
            ret += '\n' + k + ': ' + str(v)
        ret += '\n### DATA ###'
        for k, v in self.data_dict.items():
            ret += '\n--- ' + k + ' ---\n' + str(v)

        return ret
