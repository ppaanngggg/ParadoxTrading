import logging
import typing

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import MarketEvent, SignalEvent, SignalType, SettlementEvent
from ParadoxTrading.Fetch import RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class StrategyAbstract:
    def __init__(self, _name: str):
        """
        base class of strategy

        :param _name: name of this strategy
        """

        self.name: typing.AnyStr = _name
        self.last_signal: int = None

        # common variables
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None
        self.register_dict: typing.Dict[str, RegisterAbstract] = {}

        # store the portfolio for this strategy
        self.portfolio: ParadoxTrading.Engine.Portfolio.PortfolioMgr = None

        # # run user define init
        # self.init()

    def setEngine(self,
                  _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        """
        PROTECTED !!!

        :param _engine: ref to engine
        :return:
        """
        self.engine = _engine

    def setPortfolio(self,
                     _portfolio: 'ParadoxTrading.Engine.Portfolio.PortfolioMgr'):
        self.portfolio = _portfolio

    def deal(self, _market_event: MarketEvent):
        """
        user defined deal, it will be called when there is
        market event for this strategy

        :param _market_event:
        :return:
        """
        raise NotImplementedError('deal not implemented!')

    def settlement(self, _settlement_event: SettlementEvent):
        """
        user defined settlement, it will be called when there is
        a settlement event

        :param _settlement_event:
        :return:
        """
        raise NotImplementedError('settlement not implemented!')

    def addMarketRegister(
            self,
            _market_register: RegisterAbstract
    ) -> str:
        """
        used in init() to register market data

        :type _market_register: object
        :return: json str key of market register
        """

        key = _market_register.toJson()
        assert key not in self.register_dict.keys()
        # alloc position for market register object
        self.register_dict[key] = None

        return key

    def getMarketRegister(
            self, _market_register_key: str
    ) -> RegisterAbstract:
        """
        get this strategy's market register by key.

        :param _market_register_key: key of market register,
            usually get from market event
        :return: market register object
        """
        return self.register_dict[_market_register_key]

    def addEvent(self,
                 _symbol: str,
                 _signal_type: int,
                 _strength: typing.Any = None):
        """
        add signal event to event queue.

        :param _symbol: which symbol is the signal for
        :param _signal_type: long or short
        :param _strength: defined by user
        :return:
        """
        self.engine.addEvent(
            SignalEvent(
                _symbol=_symbol,
                _strategy_name=self.name,
                _tradingday=self.engine.getTradingDay(),
                _datetime=self.engine.getDatetime(),
                _signal_type=_signal_type,
                _strength=_strength, ))
        logging.info('Strategy({}) send {} {} {} when {}'.format(
            self.name, _symbol,
            SignalType.toStr(_signal_type),
            _strength,
            self.engine.getDatetime()
        ))
        self.setLastSignal(_signal_type)

    def setLastSignal(self, _signal_type: int):
        self.last_signal = _signal_type

    def getLastSignal(self) -> int:
        return self.last_signal

    def getSymbolData(self, _symbol: str) -> DataStruct:
        """
        get data of one symbol from marketsupply through engine

        :param _symbol:
        :return:
        """
        return self.engine.getSymbolData(_symbol)

    def __repr__(self) -> str:
        ret: str = 'Strategy: ' + '\n' + \
                   '\t' + self.name + '\n' + \
                   'Market Register:' + '\n'
        for k in self.register_dict.keys():
            ret += '\t' + k + '\n'
        return ret
