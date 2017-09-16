import logging
import typing

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import MarketEvent, SignalEvent, SignalType, SettlementEvent
from ParadoxTrading.Fetch import RegisterAbstract
from ParadoxTrading.Utils import Serializable


class StrategyAbstract(Serializable):
    def __init__(self, _name: str):
        """
        base class of strategy

        :param _name: name of this strategy
        """
        super().__init__()
        self.name: str = _name

        # common variables
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None
        self.registers: typing.Set[str] = set()

    def setEngine(self,
                  _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        """
        PROTECTED !!!

        :param _engine: ref to engine
        :return:
        """
        self.engine = _engine

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
        assert key not in self.registers
        # alloc position for market register object
        self.registers.add(key)

        return key

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
        assert isinstance(_symbol, str)
        self.engine.addEvent(SignalEvent(
            _symbol=_symbol,
            _strategy=self.name,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _signal_type=_signal_type,
            _strength=_strength,
        ))
        logging.info('Strategy({}) send {} {} {} when {}'.format(
            self.name, _symbol,
            SignalType.toStr(_signal_type),
            _strength,
            self.engine.getDatetime()
        ))

    def save(self, _path: str, _filename: str = None):
        filename = self.name
        if _filename is not None:
            filename = _filename
        super().save(_path, filename)
        logging.debug(
            'Strategy({}) save to {}'.format(self.name, _path)
        )

    def load(self, _path: str, _filename: str = None):
        filename = self.name
        if _filename is not None:
            filename = _filename
        super().load(_path, filename)
        logging.debug(
            'Strategy({}) load from {}'.format(self.name, _path)
        )

    def __repr__(self) -> str:
        ret = 'Strategy:\n\t{}\nMarket Register:\n\t{}'
        return ret.format(
            self.name,
            self.registers
        )
