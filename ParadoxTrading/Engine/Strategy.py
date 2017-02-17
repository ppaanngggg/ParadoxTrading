import json
import typing

from ParadoxTrading.Engine.Event import SignalEvent
import ParadoxTrading.Engine
import ParadoxTrading.Engine.Engine


class StrategyAbstract:
    def __init__(self, _name: str):
        """
        base class of strategy

        :param _name: name of this strategy
        """

        self.name: str = _name

        # common variables
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None
        self.market_register_dict: typing.Dict[str, ParadoxTrading.Engine.MarketRegister] = {}

        # run user define init
        self.init()

    def _setEngine(self, _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'):
        """
        PROTECTED !!!

        :param _engine: ref to engine
        :return:
        """
        self.engine = _engine

    def init(self):
        """
        user defined init, it will be called when create object

        :return:
        """
        raise NotImplementedError('init not implemented!')

    def deal(self, _market_register_key: str):
        """
        user defined deal, it will be called when there is market event for this strategy

        :param _market_register_key: key for market register
        :return:
        """
        raise NotImplementedError('deal not implemented!')

    def addMarketRegister(
            self, _product: str = None, _instrument: str = None, _sub_dominant: bool = False,
            _second_skip: int = 0, _minute_skip: int = 0, _hour_skip: int = 0
    ) -> str:
        """
        used in init() to register market data

        :param _product: reg which product, if not None, ignore instrument
        :param _instrument: reg which instrument
        :param _sub_dominant: only work when use product, false means using dominant inst, true means sub dominant one
        :param _second_skip: split into n second bar, if all skips are 0, use tick data
        :param _minute_skip: same as above
        :param _hour_skip: same as above
        :return: json str key of market register
        """

        assert _product is not None or _instrument is not None

        # gen json key for market register
        key = json.dumps((
            ('product', _product),
            ('instrument', _instrument),
            ('sub_dominant', _sub_dominant),
            ('second_skip', _second_skip),
            ('minute_skip', _minute_skip),
            ('hour_skip', _hour_skip),
        ))
        # alloc position for market register object
        self.market_register_dict[key] = None

        return key

    def getMarketRegister(
            self, _market_register_key: str
    ) -> 'ParadoxTrading.Engine.MarketRegister':
        """
        get this strategy's market register by key.

        :param _market_register_key: key of market register, usually get from market event
        :return: market register object
        """
        return self.market_register_dict[_market_register_key]

    def addEvent(
            self, _instrument: str, _signal_type: int, _strength: float = None
    ):
        """
        add signal event to event queue.

        :param _instrument: which instrument is the signal for
        :param _signal_type: long or short
        :param _strength: defined by user
        :return:
        """
        self.engine.addEvent(SignalEvent(
            _instrument=_instrument,
            _strategy_name=self.name,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getCurDatetime(),
            _signal_type=_signal_type,
            _strength=_strength,
        ))
