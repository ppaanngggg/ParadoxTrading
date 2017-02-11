import json

# from ParadoxTrading.Engine.Engine import EngineAbstract
from ParadoxTrading.Engine.Event import SignalEvent
from ParadoxTrading.Engine.MarketSupply import MarketRegister


class StrategyAbstract:

    def __init__(self, _name: str):
        self.name = _name
        self.engine = None  # EngineAbstract
        self.market_register_dict = {}  # typing.Dict[str, MarketRegister]
        self.init()

    def _setEngine(self, _engine):
        self.engine = _engine

    def init(self):
        raise NotImplementedError('init not implemented!')

    def deal(self, _market_register_key: str):
        raise NotImplementedError('deal not implemented!')

    def addMarketRegister(
        self, _product: str=None, _instrument: str=None, _sub_dominant: bool=False,
        _second_skip: int=0, _minute_skip: int =0, _hour_skip: int =0
    ) -> str:
        assert _product is not None or _instrument is not None

        key = json.dumps((
            ('product', _product),
            ('instrument', _instrument),
            ('sub_dominant', _sub_dominant),
            ('second_skip', _second_skip),
            ('minute_skip', _minute_skip),
            ('hour_skip', _hour_skip),
        ))
        self.market_register_dict[key] = None
        return key

    def getMarketRegister(self, _market_register_key:str) -> MarketRegister:
        return self.market_register_dict[_market_register_key]

    def addEvent(
            self, _instrument: str, _signal_type: int, _strength: float=None
    ):
        self.engine.addEvent(SignalEvent(
            _instrument, self.name, _signal_type,
            self.engine.getCurDatetime(), _strength
        ))
