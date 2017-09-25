import typing

from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, SettlementEvent, SignalType


class MarketEventMgr:
    def __init__(self, *args):
        self.product_set = set(args)
        self.product_index = None
        self.product_dict = {}

    def addMarketEvent(self, _market_event: MarketEvent):
        index_value = _market_event.data.index()[0]
        if self.product_index != index_value:
            # time changes
            self.product_index = index_value
            self.product_dict = {
                _market_event.symbol: _market_event
            }
        else:
            assert _market_event.symbol in self.product_set
            assert _market_event.symbol not in self.product_dict.keys()
            self.product_dict[_market_event.symbol] = _market_event
            # if match, then return
            rest_keys = self.product_set - self.product_dict.keys()
            if not rest_keys:
                return self.product_index, self.product_dict


class CFStrategy(StrategyAbstract):
    def __init__(self, _name: str, *args):
        super().__init__(_name)

        self.market_event_mgr = MarketEventMgr(*args)
        self.addPickleSet('market_event_mgr')

    def addEvent(
            self, _symbol: str,
            _strength: float,
            _signal_type: int = None,
    ):
        if _strength > 0:
            signal_type = SignalType.LONG
        elif _strength < 0:
            signal_type = SignalType.SHORT
        else:
            signal_type = SignalType.EMPTY
        super().addEvent(_symbol, signal_type, _strength)

    def deal(self, _market_event: MarketEvent):
        ret = self.market_event_mgr.addMarketEvent(_market_event)
        if ret is not None:
            self.do_deal(ret[0], ret[1])

    def do_deal(self, _index, _product_dict: typing.Dict[str, MarketEvent]):
        raise NotImplementedError('deal not implemented')

    def settlement(self, _settlement_event: SettlementEvent):
        raise NotImplementedError('settlement not implemented')
