import typing

from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, \
    SettlementEvent
from ParadoxTrading.Fetch import FetchAbstract


class MarketEventMgr:
    def __init__(self, _fetcher, *args):
        assert isinstance(_fetcher, FetchAbstract)
        self.fetcher = _fetcher
        self.product_set = set(args)

        self.available_product = None
        self.product_index = None
        self.product_dict = {}

    def addMarketEvent(
            self, _tradingday: str, _market_event: MarketEvent
    ):
        if self.available_product is None:
            # update availbale product current tradingday
            tmp = self.fetcher.fetchAvailableProduct(_tradingday)
            self.available_product = set(tmp) & self.product_set

        index_value = _market_event.data.index()[0]
        if self.product_index != index_value:
            # time changes
            self.product_index = index_value
            self.product_dict = {
                _market_event.symbol: _market_event
            }
        else:
            assert _market_event.symbol in self.available_product
            assert _market_event.symbol not in self.product_dict.keys()
            self.product_dict[_market_event.symbol] = _market_event

        # if match, then return
        if not (self.available_product - self.product_dict.keys()):
            return self.product_index, self.product_dict

    def resetTradingDayInfo(self):
        self.available_product = None


class ArbitrageStrategy(StrategyAbstract):
    def __init__(self, _name: str, _fetcher: str, *args):
        super().__init__(_name)

        self.market_event_mgr = MarketEventMgr(
            _fetcher, *args
        )
        self.addPickleKey('market_event_mgr')

    def deal(self, _market_event: MarketEvent):
        ret = self.market_event_mgr.addMarketEvent(
            self.engine.getTradingDay(), _market_event
        )
        if ret is not None:
            self.do_deal(ret[0], ret[1])

    def do_deal(
            self, _index, _product_dict: typing.Dict[str, MarketEvent]
    ):
        raise NotImplementedError('deal not implemented')

    def settlement(self, _settlement_event: SettlementEvent):
        self.do_settlement(_settlement_event)
        self.market_event_mgr.resetTradingDayInfo()

    def do_settlement(self, _settlement_event: SettlementEvent):
        raise NotImplementedError('settlement not implemented')
