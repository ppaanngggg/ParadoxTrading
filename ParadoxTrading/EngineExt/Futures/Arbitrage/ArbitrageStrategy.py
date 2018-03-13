import typing

from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, \
    SettlementEvent
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase


class MarketEventMgr:
    def __init__(self, _fetcher: FetchBase, *args):
        """
        manage market event, send data_dict when all
        available product data arrived at the same time

        :param _fetcher: the fetcher to get data
        :param args: product list
        """
        assert isinstance(_fetcher, FetchBase)
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
        self.product_index = None
        self.product_dict = {}


class ArbitrageStrategy(StrategyAbstract):
    def __init__(self, _name: str, _fetcher: FetchBase, *args):
        """
        Strategy base class for chinese futures arbitrage.
        it will merge all available data of the same time,
        and resend to itself. The true deal logic should be
        dealt in do_deal(...) function. And the true settlement
        logic is in do_settlement(...)

        :param _name: strategy name
        :param _fetcher: the data fetcher, to get some instrument info
        :param args: product list
        """
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
