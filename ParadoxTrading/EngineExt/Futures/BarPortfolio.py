import logging
import sys

from ParadoxTrading.Engine import FillEvent, SignalEvent
from ParadoxTrading.Engine import PortfolioAbstract
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Utils import DataStruct


class BarPortfolio(PortfolioAbstract):
    """
    It is a simple portfolio management mod.
    When it receive a signal event, it will send a 1 hand limit order,
    and the limit price will be set as the latest closeprice. And it will
    check the portfolio of strategy which sends the event, it will close 
    positions if possible
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(_init_fund, _margin_rate)

        self.fetcher = _fetcher
        self.settlement_price_index = _settlement_price_index

    def dealSignal(self, _event: SignalEvent):
        self.portfolio_mgr.dealSignal(_event)

    def dealFill(self, _event: FillEvent):
        # self.portfolio_mgr.dealFill(, _event)
        pass

    def dealSettlement(self, _tradingday: str):
        assert _tradingday

        # do settlement for cur positions
        symbol_price_dict = {}
        for symbol in self.portfolio_mgr.getSymbolList():
            try:
                symbol_price_dict[symbol] = self.fetcher.fetchData(
                    _tradingday, symbol
                )[self.settlement_price_index][0]
            except TypeError as e:
                logging.error('Tradingday: {}, Symbol: {}, e: {}'.format(
                    _tradingday, symbol, e
                ))
                sys.exit(1)
        self.portfolio_mgr.dealSettlement(
            _tradingday, symbol_price_dict
        )

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass
