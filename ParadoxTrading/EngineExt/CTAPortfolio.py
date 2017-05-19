from ParadoxTrading.Engine import FillEvent, SignalEvent
from ParadoxTrading.Engine import PortfolioAbstract
from ParadoxTrading.Utils import DataStruct

class CTAPortfolio(PortfolioAbstract):
    """
    It is a simple portfolio management mod.
    When it receive a signal event, it will send a 1 hand limit order,
    and the limit price will be set as the latest closeprice. And it will
    check the portfolio of strategy which sends the event, it will close
    positions if possible

    :param _price_index: the index of price which will be used to deal
    """

    def __init__(
            self,
            _fetcher,
            _price_index: str = 'closeprice',
    ):
        super().__init__()

        self.price_index = _price_index
        self.fetcher = _fetcher

        self.signal_table = {}
        self.position_table = {}

    def setPriceIndex(self, _index: str):
        self.price_index = _index

    def dealSignal(self, _event: SignalEvent):
        # add signal to buf
        pass

    def dealFill(self, _event: FillEvent):
        # adjust
        self.getPortfolioByIndex(_event.index).dealFillEvent(_event)

    def dealSettlement(self, _tradingday, _next_tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # get price dict for each portfolio
        symbol_price_dict = {}
        for symbol in self.engine.getSymbolList():
            symbol_price_dict[symbol] = \
                self.engine.getSymbolData(symbol)[self.price_index][-1]

        # update each portfolio settlement
        for v in self.strategy_portfolio_dict.values():
            v.dealSettlement(
                _tradingday, _next_tradingday,
                symbol_price_dict
            )

        print(_tradingday, _next_tradingday)
        input()

    def dealMarket(self, _symbol: str, _data: DataStruct):
        #
        print(_symbol, _data)
        input()
