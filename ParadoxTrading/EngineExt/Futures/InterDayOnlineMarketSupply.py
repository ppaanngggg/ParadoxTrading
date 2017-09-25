import logging
import typing
from datetime import datetime

from ParadoxTrading.Engine import MarketSupplyAbstract, ReturnMarket, ReturnSettlement
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Utils import DataStruct


class CTAOnlineMarketSupply(MarketSupplyAbstract):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _tradingday: str = None
    ):
        super().__init__(_fetcher)

        # default tradingday is today
        self.tradingday = datetime.now().strftime('%Y%m%d')
        if _tradingday is not None:
            self.tradingday = _tradingday

        # map symbol to data
        self.data_dict: typing.Dict[str, DataStruct] = {}

        self.flag = True
        self.is_finish = False

    def _get_data(self):
        # fetch data from database
        for k, v in self.register_dict.items():
            symbol = self.fetcher.fetchSymbol(
                self.tradingday, **v.toKwargs()
            )
            # whether symbol exists
            if symbol is not None:
                if symbol not in self.data_dict.keys():
                    # fetch data and set index to 0 init
                    self.data_dict[symbol] = self.fetcher.fetchData(
                        self.tradingday, _symbol=symbol
                    )

                # map symbol to market register key
                try:
                    self.symbol_dict[symbol].add(k)
                except KeyError:
                    self.symbol_dict[symbol] = {k}
        self.flag = False

    def updateData(self) -> typing.Union[
        None, ReturnMarket, ReturnSettlement
    ]:
        if self.flag:
            self._get_data()
            self.is_finish = not self.data_dict
            if self.data_dict:
                logging.info('TradingDay: {}, Product: {}'.format(
                    self.tradingday, self.symbol_dict.keys()
                ))

        if self.is_finish:
            return None
        try:
            k, v = self.data_dict.popitem()
            return self.addMarketEvent(k, v)
        except KeyError:
            self.is_finish = True
            return self.addSettlementEvent(self.tradingday)

    def getTradingDay(self) -> str:
        return self.tradingday

    def getDatetime(self) -> typing.Union[str, datetime]:
        return self.tradingday
