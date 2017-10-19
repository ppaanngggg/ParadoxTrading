import typing

from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Utils import DataStruct


class FetchInstrumentDayData(FetchBase):
    def __init__(
            self, _mongo_host='localhost', _psql_host='localhost',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__(
            _mongo_host, _psql_host, _psql_user, _psql_password, _cache_path
        )

        self.psql_dbname: str = 'ChineseFuturesInstrumentDayData'
        self.market_key: str = 'ChineseFuturesInstrumentDayData_{}_{}'
        self.columns = [
            'tradingday',
            'openprice', 'highprice', 'lowprice', 'closeprice',
            'settlementprice',
            'pricediff_1', 'pricediff_2',
            'volume', 'openinterest', 'openinterestdiff',
            'presettlementprice',
        ]

    def fetchData(
            self, _tradingday: str, _symbol: str,
            _cache=True, _index='TradingDay'
    ) -> typing.Union[None, DataStruct]:
        return super().fetchData(
            _tradingday, _symbol, _cache, _index
        )

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _symbol: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        return super().fetchDayData(
            _begin_day, _end_day, _symbol, _index
        )
