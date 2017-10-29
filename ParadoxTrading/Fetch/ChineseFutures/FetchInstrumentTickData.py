import typing

from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Utils import DataStruct


class FetchInstrumentTickData(FetchBase):
    def __init__(
            self, _mongo_host='localhost', _psql_host='localhost',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__(
            _mongo_host, _psql_host, _psql_user, _psql_password, _cache_path
        )

        self.psql_dbname: str = 'ChineseFuturesInstrumentTickData'
        self.market_key: str = 'ChineseFuturesInstrumentTickData_{}_{}'
        self.columns = [
            'tradingday',
            'lastprice', 'highestprice', 'lowestprice',
            'volume', 'turnover', 'openinterest',
            'upperlimitprice', 'lowerlimitprice',
            'askprice', 'askvolume', 'bidprice', 'bidvolume',
            'happentime',
        ]
