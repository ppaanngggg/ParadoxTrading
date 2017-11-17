from ParadoxTrading.Fetch.ChineseFutures.FetchBase import RegisterIndex
from ParadoxTrading.Fetch.ChineseFutures.FetchInstrumentDayData import FetchInstrumentDayData


class FetchDominantIndex(FetchInstrumentDayData):
    def __init__(
            self, _mongo_host='localhost', _psql_host='localhost',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__(
            _mongo_host, _psql_host, _psql_user, _psql_password, _cache_path
        )

        self.register_type = RegisterIndex

        self.psql_dbname: str = 'ChineseFuturesDominantIndex'
        self.market_key: str = 'ChineseFuturesDominantIndex_{}_{}'

        self.columns = [
            'tradingday',
            'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'openinterest'
        ]

    def fetchSymbol(
            self, _tradingday: str, _product: str = None, **kwargs
    ):
        assert _product is not None
        _product = _product.lower()

        if self.productIsAvailable(_product, _tradingday):
            return _product
        return None
