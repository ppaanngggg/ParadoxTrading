from ParadoxTrading.Fetch.ChineseFutures.FetchDominantIndex import FetchDominantIndex


class FetchProductIndex(FetchDominantIndex):
    def __init__(
            self, _mongo_host='localhost', _psql_host='localhost',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__(
            _mongo_host, _psql_host, _psql_user, _psql_password, _cache_path
        )

        self.psql_dbname: str = 'ChineseFuturesProductIndex'
        self.market_key: str = 'ChineseFuturesProductIndex_{}_{}'
