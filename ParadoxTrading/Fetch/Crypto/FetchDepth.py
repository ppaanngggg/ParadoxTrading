import typing

from ParadoxTrading.Fetch.Crypto.FetchBase import FetchBase


class FetchDepth(FetchBase):

    def __init__(
            self, _psql_host='localhost', _psql_dbname='data',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__(
            _psql_host=_psql_host, _psql_dbname=_psql_dbname,
            _psql_user=_psql_user, _psql_password=_psql_password,
            _cache_path=_cache_path
        )

        self.table_key: str = '{}_rs_{}_depth'
        self.columns: typing.List[str] = []
        for i in range(10):
            self.columns.append('askprice{}'.format(i))
            self.columns.append('askamount{}'.format(i))
        for i in range(10):
            self.columns.append('bidprice{}'.format(i))
            self.columns.append('bidamount{}'.format(i))
        self.columns.append('datetime')
