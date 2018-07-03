import json
import typing

import arrow
import psycopg2
import psycopg2.extensions
from diskcache import Cache

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterSymbol(RegisterAbstract):

    def __init__(
            self, _exname: str = 'binance',
            _symbol: str = 'BTC_USDT',
    ):
        """
        store market sub information, used by MarketSupply to push
        data to strategy or portfolio

        :param _exname:
        :param _symbol:
        """
        super().__init__()

        self.exname = _exname
        self.symbol = _symbol

    def toJson(self) -> str:
        return json.dumps((
            ('exname', self.exname),
            ('symbol', self.symbol),
        ))

    def toKwargs(self) -> dict:
        return {
            '_exname': self.exname,
            '_symbol': self.symbol,
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterSymbol':
        data = dict(json.loads(_json_str))
        return RegisterSymbol(
            data['exname'],
            data['symbol'],
        )


class FetchBase(FetchAbstract):
    def __init__(
            self, _psql_host='localhost', _psql_dbname='data',
            _psql_user='', _psql_password='', _cache_path='cache'
    ):
        super().__init__()
        self.register_type = RegisterSymbol

        self.psql_host: str = _psql_host
        self.psql_dbname: str = _psql_dbname
        self.psql_user: str = _psql_user
        self.psql_password: str = _psql_password

        self.table_key: str = None

        self.cache: Cache = Cache(_cache_path)
        self.market_key: str = 'crypto_{}_{}'

        self._psql_con: psycopg2.extensions.connection = None
        self._psql_cur: psycopg2.extensions.cursor = None

        self.columns: typing.List[str] = []

    def _get_psql_con_cur(self) -> typing.Tuple[
        psycopg2.extensions.connection, psycopg2.extensions.cursor
    ]:
        if not self._psql_con:
            self._psql_con: psycopg2.extensions.connection = \
                psycopg2.connect(
                    dbname=self.psql_dbname,
                    host=self.psql_host,
                    user=self.psql_user,
                    password=self.psql_password,
                )
        if not self._psql_cur:
            self._psql_cur: psycopg2.extensions.cursor = \
                self._psql_con.cursor()

        return self._psql_con, self._psql_cur

    def fetchSymbol(
            self, _tradingday: str, _exname: str = None, _symbol: str = None
    ) -> typing.Tuple[str, str]:
        assert _exname is not None
        assert _symbol is not None

        return (_exname, _symbol)

    def fetchData(
            self, _tradingday: str, _symbol: typing.Tuple[str, str],
            _cache=True, _index: str = 'datetime'
    ) -> typing.Union[None, DataStruct]:
        exname, symbol = _symbol

        table_name = self.table_key.format(exname, symbol)
        key = self.market_key.format(table_name, _tradingday)
        if _cache:
            try:
                return self.cache[key]
            except KeyError:
                pass

        # fetch from database
        begin_date = arrow.get(_tradingday, 'YYYYMMDD')
        end_date = begin_date.shift(days=1)
        con, cur = self._get_psql_con_cur()
        cur.execute(
            "SELECT * FROM {} WHERE datetime>='{}' AND datetime<'{}' "
            "ORDER BY {}".format(
                table_name, begin_date, end_date, _index
            )
        )
        data = list(cur.fetchall())
        if len(data):
            data = DataStruct(self.columns, _index, data)
        else:
            data = None

        if _cache:
            self.cache[key] = data
        return data

    def fetchDayData(
            self, _begin_day: str, _end_day: str,
            _symbol: str, _index: str = 'datetime'
    ) -> DataStruct:
        exname, symbol = _symbol

        table_name = self.table_key.format(exname, symbol)
        con, cur = self._get_psql_con_cur()
        cur.execute(
            "SELECT * FROM {} WHERE datetime>='{}' AND datetime<'{}' "
            "ORDER BY {}".format(
                table_name, _begin_day, _end_day, _index
            )
        )
        data = list(cur.fetchall())

        return DataStruct(self.columns, _index, data)
