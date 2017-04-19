import json
import typing

import psycopg2
import psycopg2.extensions

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterLMEDay(RegisterAbstract):
    def __init__(self, _product):
        super().__init__()

        self.product = _product

    def toJson(self) -> str:
        return json.dumps((
            ('product', self.product)
        ))

    def toKwargs(self) -> dict:
        return {
            '_product': self.product
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterLMEDay(data['product'])


class FetchLMEDay(FetchAbstract):
    def __init__(self):
        super().__init__()
        self.register_type = RegisterLMEDay

        self.psql_host: str = 'localhost'
        self.psql_dbname: str = 'LMEDay'

        del self.cache_path

        self._psql_con: psycopg2.extensions.connection = None
        self._psql_cur: psycopg2.extensions.cursor = None

        self.columns = [
            'tradingday', 'cash_buyer', 'cash_seller',
            'months_3_buyer', 'months_3_seller',
            'months_15_buyer', 'months_15_seller',
            'dec_1_buyer', 'dec_1_seller', 'dec_2_buyer', 'dec_2_seller',
            'dec_3_buyer', 'dec_3_seller'
        ]

    def fetchSymbol(
            self, _tradingday: str, _product: str = None, _index='tradingday'
    ) -> typing.Union[None, str]:
        assert _product is not None

        con, cur = self._get_psql_con_cur()

        try:
            query = "select * from {} where {} = '{}'".format(
                _product, _index, _tradingday
            )
            cur.execute(query)
            datas = list(cur.fetchall())
            if datas:
                return _product
            else:
                return None
        except psycopg2.DatabaseError as e:
            # table not exists
            if e.pgcode == '42P01':
                con.rollback()
                return None

    def fetchData(
            self, _tradingday: str, _symbol: str, _index: str = 'tradingday'
    ) -> typing.Union[None, DataStruct]:
        if _symbol is None:
            return None
        assert isinstance(_symbol, str)
        symbol = _symbol.lower()

        ret = self.fetchDayData(
            _tradingday, _tradingday, symbol, _index
        )
        if len(ret) > 0:
            return ret
        return None

    def fetchDayData(
            self, _begin_day: str, _end_day: str, _symbol: str,
            _index: str = 'tradingday'
    ) -> DataStruct:
        begin_day = _begin_day
        end_day = _end_day
        if _end_day is None:
            end_day = begin_day

        con, cur = self._get_psql_con_cur()

        query = "select * from {} where {} >= '{}' and {} <= '{}'".format(
            _symbol.lower(),
            _index.lower(), begin_day,
            _index.lower(), end_day,
        )
        cur.execute(query)
        datas = list(cur.fetchall())

        return DataStruct(self.columns, _index.lower(), datas)
