import typing

import psycopg2

from ParadoxTrading.Fetch.FetchFutureTick import RegisterFuture, FetchFutureTick
from ParadoxTrading.Utils import DataStruct


class RegisterFutureDay(RegisterFuture):
    pass


class FetchFutureDay(FetchFutureTick):
    def __init__(self):
        super().__init__()
        # point dbname to min
        self.psql_dbname = 'FutureDay'
        # point cache to min
        del self.cache_path

    def fetchData(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
            _cache=True, _index='TradingDay'
    ) -> typing.Union[None, DataStruct]:
        if isinstance(_product, str):
            _product = _product.lower()
        if isinstance(_instrument, str):
            _instrument = _instrument.lower()

        inst: str = self.fetchSymbol(
            _tradingday, _product, _instrument,
            _product_index, _sub_dominant)
        if inst is None:
            return None

        ret = self.fetchDayData(
            _tradingday, _instrument=inst, _index=_index)
        if len(ret) > 0:
            return ret
        return None

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _instrument: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        begin_day = _begin_day
        end_day = _end_day
        if _end_day is None:
            end_day = begin_day

        con = psycopg2.connect(
            dbname=self.psql_dbname,
            host=self.psql_host,
            user=self.psql_user,
            password=self.psql_password,
        )
        cur = con.cursor()

        # get all column names
        cur.execute(
            "select column_name, data_type "
            "from information_schema.columns "
            "where table_name='" + _instrument.lower() + "'"
        )
        columns = [d[0] for d in cur.fetchall()]

        query = "select * from {} where {} >= '{}' and {} <= '{}'".format(
            _instrument.lower(),
            _index.lower(), begin_day,
            _index.lower(), end_day,
        )
        cur.execute(query)
        datas = list(cur.fetchall())

        cur.close()
        con.close()

        return DataStruct(columns, _index.lower(), datas)
