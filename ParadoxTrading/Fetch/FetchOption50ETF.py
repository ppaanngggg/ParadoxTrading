import json
import typing

import psycopg2
import psycopg2.extensions
import pymongo
import pymongo.collection
import pymongo.database
from pymongo import MongoClient

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterOption50ETFTick(RegisterAbstract):
    def __init__(self, _contract: str):
        super().__init__()

        self.contract = _contract

    def toJson(self) -> str:
        return json.dumps((
            ('contract', self.contract),
        ))

    def toKwargs(self) -> dict:
        return {
            '_contract': self.contract
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterOption50ETFTick(
            data['contract']
        )


class FetchOption50ETFTick(FetchAbstract):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterOption50ETFTick

        self.mongo_host: str = 'localhost'
        self.mongo_contract_db: str = 'Option50ETF'

        self.psql_host: str = 'localhost'
        self.psql_dbname: str = 'option50etf_tick'
        self.psql_user: str = ''
        self.psql_password: str = ''

        self.cache_path = 'Option50ETFTick.hdf5'

        self._mongo_client: MongoClient = None
        self._mongo_contract: pymongo.database.Database = None
        self._psql_con: psycopg2.extensions.connection = None
        self._psql_cur: psycopg2.extensions.cursor = None

        self.columns = [
            'tradingday', 'volume', 'turover',
            'settle', 'position', 'accvolume', 'accturover',
            'high', 'low', 'open', 'price', 'pre_close', 'pre_settle',
            'ask1', 'ask2', 'ask3', 'ask4', 'ask5',
            'bid1', 'bid2', 'bid3', 'bid4', 'bid5',
            'asize1', 'asize2', 'asize3', 'asize4', 'asize5',
            'bsize1', 'bsize2', 'bsize3', 'bsize4', 'bsize5',
            'happentime',
        ]
        self.types = ['character'] + ['double precision'] * (
            len(self.columns) - 2) + ['timestamp without time zone']

    def _get_mongo_contract(self) -> pymongo.database.Database:
        if not self._mongo_contract:
            if not self._mongo_client:
                self._mongo_client: MongoClient = MongoClient(
                    host=self.mongo_host)
            self._mongo_contract: pymongo.database.Database = \
                self._mongo_client[
                    self.mongo_contract_db]
        return self._mongo_contract

    def fetchContractInfo(
            self, _tradingday
    ) -> typing.Union[None, DataStruct]:
        coll: pymongo.collection.Collection = self._get_mongo_contract()['info']
        data = coll.find_one({'TradingDay': _tradingday})
        if data is None:
            return None
        values = list(data['ContractDict'].values())
        return DataStruct(values[0].keys(), 'SecurityID', _dicts=values)

    def fetchSymbol(
            self, _tradingday: str, _contract: str = None
    ) -> typing.Union[None, str]:
        assert _contract is not None

        coll: pymongo.collection.Collection = self._get_mongo_contract()['info']
        data = coll.find_one({'TradingDay': _tradingday})
        if data is None:
            return None
        if _contract in data['ContractDict'].keys():
            return _contract
        return None

    def fetchData(
            self, _tradingday: str, _symbol: str, _cache: bool = True,
            _index: str = 'happentime'
    ) -> typing.Union[None, DataStruct]:
        if _symbol is None:
            return None
        assert isinstance(_symbol, str)
        symbol = _symbol.lower()

        if _cache:
            # if found in cache, then return
            ret = self.cache2DataStruct(
                symbol, _tradingday, _index)
            if ret is not None:
                return ret

        # fetch from database
        con, cur = self._get_psql_con_cur()

        # get all ticks
        cur.execute(
            "SELECT * FROM {} WHERE TradingDay='{}' ORDER BY {}".format(
                symbol, _tradingday, _index.lower())
        )
        datas = list(cur.fetchall())

        # turn into datastruct
        datastruct = DataStruct(self.columns, _index.lower(), datas)

        if len(datastruct):
            if _cache:
                self.DataStruct2cache(
                    symbol, _tradingday,
                    self.columns, self.types, datastruct
                )
            return datastruct
        else:
            return None

    def fetchDayData(
            self, _begin_day: str, _end_day: str, _symbol: str, **kwargs
    ) -> DataStruct:
        pass


class RegisterOption50ETFDay(RegisterOption50ETFTick):
    pass


class FetchOption50ETFDay(FetchOption50ETFTick):
    def __init__(self):
        super().__init__()
        self.psql_dbname = 'option50etf_day'
        del self.cache_path

        self.register_type = RegisterOption50ETFDay

        self.columns = [
            'tradingday', 'volume', 'turover',
            'open', 'high', 'low', 'price',
            'accvolume', 'accturover', 'pre_close',
        ]
        del self.types

    def fetchData(
            self, _tradingday: str, _symbol: str, _cache: bool = True,
            _index: str = 'tradingday'
    ) -> typing.Union[None, DataStruct]:
        if _symbol is None:
            return None
        assert isinstance(_symbol, str)
        symbol = _symbol.lower()

        ret = self.fetchDayData(
            _tradingday, _symbol=symbol, _index=_index)
        if len(ret) > 0:
            return ret
        return None

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _symbol: str = None, _index: str = 'tradingday'
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
