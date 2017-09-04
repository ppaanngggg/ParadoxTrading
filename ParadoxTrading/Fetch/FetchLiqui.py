import json
import typing

import pymongo
import pymongo.collection
import pymongo.database
from diskcache import Cache
from pymongo import MongoClient

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterLiqui(RegisterAbstract):
    def __init__(self, _pair: str):
        super().__init__()

        self.pair = _pair

    def toJson(self) -> str:
        return json.dumps((
            ('pair', self.pair),
        ))

    def toKwargs(self) -> dict:
        return {
            '_pair': self.pair
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterLiqui(
            data['pair']
        )


class FetchLiqui(FetchAbstract):
    def __init__(self):
        super().__init__()

        self.register_type: RegisterAbstract = RegisterLiqui

        self.mongo_host: str = 'localhost'
        self.mongo_info_db: str = 'LiquiInfo'
        self.mongo_depth_db: str = 'LiquiDepth'

        self.cache: Cache = Cache('cache')

        self._mongo_client: MongoClient = None
        self._mongo_info: pymongo.database.Database = None
        self._mongo_depth: pymongo.database.Database = None

    def _get_mongo_info(self) -> pymongo.database.Database:
        if not self._mongo_info:
            if not self._mongo_client:
                self._mongo_client: MongoClient = MongoClient(
                    host=self.mongo_host
                )
            self._mongo_info: pymongo.database.Database = self._mongo_client[
                self.mongo_info_db
            ]
        return self._mongo_info

    def fetchAllPairs(self, _tradingday: str, _not_hidden=True) -> typing.Iterable[str]:
        info = self.fetchInfo(_tradingday)

        if _not_hidden:
            return [k for k, v in info['pairs'].items() if v['hidden'] == 0]
        else:
            return list(info['pairs'].keys())

    def fetchInfo(self, _tradingday: str) -> typing.Dict:
        key = 'info_{}'.format(_tradingday)
        try:
            return self.cache[key]
        except KeyError:
            db = self._get_mongo_info()
            data = db['info'].find_one({'tradingday': _tradingday})
            self.cache[key] = data
            return data

    def fetchData(self, _tradingday: str, _symbol: str, **kwargs) -> typing.Union[None, DataStruct]:
        pass

    def fetchSymbol(self, _tradingday: str, **kwargs) -> typing.Union[None, str]:
        pass

    def fetchDayData(self, _begin_day: str, _end_day: str, _symbol: str, **kwargs) -> DataStruct:
        pass
