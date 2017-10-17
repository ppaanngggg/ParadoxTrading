import typing

import arrow
import pymongo
from pymongo import MongoClient


class ReceiveDailyAbstract:
    DATABASE_NAME = 'ChineseFuturesRaw'
    COLLECTION_NAME = None

    def __init__(self):
        db = MongoClient()[self.DATABASE_NAME]
        self.mongo_coll = db[self.COLLECTION_NAME]
        if self.COLLECTION_NAME not in db.collection_names():
            self.mongo_coll.create_index([(
                'TradingDay', pymongo.ASCENDING
            )], unique=True)

    def fetchRaw(self, _tradingday: str) -> typing.Any:
        raise NotImplementedError

    def storeRaw(self, _tradingday: str, _raw_data: typing.Any):
        self.mongo_coll.insert_one({
            'TradingDay': _tradingday,
            'Raw': _raw_data,
        })

    @staticmethod
    def rawToDicts(_tradingday: str, _raw_data: typing.Any):
        raise NotImplementedError

    @staticmethod
    def iterTradingDay(
            _begin_date: str, _end_date: str = None
    ):
        if _end_date is None:
            _end_date = arrow.now().format('YYYYMMDD')

        tradingday = _begin_date
        while tradingday < _end_date:
            yield tradingday
            tradingday = arrow.get(
                tradingday, 'YYYYMMDD'
            ).shift(days=1).format('YYYYMMDD')
