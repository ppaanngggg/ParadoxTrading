import logging
import typing

import arrow
import pymongo
import pymongo.errors
from pymongo import MongoClient

class ReceiveDailyAbstract:
    DATABASE_NAME = 'ChineseFuturesRaw'
    COLLECTION_NAME = None
    REPLACE_ALL = False

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
        logging.info('{} storeRaw: {}'.format(
            self.COLLECTION_NAME, _tradingday
        ))
        try:
            self.mongo_coll.insert_one({
                'TradingDay': _tradingday,
                'Raw': _raw_data,
            })
        except pymongo.errors.DuplicateKeyError as e:
            logging.warning(e)
            if self.REPLACE_ALL:
                self.mongo_coll.replace_one(
                    {'TradingDay': _tradingday},
                    {'TradingDay': _tradingday, 'Raw': _raw_data}
                )
            else:
                tmp = input('Replace existing data?(y/n/a): ')
                if tmp == 'y' or tmp == 'a':
                    self.mongo_coll.replace_one(
                        {'TradingDay': _tradingday},
                        {'TradingDay': _tradingday, 'Raw': _raw_data}
                    )
                    if tmp == 'a':
                        self.REPLACE_ALL = True

    def loadRaw(self, _tradingday: str):
        ret = self.mongo_coll.find_one({
            'TradingDay': _tradingday
        })
        if ret is None:
            return None
        return ret['Raw']

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
