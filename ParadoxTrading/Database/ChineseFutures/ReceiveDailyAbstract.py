import logging
import typing

import arrow
import pymongo
import pymongo.errors
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
        # whether to replace all conflicted data
        self.replace_all: bool = False

    def fetchRaw(self, _tradingday: str) -> typing.Any:
        """
        fetch raw data of _tradingday and return

        :param _tradingday: which day to fetch
        :return: the raw data
        """
        raise NotImplementedError

    def storeRaw(self, _tradingday: str, _raw_data: typing.Any):
        """
        store raw data into mongodb

        :param _tradingday: which day to store
        :param _raw_data: raw data from fetchRaw(...)
        :return: None
        """
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
            if self.replace_all:
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
                        self.replace_all = True

    def loadRaw(self, _tradingday: str) -> typing.Any:
        """
        load raw from mongodb

        :param _tradingday: which day to load
        :return: the raw data of _tradingday
        """
        ret = self.mongo_coll.find_one({
            'TradingDay': _tradingday
        })
        if ret is None:
            return None
        return ret['Raw']

    @staticmethod
    def rawToDicts(
            _tradingday: str, _raw_data: typing.Any
    ) -> typing.Tuple[dict, dict, dict]:
        """
        turn raw data into dicts

        :param _tradingday: which tradingday
        :param _raw_data: raw data turned
        :return: return data_dict, instrument_dict, product_dict
        """
        raise NotImplementedError

    @staticmethod
    def iterTradingDay(
            _begin_date: str, _end_date: str = None
    ) -> str:
        """
        iter day from _begin_date to _end_date day by day

        :param _begin_date: the begin day
        :param _end_date: the end day, excluded
        :return: day
        """
        if _end_date is None:
            _end_date = arrow.now().format('YYYYMMDD')

        tradingday = _begin_date
        while tradingday < _end_date:
            yield tradingday
            tradingday = arrow.get(
                tradingday, 'YYYYMMDD'
            ).shift(days=1).format('YYYYMMDD')

    def lastTradingDay(self) -> typing.Union[None, str]:
        """
        get the last tradingday stored in mongodb

        :return: str
        """
        ret = self.mongo_coll.find_one(
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        if ret:
            return ret['TradingDay']
        return None

    def iterFetchAndStore(self, _begin_date):
        for tradingday in self.iterTradingDay(_begin_date):
            self.storeRaw(tradingday, self.fetchRaw(tradingday))
