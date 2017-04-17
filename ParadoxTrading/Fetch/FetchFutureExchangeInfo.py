import pymongo
import pymongo.collection
import pymongo.database
from pymongo import MongoClient


class FetchFutureExchangeInfo:
    def __init__(self):
        self.mongo_rank_host: str = 'localhost'
        self.mongo_rank_prod_db: str = None
        self.mongo_rank_inst_db: str = None

        self._mongo_rank_client: MongoClient = None
        self._mongo_rank_prod: pymongo.database.Database = None
        self._mongo_rank_inst: pymongo.database.Database = None

    def _get_mongo_rank_prod(self) -> pymongo.database.Database:
        if not self._mongo_rank_prod:
            if not self._mongo_rank_client:
                self._mongo_rank_client: MongoClient = MongoClient(
                    host=self.mongo_rank_host)
            self._mongo_rank_prod: pymongo.database.Database = \
                self._mongo_rank_client[self.mongo_rank_prod_db]
        return self._mongo_rank_prod

    def _get_mongo_rank_inst(self) -> pymongo.database.Database:
        if not self._mongo_rank_inst:
            if not self._mongo_rank_client:
                self._mongo_rank_client: MongoClient = MongoClient(
                    host=self.mongo_rank_host)
            self._mongo_rank_inst: pymongo.database.Database = \
                self._mongo_rank_client[self.mongo_rank_inst_db]
        return self._mongo_rank_inst

    def fetchProductRank(self, _tradingday: str, _product: str):
        db = self._get_mongo_rank_prod()
        coll = db[_product.lower()]
        return coll.find_one({'TradingDay': _tradingday})

    def fetchInstrumentRank(self, _tradingday: str, _instrument: str):
        db = self._get_mongo_rank_inst()
        coll = db[_instrument.lower()]
        return coll.find_one({'TradingDay': _tradingday})
