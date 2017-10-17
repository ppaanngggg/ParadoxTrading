import pymongo
from pymongo import MongoClient


class StoreDailyData:
    def __init__(self):
        self.mongo_client = MongoClient()
        self.instrument_db = self.mongo_client['ExchangeInst']
        self.product_db = self.mongo_client['ExchangeProd']
        self.tradingday_db = self.mongo_client['ExchangeTradingDay']

    def updateDominantInfo(
            self, _tradingday, _data_dict,
            _instrument_dict, _product_dict
    ):
        for k, v in _product_dict.items():
            tmp_list = [(
                d, _instrument_dict[d]['DeliveryMonth'],
                _data_dict[d]['OpenInterest']
            ) for d in v['InstrumentList']]
            tmp_list = sorted(
                tmp_list, key=lambda x: x[2], reverse=True
            )

            last_product_info = self.product_db[k].find_one(
                {'TradingDay': {'$lt': _tradingday}},
                sort=[('TradingDay', pymongo.DESCENDING)]
            )
            last_dominant_delivery = None
            last_sub_dominant_delivery = None
            if last_product_info is not None:
                last_dominant_delivery = self.instrument_db[
                    last_product_info['Dominant']
                ].find_one({
                    'TradingDay': last_product_info['TradingDay']
                })['DeliveryMonth']
                if last_product_info['SubDominant'] is not None:
                    last_sub_dominant_delivery = self.instrument_db[
                        last_product_info['SubDominant']
                    ].find_one({
                        'TradingDay': last_product_info['TradingDay']
                    })['DeliveryMonth']

            if last_dominant_delivery is None:
                cur_dominant = tmp_list[0][0]

    def store(
            self, _tradingday, _data_dict,
            _instrument_dict, _product_dict
    ):
        self.updateDominantInfo(
            _tradingday, _data_dict,
            _instrument_dict, _product_dict
        )
