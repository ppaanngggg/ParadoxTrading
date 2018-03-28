import logging
import sys
import typing

import numpy as np
import psycopg2
import pymongo
import pymongo.errors
from pymongo import MongoClient

INSTRUMENT_TABLE_KEYS = [
    'TradingDay', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
    'SettlementPrice', 'Volume', 'OpenInterest', 'PreSettlementPrice',
]
INDEX_TABLE_KEYS = [
    'TradingDay', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
    'Volume', 'OpenInterest'
]


class StoreDailyData:
    def __init__(self):
        self.mongo_client = MongoClient()
        self.mongo_db = self.mongo_client['ChineseFutures']

        self.instrument_day_data_con = psycopg2.connect(
            dbname='ChineseFuturesInstrumentDayData'
        )
        self.instrument_day_data_cur = self.instrument_day_data_con.cursor()
        self.product_index_con = psycopg2.connect(
            dbname='ChineseFuturesProductIndex'
        )
        self.product_index_cur = self.product_index_con.cursor()
        self.dominant_index_con = psycopg2.connect(
            dbname='ChineseFuturesDominantIndex'
        )
        self.dominant_index_cur = self.dominant_index_con.cursor()

    def _get_last_delivery(self, _product, _tradingday) -> typing.Tuple[str, str]:
        """
        get the dominant's delivery and sub-dominant's delivery of
        last tradingday

        :param _product: which product
        :param _tradingday: which tradingday
        :return: (str, str)
        """
        last_product_info = self.mongo_db.product.find_one(
            {'TradingDay': {'$lt': _tradingday}, 'Product': _product},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        last_dominant_delivery = None
        last_sub_dominant_delivery = None
        if last_product_info is not None:
            last_dominant_delivery = self.mongo_db.instrument.find_one({
                'TradingDay': last_product_info['TradingDay'],
                'Instrument': last_product_info['Dominant'],
            })['DeliveryMonth']
            if last_product_info['SubDominant'] is not None:
                last_sub_dominant_delivery = self.mongo_db.instrument.find_one({
                    'TradingDay': last_product_info['TradingDay'],
                    'Instrument': last_product_info['SubDominant'],
                })['DeliveryMonth']
        return last_dominant_delivery, last_sub_dominant_delivery

    @staticmethod
    def _get_cur_dominant(_last_delivery, _sorted_list) -> typing.Tuple[str, str]:
        """
        get cur dominant instrument

        :param _last_delivery: last delivery from _get_last_delivery(...)
        :param _sorted_list: the sorted instrument list
        :return: cur dominant and cur dominant's delivery
        """
        if _last_delivery is None:
            cur_dominant = _sorted_list[0][0]
            cur_dominant_delivery = _sorted_list[0][1]
        else:
            for d in _sorted_list:
                if d[1] >= _last_delivery:
                    cur_dominant = d[0]
                    cur_dominant_delivery = d[1]
                    break
            else:
                cur_dominant = None
                cur_dominant_delivery = None
        return cur_dominant, cur_dominant_delivery

    @staticmethod
    def _get_cur_sub_dominant(
            _dominant_delivery, _last_delivery, _sorted_list
    ):
        """
        get cur sub-dominant instrument

        :param _dominant_delivery: cur dominant's delivery
        :param _last_delivery: sub-dominant's delivery of last tradingday
        :param _sorted_list: the sorted instrument list
        :return: cur sub-dominant
        """
        if _last_delivery is None:
            for d in _sorted_list:
                if d[1] > _dominant_delivery:
                    cur_sub_dominant = d[0]
                    break
            else:
                cur_sub_dominant = None
        else:
            for d in _sorted_list:
                if d[1] > _dominant_delivery \
                        and d[1] >= _last_delivery:
                    cur_sub_dominant = d[0]
                    break
            else:
                cur_sub_dominant = None
        return cur_sub_dominant

    def updateDominantInfo(
            self, _tradingday, _data_dict,
            _instrument_dict, _product_dict
    ):
        """
        update dominant and sub-dominant instrument

        :param _tradingday: which tradingday
        :param _data_dict:
        :param _instrument_dict:
        :param _product_dict:
        :return:
        """
        for k, v in _product_dict.items():
            tmp_list = [(
                d, _instrument_dict[d]['DeliveryMonth'],
                float(_data_dict[d]['OpenInterest']),
            ) for d in v['InstrumentList']]
            tmp_list = sorted(
                tmp_list, key=lambda x: x[2], reverse=True
            )  # sorted by openinterest

            last_dominant_delivery, last_sub_dominant_delivery = \
                self._get_last_delivery(k, _tradingday)

            cur_dominant, cur_dominant_delivery = \
                self._get_cur_dominant(
                    last_dominant_delivery, tmp_list
                )
            assert cur_dominant is not None
            assert cur_dominant_delivery is not None

            cur_sub_dominant = self._get_cur_sub_dominant(
                cur_dominant_delivery, last_sub_dominant_delivery,
                tmp_list
            )

            v['Dominant'] = cur_dominant
            v['SubDominant'] = cur_sub_dominant

    def _store_instrument_day_data(self, _instrument, _data):
        self.instrument_day_data_cur.execute(
            "INSERT INTO {} VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (TradingDay) DO UPDATE SET "
            "OpenPrice = EXCLUDED.OpenPrice,"
            "HighPrice = EXCLUDED.HighPrice,"
            "LowPrice = EXCLUDED.LowPrice,"
            "ClosePrice = EXCLUDED.ClosePrice,"
            "SettlementPrice = EXCLUDED.SettlementPrice,"
            "Volume = EXCLUDED.Volume,"
            "OpenInterest = EXCLUDED.OpenInterest,"
            "PreSettlementPrice = EXCLUDED.PreSettlementPrice".format(
                _instrument
            ), [_data[k] for k in INSTRUMENT_TABLE_KEYS]
        )
        self.instrument_day_data_con.commit()

    def _create_instrument_day_data_table(self, _instrument):
        try:
            self.instrument_day_data_cur.execute(
                "CREATE TABLE {}"
                "("
                "TradingDay char(8) PRIMARY KEY,"
                "OpenPrice double precision,"
                "HighPrice double precision,"
                "LowPrice double precision,"
                "ClosePrice double precision,"
                "SettlementPrice double precision,"
                "Volume integer,"
                "OpenInterest double precision,"
                "PreSettlementPrice double precision"
                ")".format(_instrument))
            self.instrument_day_data_con.commit()
        except psycopg2.DatabaseError as e:
            logging.error(e)
            self.instrument_day_data_con.rollback()
            sys.exit(1)

    def storeInstrumentDayData(self, _data_dict: typing.Dict):
        for instrument, v in _data_dict.items():
            try:
                self._store_instrument_day_data(instrument, v)
            except psycopg2.DatabaseError as e:
                self.instrument_day_data_con.rollback()
                if e.pgcode == '42P01':
                    logging.warning(e)
                    self._create_instrument_day_data_table(instrument)
                    self._store_instrument_day_data(instrument, v)
                else:
                    logging.error(e)
                    sys.exit(1)

    @staticmethod
    def _get_arr(_data_list, _index):
        return np.array([d[_index] for d in _data_list])

    def _store_product_index(self, _product, _data):
        self.product_index_cur.execute(
            "INSERT INTO {} VALUES "
            "(%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (TradingDay) DO UPDATE SET "
            "OpenPrice = EXCLUDED.OpenPrice,"
            "HighPrice = EXCLUDED.HighPrice,"
            "LowPrice = EXCLUDED.LowPrice,"
            "ClosePrice = EXCLUDED.ClosePrice,"
            "Volume = EXCLUDED.Volume,"
            "OpenInterest = EXCLUDED.OpenInterest".format(_product),
            [_data[k] for k in INDEX_TABLE_KEYS]
        )
        self.product_index_con.commit()

    def _create_product_index_table(self, _product):
        try:
            self.product_index_cur.execute(
                "CREATE TABLE {}"
                "("
                "TradingDay char(8) PRIMARY KEY,"
                "OpenPrice double precision,"
                "HighPrice double precision,"
                "LowPrice double precision,"
                "ClosePrice double precision,"
                "Volume integer,"
                "OpenInterest double precision"
                ")".format(_product))
            self.product_index_con.commit()
        except psycopg2.DatabaseError as e:
            logging.error(e)
            self.product_index_con.rollback()
            sys.exit(1)

    def storeProductIndex(self, _product_dict: typing.Dict):
        for product, v in _product_dict.items():
            # for each product, compute index of product
            tmp_data_list = []
            for instrument in v['InstrumentList']:
                self.instrument_day_data_cur.execute(
                    "SELECT openprice, highprice, lowprice, closeprice, "
                    "volume, openinterest FROM {} WHERE tradingday='{}'".format(
                        instrument, v['TradingDay']
                    )
                )
                values = self.instrument_day_data_cur.fetchone()
                tmp_data_list.append(dict(zip(
                    INDEX_TABLE_KEYS[1:], values
                )))

            openprice_arr = self._get_arr(tmp_data_list, 'OpenPrice')
            highprice_arr = self._get_arr(tmp_data_list, 'HighPrice')
            lowprice_arr = self._get_arr(tmp_data_list, 'LowPrice')
            closeprice_arr = self._get_arr(tmp_data_list, 'ClosePrice')
            volume_arr = self._get_arr(tmp_data_list, 'Volume')
            openinterest_arr = self._get_arr(tmp_data_list, 'OpenInterest')

            total_openinterest = openinterest_arr.sum()
            if total_openinterest == 0:
                index_dict = {
                    'TradingDay': v['TradingDay'],
                    'OpenPrice': openprice_arr.mean(),
                    'HighPrice': highprice_arr.mean(),
                    'LowPrice': lowprice_arr.mean(),
                    'ClosePrice': closeprice_arr.mean(),
                    'Volume': int(volume_arr.sum()),
                    'OpenInterest': total_openinterest,
                }
            else:
                tmp_rate = openinterest_arr / float(total_openinterest)
                index_dict = {
                    'TradingDay': v['TradingDay'],
                    'OpenPrice': np.sum(tmp_rate * openprice_arr),
                    'HighPrice': np.sum(tmp_rate * highprice_arr),
                    'LowPrice': np.sum(tmp_rate * lowprice_arr),
                    'ClosePrice': np.sum(tmp_rate * closeprice_arr),
                    'Volume': int(volume_arr.sum()),
                    'OpenInterest': total_openinterest
                }
            # true store part
            try:
                self._store_product_index(product, index_dict)
            except psycopg2.DatabaseError as e:
                self.product_index_con.rollback()
                if e.pgcode == '42P01':
                    logging.warning(e)
                    self._create_product_index_table(product)
                    self._store_product_index(product, index_dict)
                else:
                    logging.error(e)
                    sys.exit(1)

    def _create_dominant_index_table(self, _product):
        try:
            self.dominant_index_cur.execute(
                "CREATE TABLE {}"
                "("
                "TradingDay char(8) PRIMARY KEY,"
                "OpenPrice double precision,"
                "HighPrice double precision,"
                "LowPrice double precision,"
                "ClosePrice double precision,"
                "Volume integer,"
                "OpenInterest double precision"
                ")".format(_product))
            self.dominant_index_con.commit()
        except psycopg2.DatabaseError as e:
            logging.error(e)
            self.dominant_index_con.rollback()
            sys.exit(1)

    def _get_last_dominant_index(self, _product, _tradingday):
        try:
            self.dominant_index_cur.execute(
                "SELECT closeprice FROM {} WHERE tradingday<'{}' "
                "ORDER BY tradingday DESC LIMIT 1".format(
                    _product, _tradingday
                )
            )
            for d in self.dominant_index_cur:
                return d[0]
        except psycopg2.ProgrammingError as e:
            logging.warning(e)
            assert e.pgcode == '42P01'
            self.dominant_index_con.rollback()
            self._create_dominant_index_table(_product)
            return None

    def storeDominantIndex(self, _product_dict: typing.Dict):
        for product, v in _product_dict.items():
            # for each product, compute index of dominant
            dominant = v['Dominant']
            self.instrument_day_data_cur.execute(  # get the dominant data
                "SELECT openprice, highprice, lowprice, closeprice, "
                "volume, openinterest FROM {} WHERE tradingday='{}'".format(
                    dominant, v['TradingDay']
                )
            )
            values = self.instrument_day_data_cur.fetchone()
            cur_data = dict(zip(INDEX_TABLE_KEYS[1:], values))
            self.instrument_day_data_cur.execute(  # get the closeprice of last data
                "SELECT closeprice FROM {} WHERE tradingday<'{}' "
                "ORDER BY tradingday DESC LIMIT 1".format(
                    dominant, v['TradingDay']
                )
            )
            values = self.instrument_day_data_cur.fetchone()
            if values is not None:
                return_rate = cur_data['ClosePrice'] / values[0]
            else:
                return_rate = 1.0

            last_index_price = self._get_last_dominant_index(
                product, v['TradingDay']
            )
            if last_index_price is None:
                last_index_price = 1000.0
            new_index_price = last_index_price * return_rate
            price_scale = new_index_price / cur_data['ClosePrice']
            new_openprice = cur_data['OpenPrice'] * price_scale
            new_highprice = cur_data['HighPrice'] * price_scale
            new_lowprice = cur_data['LowPrice'] * price_scale

            self.dominant_index_cur.execute(
                "INSERT INTO {} VALUES "
                "(%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (TradingDay) DO UPDATE SET "
                "OpenPrice = EXCLUDED.OpenPrice,"
                "HighPrice = EXCLUDED.HighPrice,"
                "LowPrice = EXCLUDED.LowPrice,"
                "ClosePrice = EXCLUDED.ClosePrice,"
                "Volume = EXCLUDED.Volume,"
                "OpenInterest = EXCLUDED.OpenInterest"
                "".format(product),
                [
                    v['TradingDay'],
                    new_openprice, new_highprice,
                    new_lowprice, new_index_price,
                    cur_data['Volume'], cur_data['OpenInterest']
                ]
            )
            self.dominant_index_con.commit()

    def storeInstrumentInfo(self, _instrument_dict):
        if 'instrument' not in self.mongo_db.collection_names():
            self.mongo_db.instrument.create_index([
                ('TradingDay', pymongo.ASCENDING),
                ('Instrument', pymongo.ASCENDING)
            ], unique=True)
        for _, v in _instrument_dict.items():
            self.mongo_db.instrument.replace_one({
                'TradingDay': v['TradingDay'],
                'Instrument': v['Instrument'],
            }, v, True)

    def storeProductInfo(self, _product_dict):
        if 'product' not in self.mongo_db.collection_names():
            self.mongo_db.product.create_index([
                ('TradingDay', pymongo.ASCENDING),
                ('Product', pymongo.ASCENDING)
            ], unique=True)
        for _, v in _product_dict.items():
            v['InstrumentList'] = list(v['InstrumentList'])
            self.mongo_db.product.replace_one({
                'TradingDay': v['TradingDay'],
                'Product': v['Product'],
            }, v, True)

    def storeTradingDayInfo(self, _tradingday, _product_dict):
        if 'tradingday' in self.mongo_db.collection_names():
            self.mongo_db.tradingday.create_index([(
                'TradingDay', pymongo.ASCENDING
            )], unique=True)
        product_list = list(_product_dict.keys())
        if product_list:
            self.mongo_db.tradingday.replace_one(
                {'TradingDay': _tradingday},
                {
                    'TradingDay': _tradingday,
                    'ProductList': product_list
                },
                True
            )

    def store(
            self, _tradingday, _data_dict,
            _instrument_dict, _product_dict
    ):
        if not _data_dict:
            return

        logging.info('updateDominantInfo: {}'.format(_tradingday))
        self.updateDominantInfo(
            _tradingday, _data_dict,
            _instrument_dict, _product_dict
        )
        logging.info('storeInstrumentDayData: {}'.format(_tradingday))
        self.storeInstrumentDayData(_data_dict)
        logging.info('storeProductIndex: {}'.format(_tradingday))
        self.storeProductIndex(_product_dict)
        logging.info('storeDominantIndex: {}'.format(_tradingday))
        self.storeDominantIndex(_product_dict)
        logging.info('storeInstrumentInfo: {}'.format(_tradingday))
        self.storeInstrumentInfo(_instrument_dict)
        logging.info('storeProductInfo: {}'.format(_tradingday))
        self.storeProductInfo(_product_dict)
        logging.info('storeTradingDayInfo: {}'.format(_tradingday))
        self.storeTradingDayInfo(_tradingday, _product_dict)

    def lastTradingDay(self):
        ret = self.mongo_db.tradingday.find_one(sort=[(
            'TradingDay', -1
        )])
        if ret:
            return ret['TradingDay']
        return None
