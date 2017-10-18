import logging
import pickle
import re
import sys

from ParadoxTrading.Database.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract


def inst2prod(_inst):
    return re.findall(r"[a-zA-Z]+", _inst)[0]


class ReceiveDailyCTP(ReceiveDailyAbstract):
    COLLECTION_NAME = 'DailyCTP'

    def __init__(self, _path):
        super().__init__()
        self.path = _path

    def fetchRaw(self, _tradingday):
        try:
            f = open('{}/{}.pkl'.format(self.path, _tradingday), 'rb')
        except FileNotFoundError:
            logging.warning('file {}.pkl not found'.format(_tradingday))
            return None

        return pickle.load(f)

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
        logging.info('DailyCTP rawToDicts: {}'.format(_tradingday))

        data_dict = {}  # map instrument to data
        instrument_dict = {}  # map instrument to instrument info
        product_dict = {}  # map product to product info

        if _raw_data is None:
            return data_dict, instrument_dict, product_dict

        for k, v in _raw_data.items():
            instrument = k.lower()
            delivery_month = instrument[-3:]
            product = inst2prod(instrument)
            tradingday_month = _tradingday[2:6]
            tmp = int(tradingday_month[0])
            new_delivery_month = '{}{}'.format(tmp, delivery_month)
            if new_delivery_month < tradingday_month:
                new_delivery_month = '{}{}'.format(tmp + 1, delivery_month)
            delivery_month = new_delivery_month

            try:
                product_dict[product]['InstrumentList'].add(instrument)
            except KeyError:
                product_dict[product] = {
                    'InstrumentList': {instrument},
                    'TradingDay': _tradingday
                }

            instrument_dict[instrument] = {
                'ProductID': product,
                'DeliveryMonth': delivery_month,
                'TradingDay': _tradingday,
            }

            data = {
                'TradingDay': _tradingday,
                'OpenPrice': v['OpenPrice'],
                'HighPrice': v['HighestPrice'],
                'LowPrice': v['LowestPrice'],
                'ClosePrice': v['ClosePrice'],
                'SettlementPrice': v['SettlementPrice'],
                'Volume': v['Volume'],
                'OpenInterest': v['OpenInterest'],
                'OpenInterestDiff': v['OpenInterest'] - v['PreOpenInterest'],
                'PreSettlementPrice': v['PreSettlementPrice'],
            }

            if data['SettlementPrice'] != sys.float_info.max \
                    and data['SettlementPrice'] != 0:
                base_price = data['SettlementPrice']
            else:
                base_price = data['PreSettlementPrice']
                data['SettlementPrice'] = base_price

            if data['OpenPrice'] == sys.float_info.max \
                    or data['OpenPrice'] == 0:
                data['OpenPrice'] = base_price
            if data['HighPrice'] == sys.float_info.max \
                    or data['HighPrice'] == 0:
                data['HighPrice'] = base_price
            if data['LowPrice'] == sys.float_info.max \
                    or data['LowPrice'] == 0:
                data['LowPrice'] = base_price
            if data['ClosePrice'] == sys.float_info.max \
                    or data['ClosePrice'] == 0:
                data['ClosePrice'] = base_price
            data['PriceDiff_1'] = data['ClosePrice'] - \
                data['PreSettlementPrice']
            data['PriceDiff_2'] = data['SettlementPrice'] - \
                data['PreSettlementPrice']
            data_dict[instrument] = data

        return data_dict, instrument_dict, product_dict
