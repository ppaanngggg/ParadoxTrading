import logging

import requests
import requests.adapters

from ParadoxTrading.Database.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract

SHFE_MARKET_URL = 'http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat'


class ReceiveSHFE(ReceiveDailyAbstract):
    COLLECTION_NAME = 'SHFE'

    def __init__(self):
        super().__init__()

        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=10)
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        logging.info('SHFE fetchRaw: {}'.format(_tradingday))
        r = self.session.get(
            SHFE_MARKET_URL.format(_tradingday)
        )
        if r.status_code == 200:
            return r.json()
        else:
            assert r.status_code == 404
            return None

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
        logging.info('SHFE rawToDicts: {}'.format(_tradingday))

        data_dict = {}  # map instrument to data
        instrument_dict = {}  # map instrument to instrument info
        product_dict = {}  # map product to product info

        if _raw_data is None:
            return data_dict, instrument_dict, product_dict

        for d in _raw_data['o_curinstrument']:
            # skip summary data
            if d['PRODUCTID'] == '总计':
                continue
            if d['DELIVERYMONTH'] == '小计':
                continue
            if d['DELIVERYMONTH'] == 'efp':
                continue

            product = d['PRODUCTID'].strip()[:-2]
            delivery_month = d['DELIVERYMONTH'].strip()
            instrument = product + delivery_month

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
                'OpenPrice': d['OPENPRICE'],
                'HighPrice': d['HIGHESTPRICE'],
                'LowPrice': d['LOWESTPRICE'],
                'ClosePrice': d['CLOSEPRICE'],
                'SettlementPrice': d['SETTLEMENTPRICE'],
                'PriceDiff_1': d['ZD1_CHG'],
                'PriceDiff_2': d['ZD2_CHG'],
                'Volume': d['VOLUME'],
                'OpenInterest': d['OPENINTEREST'],
                'OpenInterestDiff': d['OPENINTERESTCHG'],
                'PreSettlementPrice': d['PRESETTLEMENTPRICE'],
            }
            if not data['OpenPrice']:
                data['OpenPrice'] = data['ClosePrice']
            if not data['HighPrice']:
                data['HighPrice'] = data['ClosePrice']
            if not data['LowPrice']:
                data['LowPrice'] = data['ClosePrice']
            if not data['PriceDiff_1']:
                data['PriceDiff_1'] = 0
            if not data['PriceDiff_2']:
                data['PriceDiff_2'] = 0
            data_dict[instrument] = data

        return data_dict, instrument_dict, product_dict
