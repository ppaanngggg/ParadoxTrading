import logging

import arrow
import requests
import requests.adapters

SHFE_MARKET_URL = 'http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat'


class ReceiveSHFE:
    def __init__(self):
        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=10)
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        r = self.session.get(
            SHFE_MARKET_URL.format(_tradingday)
        )
        if r.status_code == 200:
            return r.json()
        else:
            assert r.status_code == 404
            return None

    def iterFetchRaw(
            self, _begin_date: str = '20020101', _end_date: str = None
    ):
        if _end_date is None:
            _end_date = arrow.now().format('YYYYMMDD')

        ret = []
        tradingday = _begin_date
        while tradingday < _end_date:
            logging.info('TradingDay: {}'.format(tradingday))
            ret.append({
                'TradingDay': tradingday,
                'Raw': self.fetchRaw(tradingday)
            })

            tradingday = arrow.get(
                tradingday, 'YYYYMMDD'
            ).shift(days=1).format('YYYYMMDD')

        return ret

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
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

    @staticmethod
    def iterRawToDicts(_raw_list):
        ret_list = []
        for _raw in _raw_list:
            ret = ReceiveSHFE.rawToDicts(
                _raw['TradingDay'], _raw['Raw']
            )
            ret_list.append({
                'TradingDay': _raw['TradingDay'],
                'DataDict': ret[0],
                'InstrumentDict': ret[1],
                'ProductDict': ret[2],
            })

        return ret_list


if __name__ == '__main__':
    recv = ReceiveSHFE()
    tmp = recv.iterFetchRaw('20171012')
    tmp = recv.iterRawToDicts(tmp)
    print(tmp)