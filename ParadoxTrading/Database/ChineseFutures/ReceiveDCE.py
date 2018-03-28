import logging
import re

import arrow
import requests
import requests.adapters

from ParadoxTrading.Database.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract

DCE_MARKET_URL = "http://www.dce.com.cn/publicweb/" \
                 "quotesdata/exportDayQuotesEnData.html?" \
                 "year={:4d}&month={:02d}&day={:02d}"


class ReceiveDCE(ReceiveDailyAbstract):
    COLLECTION_NAME = 'DCE'

    def __init__(self):
        super().__init__()

        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=10)
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        logging.info('DCE fetchRaw: {}'.format(_tradingday))
        date = arrow.get(_tradingday, 'YYYYMMDD').date()
        url = DCE_MARKET_URL.format(
            date.year, date.month - 1, date.day
        )
        txt = self.session.get(url).content.decode()
        return [
            [w.strip() for w in d.strip().split()]
            for d in txt.split('\n') if d
        ]

    @staticmethod
    def concat(_str):
        return ''.join(_str.split(','))

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
        logging.info('DCE rawToDicts: {}'.format(_tradingday))

        data_dict = {}  # map instrument to data
        instrument_dict = {}  # map instrument to instrument info
        product_dict = {}  # map product to product info

        if _raw_data is None:
            return data_dict, instrument_dict, product_dict

        for line in _raw_data:
            # skip summary data
            if line[0] == 'Contract' or line[1] == 'Subtotal':
                continue
            if line[0] == 'Total':
                break

            instrument = line[0]
            delivery_month = instrument[-4:]
            product = re.findall(r'[a-zA-Z]+', instrument)[0]

            try:
                product_dict[product]['InstrumentList'].add(instrument)
            except KeyError:
                product_dict[product] = {
                    'TradingDay': _tradingday,
                    'Product': product,
                    'InstrumentList': {instrument},
                }

            instrument_dict[instrument] = {
                'TradingDay': _tradingday,
                'Instrument': instrument,
                'ProductID': product,
                'DeliveryMonth': delivery_month,
            }

            tmp_dict = {
                'TradingDay': _tradingday,
                'OpenPrice': ReceiveDCE.concat(line[1]),
                'HighPrice': ReceiveDCE.concat(line[2]),
                'LowPrice': ReceiveDCE.concat(line[3]),
                'ClosePrice': ReceiveDCE.concat(line[4]),
                'SettlementPrice': ReceiveDCE.concat(line[6]),
                'Volume': ReceiveDCE.concat(line[8]),
                'OpenInterest': ReceiveDCE.concat(line[9]),
                'PreSettlementPrice': ReceiveDCE.concat(line[5]),
            }
            if float(tmp_dict['OpenPrice']) == 0:
                tmp_dict['OpenPrice'] = tmp_dict['ClosePrice']
            if float(tmp_dict['HighPrice']) == 0:
                tmp_dict['HighPrice'] = tmp_dict['ClosePrice']
            if float(tmp_dict['LowPrice']) == 0:
                tmp_dict['LowPrice'] = tmp_dict['ClosePrice']

            data_dict[instrument] = tmp_dict

        return data_dict, instrument_dict, product_dict

    def iterFetchAndStore(self, _begin_date='20030101'):
        super().iterFetchAndStore(_begin_date)


if __name__ == '__main__':
    from pprint import pprint

    logging.basicConfig(level=logging.INFO)
    receiver = ReceiveDCE()
    # logging.info('last tradingday: {}'.format(
    #     receiver.lastTradingDay()
    # ))
    # receiver.iterFetchAndStore()

    raw = receiver.loadRaw('20180326')
    data_dict, instrument_dict, product_dict = receiver.rawToDicts(
        '20180326', raw)
    pprint(data_dict)
