import logging

import arrow
import requests
import requests.adapters
from bs4 import BeautifulSoup

from ParadoxTrading.Database.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract

CFFEX_MARKET_URL = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/index.xml'


def element2str(_elem):
    if _elem.string is None:
        return None
    else:
        return _elem.string.strip().lower()


class ReceiveCFFEX(ReceiveDailyAbstract):
    COLLECTION_NAME = 'CFFEX'

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=10)
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        logging.info('CFFEX fetchRaw: {}'.format(_tradingday))

        date = arrow.get(_tradingday, 'YYYYMMDD')

        r = self.session.get(
            CFFEX_MARKET_URL.format(date.format('YYYYMM'), date.format('DD'))
        )
        if 'error_page' in r.url:
            return

        ret = []
        soup = BeautifulSoup(r.content, 'xml')
        for dailydata in soup.find_all('dailydata'):
            ret.append({
                'Instrument': element2str(dailydata.instrumentid),
                'TradingDay': _tradingday,
                'OpenPrice': element2str(dailydata.openprice),
                'HighPrice': element2str(dailydata.highestprice),
                'LowPrice': element2str(dailydata.lowestprice),
                'ClosePrice': element2str(dailydata.closeprice),
                'SettlementPrice': element2str(dailydata.settlementprice),
                'PreSettlementPrice': element2str(dailydata.presettlementprice),
                'Volume': element2str(dailydata.volume),
                'OpenInterest': element2str(dailydata.openinterest),
                'Product': element2str(dailydata.productid),
            })
        return ret

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
        logging.info('CFFEX rawToDicts: {}'.format(_tradingday))

        data_dict = {}  # map instrument to data
        instrument_dict = {}  # map instrument to instrument info
        product_dict = {}  # map product to product info

        if _raw_data is None:
            return data_dict, instrument_dict, product_dict

        for d in _raw_data:
            instrument = d['Instrument']
            product = d['Product']
            delivery_month = instrument[-4:]

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

            if not d['OpenPrice']:
                d['OpenPrice'] = d['ClosePrice']
            if not d['HighPrice']:
                d['HighPrice'] = d['ClosePrice']
            if not d['LowPrice']:
                d['LowPrice'] = d['ClosePrice']
            data_dict[instrument] = d

        return data_dict, instrument_dict, product_dict

    def iterFetchAndStore(self, _begin_date='20100415'):
        super().iterFetchAndStore(_begin_date)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    receiver = ReceiveCFFEX()
    logging.info('last tradingday: {}'.format(
        receiver.lastTradingDay()
    ))
    receiver.iterFetchAndStore()
