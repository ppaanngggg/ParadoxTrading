import logging
import re
from time import sleep

import arrow
import requests
import requests.adapters
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry

from ParadoxTrading.Database.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract

CZCE_MARKET_URL = 'http://www.czce.com.cn/cms/cmsface/czce/exchangefront/calendarnewquery.jsp'

KEYS = [
    'PreSettlementPrice', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
    'SettlementPrice', 'PriceDiff_2', 'Volume', 'OpenInterest', 'OpenInterestDiff'
]


def inst2prod(_inst):
    return re.findall(r"[a-zA-Z]+", _inst)[0]


class ReceiveCZCE(ReceiveDailyAbstract):
    COLLECTION_NAME = 'CZCE'

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(
            max_retries=Retry(method_whitelist=frozenset(['GET', 'POST']))
        )
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        logging.info('CZCE fetchRaw: {}'.format(_tradingday))
        payload = {
            'dataType': 'DAILY',
            'pubDate': arrow.get(_tradingday, 'YYYYMMDD').format('YYYY-MM-DD'),
            'commodity': '',
        }
        while True:
            sleep(0.1)
            r = self.session.post(CZCE_MARKET_URL, data=payload)
            if r.status_code != 200:
                logging.warning('code is {}'.format(r.status_code))
                continue
            if 'dataempty' in r.url:
                return
            elif 'jyxx' in r.url:
                soup = BeautifulSoup(r.content, 'html.parser')
                table = soup.table.table.table
                ret = []
                for tr in table.find_all('tr')[1:]:
                    line = []
                    for td in tr.find_all('td')[:7]:
                        if td.string is not None:
                            line.append(''.join(td.string.strip().lower().split(',')))
                        else:
                            line.append('')
                    for td in tr.find_all('td')[-6:]:
                        if td.string is not None:
                            line.append(''.join(td.string.strip().lower().split(',')))
                        else:
                            line.append('')
                    if line:
                        ret.append(line)
                    if tr.td.string.strip() == '总计':
                        break
                return ret
            elif 'datadaily' in r.url:
                txt_url = r.url.replace('htm', 'txt')
                r = self.session.get(txt_url)
                assert r.status_code == 200
                ret = []
                lines = r.content.decode('gb2312').strip().split('\n')
                for line in lines[1:]:
                    line = [d.strip().lower() for d in line.strip().split(',')]
                    del line[7]
                    ret.append(line)
                return ret
            elif 'FutureDataDaily' in r.url:
                txt_url = r.url.replace('htm', 'txt')
                r = self.session.get(txt_url)
                assert r.status_code == 200
                ret = []
                lines = r.content.decode('gb2312').strip().split('\n')
                for line in lines[2:]:
                    line = line.strip().split('|')
                    del line[7]
                    line = [''.join(d.strip().lower().split(',')) for d in line]
                    ret.append(line)
                return ret
            else:
                raise Exception('unknown url type')

    @staticmethod
    def rawToDicts(_tradingday, _raw_data):
        data_dict = {}  # map instrument to data
        instrument_dict = {}  # map instrument to instrument info
        product_dict = {}  # map product to product info

        if _raw_data is None:
            return data_dict, instrument_dict, product_dict

        for d in _raw_data:
            # skip summary data
            instrument = d[0].lower()
            if instrument in ['总计', '小计']:
                continue
            product = inst2prod(instrument)
            delivery_month = instrument[-3:]
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

            tmp_dict = {'TradingDay': _tradingday}
            tmp_dict.update(dict(zip(
                KEYS, d[1:-2]
            )))
            if float(tmp_dict['OpenPrice']) == 0:
                tmp_dict['OpenPrice'] = tmp_dict['PreSettlementPrice']
            if float(tmp_dict['HighPrice']) == 0:
                tmp_dict['HighPrice'] = tmp_dict['PreSettlementPrice']
            if float(tmp_dict['LowPrice']) == 0:
                tmp_dict['LowPrice'] = tmp_dict['PreSettlementPrice']
            if float(tmp_dict['ClosePrice']) == 0:
                tmp_dict['ClosePrice'] = tmp_dict['PreSettlementPrice']
            tmp_dict['Volume'] = int(float(tmp_dict['Volume']))
            tmp_dict['PriceDiff_1'] = float(tmp_dict['ClosePrice']) - \
                                      float(tmp_dict['PreSettlementPrice'])

            data_dict[instrument] = tmp_dict

        return data_dict, instrument_dict, product_dict
