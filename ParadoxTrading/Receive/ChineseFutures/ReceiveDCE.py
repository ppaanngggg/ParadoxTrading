import logging

import arrow
import requests
import requests.adapters

DCE_MARKET_URL = "http://www.dce.com.cn/publicweb/" \
                 "quotesdata/exportDayQuotesChData.html?" \
                 "year={:4d}&month={:02d}&day={:02d}"
NAME_PRODUCT_TABLE = {
    '豆一': 'a',
    '豆二': 'b',
    '豆粕': 'm',
    '豆油': 'y',
    '棕榈油': 'p',
    '玉米': 'c',
    '玉米淀粉': 'cs',
    '鸡蛋': 'jd',
    '纤维板': 'fb',
    '胶合板': 'bb',
    '聚乙烯': 'l',
    '聚氯乙烯': 'v',
    '聚丙烯': 'pp',
    '焦炭': 'j',
    '焦煤': 'jm',
    '铁矿石': 'i',
}
KEYS = [
    'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
    'PreSettlementPrice', 'SettlementPrice', 'PriceDiff_1', 'PriceDiff_2',
    'Volume', 'OpenInterest', 'OpenInterestDiff', 'Turnover'
]


class ReceiveDCE:
    def __init__(self):
        self.session = requests.Session()
        a = requests.adapters.HTTPAdapter(max_retries=10)
        self.session.mount('http://', a)

    def fetchRaw(self, _tradingday):
        date = arrow.get(_tradingday, 'YYYYMMDD').date()
        txt = self.session.get(DCE_MARKET_URL.format(
            date.year, date.month - 1, date.day
        )).content.decode()
        return [
            [w.strip() for w in d.strip().split()]
            for d in txt.split('\n') if d
        ]

    def iterFetchRaw(
            self, _begin_date: str = '20030101', _end_date: str = None
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

        for d in _raw_data:
            # skip summary data
            instrument = d[0]
            if instrument in ['商品名称', '总计', '小计']:
                continue

            try:
                product = NAME_PRODUCT_TABLE[instrument]
                delivery_month = d[1][-4:]
                instrument = product + delivery_month
            except KeyError:
                logging.warning('product {} not exist'.format(instrument))
                continue

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
                KEYS, [''.join(d.split(',')) for d in d[2:-1]]
            )))
            if float(tmp_dict['OpenPrice']) == 0:
                tmp_dict['OpenPrice'] = tmp_dict['ClosePrice']
            if float(tmp_dict['HighPrice']) == 0:
                tmp_dict['HighPrice'] = tmp_dict['ClosePrice']
            if float(tmp_dict['LowPrice']) == 0:
                tmp_dict['LowPrice'] = tmp_dict['ClosePrice']

            data_dict[instrument] = tmp_dict

        return data_dict, instrument_dict, product_dict

    @staticmethod
    def iterRawToDicts(_raw_list):
        ret_list = []
        for _raw in _raw_list:
            ret = ReceiveDCE.rawToDicts(
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
    recv = ReceiveDCE()
    tmp = recv.iterFetchRaw('20171012')
    tmp = recv.iterRawToDicts(tmp)
    print(tmp)
