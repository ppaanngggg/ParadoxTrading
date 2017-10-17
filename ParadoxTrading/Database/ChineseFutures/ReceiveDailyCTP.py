import logging
import pickle
import re
import sys

from ParadoxTrading.Receive.ChineseFutures.ReceiveDailyAbstract import ReceiveDailyAbstract


def inst2prod(_inst):
    return re.findall(r"[a-zA-Z]+", _inst)[0]


class ReceiveDailyCTP(ReceiveDailyAbstract):
    def __init__(self, _path):
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
                'PriceDiff_1': v['ClosePrice'] - v['PreSettlementPrice'],
                'PriceDiff_2': v['SettlementPrice'] - v['PreSettlementPrice'],
                'Volume': v['Volume'],
                'OpenInterest': v['OpenInterest'],
                'OpenInterestDiff': v['OpenInterest'] - v['PreOpenInterest'],
                'PreSettlementPrice': v['PreSettlementPrice'],
            }
            if data['OpenPrice'] == sys.float_info.max or data['OpenPrice'] == 0:
                data['OpenPrice'] = data['SettlementPrice']
            if data['HighPrice'] == sys.float_info.max or data['HighPrice'] == 0:
                data['HighPrice'] = data['SettlementPrice']
            if data['LowPrice'] == sys.float_info.max or data['LowPrice'] == 0:
                data['LowPrice'] = data['SettlementPrice']
            if data['ClosePrice'] == sys.float_info.max or data['ClosePrice'] == 0:
                data['ClosePrice'] = data['SettlementPrice']
            data_dict[instrument] = data

        return data_dict, instrument_dict, product_dict


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    recv = ReceiveDailyCTP('/home/pang/Workspace/ReceiverDailyCTP/save')
    tmp = recv.iterFetchRaw('20171013')
    recv.iterRawToDicts(tmp)
