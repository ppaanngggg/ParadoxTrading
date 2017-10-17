import logging

import arrow


class ReceiveDailyAbstract:
    def fetchRaw(self, _tradingday):
        raise NotImplementedError

    def iterFetchRaw(
            self, _begin_date: str, _end_date: str = None
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
        raise NotImplementedError

    def iterRawToDicts(self, _raw_list):
        ret_list = []
        for _raw in _raw_list:
            ret = self.rawToDicts(
                _raw['TradingDay'], _raw['Raw']
            )
            ret_list.append({
                'TradingDay': _raw['TradingDay'],
                'DataDict': ret[0],
                'InstrumentDict': ret[1],
                'ProductDict': ret[2],
            })

        return ret_list
