import logging
import typing
from datetime import datetime
from time import time

from TdServer.utilPool.dataFetcher import DataSub
from TdServer.utilPool.dataProcessor import clip_depth

from ParadoxTrading.Engine import MarketSupplyAbstract, ReturnMarket, ReturnSettlement
from ParadoxTrading.Fetch.Crypto.FetchBase import FetchBase
from ParadoxTrading.Utils import DataStruct

LEVEL = 10
TIMEOUT = 1000


class DepthMarketSupply(MarketSupplyAbstract):
    def __init__(self):
        super().__init__(FetchBase())

        self.data_sub = DataSub()
        self.cur_datetime = datetime.utcnow()
        self.tradingday = self.cur_datetime.date().strftime('%Y%m%d')

        self.column_list = []  # datastruct columns
        for i in range(LEVEL):
            self.column_list.append('askprice{}'.format(i))
            self.column_list.append('askamount{}'.format(i))
        for i in range(LEVEL):
            self.column_list.append('bidprice{}'.format(i))
            self.column_list.append('bidamount{}'.format(i))
        self.column_list.append('datetime')
        self.last_data_dict = {}  # map symbol to data tuple

    def getTradingDay(self) -> str:
        return self.tradingday

    def getDatetime(self) -> typing.Union[str, datetime]:
        return self.cur_datetime

    def _set_symbol_dict(self):
        for k, v in self.register_dict.items():
            symbol = self.fetcher.fetchSymbol(
                self.getTradingDay(), **v.toKwargs()
            )
            try:
                tmp_set = self.symbol_dict[symbol]
            except KeyError:
                tmp_set = self.symbol_dict[symbol] = set()
            tmp_set.add(k)

    def _sub_data(self):
        tmp_list = []
        for k in self.symbol_dict.keys():
            exname, symbol = k
            tmp_list.append(('rs', 'depth', exname, symbol))
        self.data_sub.sub(tmp_list)

    def updateData(self) -> typing.Union[
        None, ReturnMarket, ReturnSettlement
    ]:
        if not self.symbol_dict:
            self._set_symbol_dict()
            self._sub_data()

        while True:
            raw_data = self.data_sub.pop()
            # update datetime and tradingday, and check settlement
            self.cur_datetime = datetime.utcnow()
            cur_tradingday = self.cur_datetime.date().strftime('%Y%m%d')
            if self.tradingday != cur_tradingday:
                logging.info('New TradingDay: {}'.format(cur_tradingday))
                tmp = self.tradingday
                self.tradingday = cur_tradingday
                return self.addSettlementEvent(tmp)
            # check lts
            data_lts = raw_data['lts']
            cur_lts = time() * 1000
            if data_lts + TIMEOUT < cur_lts:  # too old data
                continue

            symbol = (raw_data['exname'], raw_data['symbol'])
            tuple_data = clip_depth(raw_data['data'], LEVEL)
            try:
                last_data = self.last_data_dict[symbol]
                if last_data != tuple_data:
                    return self.addMarketEvent(symbol, DataStruct(
                        self.column_list, 'datetime',
                        [tuple_data + (self.cur_datetime,)]
                    ))
                else:
                    continue
            except KeyError:
                datastruct = DataStruct(
                    self.column_list, 'datetime',
                    [tuple_data + (self.cur_datetime,)]
                )
                return self.addMarketEvent(symbol, datastruct)
            finally:
                self.last_data_dict[symbol] = tuple_data
