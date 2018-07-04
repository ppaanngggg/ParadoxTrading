import logging
import operator
import typing
from datetime import datetime, timedelta

from ParadoxTrading.Engine import (MarketSupplyAbstract, ReturnMarket,
                                   ReturnSettlement)
from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class DataGenerator:
    """
    JUST FOR BACKTEST !!!
    """

    def __init__(
            self,
            _tradingday: str,
            _register_dict: typing.Dict[str, RegisterAbstract],
            _symbol_dict: typing.Dict[str, typing.Set[str]],
            _fetcher: FetchAbstract
    ):
        """
        fetch data according to market registers,
        and pop tick data by happentime

        :param _tradingday: the day to fetch
        :param _register_dict:
        :param _symbol_dict:
        """
        self.data_dict: typing.Dict[str, DataStruct] = {}
        self.index_dict: typing.Dict[str, int] = {}
        self.datetime: typing.Union[str, datetime] = None

        # have to reset it, it is a ref to market supply's dict
        _symbol_dict.clear()

        for k, v in _register_dict.items():
            symbol = _fetcher.fetchSymbol(
                _tradingday, **v.toKwargs()
            )
            if symbol is None:
                continue

            if symbol not in self.data_dict.keys():
                # fetch data and set index to 0 init
                data = _fetcher.fetchData(_tradingday, _symbol=symbol)
                if data is None:
                    logging.warning('data {} not available'.format(symbol))
                    continue
                self.data_dict[symbol] = data
                self.index_dict[symbol] = 0

            # map symbol to market register key
            try:
                _symbol_dict[symbol].add(k)
            except KeyError:
                _symbol_dict[symbol] = {k}
        logging.debug('Available symbol: {}'.format(_symbol_dict.keys()))

    def gen(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        """
        gen one tick data

        :return: (symbol, one tick datastruct) or None
        """

        # get latest one of each symbol
        assert self.data_dict and self.index_dict
        tmp = []
        for k, v in self.index_dict.items():
            d = self.data_dict[k]
            if v < len(d):
                tmp.append((k, d.index()[v]))

        if tmp:
            # get the latest market data of all
            tmp.sort(key=operator.itemgetter(1))

            symbol = tmp[0][0]

            index = self.index_dict[symbol]
            ret: typing.Tuple[str, DataStruct] = (
                symbol, self.data_dict[symbol].iloc[index])
            self.index_dict[symbol] += 1  # point to next one

            # set cur datetime to latest tick's happentime
            self.datetime = tmp[0][1]

            return ret
        else:
            # 1. data_dict and index_dict are empty
            # 2. all symbols reach the end
            return None


class BacktestMarketSupply(MarketSupplyAbstract):
    def __init__(
            self: 'BacktestMarketSupply',
            _begin_day: str, _end_day: str,
            _fetcher: FetchAbstract
    ):
        """
        market supply for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        """
        super().__init__(_fetcher)

        self.begin_day: str = _begin_day
        self.end_day: str = _end_day

        self.tradingday: str = self.begin_day
        self.tradingday_obj: datetime = datetime.strptime(
            self.tradingday, '%Y%m%d'
        )
        self.datetime: typing.Union[str, datetime] = None
        self.data_generator: DataGenerator = None

    def incDate(self) -> str:
        """
        inc cur date and return

        :return: cur date
        """
        self.tradingday_obj += timedelta(days=1)
        self.tradingday = self.tradingday_obj.strftime('%Y%m%d')
        return self.tradingday

    def updateData(self) -> typing.Union[
        None, ReturnMarket, ReturnSettlement
    ]:
        """
        update data tick by tick, or bar by bar

        :return: flag of current market status
        """

        while self.data_generator is None:
            if self.tradingday >= self.end_day:
                return None

            self.data_generator = DataGenerator(
                _tradingday=self.tradingday,
                _register_dict=self.register_dict,
                _symbol_dict=self.symbol_dict,
                _fetcher=self.fetcher
            )
            if not self.symbol_dict:
                self.incDate()
                self.data_generator: DataGenerator = None
            else:
                logging.info('TradingDay: {}, Product: {}'.format(
                    self.tradingday, self.symbol_dict.keys()
                ))
        # try to gen one tick data from data generator
        ret = self.data_generator.gen()
        if ret is None:  # this tradingday is end
            tmp_day = self.tradingday
            self.incDate()
            self.data_generator: DataGenerator = None
            return self.addSettlementEvent(tmp_day)
        else:
            self.datetime = self.data_generator.datetime
            return self.addMarketEvent(*ret)

    def getTradingDay(self) -> str:
        return self.tradingday

    def getDatetime(self) -> typing.Union[None, datetime, str]:
        """
        :return: latest market happentime
        """
        return self.datetime
