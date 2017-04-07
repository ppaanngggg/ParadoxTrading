import operator
from datetime import datetime, timedelta
import logging
import typing

from ParadoxTrading.Engine import MarketSupplyAbstract
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
        logging.info('Fetch Day: {}'.format(_tradingday))
        self.data_dict: typing.Dict[str, DataStruct] = {}
        self.index_dict: typing.Dict[str, int] = {}
        self.cur_datetime: datetime = None

        # have to reset it, it is a ref to market supply's dict
        _symbol_dict.clear()

        for k, v in _register_dict.items():
            inst = _fetcher.fetchSymbol(
                _tradingday, **v.toKwargs()
            )
            # whether inst exists
            if inst is not None:
                # fetch data and set index to 0 init
                self.data_dict[inst] = _fetcher.fetchData(
                    _tradingday, _symbol=inst)
                self.index_dict[inst] = 0

                # map symbol to market register key
                try:
                    _symbol_dict[inst].add(k)
                except KeyError:
                    _symbol_dict[inst] = {k}
        logging.info('Available symbol: {}'.format(_symbol_dict.keys()))

    def gen(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        """
        gen one tick data

        :return: (symbol, one tick data struct)
        """

        # get latest one of each symbol
        tmp = []
        for k, v in self.index_dict.items():
            d = self.data_dict[k]
            if v < len(d):
                tmp.append((k, d.index()[v]))

        if tmp:
            # get the latest market data of all
            tmp.sort(key=operator.itemgetter(1))

            inst = tmp[0][0]

            index = self.index_dict[inst]
            ret: typing.Tuple[str, DataStruct] = (
                inst, self.data_dict[inst].iloc[index])
            self.index_dict[inst] += 1  # point to next one

            # set cur datetime to latest tick's happentime
            self.cur_datetime = tmp[0][1]

            return ret
        else:
            # 1. data_dict and index_dict are empty
            # 2. all symbols reach the end
            return None


class BacktestMarketSupply(MarketSupplyAbstract):
    def __init__(
            self, _begin_day: str, _end_day: str,
            _fetcher: FetchAbstract
    ):
        """
        market supply for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        """
        super().__init__(_fetcher)

        self.begin_day: str = _begin_day
        self.cur_day: str = self.begin_day
        self.end_day: str = _end_day

        self.data_generator: DataGenerator = None

    def incDate(self) -> str:
        """
        inc cur date and return

        :return: cur date
        """
        tmp = datetime.strptime(self.cur_day, '%Y%m%d')
        tmp += timedelta(days=1)
        self.cur_day = tmp.strftime('%Y%m%d')
        return self.cur_day

    def updateData(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        """
        update data tick by tick

        :return: whether there is data
        """

        # reach end, so return false to end backtest
        if self.cur_day > self.end_day:
            return None

        # if there is no data generator, create one
        if self.data_generator is None:
            self.data_generator = DataGenerator(
                _tradingday=self.cur_day,
                _register_dict=self.register_dict,
                _symbol_dict=self.symbol_dict,
                _fetcher=self.fetcher
            )

        # gen one tick data from data generator
        ret = self.data_generator.gen()

        if ret is None:
            # if data is None:
            # 1. this day is not a tradingday
            # 2. all symbols reach the end
            # how to deal:
            # 1. inc cur date and reset data generator
            # 2. try updateData again
            self.incDate()
            self.data_generator = None
            return self.updateData()
        else:
            self.addEvent(*ret)
            return ret

    def getTradingDay(self) -> str:
        return self.cur_day

    def getDatetime(self) -> typing.Union[None, datetime]:
        """
        :return: latest market happentime
        """
        if self.data_generator is not None:
            return self.data_generator.cur_datetime
        return None
