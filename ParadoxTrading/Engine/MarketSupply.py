"""该模块

"""

import json
import operator
import typing
from datetime import datetime, timedelta

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import MarketEvent
from ParadoxTrading.Utils import (DataStruct, Fetch)


class MarketRegister:
    def __init__(
            self,
            _product: str = None,
            _instrument: str = None,
            _sub_dominant: bool = False, ):
        """
        Market Register is used to store market sub information,
        pre-processed data and strategies used it

        :param _product: reg which product, if not None, ignore instrument
        :param _instrument: reg which instrument
        :param _sub_dominant: only work when use product,
            false means using dominant inst,
            true means sub dominant one
        """
        assert _product is not None or _instrument is not None

        # market register info
        self.product = _product
        self.instrument = _instrument
        self.sub_dominant = _sub_dominant

        # strategies linked to this market register
        self.strategy_set: typing.Set[typing.AnyStr] = set()

    def addStrategy(
            self,
            _strategy: 'ParadoxTrading.Engine.Strategy.StrategyAbstract'):
        """
        link strategy to self

        :param _strategy: strategy object (just use its name)
        :return:
        """
        self.strategy_set.add(_strategy.name)

    def toJson(self) -> str:
        """
        encode register info into json str

        :return: json str
        """
        return json.dumps(
            (('product', self.product), ('instrument', self.instrument),
             ('sub_dominant', self.sub_dominant),))

    @staticmethod
    def fromJson(_json_str: str) -> 'MarketRegister':
        """
        create object from a json str

        :param _json_str: json str stores register info
        :return: market register object
        """
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return MarketRegister(
            data['product'],
            data['instrument'],
            data['sub_dominant']
        )

    def __repr__(self):
        return 'Key:' + '\n' + \
               '\t' + self.toJson() + '\n' + \
               'Params:' + '\n' + \
               '\tproduct: ' + str(self.product) + '\n' + \
               '\tinstrument: ' + str(self.instrument) + '\n' + \
               '\tsub_dominant: ' + str(self.sub_dominant) + '\n' + \
               'Strategy: ' + '\n' + \
               '\t' + '; '.join(self.strategy_set)


class MarketSupplyAbstract:
    def __init__(self, _engine: 'ParadoxTrading.Engine.EngineAbstract'):
        """
        base class market supply

        :param _engine: ref to backtest engine
        """

        # map market register's key to its object
        self.market_register_dict: typing.Dict[str, MarketRegister] = {}
        # map instrument to set of market register
        self.instrument_dict: typing.Dict[str, typing.Set[str]] = {}
        self.data_dict: typing.Dict[str, DataStruct] = {}

        self.engine = _engine

    def addStrategy(
            self,
            _strategy: 'ParadoxTrading.Engine.StrategyAbstract'):
        """
        Add strategy into market supply

        :param _strategy: strategy oject
        :return: None
        """

        # for each market register
        for key in _strategy.market_register_dict.keys():
            if key not in self.market_register_dict.keys():
                # if not existed, create it
                self.market_register_dict[key] = MarketRegister.fromJson(key)
            # add strategy into market register
            self.market_register_dict[key].addStrategy(_strategy)
            _strategy.market_register_dict[key] = self.market_register_dict[
                key]

    def addEvent(self, _instrument: str, _data: DataStruct):
        """
        add new tick data into market register, and add event

        :param _instrument:
        :param _data:
        :return:
        """
        for k in self.instrument_dict[_instrument]:
            # add event for each strategy if necessary
            for strategy in self.market_register_dict[k].strategy_set:
                self.engine.addEvent(
                    MarketEvent(k, strategy, _instrument, _data))

    def getTradingDay(self) -> str:
        raise NotImplementedError('getTradingDay not implemented')

    def getCurDatetime(self) -> datetime:
        raise NotImplementedError('getCurDatetime not implemented')

    def getInstrumentData(self, _instrument: str) -> DataStruct:
        return self.data_dict[_instrument]

    def updateData(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        raise NotImplementedError('updateData not implemented')

    def __repr__(self):
        ret = '### MARKET REGISTER ###'
        for k, v in self.market_register_dict.items():
            ret += '\n' + k
        ret += '\n### INSTRUMENT ###'
        for k, v in self.instrument_dict.items():
            ret += '\n' + k + ': ' + str(v)
        ret += '\n### DATA ###'
        for k, v in self.data_dict.items():
            ret += '\n--- ' + k + ' ---\n' + str(v)

        return ret


class DataGenerator:
    """
    JUST FOR BACKTEST !!!
    """

    def __init__(self,
                 _tradingday: str,
                 _backtest_type: str,
                 _market_register_dict: typing.Dict[str, MarketRegister],
                 _instrument_dict: typing.Dict[str, typing.Set[str]],
                 _time_index: str = None):
        """
        fetch data according to market registers,
        and pop tick data by happentime

        :param _tradingday: the day to fetch
        :param _market_register_dict:
        :param _instrument_dict:
        """
        self.data_dict: typing.Dict[str, DataStruct] = {}
        self.index_dict: typing.Dict[str, int] = {}
        self.cur_datetime: datetime = None

        # have to reset it, it is a ref to market supply's dict
        _instrument_dict.clear()

        for k, v in _market_register_dict.items():
            inst = Fetch._fetchInstrument(
                _tradingday, v.product, v.instrument, v.sub_dominant)
            # whether inst exists
            if inst is not None:
                # fetch data and set index to 0 init
                if _backtest_type == BacktestMarketSupply.TICK:
                    time_index = 'HappenTime'
                    if _time_index is not None:
                        time_index = _time_index
                    self.data_dict[inst] = Fetch.fetchIntraDayData(
                        _tradingday,
                        _instrument=inst,
                        _data_type=Fetch.pgsql_tick_dbname,
                        _index=time_index.lower())
                elif _backtest_type == BacktestMarketSupply.MIN:
                    time_index = 'BarEndTime'
                    if _time_index is not None:
                        time_index = _time_index
                    self.data_dict[inst] = Fetch.fetchIntraDayData(
                        _tradingday,
                        _instrument=inst,
                        _data_type=Fetch.pgsql_min_dbname,
                        _index=time_index.lower())
                elif _backtest_type == BacktestMarketSupply.HOUR:
                    time_index = 'BarEndTime'
                    if _time_index is not None:
                        time_index = _time_index
                    self.data_dict[inst] = Fetch.fetchIntraDayData(
                        _tradingday,
                        _instrument=inst,
                        _data_type=Fetch.pgsql_hour_dbname,
                        _index=time_index.lower())
                elif _backtest_type == BacktestMarketSupply.DAY:
                    self.data_dict[inst] = Fetch.fetchInterDayData(inst,
                                                                   _tradingday)
                else:
                    raise Exception('unknown backtest type')

                self.index_dict[inst] = 0

                # map instrument to market register key
                try:
                    _instrument_dict[inst].add(k)
                except KeyError:
                    _instrument_dict[inst] = {k}

    def gen(self) -> typing.Union[None, typing.Tuple[str, DataStruct]]:
        """
        gen one tick data

        :return: (instrument, one tick data struct)
        """

        # get latest one of each instrument
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
            # 2. all instruments reach the end
            return None


class BacktestMarketSupply(MarketSupplyAbstract):
    TICK = 't'
    MIN = 'm'
    HOUR = 'h'
    DAY = 'd'

    def __init__(self,
                 _begin_day: str,
                 _end_day: str,
                 _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract',
                 _backtest_type: str):
        """
        market supply for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        :param _engine: ref to backtest engine
        """
        self.begin_day: str = _begin_day
        self.cur_day: str = self.begin_day
        self.end_day: str = _end_day

        self.backtest_type = _backtest_type

        self.data_generator: DataGenerator = None

        super().__init__(_engine)

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
                self.cur_day, self.backtest_type, self.market_register_dict,
                self.instrument_dict)

        # gen one tick data from data generator
        ret = self.data_generator.gen()

        if ret is None:
            # if data is None:
            # 1. this day is not a tradingday
            # 2. all instruments reach the end
            # how to deal:
            # 1. inc cur date and reset data generator
            # 2. try updateData again
            self.incDate()
            self.data_generator = None
            return self.updateData()
        else:
            # process new tick data
            try:
                self.data_dict[ret[0]].merge(ret[1])
            except KeyError:
                self.data_dict[ret[0]] = ret[1]
            self.addEvent(*ret)
            return ret

    def getTradingDay(self) -> str:
        return self.cur_day

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        """
        :return: latest market happentime
        """
        if self.data_generator is not None:
            return self.data_generator.cur_datetime
        return None
