import json
import operator
import typing
from datetime import datetime, timedelta

import ParadoxTrading.Engine.Engine
import ParadoxTrading.Engine.Strategy
from ParadoxTrading.Engine.Event import MarketEvent
from ParadoxTrading.Utils import (DataStruct, Fetch, SplitIntoHour,
                                  SplitIntoMinute, SplitIntoSecond)
from ParadoxTrading.Utils.Split import SplitAbstract


class MarketRegister:
    def __init__(
            self, _product: str = None, _instrument: str = None, _sub_dominant: bool = False,
            _second_skip: int = 0, _minute_skip: int = 0, _hour_skip: int = 0
    ):
        """
        Market Register is used to store market sub information, pre-processed data and
        strategies used it

        :param _product: reg which product, if not None, ignore instrument
        :param _instrument: reg which instrument
        :param _sub_dominant: only work when use product, false means using dominant inst, true means sub dominant one
        :param _second_skip: split into n second bar, if all skips are 0, use tick data
        :param _minute_skip: same as above
        :param _hour_skip: same as above
        """
        assert _product is not None or _instrument is not None

        # market register info
        self.product = _product
        self.instrument = _instrument
        self.sub_dominant = _sub_dominant

        self.second_skip = _second_skip
        self.minute_skip = _minute_skip
        self.hour_skip = _hour_skip

        # strategies linked to this market register
        self.strategy_set: typing.Set[str] = set()

        # store cur instrument received
        self.cur_data_inst: str = None

        self.data: typing.Union[DataStruct, SplitAbstract] = None
        if self.second_skip > 0:
            self.data = SplitIntoSecond(self.second_skip)
        elif self.minute_skip > 0:
            self.data = SplitIntoMinute(self.minute_skip)
        elif self.hour_skip > 0:
            self.data = SplitIntoHour(self.hour_skip)
        else:
            # use tick data
            pass

    def addStrategy(self, _strategy: 'ParadoxTrading.Engine.Strategy.StrategyAbstract'):
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
        return json.dumps((
            ('product', self.product),
            ('instrument', self.instrument),
            ('sub_dominant', self.sub_dominant),
            ('second_skip', self.second_skip),
            ('minute_skip', self.minute_skip),
            ('hour_skip', self.hour_skip),
        ))

    @staticmethod
    def fromJson(_json_str: str) -> 'MarketRegister':
        """
        create object from a json str

        :param _json_str: json str stores register info
        :return: market register object
        """
        data = dict(json.loads(_json_str))
        return MarketRegister(
            data['product'], data['instrument'], data['sub_dominant'],
            data['second_skip'], data['minute_skip'], data['hour_skip']
        )

    def __repr__(self):
        return '- Params:' + '\n' + \
               '\tproduct: ' + str(self.product) + '\n' + \
               '\tinstrument: ' + str(self.instrument) + '\n' + \
               '\tsub_dominant: ' + str(self.sub_dominant) + '\n' + \
               '\tsecond_skip: ' + str(self.second_skip) + '\n' + \
               '\tminute_skip: ' + str(self.minute_skip) + '\n' + \
               '\thour_skip: ' + str(self.hour_skip) + '\n' + \
               '- Strategy: ' + '\n' + \
               '\t' + '; '.join(self.strategy_set) + '\n' + \
               '- Data: ' + '\n' + \
               '\t' + str(self.cur_data_inst)

    def add(self, _data: DataStruct) -> bool:
        """
        add data into self.data

        :param _data: tick line data
        :return: whether to add a MarketEvent
        """

        if isinstance(self.data, SplitAbstract):
            # if use split data, return true when gen a new bar
            return self.data.addOne(_data)
        elif self.data is None:
            # then use tick data, and this is the first line
            self.data = _data
            return True
        elif isinstance(self.data, DataStruct):
            # use tick data, and add into datastruct
            self.data.merge(_data)
            return True
        else:
            # interesting.....
            raise Exception()


class MarketSupplyAbstract:
    def __init__(self, _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'):
        """
        base class market supply

        :param _engine: ref to backtest engine
        """

        # map market register's key to its object
        self.market_register_dict: typing.Dict[str, MarketRegister] = {}
        # map instrument to set of market register
        self.instrument_dict: typing.Dict[str, typing.Set[str]] = {}

        self.engine = _engine

    def addStrategy(self, _strategy: 'ParadoxTrading.Engine.Strategy.StrategyAbstract'):
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
            _strategy.market_register_dict[key] = self.market_register_dict[key]

    def addEvent(self, _instrument: str, _data: DataStruct):
        """
        add new tick data into market register, and add event

        :param _instrument:
        :param _data:
        :return:
        """
        for k in self.instrument_dict[_instrument]:
            # add tick data into market register
            if self.market_register_dict[k].add(_data):
                # add event for each strategy if necessary
                for strategy in self.market_register_dict[k].strategy_set:
                    self.engine.addEvent(MarketEvent(k, strategy))

    def getCurDatetime(self) -> datetime:
        raise NotImplementedError('getCurDatetime not implemented')

    def updateData(self) -> bool:
        raise NotImplementedError('updateData not implemented')


class DataGenerator:
    def __init__(
            self, _tradingday: str,
            _market_register_dict: typing.Dict[str, MarketRegister],
            _instrument_dict: typing.Dict[str, set]
    ):
        """
        fetch data according to market registers, and pop tick data by happentime

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
                _tradingday, v.product,
                v.instrument, v.sub_dominant
            )
            # whether inst exists
            if inst is not None:
                # set market register's cur inst
                v.cur_data_inst = inst

                # fetch data and set index to 0 init
                self.data_dict[inst] = Fetch.fetchIntraDayData(
                    _tradingday, _instrument=inst)
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

        # get latest tick of each instrument
        tmp = []
        for k, v in self.index_dict.items():
            d = self.data_dict[k]
            if v < len(d):
                tmp.append((k, d.index()[v]))

        if tmp:
            # get the latest market data of all
            tmp.sort(key=operator.itemgetter(1))

            inst = tmp[0][0]
            self.cur_datetime = tmp[0][1]  # set cur datetime to latest tick's happentime

            index = self.index_dict[inst]
            ret: typing.Tuple[str, DataStruct] = (inst, self.data_dict[inst].iloc[index])
            self.index_dict[inst] += 1  # point to next tick

            return ret
        else:
            # 1. data_dict and index_dict are empty
            # 2. all instruments reach the end
            return None


class BacktestMarketSupply(MarketSupplyAbstract):
    def __init__(
            self, _begin_day: str, _end_day: str,
            _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'
    ):
        """
        market supply for backtest

        :param _begin_day: begin date of backtest, like '20170123'
        :param _end_day: end date of backtest, like '20170131'
        :param _engine: ref to backtest engine
        """
        self.begin_day: str = _begin_day
        self.cur_day: str = self.begin_day
        self.end_day: str = _end_day

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

    def updateData(self) -> bool:
        """
        update data tick by tick

        :return: whether there is data
        """

        # reach end, so return false to end backtest
        if self.cur_day > self.end_day:
            return False

        # if there is no data generator, create one
        if self.data_generator is None:
            self.data_generator = DataGenerator(
                self.cur_day,
                self.market_register_dict,
                self.instrument_dict
            )

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
            self.addEvent(ret[0], ret[1])
            return True

    def getCurDatetime(self) -> typing.Union[None, datetime]:
        """
        :return: latest market happentime
        """
        if self.data_generator is not None:
            return self.data_generator.cur_datetime
        return None
