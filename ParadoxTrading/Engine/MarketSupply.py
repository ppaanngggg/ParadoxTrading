import json
import operator
import typing
from collections import deque
from datetime import datetime, timedelta

from ParadoxTrading.Engine.Event import MarketEvent
from ParadoxTrading.Utils import (DataStruct, Fetch, SplitIntoHour,
                                  SplitIntoMinute, SplitIntoSecond)
from ParadoxTrading.Utils.Split import SplitAbstract


class MarketRegister:

    def __init__(
        self, _product: str=None, _instrument: str=None, _sub_dominant: bool=False,
        _second_skip: int=0, _minute_skip: int =0, _hour_skip: int =0
    ):
        assert _product is not None or _instrument is not None

        self.product = _product
        self.instrument = _instrument
        self.sub_dominant = _sub_dominant

        self.second_skip = _second_skip
        self.minute_skip = _minute_skip
        self.hour_skip = _hour_skip

        self.strategy_list = set()

        self.cur_data_inst = None
        self.data = None
        if self.second_skip > 0:
            self.data = SplitIntoSecond(self.second_skip)
        elif self.minute_skip > 0:
            self.data = SplitIntoMinute(self.minute_skip)
        elif self.hour_skip > 0:
            self.data = SplitIntoHour(self.hour_skip)
        else:
            pass

    def addStrategy(self, _strategy_name: str):
        self.strategy_list.add(_strategy_name)

    def toJson(self) -> str:
        return json.dumps((
            ('product', self.product),
            ('instrument', self.instrument),
            ('sub_dominant', self.sub_dominant),
            ('second_skip', self.second_skip),
            ('minute_skip', self.minute_skip),
            ('hour_skip', self.hour_skip),
        ))

    def fromJson(_json_str: str) -> 'MarketRegister':
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
            '\t' + '; '.join(self.strategy_list) + '\n' + \
            '- Data: ' + '\n' + \
            '\t' + self.cur_data_inst

    def add(self, _data: DataStruct) -> bool:
        if isinstance(self.data, SplitAbstract):
            return self.data.addOne(_data)
        elif self.data is None:
            self.data = _data
            return True
        elif isinstance(self.data, DataStruct):
            self.data.merge(_data)
            return True
        else:
            raise Exception()


class MarketSupplyAbstract:

    def __init__(self, _event_queue: deque):
        self.market_register_dict = {}  # typing.Dict[str, MarketRegister]
        self.instrument_dict = {}  # typing.Dict[str, set]

        self.event_queue = _event_queue

    def registerStrategy(self, _strategy_name: str, _market_register_key: str):
        if _market_register_key not in self.market_register_dict.keys():
            self.market_register_dict[_market_register_key] = \
                MarketRegister.fromJson(_market_register_key)
        self.market_register_dict[_market_register_key].addStrategy(
            _strategy_name)

    def addEvent(self, _instrument: str, _data: DataStruct):
        for k in self.instrument_dict[_instrument]:
            if (self.market_register_dict[k].add(_data)):
                for strategy in self.market_register_dict[k].strategy_list:
                    self.event_queue.append(MarketEvent(k, strategy))

    def getCurDatetime(self) ->datetime:
        raise NotImplementedError('getCurDatetime not implemented')

    def updateData(self) -> bool:
        raise NotImplementedError('updateData not implemented')


class DataGenerator:

    def __init__(
        self, _tradingday: str,
        _market_register_dict: typing.Dict[str, MarketRegister],
        _instrument_dict: typing.Dict[str, set]
    ):
        self.data_dict = {}  # typing.Dict[str, DataStruct]
        self.index_dict = {}  # typing.Dict[str, int]
        self.cur_datetime = None  # datetime

        _instrument_dict.clear()

        for k, v in _market_register_dict.items():
            inst = Fetch._fetchInstrument(
                _tradingday, v.product,
                v.instrument, v.sub_dominant
            )
            if inst is not None:
                v.cur_data_inst = inst
                self.data_dict[inst] = Fetch.fetchIntraDayData(
                    _tradingday, _instrument=inst)
                self.index_dict[inst] = 0
                try:
                    _instrument_dict[inst].add(k)
                except KeyError:
                    _instrument_dict[inst] = set([k])

    def gen(self) -> (str, DataStruct):
        tmp = []
        for k, v in self.index_dict.items():
            d = self.data_dict[k]
            if v < len(d):
                tmp.append((k, d.index()[v]))
        if tmp:
            tmp.sort(key=operator.itemgetter(1))
            inst = tmp[0][0]
            self.cur_datetime = tmp[0][1]
            index = self.index_dict[inst]
            ret = (inst, self.data_dict[inst].iloc[index])
            self.index_dict[inst] += 1
            return ret
        else:
            return None


class BacktestMarketSupply(MarketSupplyAbstract):

    def __init__(self, _begin_day: str, _end_day: str, _event_queue: deque):
        self.begin_day = _begin_day
        self.cur_day = self.begin_day
        self.end_day = _end_day

        self.data_generator = None  # DataGenerator

        super().__init__(_event_queue)

    def incDate(self):
        tmp = datetime.strptime(self.cur_day, '%Y%m%d')
        tmp += timedelta(days=1)
        self.cur_day = tmp.strftime('%Y%m%d')

    def updateData(self) -> bool:
        if self.cur_day > self.end_day:
            return False
        if self.data_generator is None:
            self.data_generator = DataGenerator(
                self.cur_day,
                self.market_register_dict,
                self.instrument_dict
            )
        ret = self.data_generator.gen()
        if ret is None:
            self.incDate()
            self.data_generator = None
            return self.updateData()
        else:
            self.addEvent(ret[0], ret[1])
            return True

    def getCurDatetime(self) -> datetime:
        if self.data_generator is not None:
            return self.data_generator.cur_datetime
        return None
