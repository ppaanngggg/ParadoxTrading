import json
import typing
from collections import deque

from ParadoxTrading.Utils import (Fetch, SplitIntoHour, SplitIntoMinute,
                                  SplitIntoSecond)

from Event import MarketEvent


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
            '\t' + '; '.join(self.strategy_list)

    def add(self):
        pass


class MarketSupplyAbstract:

    def __init__(self):
        self.market_register_dict = {}  # typing.Dict[str, MarketRegister]
        self.instrument_dict = {}  # typing.Dict[str, typing.List[str]]

    def registerStrategy(self, _strategy_name: str, _market_register_key: str):
        if _market_register_key not in self.market_register_dict.keys():
            self.market_register_dict[_market_register_key] = \
                MarketRegister.fromJson(_market_register_key)
        self.market_register_dict[_market_register_key].addStrategy(
            _strategy_name)


class BacktestMarketSupply(MarketSupplyAbstract):

    class DataGenerator:

        def __init__(self, _keys: typing.List[str]):
            self.data_dict = {}  # typing.Dict[str, DataStruct]
            self.index_dict = {}  # typing.Dict[str, int]

        def gen(self):
            pass

    def __init__(self, _begin_day: str, _end_day: str):
        self.begin_day = _begin_day
        self.cur_day = self.begin_day
        self.end_day = _end_day

        self.data_generator = None  # DataGenerator

        super().__init__()

    def createDataGenerator(self) -> DataGenerator:
        for k, v in self.market_register_dict.items():
            inst = None
            if v.product is not None:
                if v.sub_dominant:
                    inst = Fetch.fetchSubDominant(v._product, self.cur_day)
                else:
                    inst = Fetch.fetchDominant(v._product, self.cur_day)
            else:
                inst =

    def updateData(self):
        if self.data_generator is None:
            self.data_generator = self.createDataGenerator()

        data = self.data_generator.gen()
        if data is None:
            self.data_generator = self.createDataGenerator()
