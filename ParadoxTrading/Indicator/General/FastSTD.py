import math
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class FastSTD(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'std'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.sum_of_pow = 0.0
        self.mean = 0.0
        self.period = _period
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        value = _data_struct[self.use_key][0]
        index = _data_struct.index()[0]
        if len(self.buf) >= self.period:
            last_value = self.buf.popleft()
            self.buf.append(value)
            self.sum_of_pow += value ** 2
            self.sum_of_pow -= last_value ** 2
            self.mean += (value - last_value) / self.period
        else:
            n = len(self.buf)
            self.buf.append(value)
            self.sum_of_pow += value ** 2
            self.mean = (self.mean * n + value) / len(self.buf)

        self.data.addDict({
            self.idx_key: index,
            self.ret_key: math.sqrt(
                self.sum_of_pow / len(self.buf) - self.mean ** 2
            )
        })
