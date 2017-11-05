import math
from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct
from collections import deque


class FastBBands(IndicatorAbstract):
    def __init__(
        self, _period: int = 26, _use_key: str = 'closeprice',
        _rate: float = 2.0, _idx_key: str = 'time',
        _ret_key=('upband', 'midband', 'downband')
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.keys = [self.idx_key] + list(_ret_key)

        self.data = DataStruct(
            self.keys, self.idx_key
        )

        self.period = _period
        self.rate = _rate
        self.buf = deque(maxlen=self.period)

        self.mean = 0.0
        self.sum_of_pow = 0.0

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

        std = math.sqrt(
            self.sum_of_pow / len(self.buf) - self.mean ** 2
        )
        self.data.addRow([
            index, self.mean + self.rate * std,
            self.mean, self.mean - self.rate * std
        ], self.keys)
