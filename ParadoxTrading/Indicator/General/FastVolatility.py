import math
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class FastVolatility(IndicatorAbstract):
    def __init__(
            self, _period: int,
            _factor: int = 1,
            _smooth: int = 1,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'volatility',
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.last_price = None
        self.period = _period
        self.factor = math.sqrt(_factor)
        self.smooth = _smooth

        self.buf = deque(maxlen=self.period)
        self.mean = 0.0
        self.sum_of_pow = 0.0

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        price_value = _data_struct[self.use_key][0]
        if self.last_price is not None:
            chg_rate = price_value / self.last_price - 1

            if len(self.buf) >= self.period:
                last_value = self.buf.popleft()
                self.buf.append(chg_rate)
                self.sum_of_pow += chg_rate ** 2
                self.sum_of_pow -= last_value ** 2
                self.mean += (chg_rate - last_value) / self.period
            else:
                n = len(self.buf)
                self.buf.append(chg_rate)
                self.sum_of_pow += chg_rate ** 2
                self.mean = (self.mean * n + chg_rate) / len(self.buf)

            std_value = math.sqrt(
                self.sum_of_pow / len(self.buf) - self.mean ** 2
            )
            if self.smooth > 1 and len(self.data):
                last_std_value = self.data[self.ret_key][-1]
                std_value = (
                    (self.smooth - 1) * last_std_value + std_value
                ) / self.smooth

            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: std_value,
            })
        self.last_price = price_value
