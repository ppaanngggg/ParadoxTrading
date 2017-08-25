import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class KDJ(IndicatorAbstract):
    def __init__(
            self,
            _k_period: int = 20,
            _d_period: int = 3,
            _j_period: int = 3,
            _close_key: str = 'closeprice',
            _high_key: str = 'highprice',
            _low_key: str = 'lowprice',
            _idx_key: str = 'time',
            _ret_key=('k', 'd', 'j')
    ):
        super().__init__()

        self.k_period = _k_period
        self.d_period = _d_period
        self.j_period = _j_period

        self.close_key = _close_key
        self.high_key = _high_key
        self.low_key = _low_key

        self.idx_key = _idx_key
        self.keys = [self.idx_key] + list(_ret_key)

        self.high_buf = deque(maxlen=self.k_period)
        self.low_buf = deque(maxlen=self.k_period)
        self.k_buf = deque(maxlen=self.d_period)

        self.data = DataStruct(
            self.keys, self.idx_key
        )

    def _addOne(self, _data: DataStruct):
        index_value = _data.index()[0]
        closeprice = _data[self.close_key][0]
        highprice = _data[self.high_key][0]
        lowprice = _data[self.low_key][0]

        self.high_buf.append(highprice)
        self.low_buf.append(lowprice)

        high_mean = statistics.mean(self.high_buf)
        low_mean = statistics.mean(self.low_buf)
        k = 100 * (closeprice - high_mean) / (high_mean - low_mean)
        self.k_buf.append(k)
        d = statistics.mean(self.k_buf)
        j = self.j_period * k - (self.j_period - 1) * d

        self.data.addRow(
            [index_value, k, d, j],
            self.keys
        )
