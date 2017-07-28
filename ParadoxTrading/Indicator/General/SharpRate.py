import math
import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class SharpRate(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'sharprate',
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
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        price_value = _data_struct[self.use_key][0]
        if self.last_price is not None:
            chg_rate = math.log(price_value / self.last_price)
            self.buf.append(chg_rate)
            buf_std = statistics.pstdev(self.buf)
            if buf_std != 0:
                self.data.addDict({
                    self.idx_key: index_value,
                    self.ret_key: statistics.mean(self.buf) / buf_std,
                })
        self.last_price = price_value
