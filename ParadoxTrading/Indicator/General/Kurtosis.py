import math
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct
from scipy.stats import kurtosis


class Kurtosis(IndicatorAbstract):

    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'kurtosis'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.period = _period
        self.last_price = None
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]
        if self.last_price:
            self.buf.append(math.log(price / self.last_price))
            if len(self.buf) >= self.period:
                self.data.addDict({
                    self.idx_key: index,
                    self.ret_key: kurtosis(self.buf),
                })
        self.last_price = price
