from collections import deque

import numpy as np

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class Volatility(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'volatility',
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
            chg_rate = np.log(price_value / self.last_price)
            self.buf.append(chg_rate)
            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: np.std(self.buf, ddof=1),
            })
        self.last_price = price_value
