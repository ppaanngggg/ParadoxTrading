from collections import deque

import numpy as np

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class MA(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str,
            _idx_key: str = 'time', _ret_key: str = 'ma'
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
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data: DataStruct):
        index_value = _data.index()[0]
        self.buf.append(_data.getColumn(self.use_key)[0])
        tmp_value = np.mean(self.buf)
        self.data.addRow([index_value, tmp_value], [self.idx_key, self.ret_key])
