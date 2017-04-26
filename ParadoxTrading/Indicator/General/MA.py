from collections import deque
from datetime import datetime
import typing

import numpy as np

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class MA(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str,
            _idx_key: str = 'time', _ret_key: str = 'ma'
    ):
        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.period = _period
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _index: typing.Union[str, datetime], _data: DataStruct):
        assert len(_data) == 1
        self.buf.append(_data.getColumn(self.use_key)[0])
        tmp_value = np.mean(self.buf)
        self.data.addRow([_index, tmp_value], [self.idx_key, self.ret_key])
