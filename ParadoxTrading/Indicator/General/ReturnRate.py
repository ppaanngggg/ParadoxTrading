import math
import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ReturnRate(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'returnrate'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )
        self.last_value = None
        self.period = _period
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct.getColumn(self.use_key)[0]
        if self.last_value is not None:
            chg_rate = value / self.last_value - 1
            self.buf.append(chg_rate)
            self.data.addDict({
                self.idx_key: index,
                self.ret_key: statistics.mean(self.buf)
            })
        self.last_value = value
