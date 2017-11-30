import math
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class LogReturn(IndicatorAbstract):
    def __init__(
            self,
            _skip_period: int = 1,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'logreturn'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.skip_period = _skip_period
        self.buf = deque(maxlen=self.skip_period)

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]
        if len(self.buf) >= self.skip_period:
            last_value = self.buf.popleft()
            chg_rate = math.log(value / last_value)
            self.data.addDict({
                self.idx_key: index,
                self.ret_key: chg_rate,
            })
        self.buf.append(value)
