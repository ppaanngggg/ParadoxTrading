from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ReturnRate(IndicatorAbstract):
    def __init__(
            self,
            _smooth_period: int = 1,
            _skip_period: int = 1,
            _use_abs: bool=False,
            _use_percent: bool=False,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'returnrate'
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
        self.smooth_period = _smooth_period
        self.buf = deque(maxlen=self.skip_period)
        self.last_rate = None

        self.use_abs = _use_abs
        self.use_percent = _use_percent

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]
        if len(self.buf) >= self.skip_period:
            last_value = self.buf.popleft()
            chg_rate = value / last_value - 1
            if self.use_abs:
                chg_rate = abs(chg_rate)
            if self.use_percent:
                chg_rate *= 100.0
            if self.last_rate is None:
                self.last_rate = chg_rate
            else:
                self.last_rate = (chg_rate - self.last_rate) / \
                    self.smooth_period + self.last_rate
            self.data.addDict({
                self.idx_key: index,
                self.ret_key: self.last_rate
            })
        self.buf.append(value)
