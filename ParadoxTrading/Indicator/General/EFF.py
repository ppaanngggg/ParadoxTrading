from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class EFF(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'eff'
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

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        self.buf.append(_data_struct.getColumn(self.use_key)[0])
        if len(self.buf) == self.period:
            buf_list = list(self.buf)
            tmp = 0.0
            for a, b in zip(buf_list[:-1], buf_list[1:]):
                tmp += abs(b - a)
            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: (self.buf[-1] - self.buf[0]) / tmp,
            })
