import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class STD(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'std'
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
        self.data.addDict({
            self.idx_key: index_value,
            self.ret_key: statistics.pstdev(self.buf),
        })
