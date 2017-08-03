import statistics

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class AdaBBands(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str,
            _init_n: int = 20, _min_n: int = 20, _max_n: int = 60,
            _rate: float = 2.0, _idx_key: str = 'time'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.keys = [self.idx_key, 'upband', 'midband', 'downband']

        self.data = DataStruct(
            self.keys, self.idx_key
        )

        self.period = _period
        self.rate = _rate
        self.buf = []

        self.prev_std = None

        self.dynamic_n = float(_init_n)
        self.min_n = _min_n
        self.max_n = _max_n

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        self.buf.append(_data_struct.getColumn(self.use_key)[0])

        if len(self.data) > self.period:
            const_std = statistics.pstdev(self.buf[-self.period:])
            self.dynamic_n *= const_std / self.prev_std
            self.dynamic_n = max(self.min_n, self.dynamic_n)
            self.dynamic_n = min(self.max_n, self.dynamic_n)
            tmp_n = int(round(self.dynamic_n))

            mean = statistics.mean(self.buf[-tmp_n:])
            std = statistics.pstdev(self.buf[-tmp_n:])

            self.data.addRow(
                [index_value, mean + self.rate * std,
                 mean, mean - self.rate * std],
                self.keys
            )

            self.prev_std = const_std
        else:
            if len(self.data) == self.period:
                self.prev_std = statistics.pstdev(self.buf)

            self.data.addRow(
                [index_value, None, None, None],
                self.keys
            )
