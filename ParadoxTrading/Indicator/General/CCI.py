import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class CCI(IndicatorAbstract):
    """
    rolling ma
    """

    def __init__(
            self, _period: int, _constant: float = 0.15,
            _close_key: str = 'closeprice',
            _high_key: str = 'highprice',
            _low_key: str = 'lowprice',
            _idx_key: str = 'time', _ret_key: str = 'cci',
    ):
        super().__init__()

        self.close_key = _close_key
        self.high_key = _high_key
        self.low_key = _low_key

        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.period = _period
        self.constant = _constant
        self.tp_buf = deque(maxlen=self.period)
        self.dev_buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        close_price = _data_struct[self.close_key][0]
        high_price = _data_struct[self.high_key][0]
        low_price = _data_struct[self.low_key][0]

        tp = (close_price + high_price + low_price) / 3
        if len(self.tp_buf) == 0:
            dev = high_price - low_price
        else:
            dev = abs(tp - self.tp_buf[-1])
        self.tp_buf.append(tp)
        self.dev_buf.append(dev)

        self.data.addDict({
            self.idx_key: index_value,
            self.ret_key: (tp - statistics.mean(self.tp_buf)) / (
                self.constant * statistics.mean(self.dev_buf)
            ),
        })
