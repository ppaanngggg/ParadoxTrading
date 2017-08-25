import statistics
from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class RSI(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'rsi'
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
        self.gain_buf = deque(maxlen=self.period)
        self.loss_buf = deque(maxlen=self.period)

        self.last_price = None

    def _addOne(self, _data_struct: DataStruct):
        price = _data_struct[self.use_key][0]
        if self.last_price is not None:
            price_diff = price - self.last_price
            if price_diff >= 0:
                self.gain_buf.append(price_diff)
                self.loss_buf.append(0)
            else:
                self.gain_buf.append(0)
                self.loss_buf.append(-price_diff)

            index_value = _data_struct.index()[0]
            gain_mean = statistics.mean(self.gain_buf)
            loss_mean = max(statistics.mean(self.loss_buf), 0.01)
            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: 100 - 100 / (1 + gain_mean / loss_mean),
            })

        self.last_price = price
