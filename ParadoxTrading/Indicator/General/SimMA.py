from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class SimMA(IndicatorAbstract):
    EMPTY = 0
    LONG = 1
    SHORT = 2

    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'simma'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.value = 1.0
        self.last_price = None
        self.last_status = self.EMPTY

        self.period = _period
        self.buf = deque(maxlen=self.period)

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]

        self.buf.append(price)
        ma_value = sum(self.buf) / len(self.buf)

        if self.last_status == self.LONG:
            self.value *= price / self.last_price
        if self.last_status == self.SHORT:
            self.value /= price / self.last_price

        self.data.addDict({
            self.idx_key: index,
            self.ret_key: self.value,
        })

        if price > ma_value:
            self.last_status = self.LONG
        elif price < ma_value:
            self.last_status = self.SHORT
        else:
            self.last_status = self.EMPTY
        self.last_price = price
