import typing
from datetime import datetime

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ZigZag(IndicatorAbstract):
    UNKNOWN = 0
    UP = 1
    DOWN = 2

    def __init__(
            self, _threshold: float,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'zigzag'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.threshold = _threshold
        self.status = ZigZag.UNKNOWN

        self.high_time: typing.Union[str, datetime] = None
        self.high_price: float = None
        self.low_time: typing.Union[str, datetime] = None
        self.low_price: float = None

    def _addOne(self, _data_struct: DataStruct):
        time: typing.Union[str, datetime] = _data_struct.index()[0]
        price: float = _data_struct[self.use_key][0]

        if self.status == ZigZag.UNKNOWN:
            # update high and low point
            if self.high_price is None or price > self.high_price:
                self.high_time = time
                self.high_price = price
            if self.low_price is None or price < self.low_price:
                self.low_time = time
                self.low_price = price
            # if change status
            if (price - self.low_price) > self.threshold * self.low_price:
                self.status = ZigZag.UP
                self.data.addDict({
                    self.idx_key: self.low_time,
                    self.ret_key: self.low_price,
                })
                # reset high
                self.high_time = time
                self.high_price = price
                # clear low
                self.low_time = None
                self.low_price = None
            if (self.high_price - price) > self.threshold * self.high_price:
                self.status = ZigZag.DOWN
                self.data.addDict({
                    self.idx_key: self.high_time,
                    self.ret_key: self.high_price,
                })
                # reset low
                self.low_time = time
                self.low_price = price
                # clear high
                self.high_time = None
                self.high_price = None
        elif self.status == ZigZag.UP:
            if price > self.high_price:
                self.high_time = time
                self.high_price = price
            if (self.high_price - price) > self.threshold * self.high_price:
                self.status = ZigZag.DOWN
                self.data.addDict({
                    self.idx_key: self.high_time,
                    self.ret_key: self.high_price,
                })
                # reset low
                self.low_time = time
                self.low_price = price
                # clear high
                self.high_time = None
                self.high_price = None
        elif self.status == ZigZag.DOWN:
            if price < self.low_price:
                self.low_time = time
                self.low_price = price
            if (price - self.low_price) > self.threshold * self.low_price:
                self.status = ZigZag.UP
                self.data.addDict({
                    self.idx_key: self.low_time,
                    self.ret_key: self.low_price,
                })
                # reset high
                self.high_time = time
                self.high_price = price
                # clear low
                self.low_time = None
                self.low_price = None
        else:
            raise Exception('unknown status')
