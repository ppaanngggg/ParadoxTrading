from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class SAR(IndicatorAbstract):
    BEGIN = 0
    RISING = 1
    FALLING = -1

    def __init__(
            self,
            _init_step: float = 0.02,
            _max_step: float = 0.2,
            _close_key: str = 'closeprice',
            _high_key: str = 'highprice',
            _low_key: str = 'lowprice',
            _idx_key: str = 'time',
            _ret_key: str = 'sar',
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

        self.init_step = _init_step
        self.max_step = _max_step

        self.status = self.BEGIN
        self.ep = None
        self.step = None
        self.sar = None

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        close_price = _data_struct[self.close_key][0]
        high_price = _data_struct[self.high_key][0]
        low_price = _data_struct[self.low_key][0]

        if self.status == self.RISING:  # last status if rising
            if low_price < self.sar:  # if break sar, change status
                self.status = self.FALLING
                self.sar = self.ep  # new sar if the ep of last status
                self.ep = low_price
                self.step = self.init_step
            else:  # else continue rising
                self.sar += self.step * (self.ep - self.sar)
                if high_price > self.ep:
                    self.ep = high_price
                    self.step = min(self.max_step, self.step + self.init_step)
        elif self.status == self.FALLING:
            if high_price > self.sar:  # if break sar, change status
                self.status = self.RISING
                self.sar = self.ep
                self.ep = high_price
                self.step = self.init_step
            else:  # else continue falling
                self.sar -= self.step * (self.sar - self.ep)
                if low_price < self.ep:
                    self.ep = low_price
                    self.step = min(self.max_step, self.step + self.init_step)
        else:  # just begin
            if close_price >= (high_price + low_price) / 2:
                # close price greater than mid of high and low
                self.status = self.RISING
                self.ep = high_price
                self.step = self.init_step
                self.sar = low_price
            else:
                self.status = self.FALLING
                self.ep = low_price
                self.step = self.init_step
                self.sar = high_price

        self.data.addDict({
            self.idx_key: index_value,
            self.ret_key: self.sar,
        })
