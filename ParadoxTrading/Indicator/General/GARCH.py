import math

import numpy as np
from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct
from arch import arch_model


class GARCH(IndicatorAbstract):

    def __init__(
            self,
            _fit_period: int = 60,
            _fit_length: int = 500,
            _smooth_period: int = 1,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'predict'
    ):
        super().__init__()

        self.fit_count = 0
        self.fit_period = _fit_period
        self.fit_length = _fit_length
        self.smooth_period = _smooth_period

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.last_price = None
        self.rate_buf = []
        self.param = None
        self.sigma2 = None

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]

        if self.last_price is not None:
            rate = math.log(price / self.last_price)
            self.rate_buf.append(rate)

            self.fit_count += 1
            if self.fit_count > self.fit_period:
                rate_arr = np.array(self.rate_buf[-self.fit_length:])
                am = arch_model(rate_arr, mean='Zero')
                res = am.fit(disp='off', show_warning=False)
                self.param = res.params.values
                self.sigma2 = res.conditional_volatility[-1] ** 2
                self.fit_count = 0

            if self.param is not None:
                self.sigma2 = self.param[0] + \
                              self.param[1] * rate * rate + \
                              self.param[2] * self.sigma2
                tmp = math.sqrt(self.sigma2)
                if self.smooth_period > 1 and len(self.data): # smooth
                    last_value = self.data[self.ret_key][-1]
                    tmp = (tmp - last_value) / self.smooth_period + last_value
                self.data.addDict({
                    self.idx_key: index,
                    self.ret_key: tmp,
                })

        self.last_price = price
