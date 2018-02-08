import math
import typing

import numpy as np
from arch import arch_model

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class GARCH(IndicatorAbstract):

    def __init__(
            self,
            _fit_period: int = 60,
            _fit_begin: int = 252,
            _factor: int = 1,
            _smooth_period: int = 1,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: typing.Tuple[str] = ('estimate', 'predict')
    ):
        super().__init__()

        self.fit_count = 0
        self.fit_period = _fit_period
        self.fit_begin = _fit_begin
        self.factor = math.sqrt(_factor)
        self.smooth_period = _smooth_period

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key[0], self.ret_key[1]],
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
            if self.fit_count > self.fit_period and \
                    len(self.rate_buf) >= self.fit_begin:
                # retrain model and reset sigma2
                rate_arr = np.array(self.rate_buf)
                am = arch_model(rate_arr, mean='Zero')
                res = am.fit(disp='off', show_warning=False)
                # input(res.summary())
                self.param = res.params.values
                self.sigma2 = res.conditional_volatility[-1] ** 2
                self.fit_count = 0

            if self.param is not None:
                estimate = math.sqrt(self.sigma2) * self.factor
                self.sigma2 = self.param[0] + \
                              self.param[1] * rate * rate + \
                              self.param[2] * self.sigma2
                predict = math.sqrt(self.sigma2)
                predict *= self.factor
                if self.smooth_period > 1 and len(self.data):  # smooth
                    last_value = self.data[self.ret_key[1]][-1]
                    predict = (predict - last_value) / \
                              self.smooth_period + last_value
                self.data.addDict({
                    self.idx_key: index,
                    self.ret_key[0]: estimate,
                    self.ret_key[1]: predict,
                })

        self.last_price = price
