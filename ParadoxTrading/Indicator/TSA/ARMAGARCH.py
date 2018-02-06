import math
import typing

from TorchTSA.model import ARMAIGARCHModel

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ARMAGARCH(IndicatorAbstract):

    def __init__(
            self,
            _fit_period: int = 60,
            _fit_begin: int = 252,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: typing.Tuple[str] = ('mean', 'std'),
    ):
        super().__init__()

        # fitting control
        self.fit_count = 0
        self.fit_period = _fit_period
        self.fit_begin = _fit_begin

        self.model = ARMAIGARCHModel(_use_mu=False)

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key[0], self.ret_key[1]],
            self.idx_key
        )
        # log return buf
        self.last_price = None
        self.return_buf = []
        # model params
        self.phi = None
        self.theta = None
        self.alpha = None
        self.beta = None
        self.const = None
        self.new_mean = None
        self.new_info = None
        self.new_var = None

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]

        if self.last_price is not None:
            rate = math.log(price) - math.log(self.last_price)
            if self.new_mean is not None:
                self.new_info = rate - self.new_mean
            self.return_buf.append(rate)

            self.fit_count += 1  # retrain model
            if self.fit_count > self.fit_period and \
                    len(self.return_buf) >= self.fit_begin:
                self.model.fit(self.return_buf)
                self.phi = self.model.getPhis()[0]
                self.theta = self.model.getThetas()[0]
                self.alpha = self.model.getAlphas()[0]
                self.beta = self.model.getBetas()[0]
                self.const = self.model.getConst()[0]
                self.new_info = self.model.latent_arma_arr[-1]
                self.new_var = self.model.latent_garch_arr[-1]
                self.fit_count = 0

            if self.new_info is not None:  # predict value
                self.new_mean = self.phi * rate + self.theta * self.new_info
                self.new_var = self.alpha * self.new_info ** 2 + \
                               self.beta * self.new_var + self.const
                self.data.addDict({
                    self.idx_key: index,
                    self.ret_key[0]: self.new_mean,
                    self.ret_key[1]: math.sqrt(self.new_var),
                })

        self.last_price = price
