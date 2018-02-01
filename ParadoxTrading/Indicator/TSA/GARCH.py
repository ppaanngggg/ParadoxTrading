import math

from TorchTSA.model import IGARCHModel

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
            _ret_key: str = 'predict',
    ):
        super().__init__()

        # fitting control
        self.fit_count = 0
        self.fit_period = _fit_period
        self.fit_begin = _fit_begin
        # scale the volatility
        self.factor = math.sqrt(_factor)
        self.smooth_period = _smooth_period

        self.model = IGARCHModel(_use_mu=False)

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )
        # log return buf
        self.last_price = None
        self.return_buf = []
        # model params
        self.alpha = None
        self.beta = None
        self.const = None
        self.latent = None

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]

        if self.last_price is not None:
            rate = math.log(price) - math.log(self.last_price)
            self.return_buf.append(rate)

            self.fit_count += 1
            if self.fit_count > self.fit_period and \
                    len(self.return_buf) >= self.fit_begin:
                # retrain model and reset sigma2
                self.model.fit(self.return_buf)
                # print(
                #     self.model.getAlphas(), self.model.getBetas(),
                #     self.model.getConst(), self.model.getMu()
                # )
                self.alpha = self.model.getAlphas()[0]
                self.beta = self.model.getBetas()[0]
                self.const = self.model.getConst()[0]
                self.latent = self.model.latent_arr[-1]
                self.fit_count = 0

            if self.latent is not None:
                self.latent = self.alpha * rate * rate + \
                    self.beta * self.latent + self.const
                predict = math.sqrt(self.latent)
                predict *= self.factor
                if self.smooth_period > 1 and len(self.data):  # smooth
                    last_value = self.data[self.ret_key][-1]
                    predict = (predict - last_value) / \
                        self.smooth_period + last_value
                self.data.addDict({
                    self.idx_key: index,
                    self.ret_key: predict,
                })

        self.last_price = price
