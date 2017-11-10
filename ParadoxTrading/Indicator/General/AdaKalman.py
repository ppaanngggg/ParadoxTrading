from ParadoxTrading.Indicator.General.FastSTD import FastSTD
from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class AdaKalman(IndicatorAbstract):
    def __init__(
        self, _ada_period: int=30,
        _init_x: float = 0.0, _init_P: float =1.0,
        _init_R: float = 0.1 ** 2, _init_Q: float = 0.01 ** 2,
        _use_key: str = 'closeprice',
        _idx_key: str = 'time', _ret_key: str = 'kalman'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.ada_period = _ada_period
        self.value_std = FastSTD(_ada_period, _use_key=self.use_key)
        self.x_std = FastSTD(_ada_period, _use_key='x')

        self.R = _init_R
        self.Q = _init_Q

        self.x = _init_x
        self.P = _init_P

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]
        self.value_std.addOne(_data_struct)

        if len(self.value_std) > 1:
            self.R = self.value_std.getLastData()['std'][0] ** 2
        if len(self.x_std) > 1:
            self.Q = self.x_std.getLastData()['std'][0] ** 2

        # predict
        # self.x += 0.0  # x assume not changed
        self.P += self.Q

        # update
        k = self.P / (self.P + self.R)
        x_diff_value = k * (value - self.x)
        self.x += x_diff_value
        self.P = (1 - k) * self.P

        self.data.addDict({
            self.idx_key: index,
            self.ret_key: self.x
        })
