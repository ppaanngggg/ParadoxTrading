from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class Kalman(IndicatorAbstract):
    def __init__(
            self, _init_x: float = 0.0, _init_P: float = 1.0,
            _R: float = 0.1 ** 2, _Q: float = 0.01 ** 2,
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

        self.x = _init_x
        self.P = _init_P

        self.R = _R
        self.Q = _Q

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]

        self.P += self.Q
        k = self.P / (self.P + self.R)
        self.x += k * (value - self.x)
        self.P = (1 - k) * self.P

        self.data.addDict({
            self.idx_key: index,
            self.ret_key: self.x
        })
