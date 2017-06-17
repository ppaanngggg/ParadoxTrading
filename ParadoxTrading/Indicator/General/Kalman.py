from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class Kalman(IndicatorAbstract):
    def __init__(
            self, _use_key: str='closeprice',
            _R: float = 0.1 ** 2, _Q: float = 1e-5,
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

        self.R = _R
        self.Q = _Q

        self.xhat = 0.0
        self.P = 1.0

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]

        xhatminus = self.xhat
        pminus = self.P + self.Q
        k = pminus / (pminus + self.R)
        self.xhat = xhatminus + k * (value - xhatminus)
        self.P = (1 - k) * pminus

        self.data.addRow(
            [index, self.xhat],
            [self.idx_key, self.ret_key]
        )
