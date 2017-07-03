import math

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ReturnRate(IndicatorAbstract):
    def __init__(
            self, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'rate'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )
        self.last_value = None

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct.getColumn(self.use_key)[0]
        tmp_value = 0.0
        if self.last_value is not None:
            tmp_value = value / self.last_value - 1
        self.last_value = value
        self.data.addRow(
            [index, tmp_value],
            [self.idx_key, self.ret_key]
        )
