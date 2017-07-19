from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class EMA(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str,
            _idx_key: str = 'time', _ret_key: str = 'ema'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.period = _period

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        tmp_value = _data_struct[self.use_key][0]
        if len(self) > 0:
            last_ret = self.getLastData().toDict()[self.ret_key]
            tmp_value = (tmp_value - last_ret) / self.period + last_ret
        self.data.addDict({
            self.idx_key: index_value,
            self.ret_key: tmp_value,
        })
