from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ReturnRate(IndicatorAbstract):
    def __init__(
            self, _period: int, _use_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'returnrate'
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
        self.last_rate = None
        self.period = _period

    def _addOne(self, _data_struct: DataStruct):
        index = _data_struct.index()[0]
        value = _data_struct[self.use_key][0]
        if self.last_value is not None:
            chg_rate = value / self.last_value - 1
            if self.last_rate is None:
                self.last_rate = chg_rate
            else:
                self.last_rate = (chg_rate - self.last_rate) / \
                                 self.period + self.last_rate
            self.data.addDict({
                self.idx_key: index,
                self.ret_key: self.last_rate
            })
        self.last_value = value
