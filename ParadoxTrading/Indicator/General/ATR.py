from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ATR(IndicatorAbstract):
    def __init__(
            self, _period: int,
            _high_key: str = 'highprice',
            _low_key: str = 'lowprice',
            _close_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'atr'
    ):
        super().__init__()

        self.high_key = _high_key
        self.low_key = _low_key
        self.close_key = _close_key

        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.period = _period

        self.last_atr = None
        self.last_close_price = None

    def _addOne(self, _data_struct: DataStruct):
        if self.last_close_price is not None:
            index_value = _data_struct.index()[0]
            tr_value = max(
                _data_struct[self.high_key][0], self.last_close_price
            ) - min(
                _data_struct[self.low_key][0], self.last_close_price
            )
            if self.last_atr is None:
                self.last_atr = tr_value
            else:
                self.last_atr = (tr_value - self.last_atr) / self.period + self.last_atr
            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: self.last_atr,
            })
        self.last_close_price = _data_struct[self.close_key][0]
