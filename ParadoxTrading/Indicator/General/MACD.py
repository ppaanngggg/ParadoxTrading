from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class MACD(IndicatorAbstract):
    def __init__(
            self,
            _fast_period: int = 12,
            _slow_period: int = 26,
            _macd_period: int = 9,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key=('value', 'avg', 'diff')
    ):
        super().__init__()

        self.fast_period = _fast_period
        self.slow_period = _slow_period
        self.macd_period = _macd_period

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.keys = [self.idx_key] + list(_ret_key)

        self.fast_value = None
        self.slow_value = None
        self.macd_avg = None

        self.data = DataStruct(
            self.keys, self.idx_key
        )

    def _addOne(self, _data: DataStruct):
        index_value = _data.index()[0]
        price = _data[self.use_key][0]

        if self.fast_value is None and self.slow_value is None \
                and self.macd_avg is None:
            self.fast_value = price
            self.slow_value = price
            macd_value = self.fast_value - self.slow_value
            self.macd_avg = macd_value
        else:
            self.fast_value = (price - self.fast_value) / self.fast_period + self.fast_value
            self.slow_value = (price - self.slow_value) / self.slow_period + self.slow_value
            macd_value = self.fast_value - self.slow_value
            self.macd_avg = (macd_value - self.macd_avg) / self.macd_period + self.macd_avg
        macd_diff = macd_value - self.macd_avg

        self.data.addRow(
            [index_value, macd_value, self.macd_avg, macd_diff],
            self.keys
        )
