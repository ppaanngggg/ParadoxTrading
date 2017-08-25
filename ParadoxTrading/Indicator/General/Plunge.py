from collections import deque

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class Plunge(IndicatorAbstract):
    def __init__(
            self,
            _fast_ema_period: int = 50,
            _slow_ema_period: int = 100,
            _atr_period: int = 20,
            _extreme_period: int = 20,
            _smooth_period: int = 1,
            _high_key: str = 'highprice',
            _low_key: str = 'lowprice',
            _close_key: str = 'closeprice',
            _idx_key: str = 'time', _ret_key: str = 'plunge'
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

        self.fast_ema_period = _fast_ema_period
        self.slow_ema_period = _slow_ema_period
        self.fast_ema_value = None
        self.slow_ema_value = None

        self.atr_buf = deque(maxlen=_atr_period)

        self.high_buf = deque(maxlen=_extreme_period)
        self.low_buf = deque(maxlen=_extreme_period)

        self.ret_buf = deque(maxlen=_smooth_period)

        self.last_close_price = None

    def _addOne(self, _data_struct: DataStruct):
        self.high_buf.append(_data_struct[self.high_key][0])
        self.low_buf.append(_data_struct[self.low_key][0])
        closeprice = _data_struct[self.close_key][0]
        if self.last_close_price is not None:
            index_value = _data_struct.index()[0]
            # atr
            tr_value = max(
                _data_struct[self.high_key][0], self.last_close_price
            ) - min(_data_struct[self.low_key][0], self.last_close_price)
            self.atr_buf.append(tr_value)
            atr_value = sum(self.atr_buf) / len(self.atr_buf)
            # ema
            self.fast_ema_value = (closeprice - self.fast_ema_value) / \
                                  self.fast_ema_period + self.fast_ema_value
            self.slow_ema_value = (closeprice - self.slow_ema_value) / \
                                  self.slow_ema_period + self.slow_ema_value
            # plunge
            if self.fast_ema_value > self.slow_ema_value:
                plunge_value = (max(self.high_buf) - closeprice) / atr_value
            elif self.fast_ema_value < self.slow_ema_value:
                plunge_value = (closeprice - min(self.low_buf)) / atr_value
            else:
                plunge_value = 0.0
            self.ret_buf.append(plunge_value)
            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: sum(self.ret_buf) / len(self.ret_buf),
            })
        else:
            self.fast_ema_value = closeprice
            self.slow_ema_value = closeprice
        self.last_close_price = closeprice
