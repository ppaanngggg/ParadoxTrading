import typing

from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class StepDrawdownStop(StopIndicatorAbstract):
    def __init__(
            self,
            _data: DataStruct,
            _stop_type: SignalType.LONG,
            _profit_thresh=(0.1, 0.2, 0.3, 0.5),
            _stop_thresh=(1.0, 0.5, 0.3, 0.15),
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'stopprice',
    ):
        super().__init__()

        self.stop_type = _stop_type
        self.status = 0

        self.profit_thresh = _profit_thresh
        self.stop_thresh = _stop_thresh
        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.init_price = _data.toDict()[self.use_key]
        self.best_price = self.init_price

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key,
            [[_data.index()[0], self.init_price]]
        )

    def _set_best_price(self, _price):
        if self.stop_type == SignalType.LONG:
            self.best_price = max(self.best_price, _price)
        elif self.stop_type == SignalType.SHORT:
            self.best_price = min(self.best_price, _price)
        else:
            raise Exception('unknown type')

    def _profit_rate(self, _price) -> float:
        if self.stop_type == SignalType.LONG:
            return _price / self.init_price - 1.0
        elif self.stop_type == SignalType.SHORT:
            return 1.0 - _price / self.init_price
        else:
            raise Exception('unknown type')

    def _update_status(self, _profit_rate):
        while True:
            if self.status < len(self.profit_thresh):
                if _profit_rate > self.profit_thresh[self.status]:
                    self.status += 1
                else:
                    return
            else:
                return

    def _calc_stop_price(self) -> typing.Union[None, float]:
        if self.status > 0:
            if self.stop_type == SignalType.LONG:
                return self.init_price + \
                       (1.0 - self.stop_thresh[self.status - 1]) * \
                       (self.best_price - self.init_price)
            elif self.stop_type == SignalType.SHORT:
                return self.init_price - \
                       (1.0 - self.stop_thresh[self.status - 1]) * \
                       (self.init_price - self.best_price)
            else:
                raise Exception('unknown type')
        else:
            return self.init_price

    def _addOne(self, _data_struct: DataStruct):
        time = _data_struct.index()[0]
        price = _data_struct[self.use_key][0]

        self._set_best_price(price)

        profit_rate = self._profit_rate(price)
        self._update_status(profit_rate)

        self.data.addDict({
            self.idx_key: time,
            self.ret_key: self._calc_stop_price(),
        })

    def _isStop(self, _data_struct: DataStruct):
        price = _data_struct.toDict()[self.use_key]
        stop_price = self.data[self.ret_key][-1]
        if not self.status:
            return
        if self.stop_type == SignalType.LONG:
            if price < stop_price:
                self.is_stop = True
        elif self.stop_type == SignalType.SHORT:
            if price > stop_price:
                self.is_stop = True
        else:
            raise Exception('unknown type')
