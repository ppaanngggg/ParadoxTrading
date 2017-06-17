from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class MoveStopLoss(StopIndicatorAbstract):
    def __init__(
            self,
            _data: DataStruct,
            _stop_type: int,
            _stop_rate: float = 0.05,
            _use_key: str = 'closeprice',
            _idx_key: str = 'time',
            _ret_key: str = 'stopprice',
    ):
        super().__init__()

        assert len(_data) == 1

        self.stop_type = _stop_type
        self.stop_rate = _stop_rate
        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.best_price = _data.toDict()[self.use_key]

        time = _data.index()[0]

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key,
            [[time, self.get_stop_price()]]
        )

    def get_stop_price(self):
        if self.stop_type == SignalType.LONG:
            stop_price = self.best_price * (1 - self.stop_rate)
        elif self.stop_type == SignalType.SHORT:
            stop_price = self.best_price * (1 + self.stop_rate)
        else:
            raise Exception('unknown type')

        return stop_price

    def _addOne(self, _data_struct: DataStruct):
        price = _data_struct.toDict()[self.use_key]
        if self.stop_type == SignalType.LONG:
            self.best_price = max(price, self.best_price)
        elif self.stop_type == SignalType.SHORT:
            self.best_price = min(price, self.best_price)
        else:
            raise Exception('unknown type')
        time = _data_struct.index()[0]

        self.data.addDict({
            self.idx_key: time,
            self.ret_key: self.get_stop_price(),
        })

    def _isStop(self, _data_struct: DataStruct):
        price = _data_struct.toDict()[self.use_key]
        stop_price = self.data[self.ret_key][-1]
        if self.stop_type == SignalType.LONG:
            if price < stop_price:
                self.is_stop = True
        elif self.stop_type == SignalType.SHORT:
            if price > stop_price:
                self.is_stop = True
        else:
            raise Exception('unknown type')
