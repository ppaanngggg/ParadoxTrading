from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class RateConstStop(StopIndicatorAbstract):
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

        price = _data[self.use_key][0]
        if self.stop_type == SignalType.LONG:
            stop_price = price * (1 - self.stop_rate)
        elif self.stop_type == SignalType.SHORT:
            stop_price = price * (1 + self.stop_rate)
        else:
            raise Exception('unknown type')
        time = _data.index()[0]

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key,
            [[time, stop_price]]
        )

    def _addOne(self, _data_struct: DataStruct):
        stop_price = self.data[self.ret_key][-1]
        time = _data_struct.index()[0]

        self.data.addDict({
            self.idx_key: time,
            self.ret_key: stop_price,
        })

    def _isStop(self, _data_struct: DataStruct):
        price = _data_struct[self.use_key][0]
        stop_price = self.data[self.ret_key][-1]
        if self.stop_type == SignalType.LONG:
            if price < stop_price:
                self.is_stop = True
        elif self.stop_type == SignalType.SHORT:
            if price > stop_price:
                self.is_stop = True
        else:
            raise Exception('unknown type')
