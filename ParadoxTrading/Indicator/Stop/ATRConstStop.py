from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ATRConstStop(StopIndicatorAbstract):
    def __init__(
            self,
            _price_data: DataStruct,
            _atr_data: DataStruct,
            _stop_type: int,
            _rate: float = 3,
            _price_use_key: str = 'closeprice',
            _atr_use_key: str = 'atr',
            _idx_key: str = 'time',
            _ret_key: str = 'stopprice',
    ):
        super().__init__()

        assert len(_price_data) == 1
        assert len(_atr_data) == 1

        self.stop_type = _stop_type
        self.rate = _rate
        self.price_use_key = _price_use_key
        self.atr_use_key = _atr_use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key,
        )

        price_value = _price_data[self.price_use_key][0]
        atr_value = _atr_data[self.atr_use_key][0]

        if self.stop_type == SignalType.LONG:
            self.stopprice = price_value - self.rate * atr_value
        elif self.stop_type == SignalType.SHORT:
            self.stopprice = price_value + self.rate * atr_value
        else:
            raise Exception('unknown type')

        self._addOne(_price_data)

    def _addOne(
            self, _price_data: DataStruct,
    ):
        self.data.addDict({
            self.idx_key: _price_data.index()[0],
            self.ret_key: self.stopprice,
        })

    def _isStop(self, _data_struct: DataStruct):
        price = _data_struct.toDict()[self.price_use_key]
        stop_price = self.data[self.ret_key][-1]
        if self.stop_type == SignalType.LONG:
            if price < stop_price:
                self.is_stop = True
        elif self.stop_type == SignalType.SHORT:
            if price > stop_price:
                self.is_stop = True
        else:
            raise Exception('unknown type')
