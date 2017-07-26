from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class ATRTrailingStop(StopIndicatorAbstract):
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

        self.best_price = _price_data[self.price_use_key][0]
        self._addOne(_price_data, _atr_data)

    def get_stop_price(self, _price, _atr) -> float:
        if self.stop_type == SignalType.LONG:
            self.best_price = max(self.best_price, _price)
            return self.best_price - self.rate * _atr
        elif self.stop_type == SignalType.SHORT:
            self.best_price = min(self.best_price, _price)
            return self.best_price + self.rate * _atr
        else:
            raise Exception('unknown type')

    def addOne(
            self, _data_struct: DataStruct,
            _atr_data: DataStruct = None,
    ) -> bool:
        if not self.is_stop:
            assert len(_data_struct) == 1
            self._addOne(_data_struct, _atr_data)
            self._isStop(_data_struct)

        return self.is_stop

    def _addOne(
            self, _price_data: DataStruct,
            _atr_data: DataStruct = None,
    ):
        assert len(_atr_data) == 1

        price = _price_data.toDict()[self.price_use_key]
        atr = _atr_data.toDict()[self.atr_use_key]

        price_time = _price_data.index()[0]
        atr_time = _atr_data.index()[0]
        assert price_time == atr_time

        self.data.addDict({
            self.idx_key: price_time,
            self.ret_key: self.get_stop_price(price, atr),
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
