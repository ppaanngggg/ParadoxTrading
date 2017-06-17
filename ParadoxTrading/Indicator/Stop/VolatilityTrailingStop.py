from ParadoxTrading.Engine import SignalType
from ParadoxTrading.Indicator.Stop.StopIndicatorAbstract import StopIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class VolatilityTrailingStop(StopIndicatorAbstract):
    def __init__(
            self,
            _price_data: DataStruct,
            _volatility_data: DataStruct,
            _stop_type: int,
            _rate: float = 4,
            _price_use_key: str = 'closeprice',
            _volatility_use_key: str = 'volatility',
            _idx_key: str = 'time',
            _ret_key: str = 'stopprice',
    ):
        super().__init__()

        assert len(_price_data) == 1
        assert len(_volatility_data) == 1

        self.stop_type = _stop_type
        self.rate = _rate
        self.price_use_key = _price_use_key
        self.volatility_use_key = _volatility_use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key

        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key,
        )

        self._addOne(_price_data, _volatility_data)

    def get_stop_price(self, _price, _volatility):
        if len(self.data):
            last_stop_price = self.data[self.ret_key][-1]
            if self.stop_type == SignalType.LONG:
                return max(_price * (1 - self.rate * _volatility), last_stop_price)
            elif self.stop_type == SignalType.SHORT:
                return min(_price * (1 + self.rate * _volatility), last_stop_price)
            else:
                raise Exception('unknown type')
        else:
            if self.stop_type == SignalType.LONG:
                return _price * (1 - self.rate * _volatility)
            elif self.stop_type == SignalType.SHORT:
                return _price * (1 + self.rate * _volatility)
            else:
                raise Exception('unknown type')

    def addOne(
            self, _data_struct: DataStruct,
            _volatility_data: DataStruct = None
    ) -> bool:
        """
        add one data into buf, and return isStop or not
        :param _data_struct:
        :return:
        """
        if not self.is_stop:
            assert len(_data_struct) == 1
            self._addOne(_data_struct, _volatility_data)
            self._isStop(_data_struct)

        return self.is_stop

    def _addOne(
            self, _price_data: DataStruct,
            _volatility_data: DataStruct = None,
    ):
        assert len(_volatility_data) == 1

        price = _price_data.toDict()[self.price_use_key]
        volatility = _volatility_data.toDict()[self.volatility_use_key]

        price_time = _price_data.index()[0]
        volatility_time = _volatility_data.index()[0]
        assert price_time == volatility_time

        self.data.addDict({
            self.idx_key: price_time,
            self.ret_key: self.get_stop_price(price, volatility),
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
