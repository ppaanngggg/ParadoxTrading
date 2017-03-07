import typing

from ParadoxTrading.Fetch.FetchFutureTick import RegisterFuture, FetchFutureTick
from ParadoxTrading.Utils import DataStruct


class RegisterFutureMin(RegisterFuture):
    pass


class FetchFutureMin(FetchFutureTick):
    def __init__(self):
        super().__init__()
        # point dbname to min
        self.psql_dbname = 'FutureMin'
        # point cache to min
        self.cache_path = 'FutureMin.hdf5'

    def fetchData(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
            _cache=True, _index='BarEndTime'
    ) -> typing.Union[None, DataStruct]:
        return super().fetchData(
            _tradingday,
            _product, _instrument,
            _product_index, _sub_dominant,
            _cache, _index
        )

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _instrument: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        raise NotImplementedError()
