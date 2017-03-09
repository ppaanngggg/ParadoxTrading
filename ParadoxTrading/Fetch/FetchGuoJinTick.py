from ParadoxTrading.Fetch.FetchFutureTick import RegisterFuture, FetchFutureTick
from ParadoxTrading.Utils import DataStruct


class RegisterGuoJinTick(RegisterFuture):
    pass


class FetchGuoJinTick(FetchFutureTick):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'

        self.psql_dbname = 'GuoJinTick'

        self.cache_path = 'GuoJinTick.hdf5'

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _instrument: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        raise NotImplementedError()
