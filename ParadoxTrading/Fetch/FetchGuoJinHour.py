from ParadoxTrading.Fetch.FetchGuoJinMin import FetchGuoJinMin, RegisterFuture


class RegisterGuoJinHour(RegisterFuture):
    pass


class FetchGuoJinHour(FetchGuoJinMin):
    def __init__(self):
        super().__init__()

        self.psql_dbname = 'GuoJinHour'
        self.cache_path = 'GuoJinHour.hdf5'
