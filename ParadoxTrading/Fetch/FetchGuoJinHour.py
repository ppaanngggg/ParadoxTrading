from ParadoxTrading.Fetch.FetchFutureHour import FetchFutureHour, RegisterFuture


class RegisterGuoJinHour(RegisterFuture):
    pass


class FetchGuoJinHour(FetchFutureHour):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'

        self.psql_dbname = 'GuoJinHour'

        self.cache_path = 'GuoJinHour.hdf5'
