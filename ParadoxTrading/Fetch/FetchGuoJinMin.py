from ParadoxTrading.Fetch.FetchFutureMin import FetchFutureMin, RegisterFuture


class RegisterGuoJinMin(RegisterFuture):
    pass


class FetchGuoJinMin(FetchFutureMin):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'

        self.psql_dbname = 'GuoJinMin'

        self.cache_path = 'GuoJinMin.hdf5'
