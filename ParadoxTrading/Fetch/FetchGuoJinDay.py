from ParadoxTrading.Fetch.FetchFutureDay import RegisterFuture, FetchFutureDay


class RegisterGuoJinDay(RegisterFuture):
    pass


class FetchGuoJinDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'

        self.psql_dbname = 'GuoJinDay'
