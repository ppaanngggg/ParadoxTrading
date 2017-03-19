from ParadoxTrading.Fetch.FetchFutureDay import FetchFutureDay, RegisterFuture


class RegisterSHFEDay(RegisterFuture):
    pass


class FetchSHFEDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'SHFEProd'
        self.mongo_inst_db = 'SHFEInst'

        self.psql_dbname = 'SHFEDay'

        self.columns = [
            'tradingday',
            'openprice',
            'highprice',
            'lowprice',
            'closeprice',
            'settlementprice',
            'pricediff_1',
            'pricediff_2',
            'volume',
            'openinterest',
            'openinterestdiff',
            'presettlementprice',
        ]
        self.index_columns = [
            'tradingday',
            'highprice',
            'lowprice',
            'averageprice',
            'volume',
            'turnover',
            'yearvolume',
            'yearturnover',
        ]
