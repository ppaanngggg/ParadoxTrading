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

        self.columns = [
            'tradingday',
            'openprice',
            'highprice',
            'lowprice',
            'closeprice',
            'volume',
            'turnover',
            'openinterest',
            'precloseprice',
            'preopeninterest',
            'bartime',
            'barendtime',
        ]
        self.types = [
            'character',
            'double precision',
            'double precision',
            'double precision',
            'double precision',
            'integer',
            'double precision',
            'double precision',
            'double precision',
            'double precision',
            'timestamp without time zone',
            'timestamp without time zone',
        ]

        self.index_columns = [
            'tradingday',
            'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'openinterest',
            'bartime', 'barendtime',
        ]
        self.index_types = [
            'character',
            'double precision', 'double precision',
            'double precision', 'double precision',
            'integer', 'double precision',
            'timestamp without time zone',
            'timestamp without time zone',
        ]
