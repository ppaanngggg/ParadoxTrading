from ParadoxTrading.Fetch.FetchFutureTick import RegisterFuture, FetchFutureTick


class RegisterGuoJinTick(RegisterFuture):
    pass


class FetchGuoJinTick(FetchFutureTick):
    def __init__(self):
        super().__init__()

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'

        self.psql_dbname = 'GuoJinTick'

        self.cache_path = 'GuoJinTick.hdf5'

        self.columns = [
            'tradingday', 'lastprice', 'highestprice', 'lowestprice',
            'volume', 'turnover', 'openinterest',
            'upperlimitprice', 'lowerlimitprice',
            'askprice', 'askvolume', 'bidprice', 'bidvolume',
            'happentime',
        ]
        self.types = [
            'character',
            'double precision', 'double precision', 'double precision',
            'integer', 'double precision', 'double precision',
            'double precision', 'double precision',
            'double precision', 'integer', 'double precision', 'integer',
            'timestamp without time zone',
        ]
        self.index_columns = [
            'tradingday', 'lastprice', 'volume', 'openinterest', 'happentime'
        ]
        self.index_types = [
            'character', 'double precision', 'integer',
            'double precision', 'timestamp without time zone',
        ]
