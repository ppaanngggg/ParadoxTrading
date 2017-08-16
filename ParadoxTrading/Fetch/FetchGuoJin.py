from ParadoxTrading.Fetch.FetchFuture import (FetchFutureDay,
                                              FetchFutureDayIndex,
                                              FetchFutureMin,
                                              FetchFutureMinIndex,
                                              FetchFutureTick,
                                              FetchFutureTickIndex,
                                              RegisterFutureTick,
                                              RegisterFutureTickIndex)


class RegisterGuoJinTick(RegisterFutureTick):
    pass


class FetchGuoJinTick(FetchFutureTick):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinTick

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

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


class RegisterGuoJinTickIndex(RegisterFutureTickIndex):
    pass


class FetchGuoJinTickIndex(FetchFutureTickIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinTickIndex

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

        self.psql_dbname = 'GuoJinTick'

        self.cache_path = 'GuoJinTick.hdf5'

        self.columns = [
            'tradingday', 'lastprice', 'volume', 'openinterest', 'happentime'
        ]
        self.types = [
            'character', 'double precision', 'integer',
            'double precision', 'timestamp without time zone',
        ]


class RegisterGuoJinMin(RegisterFutureTick):
    pass


class FetchGuoJinMin(FetchFutureMin):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinMin

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

        self.psql_dbname = 'GuoJinMin'

        self.cache_path = 'GuoJinMin.hdf5'

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'turnover', 'openinterest',
            'precloseprice', 'preopeninterest',
            'bartime', 'barendtime',
        ]
        self.types = [
            'character',
            'double precision', 'double precision',
            'double precision', 'double precision',
            'integer',
            'double precision', 'double precision',
            'double precision', 'double precision',
            'timestamp without time zone', 'timestamp without time zone',
        ]


class RegisterGuoJinMinIndex(RegisterFutureTickIndex):
    pass


class FetchGuoJinMinIndex(FetchFutureMinIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinMinIndex

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

        self.psql_dbname = 'GuoJinMin'

        self.cache_path = 'GuoJinMin.hdf5'

        self.columns = [
            'tradingday',
            'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'openinterest',
            'bartime', 'barendtime',
        ]
        self.types = [
            'character',
            'double precision', 'double precision',
            'double precision', 'double precision',
            'integer', 'double precision',
            'timestamp without time zone', 'timestamp without time zone',
        ]


class RegisterGuoJinHour(RegisterFutureTick):
    pass


class FetchGuoJinHour(FetchGuoJinMin):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinHour

        self.psql_dbname = 'GuoJinHour'
        self.cache_path = 'GuoJinHour.hdf5'


class RegisterGuoJinHourIndex(RegisterFutureTickIndex):
    pass


class FetchGuoJinHourIndex(FetchGuoJinMinIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinHourIndex

        self.psql_dbname = 'GuoJinHour'
        self.cache_path = 'GuoJinHour.hdf5'


class RegisterGuoJinDay(RegisterFutureTick):
    pass


class FetchGuoJinDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinDay

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

        self.psql_dbname = 'GuoJinDay'

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'turnover', 'openinterest',
            'precloseprice', 'preopeninterest',
        ]


class RegisterGuoJinDayIndex(RegisterFutureTickIndex):
    pass


class FetchGuoJinDayIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterGuoJinDayIndex

        self.mongo_prod_db = 'GuoJinProd'
        self.mongo_inst_db = 'GuoJinInst'
        self.mongo_tradingday_db = 'GuoJinTradingDay'

        self.psql_dbname = 'GuoJinDay'

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice',
            'closeprice', 'volume', 'openinterest',
        ]
