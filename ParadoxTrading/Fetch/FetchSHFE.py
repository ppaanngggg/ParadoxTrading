from ParadoxTrading.Fetch.FetchFuture import (FetchFutureDay,
                                              FetchFutureDayIndex,
                                              RegisterFutureDay,
                                              RegisterFutureDayIndex)


class RegisterSHFEDay(RegisterFutureDay):
    pass


class FetchSHFEDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterSHFEDay

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


class RegisterSHFEDayIndex(RegisterFutureDayIndex):
    pass


class FetchSHFEDayIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterSHFEDayIndex

        self.mongo_prod_db = 'SHFEProd'
        self.mongo_inst_db = 'SHFEInst'

        self.psql_dbname = 'SHFEDay'

        self.columns = [
            'tradingday',
            'openprice',
            'highprice',
            'lowprice',
            'closeprice',
            'volume',
            'openinterest',
        ]
