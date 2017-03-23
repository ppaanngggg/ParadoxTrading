from ParadoxTrading.Fetch.FetchFuture import (FetchFutureDay,
                                              FetchFutureDayIndex,
                                              RegisterFutureDay,
                                              RegisterFutureDayIndex)


class RegisterCZCEDay(RegisterFutureDay):
    pass


class FetchCZCEDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = FetchCZCEDay

        self.mongo_prod_db = 'CZCEProd'
        self.mongo_inst_db = 'CZCEInst'

        self.psql_dbname = 'CZCEDay'

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
            'turnover',
            'presettlementprice',
        ]


class RegisterCZCEDayIndex(RegisterFutureDayIndex):
    pass


class FetchCZCEDayIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

        self.register_type = FetchCZCEDayIndex

        self.mongo_prod_db = 'CZCEProd'
        self.mongo_inst_db = 'CZCEInst'

        self.psql_dbname = 'CZCEDay'

        self.columns = [
            'tradingday',
            'highprice',
            'lowprice',
            'averageprice',
            'volume',
            'openinterest',
            'turnover',
        ]
