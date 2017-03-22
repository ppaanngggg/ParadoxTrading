from ParadoxTrading.Fetch import (FetchFutureDay, FetchFutureDayIndex,
                                  RegisterFutureDay, RegisterFutureDayIndex)


class RegisterSHFEDay(RegisterFutureDay):
    pass


class FetchSHFEDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

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


class RegisterSHFEDayIndex(RegisterFutureDayIndex):
    pass


class FetchSHFEDayIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

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
