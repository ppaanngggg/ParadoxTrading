from ParadoxTrading.Fetch.FetchFuture import (FetchFutureDay,
                                              FetchFutureDayIndex,
                                              RegisterFutureDay,
                                              RegisterFutureDayIndex)


class RegisterDCEDay(RegisterFutureDay):
    pass


class FetchDCEDay(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterDCEDay

        self.mongo_prod_db = 'DCEProd'
        self.mongo_inst_db = 'DCEInst'

        self.psql_dbname = 'DCEDay'

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


class RegisterDCEDayIndex(RegisterFutureDayIndex):
    pass


class FetchDCEDayIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterDCEDayIndex

        self.mongo_prod_db = 'DCEProd'
        self.mongo_inst_db = 'DCEInst'

        self.psql_dbname = 'DCEDay'

        self.columns = [
            'tradingday',
            'highprice',
            'lowprice',
            'averageprice',
            'volume',
            'openinterest',
            'turnover',
        ]
