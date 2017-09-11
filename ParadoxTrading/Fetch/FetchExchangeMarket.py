from ParadoxTrading.Fetch.FetchFuture import (FetchFutureDay,
                                              FetchFutureDayIndex,
                                              RegisterFutureDay,
                                              RegisterFutureDayIndex)


class RegisterExchangeMarket(RegisterFutureDay):
    pass


class FetchExchangeMarket(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterExchangeMarket

        self.mongo_prod_db = 'ExchangeProd'
        self.mongo_inst_db = 'ExchangeInst'
        self.mongo_tradingday_db = 'ExchangeTradingDay'

        self.psql_dbname = 'ExchangeMarket'

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


class RegisterExchangeMarketIndex(RegisterFutureDayIndex):
    pass


class FetchExchangeMarketIndex(FetchFutureDayIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterExchangeMarketIndex

        self.mongo_prod_db = 'ExchangeProd'
        self.mongo_inst_db = 'ExchangeInst'
        self.mongo_tradingday_db = 'ExchangeTradingDay'

        self.psql_dbname = 'ExchangeMarket'

        self.columns = [
            'tradingday',
            'openprice',
            'highprice',
            'lowprice',
            'closeprice',
            'volume',
            'openinterest',
        ]


class RegisterExchangeIndexDominant(RegisterFutureDayIndex):
    pass


class FetchExchangeIndexDominant(FetchExchangeMarketIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterExchangeIndexDominant
        self.psql_dbname = 'ExchangeIndex'

        self.market_key = 'dominant_{}_{}'