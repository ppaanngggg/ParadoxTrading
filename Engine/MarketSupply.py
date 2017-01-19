from ParadoxTrading.Utils import Fetch
from collections import deque
from Event import MarketEvent


class MarketSupply():

    def __init__(self):
        pass

    def get_last_instrument_data(self, _instrument: str):
        raise NotImplementedError(
            'Should implement get_last_instrument_data()'
        )

    def update_data(self):
        raise NotImplementedError(
            'Should implement update_data()'
        )


class BacktestMarketSupply(MarketSupply):

    def __init__(self, _events: deque, _begin_day: str, _end_day: str,
                 _products=None, _dominant=True, _sub_dominant=False,
                 _instruments=None,  _split=None):
        self.events = _events
        self.begin_day = _begin_day
        self.end_day = _end_day
        self.products = _products
        self.dominant = _dominant
        self.sub_dominant = _sub_dominant
        self.instruments = _instruments

        self.market_pool = []

    def get_last_instrument_data(self, _instrument: str):
        pass

    def update_data(self):
        if len(self.market_pool) == 0:
            pass
