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

    def __init__(self, _events: deque, _instruments: list, _begin_day: str, _end_day: str, _split=None):
        self.events = _events
        self.instruments = _instruments
        self.begin_day = _begin_day
        self.end_day = _end_day

    def get_last_instrument_data(self, _instrument: str):
        pass

    def update_data(self):
