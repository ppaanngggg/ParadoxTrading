from ParadoxTrading.Utils import Fetch
from collections import deque


class DataSupply():

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


class BacktestDataSupply(DataSupply):

    def __init__(self, _events: deque, _instruments: list, _begin_day: str, _end_day: str, _skip=None):
        self.events = _events

    def get_last_instrument_data(self, _instrument: str):
        pass

    def update_data(self):
        pass
