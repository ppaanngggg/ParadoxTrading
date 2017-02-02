from datetime import datetime


class EventType:

    MARKET = 1
    SIGNAL = 2
    ORDER = 3
    FILL = 4


class SignalType:

    LONG = 1
    SHORT = 2


class OrderType:

    MARKET = 1
    LIMIT = 2


class ActionType:

    OPEN = 1
    CLOSE = 2


class DirectionType:

    BUY = 1
    SELL = 2


class EventAbstract():

    def __init__(self):

        self.type = None


class MarketEvent(EventAbstract):

    def __init__(self, _market_register_key: str, _strategy_name: str):

        self.type = EventType.MARKET
        self.market_register_key = _market_register_key
        self.strategy_name = _strategy_name


class SignalEvent(EventAbstract):

    def __init__(
        self, _instrument: str, _strategy_name: str,
        _datetime: datetime, _signal_type: int, _strength: float
    ):

        self.type = EventType.SIGNAL
        self.instrument = _instrument
        self.strategy_name = _strategy_name
        self.datetime = _datetime
        self.signal_type = _signal_type
        self.strength = _strength


class OrderEvent(EventAbstract):

    def __init__(self, _index, _instrument, _datetime, _order_type, _quantity, _action, _direction):
        self.type = EventType.ORDER
        self.index = _index
        self.instrument = _instrument
        self.datetime = _datetime
        self.order_type = _order_type
        self.quantity = _quantity
        self.action = _action
        self.direction = _direction

    def print_order(self):
        print(
            "-------------------------\n",
            "-- Order:", self.index, '\n',
            "-- Instrument:", self.instrument, '\n',
            "-- Datetime:", self.datetime, '\n',
            "-- Type:", self.order_type, '\n',
            "-- Quantity:", self.quantity, '\n',
            "-- Action:", self.action, '\n',
            "-- Direction:", self.direction, '\n',
        )


class FillEvent(EventAbstract):

    def __init__(self, _index, _instrument, _datetime, _quantity, _action, _direction, _price, _commission):
        self.type = EventType.FILL
        self.index = _index
        self.instrument = _instrument
        self.datetime = _datetime
        self.quantity = _quantity
        self.action = _action
        self.direction = _direction
        self.price = _price
        self.commission = _commission
