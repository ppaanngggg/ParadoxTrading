from datetime import datetime
import tabulate


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


class EventAbstract:
    def __init__(self):
        self.type = None


class MarketEvent(EventAbstract):
    def __init__(self, _market_register_key: str, _strategy_name: str):
        super().__init__()
        self.type = EventType.MARKET
        self.market_register_key = _market_register_key
        self.strategy_name = _strategy_name

    def __repr__(self):
        return 'MARKET: ' + self.market_register_key + ', ' + self.strategy_name


class SignalEvent(EventAbstract):
    def __init__(
            self,
            _instrument: str, _strategy_name: str,
            _signal_type: int, _datetime: datetime,
            _strength: float = None
    ):
        super().__init__()
        self.type = EventType.SIGNAL
        self.instrument = _instrument
        self.strategy_name = _strategy_name
        self.datetime = _datetime
        self.signal_type = _signal_type
        self.strength = _strength

    def __repr__(self):
        return 'SIGNAL:' + '\n' + \
               '\tinstrument: ' + self.instrument + '\n' + \
               '\tstrategy: ' + self.strategy_name + '\n' + \
               '\tdatetime: ' + str(self.datetime) + '\n' + \
               '\tsignal: ' + str(self.signal_type) + '\n' + \
               '\tstrength: ' + str(self.strength)


class OrderEvent(EventAbstract):
    def __init__(
            self, _index: int, _instrument: str, _datetime: datetime,
            _order_type: int = None, _action: int = None, _direction: int = None,
            _quantity: int = 1, _price: float = None
    ):
        super().__init__()
        self.type = EventType.ORDER
        self.index = _index
        self.instrument = _instrument
        self.datetime = _datetime
        self.order_type = _order_type
        self.action = _action
        self.direction = _direction
        self.quantity = _quantity
        self.price = _price

    def __repr__(self):
        return 'ORDER:' + '\n' + \
               "\tindex: " + str(self.index) + '\n' + \
               "\tinstrument: " + self.instrument + '\n' + \
               "\tdatetime: " + str(self.datetime) + '\n' + \
               "\ttype: " + str(self.order_type) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\taction: " + str(self.action) + '\n' + \
               "\tdirection: " + str(self.direction) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\tprice: " + str(self.price)


class FillEvent(EventAbstract):
    def __init__(self, _index, _instrument, _datetime, _quantity, _action, _direction, _price, _commission):
        super().__init__()
        self.type = EventType.FILL
        self.index = _index
        self.instrument = _instrument
        self.datetime = _datetime
        self.quantity = _quantity
        self.action = _action
        self.direction = _direction
        self.price = _price
        self.commission = _commission
