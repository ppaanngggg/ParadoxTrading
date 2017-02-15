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

    @staticmethod
    def toStr(_value: int) -> str:
        if _value == SignalType.LONG:
            return 'long'
        elif _value == SignalType.SHORT:
            return 'short'
        else:
            raise Exception()


class OrderType:
    MARKET = 1
    LIMIT = 2

    @staticmethod
    def toStr(_value: int) -> str:
        if _value == OrderType.MARKET:
            return 'marker'
        elif _value == OrderType.LIMIT:
            return 'limit'
        else:
            raise Exception()


class ActionType:
    OPEN = 1
    CLOSE = 2

    @staticmethod
    def toStr(_value: int) -> str:
        if _value == ActionType.OPEN:
            return 'open'
        elif _value == ActionType.CLOSE:
            return 'close'
        else:
            raise Exception()


class DirectionType:
    BUY = 1
    SELL = 2

    @staticmethod
    def toStr(_value: int) -> str:
        if _value == DirectionType.BUY:
            return 'buy'
        elif _value == DirectionType.SELL:
            return 'sell'
        else:
            raise Exception()


class EventAbstract:
    def __init__(self):
        self.type = None


class MarketEvent(EventAbstract):
    def __init__(self, _market_register_key: str, _strategy_name: str):
        super().__init__()
        self.type = EventType.MARKET
        self.market_register_key = _market_register_key
        self.strategy_name = _strategy_name

    def toDict(self) -> dict:
        return {
            'market_register_key': self.market_register_key,
            'strategy_name': self.strategy_name,
        }

    @staticmethod
    def fromDict(self, _dict: dict) -> 'MarketEvent':
        return MarketEvent(
            _dict['market_register_key'],
            _dict['strategy_name']
        )

    def __repr__(self):
        return 'MARKET: ' + '\n' + \
               '\tkey: ' + self.market_register_key + '\n' + \
               '\tstrategy: ' + self.strategy_name


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
               '\tsignal: ' + SignalType.toStr(self.signal_type) + '\n' + \
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
               "\ttype: " + OrderType.toStr(self.order_type) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\taction: " + ActionType.toStr(self.action) + '\n' + \
               "\tdirection: " + DirectionType.toStr(self.direction) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\tprice: " + str(self.price)


class FillEvent(EventAbstract):
    def __init__(
            self, _index: int, _instrument: str, _datetime: datetime,
            _quantity: int, _action: int, _direction: int,
            _price: float, _commission: float
    ):
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

    def __repr__(self):
        return 'Fill:' + '\n' + \
               "\tindex: " + str(self.index) + '\n' + \
               "\tinstrument: " + self.instrument + '\n' + \
               "\tdatetime: " + str(self.datetime) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\taction: " + ActionType.toStr(self.action) + '\n' + \
               "\tdirection: " + DirectionType.toStr(self.action) + '\n' + \
               "\tprice: " + str(self.price) + '\n' + \
               "\tcommission: " + str(self.commission)
