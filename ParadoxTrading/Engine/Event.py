import typing
from datetime import datetime

from ParadoxTrading.Utils import DataStruct


class EventType:
    MARKET = 1
    SIGNAL = 2
    ORDER = 3
    FILL = 4

    @staticmethod
    def toStr(_value: int) -> str:
        if _value == EventType.MARKET:
            return 'market'
        elif _value == EventType.SIGNAL:
            return 'signal'
        elif _value == EventType.ORDER:
            return 'order'
        elif _value == EventType.FILL:
            return 'fill'
        else:
            raise Exception('unknown event type')


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
            raise Exception('unknown signal type')


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

    def toDict(self) -> dict:
        raise NotImplementedError('toDict not implemented')


class MarketEvent(EventAbstract):
    def __init__(self,
                 _market_register_key: str,
                 _strategy_name: str,
                 _symbol: str,
                 _data: typing.Union[None, DataStruct] = None):
        super().__init__()
        self.type = EventType.MARKET
        self.market_register_key = _market_register_key
        self.strategy_name = _strategy_name
        self.symbol = _symbol
        self.data = _data

    def toDict(self) -> dict:
        return {
            'type': self.type,
            'market_register_key': self.market_register_key,
            'strategy_name': self.strategy_name,
            'symbol': self.symbol,
        }

    @staticmethod
    def fromDict(_dict: dict) -> 'MarketEvent':
        return MarketEvent(
            _market_register_key=_dict['market_register_key'],
            _strategy_name=_dict['strategy_name'],
            _symbol=_dict['symbol'],
            _data=None
        )

    def __repr__(self):
        return 'MARKET: ' + '\n' + \
               '\tkey: ' + self.market_register_key + '\n' + \
               '\tstrategy: ' + self.strategy_name + '\n' + \
               '\tsymbol: ' + self.symbol


class SignalEvent(EventAbstract):
    def __init__(self,
                 _symbol: str,
                 _strategy_name: str,
                 _signal_type: int,
                 _tradingday: str,
                 _datetime: datetime,
                 _strength: float = None):
        super().__init__()
        self.type = EventType.SIGNAL
        self.symbol = _symbol
        self.strategy_name = _strategy_name
        self.signal_type = _signal_type
        self.tradingday = _tradingday
        self.datetime = _datetime
        self.strength = _strength

    def toDict(self) -> dict:
        return {
            'type': self.type,
            'symbol': self.symbol,
            'strategy_name': self.strategy_name,
            'signal_type': self.signal_type,
            'tradingday': self.tradingday,
            'datetime': self.datetime,
            'strength': self.strength
        }

    @staticmethod
    def fromDict(_dict: dict) -> 'SignalEvent':
        return SignalEvent(
            _symbol=_dict['symbol'],
            _strategy_name=_dict['strategy_name'],
            _signal_type=_dict['signal_type'],
            _tradingday=_dict['tradingday'],
            _datetime=_dict['datetime'],
            _strength=_dict['strength'])

    def __repr__(self):
        return 'SIGNAL:' + '\n' + \
               '\tsymbol: ' + self.symbol + '\n' + \
               '\tstrategy: ' + self.strategy_name + '\n' + \
               '\tsignal: ' + SignalType.toStr(self.signal_type) + '\n' + \
               '\ttradingday: ' + self.tradingday + '\n' + \
               '\tdatetime: ' + str(self.datetime) + '\n' + \
               '\tstrength: ' + str(self.strength)


class OrderEvent(EventAbstract):
    def __init__(self,
                 _index: int,
                 _symbol: str,
                 _tradingday: str,
                 _datetime: datetime,
                 _order_type: int = None,
                 _action: int = None,
                 _direction: int = None,
                 _quantity: int = 1,
                 _price: float = None):
        super().__init__()
        self.type = EventType.ORDER
        self.index = _index
        self.symbol = _symbol
        self.tradingday = _tradingday
        self.datetime = _datetime
        self.order_type = _order_type
        self.action = _action
        self.direction = _direction
        self.quantity = _quantity
        self.price = _price

    def toDict(self) -> dict:
        return {
            'type': self.type,
            'index': self.index,
            'symbol': self.symbol,
            'tradingday': self.tradingday,
            'datetime': self.datetime,
            'order_type': self.order_type,
            'action': self.action,
            'direction': self.direction,
            'quantity': self.quantity,
            'price': self.price,
        }

    @staticmethod
    def fromDict(_dict: dict) -> 'OrderEvent':
        return OrderEvent(
            _index=_dict['index'],
            _symbol=_dict['symbol'],
            _tradingday=_dict['tradingday'],
            _datetime=_dict['datetime'],
            _order_type=_dict['order_type'],
            _action=_dict['action'],
            _direction=_dict['direction'],
            _quantity=_dict['quantity'],
            _price=_dict['price'], )

    def __repr__(self):
        return 'ORDER:' + '\n' + \
               "\tindex: " + str(self.index) + '\n' + \
               "\tsymbol: " + self.symbol + '\n' + \
               "\ttradingday: " + self.tradingday + '\n' + \
               "\tdatetime: " + str(self.datetime) + '\n' + \
               "\ttype: " + OrderType.toStr(self.order_type) + '\n' + \
               "\taction: " + ActionType.toStr(self.action) + '\n' + \
               "\tdirection: " + DirectionType.toStr(self.direction) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\tprice: " + str(self.price)


class FillEvent(EventAbstract):
    def __init__(self,
                 _index: int,
                 _symbol: str,
                 _tradingday: str,
                 _datetime: datetime,
                 _quantity: int,
                 _action: int,
                 _direction: int,
                 _price: float,
                 _commission: float):
        super().__init__()
        self.type = EventType.FILL
        self.index = _index
        self.symbol = _symbol
        self.tradingday = _tradingday
        self.datetime = _datetime
        self.quantity = _quantity
        self.action = _action
        self.direction = _direction
        self.price = _price
        self.commission = _commission

    def toDict(self) -> dict:
        return {
            'type': self.type,
            'index': self.index,
            'symbol': self.symbol,
            'tradingday': self.tradingday,
            'datetime': self.datetime,
            'quantity': self.quantity,
            'action': self.action,
            'direction': self.direction,
            'price': self.price,
            'commission': self.commission,
        }

    @staticmethod
    def fromDict(_dict: dict) -> 'FillEvent':
        return FillEvent(
            _index=_dict['index'],
            _symbol=_dict['symbol'],
            _tradingday=_dict['tradingday'],
            _datetime=_dict['datetime'],
            _quantity=_dict['quantity'],
            _action=_dict['action'],
            _direction=_dict['direction'],
            _price=_dict['price'],
            _commission=_dict['commission'], )

    def __repr__(self):
        return 'Fill:' + '\n' + \
               "\tindex: " + str(self.index) + '\n' + \
               "\tsymbol: " + self.symbol + '\n' + \
               "\ttradingday: " + self.tradingday + '\n' + \
               "\tdatetime: " + str(self.datetime) + '\n' + \
               "\tquantity: " + str(self.quantity) + '\n' + \
               "\taction: " + ActionType.toStr(self.action) + '\n' + \
               "\tdirection: " + DirectionType.toStr(self.action) + '\n' + \
               "\tprice: " + str(self.price) + '\n' + \
               "\tcommission: " + str(self.commission)
