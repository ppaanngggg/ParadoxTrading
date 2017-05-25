import typing

from ParadoxTrading.Engine import StrategyAbstract, SettlementEvent, MarketEvent


class CTAStrategy(StrategyAbstract):
    EMPTY = 0
    LONG = 1
    SHORT = 2

    def __init__(self, _name: str):
        super().__init__(_name)

        self.last_status = self.EMPTY
        self.status = self.EMPTY

    def addEvent(self,
                 _symbol: str,
                 _signal_type: int,
                 _strength: typing.Any = None):
        super().addEvent(_symbol, _signal_type, _strength)

        if _strength > 0:
            self.status = self.LONG
        elif _strength < 0:
            self.status = self.SHORT
        else:
            self.status = self.EMPTY

    def deal(self, _market_event: MarketEvent):
        raise NotImplementedError('deal not implemented')

    def settlement(self, _settlement_event: SettlementEvent):
        raise NotImplementedError('settlement not implemented')

    def storeStatus(self):
        self.last_status = self.status

    def getLastStatus(self) -> int:
        return self.last_status

    def getStatus(self) -> int:
        return self.status

    def dealStatus(self, _market_event: MarketEvent):
        if self.status == self.last_status:
            self.dealStatusNotChanged(_market_event)
        else:
            self.dealStatusChanged(_market_event)

    def dealStatusChanged(self, _market_event: MarketEvent):
        raise NotImplementedError('dealStatusChanged not implemented')

    def dealStatusNotChanged(self, _market_event: MarketEvent):
        raise NotImplementedError('dealStatusNotChanged not implemented')
