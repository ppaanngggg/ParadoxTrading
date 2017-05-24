import typing

from ParadoxTrading.Engine import StrategyAbstract, SettlementEvent, MarketEvent


class CTAStrategy(StrategyAbstract):
    EMPTY = 0
    LONG = 1
    SHORT = -1

    def __init__(self, _name: str):
        super().__init__(_name)

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
