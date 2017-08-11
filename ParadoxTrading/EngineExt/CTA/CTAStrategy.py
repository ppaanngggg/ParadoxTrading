from ParadoxTrading.Engine import StrategyAbstract, SettlementEvent, MarketEvent, SignalType


class CTAStatusType(SignalType):
    pass


class CTAStatusMgr:
    def __init__(self):
        self.last_status = CTAStatusType.EMPTY
        self.status = CTAStatusType.EMPTY

    def storeStatus(self):
        self.last_status = self.status

    def setStatus(self, _strength: float):
        if _strength > 0:
            self.status = CTAStatusType.LONG
        elif _strength < 0:
            self.status = CTAStatusType.SHORT
        else:
            self.status = CTAStatusType.EMPTY

    def getStatus(self):
        return self.status

    def getLastStatus(self):
        return self.last_status


class CTAStrategy(StrategyAbstract):
    def __init__(self, _name: str):
        super().__init__(_name)

        self.status_mgr = CTAStatusMgr()
        self.addPickleSet('status_mgr')

    def addEvent(
            self, _symbol: str,
            _strength: float,
            _signal_type: int = None,
    ):
        if _strength > 0:
            signal_type = SignalType.LONG
        elif _strength < 0:
            signal_type = SignalType.SHORT
        else:
            signal_type = SignalType.EMPTY
        super().addEvent(_symbol, signal_type, _strength)

        self.status_mgr.setStatus(_strength)

    def deal(self, _market_event: MarketEvent):
        # save the last status
        self.status_mgr.storeStatus()
        # do the real deal func
        self.do_deal(_market_event)
        # deal with status change or not change
        self.dealStatus(_market_event)

    def do_deal(self, _market_event: MarketEvent):
        raise NotImplementedError('deal not implemented')

    def getStatus(self):
        return self.status_mgr.getStatus()

    def getLastStatus(self):
        return self.status_mgr.getLastStatus()

    def dealStatus(self, _market_event: MarketEvent):
        if self.getStatus() == self.getLastStatus():
            self.dealStatusNotChanged(_market_event)
        else:
            self.dealStatusChanged(_market_event)

    def settlement(self, _settlement_event: SettlementEvent):
        raise NotImplementedError('settlement not implemented')

    def dealStatusChanged(self, _market_event: MarketEvent):
        raise NotImplementedError('dealStatusChanged not implemented')

    def dealStatusNotChanged(self, _market_event: MarketEvent):
        raise NotImplementedError('dealStatusNotChanged not implemented')
