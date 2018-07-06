import logging

from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, SettlementEvent, \
    SignalType
from ParadoxTrading.EngineExt import BacktestEngine
from ParadoxTrading.EngineExt.Crypto import DepthBacktestExecution, DepthPortfolio, \
    DepthMarketSupply
from ParadoxTrading.Fetch.Crypto import RegisterSymbol

logging.basicConfig(level=logging.INFO)


class BuyAndHoldStrategy(StrategyAbstract):
    def __init__(self):
        super().__init__('buy_and_hold_btc')

        self.addMarketRegister(RegisterSymbol(
            _exname='binance', _symbol='BTC_USDT'
        ))

        self.status = SignalType.EMPTY

    def deal(self, _market_event: MarketEvent):
        if self.status == SignalType.EMPTY:
            self.addEvent(_market_event.symbol, 1)
            self.status = SignalType.LONG

    def settlement(self, _settlement_event: SettlementEvent):
        print(_settlement_event)


portfolio = DepthPortfolio(
    _store_every_day=True
)

engine = BacktestEngine(
    _market_supply=DepthMarketSupply(),
    _execution=DepthBacktestExecution(
        _commission_rate=1e-3
    ),
    _portfolio=portfolio,
    _strategy=BuyAndHoldStrategy()
)
engine.run()

# portfolio will store all record into csv daily
