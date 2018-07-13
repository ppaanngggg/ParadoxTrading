import logging

from ParadoxTrading.Engine import MarketEvent, SettlementEvent, \
    SignalType
from ParadoxTrading.EngineExt import BacktestEngine
from ParadoxTrading.EngineExt.Crypto import TickerBacktestExecution, TickerPortfolio, \
    TickerMarketSupply, OnlineMinBarStrategy
from ParadoxTrading.Fetch.Crypto import RegisterSymbol

logging.basicConfig(level=logging.INFO)


class BuyAndHoldStrategy(OnlineMinBarStrategy):
    def __init__(self):
        super().__init__('buy_and_hold_btc', _min_periods=(1, 2))

        self.addMarketRegister(RegisterSymbol(
            _exname='binance', _symbol='BTC_USDT'
        ))

        self.status = SignalType.EMPTY

    def do_deal(self, _market_event: MarketEvent, _period):
        print('period:', _period)
        print(_market_event.data)
        # if self.status == SignalType.EMPTY:
        #     self.addEvent(_market_event.symbol, 1)
        #     self.status = SignalType.LONG

    def settlement(self, _settlement_event: SettlementEvent):
        print(_settlement_event)


portfolio = TickerPortfolio(
    _store_every_day=True
)

engine = BacktestEngine(
    _market_supply=TickerMarketSupply(),
    _execution=TickerBacktestExecution(
        _commission_rate=1e-3
    ),
    _portfolio=portfolio,
    _strategy=BuyAndHoldStrategy()
)
engine.run()

# portfolio will store all record into csv daily
