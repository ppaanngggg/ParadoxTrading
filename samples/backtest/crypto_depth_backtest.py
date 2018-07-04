import logging

from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, SettlementEvent, \
    SignalType
from ParadoxTrading.EngineExt import BacktestEngine, BacktestMarketSupply
from ParadoxTrading.EngineExt.Crypto import DepthBacktestExecution, DepthPortfolio
from ParadoxTrading.Fetch.Crypto import RegisterSymbol, FetchDepth
from ParadoxTrading.Indicator.General import FastMA

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


class MAStrategy(StrategyAbstract):
    def __init__(self):
        super().__init__('ma_strategy')

        self.addMarketRegister(RegisterSymbol(
            _exname='binance', _symbol='BTC_USDT'
        ))

        self.ma_5 = FastMA(5, _use_key='askprice0')
        self.ma_10 = FastMA(10, _use_key='askprice0')

    def deal(self, _market_event: MarketEvent):
        symbol = _market_event.symbol
        data = _market_event.data

        ma_5_value = self.ma_5.addOne(data).getLastData()['ma'][0]
        ma_10_value = self.ma_10.addOne(data).getLastData()['ma'][0]

        if ma_5_value > ma_10_value:
            self.addEvent(symbol, 1)
        elif ma_5_value < ma_10_value:
            self.addEvent(symbol, -1)
        else:
            self.addEvent(symbol, 0)

    def settlement(self, _settlement_event: SettlementEvent):
        pass


fetcher = FetchDepth(
    _psql_host='psql.local', _psql_dbname='cube_data', _psql_user='postgres'
)
portfolio = DepthPortfolio()

engine = BacktestEngine(
    _market_supply=BacktestMarketSupply(
        '20180701', '20180702', fetcher
    ),
    _execution=DepthBacktestExecution(
        _commission_rate=1e-3
    ),
    _portfolio=portfolio,
    _strategy=MAStrategy()
)
