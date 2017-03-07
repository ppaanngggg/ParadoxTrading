from ParadoxTrading.Engine import BacktestEngine, MarketEvent, SignalType, \
    SimpleBarBacktestExecution, SimpleBarPortfolio, StrategyAbstract, \
    BacktestMarketSupply
from ParadoxTrading.Fetch import RegisterFutureHour, FetchFutureHour, \
    FetchFutureDay
from ParadoxTrading.Performance import FetchRecord, dailyReturn


class MAStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(RegisterFutureHour(
            _product='rb', _product_index=True
        ))
        self.addMarketRegister(RegisterFutureHour(
            _product='ag', _product_index=True
        ))

    def deal(self, _market_event: MarketEvent):
        if _market_event.symbol == 'rb':
            self.addEvent(_market_event.symbol, SignalType.LONG)
        if _market_event.symbol == 'ag':
            self.addEvent(_market_event.symbol, SignalType.SHORT)

ma_strategy = MAStrategy('ma')

market_supply = BacktestMarketSupply(
    '20170119', '20170120',
    RegisterFutureHour, FetchFutureHour)
execution = SimpleBarBacktestExecution()
portfolio = SimpleBarPortfolio()

engine = BacktestEngine()
engine.addMarketSupply(market_supply)
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(ma_strategy)

engine.run()

portfolio.storeRecords('test')

fill_list = FetchRecord.fetchFillRecords('test', 'ma')
print(dailyReturn(
    '20170119', '20170130', fill_list,
    FetchFutureDay().fetchDayData
))
