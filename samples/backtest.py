from ParadoxTrading.Engine import (BacktestEngine, MarketEvent, SignalType,
                                   SimpleBarBacktestExecution,
                                   SimpleBarPortfolio, StrategyAbstract)
from ParadoxTrading.Performance import Fetch, dailyReturn, sharpRatio


class MAStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(_product='rb', _product_index=True)
        self.addMarketRegister(_product='ag', _product_index=True)

    def deal(self, _market_event: MarketEvent):
        self.addEvent(_market_event.instrument, SignalType.LONG)


ma_strategy = MAStrategy('ma')

execution = SimpleBarBacktestExecution()
portfolio = SimpleBarPortfolio()

engine = BacktestEngine('20170119', '20170120', 'd')
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(ma_strategy)

engine.run()

portfolio.storeRecords('test')

fill_records = Fetch.fetchFillRecords('test', 'ma')
daily_return = dailyReturn(fill_records, 10000)
sharp_ratio = sharpRatio(daily_return)
print(sharp_ratio)
