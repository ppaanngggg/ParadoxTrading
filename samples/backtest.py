from ParadoxTrading.Engine import (BacktestEngine, BacktestMarketSupply,
                                   MarketEvent, SignalType,
                                   SimpleBarBacktestExecution,
                                   SimpleBarPortfolio, StrategyAbstract)
from ParadoxTrading.Fetch import FetchSHFEDay, RegisterSHFEDay


class MAStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(RegisterSHFEDay(
            _product='rb', _product_index=True
        ))
        self.addMarketRegister(RegisterSHFEDay(
            _product='ag', _product_index=True
        ))

    def deal(self, _market_event: MarketEvent):
        if _market_event.symbol == 'rb':
            self.addEvent(_market_event.symbol, SignalType.LONG)
        if _market_event.symbol == 'ag':
            self.addEvent(_market_event.symbol, SignalType.SHORT)


ma_strategy = MAStrategy('ma')

market_supply = BacktestMarketSupply(
    '20120119', '20170120',
    RegisterSHFEDay, FetchSHFEDay)
execution = SimpleBarBacktestExecution()
portfolio = SimpleBarPortfolio()
portfolio.price_index = 'averageprice'

engine = BacktestEngine()
engine.addMarketSupply(market_supply)
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(ma_strategy)

engine.run()
