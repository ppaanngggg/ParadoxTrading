from ParadoxTrading.Engine import (BacktestEngine, BacktestMarketSupply,
                                   MarketEvent, SignalType,
                                   SimpleBarBacktestExecution,
                                   SimpleBarPortfolio, StrategyAbstract)
from ParadoxTrading.Fetch import FetchSHFEDayIndex, RegisterSHFEDayIndex


class MAStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(RegisterSHFEDayIndex(
            _product='rb'
        ))
        self.addMarketRegister(RegisterSHFEDayIndex(
            _product='ag'
        ))

    def deal(self, _market_event: MarketEvent):
        if _market_event.symbol == 'rb':
            self.addEvent(_market_event.symbol, SignalType.LONG)
        if _market_event.symbol == 'ag':
            self.addEvent(_market_event.symbol, SignalType.SHORT)


ma_strategy = MAStrategy('ma')

market_supply = BacktestMarketSupply(
    '20120119', '20170120',
    RegisterSHFEDayIndex, FetchSHFEDayIndex)
execution = SimpleBarBacktestExecution()
portfolio = SimpleBarPortfolio()
portfolio.price_index = 'averageprice'

engine = BacktestEngine()
engine.addMarketSupply(market_supply)
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(ma_strategy)

engine.run()
