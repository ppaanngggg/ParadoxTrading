from ParadoxTrading.Engine import MarketEvent, SignalType, StrategyAbstract
from ParadoxTrading.EngineExt import (BacktestEngine, BacktestMarketSupply,
                                      TickBacktestExecution, TickPortfolio)
from ParadoxTrading.Fetch import FetchGuoJinTick, RegisterGuoJinTick


class MyStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(RegisterGuoJinTick(
            _product='rb'
        ))
        self.addMarketRegister(RegisterGuoJinTick(
            _product='ag'
        ))

    def deal(self, _market_event: MarketEvent):
        print(_market_event.data)
        input()
        # if _market_event.symbol == 'rb':
        #     self.addEvent(_market_event.symbol, SignalType.LONG)
        # if _market_event.symbol == 'ag':
        #     self.addEvent(_market_event.symbol, SignalType.SHORT)


ma_strategy = MyStrategy('ma')

fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

market_supply = BacktestMarketSupply(
    '20160101', '20170120', fetcher)
execution = TickBacktestExecution()
portfolio = TickPortfolio()

engine = BacktestEngine()
engine.addMarketSupply(market_supply)
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(ma_strategy)

engine.run()
