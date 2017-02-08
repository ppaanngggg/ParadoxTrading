from ParadoxTrading.Engine import BacktestEngine, StrategyAbstract


class MAStrategy(StrategyAbstract):

    def init(self):
        self.addMarketRegister(_product='rb')
        self.addMarketRegister(_product='ag', _minute_skip=1)

ma_strategy = MAStrategy('ma')

engine = BacktestEngine('20170119', '20170126')
engine.addStrategy(ma_strategy)

while True:
    ret = engine.market_supply.updateData()
    if ret:
        pass
    else:
        break
