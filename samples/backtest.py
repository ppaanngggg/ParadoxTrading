import logging

from ParadoxTrading.Engine import MarketEvent, StrategyAbstract, SignalType
from ParadoxTrading.EngineExt import (BacktestEngine, BacktestMarketSupply,
                                      BarBacktestExecution, BarPortfolio)
from ParadoxTrading.Fetch import FetchGuoJinMin, RegisterGuoJinMin
from ParadoxTrading.Indicator import HighBar, LowBar
from ParadoxTrading.Utils import SplitIntoMinute


class MyStrategy(StrategyAbstract):
    def init(self):
        self.addMarketRegister(RegisterGuoJinMin(
            _product='rb'
        ))
        self.split_rb = SplitIntoMinute(5)
        self.highest_rb = HighBar('highprice')
        self.lowest_rb = LowBar('lowprice')

    def deal(self, _market_event: MarketEvent):
        logging.debug('Strategy({}) recv {}'.format(
            self.name, _market_event.toDict()))

        if self.split_rb.addOne(_market_event.data) and len(
                self.split_rb.getBarList()) > 1:
            # create a new 5 min bar
            self.highest_rb.addOne(
                self.split_rb.getBarEndTimeList()[-2],
                self.split_rb.getBarList()[-2]
            )
            self.lowest_rb.addOne(
                self.split_rb.getBarEndTimeList()[-2],
                self.split_rb.getBarList()[-2]
            )
        if len(self.highest_rb.getAllData()):
            cur_closeprice = _market_event.data['closeprice'][0]
            last_highprice = self.highest_rb.getLastData()['high'][0]
            last_lowprice = self.lowest_rb.getLastData()['low'][0]

            if cur_closeprice > last_highprice and self.last_signal != SignalType.LONG:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                logging.info('CLOSE: {}, HIGH: {}, LOW: {}'.format(
                    cur_closeprice, last_highprice, last_lowprice
                ))
            if cur_closeprice < last_lowprice and self.last_signal != SignalType.SHORT:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                logging.info('CLOSE: {}, HIGH: {}, LOW: {}'.format(
                    cur_closeprice, last_highprice, last_lowprice
                ))


logging.basicConfig(level=logging.INFO)

strategy = MyStrategy('range_break')

fetcher = FetchGuoJinMin()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

market_supply = BacktestMarketSupply(
    '20160101', '20170101', fetcher)
execution = BarBacktestExecution(_commission_rate=0.001)
portfolio = BarPortfolio()

engine = BacktestEngine()
engine.addMarketSupply(market_supply)
engine.addExecution(execution)
engine.addPortfolio(portfolio)
engine.addStrategy(strategy)

engine.run()

portfolio.storeRecords('range_break')
