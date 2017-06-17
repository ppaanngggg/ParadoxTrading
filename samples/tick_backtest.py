import logging

from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Engine import MarketEvent, SignalType, StrategyAbstract, SettlementEvent
from ParadoxTrading.EngineExt import (BacktestEngine, BacktestMarketSupply,
                                      TickBacktestExecution, TickPortfolio)
from ParadoxTrading.Fetch import FetchGuoJinTick, RegisterGuoJinTick
from ParadoxTrading.Indicator import HighBar, LowBar
from ParadoxTrading.Performance import dailyReturn
from ParadoxTrading.Utils import SplitIntoMinute


class MyStrategy(StrategyAbstract):
    def __init__(self, _name: str):
        super().__init__(_name)

        self.addMarketRegister(RegisterGuoJinTick(
            _product='rb'
        ))
        self.split_rb = SplitIntoMinute(5)
        self.highest_rb = HighBar('lastprice')
        self.lowest_rb = LowBar('lastprice')

    def deal(self, _market_event: MarketEvent):
        logging.debug('Strategy({}) recv {}'.format(
            self.name, _market_event.toDict()))

        if self.split_rb.addOne(_market_event.data) and len(self.split_rb) > 1:
            # create a new 5 min bar
            self.highest_rb.addOne(
                self.split_rb.getBarList()[-2],
                self.split_rb.getBarEndTimeList()[-2],
            )
            self.lowest_rb.addOne(
                self.split_rb.getBarList()[-2],
                self.split_rb.getBarEndTimeList()[-2],
            )
        if len(self.highest_rb.getAllData()):
            lastprice = _market_event.data['lastprice'][0]
            last_highprice = self.highest_rb.getLastData()['high'][0]
            last_lowprice = self.lowest_rb.getLastData()['low'][0]

            if lastprice > last_highprice and \
                            self.getLastSignal() != SignalType.LONG:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                logging.info('CLOSE: {}, HIGH: {}, LOW: {}'.format(
                    lastprice, last_highprice, last_lowprice
                ))
            if lastprice < last_lowprice and \
                            self.getLastSignal() != SignalType.SHORT:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                logging.info('CLOSE: {}, HIGH: {}, LOW: {}'.format(
                    lastprice, last_highprice, last_lowprice
                ))

    def settlement(self, _settlement_event: SettlementEvent):
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    fetcher = FetchGuoJinTick()
    fetcher.psql_host = '192.168.4.102'
    fetcher.psql_user = 'ubuntu'
    fetcher.mongo_host = '192.168.4.102'

    market_supply = BacktestMarketSupply(
        '20160104', '20160131', fetcher)
    execution = TickBacktestExecution(_commission_rate=0.001)
    portfolio = TickPortfolio()
    strategy = MyStrategy('range_break')

    engine = BacktestEngine(
        _market_supply=market_supply,
        _execution=execution,
        _portfolio=portfolio,
        _strategy=strategy,
    )

    engine.run()

    portfolio.storeRecords('range_break_tick')

    daily_ret = dailyReturn('range_break_tick', 'range_break')

    wizard = Wizard()
    fund_view = wizard.addView('fund')
    fund_view.addLine('money', daily_ret.index(), daily_ret['fund'])
    wizard.show()
