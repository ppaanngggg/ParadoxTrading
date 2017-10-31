import logging
from datetime import datetime, timedelta

from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Engine import StrategyAbstract, MarketEvent, \
    SettlementEvent, SignalType
from ParadoxTrading.EngineExt.Futures import BacktestEngine, \
    BacktestMarketSupply, BarBacktestExecution, BarPortfolio
from ParadoxTrading.Fetch.ChineseFutures import RegisterInstrument, \
    FetchInstrumentMinData, FetchInstrumentDayData
from ParadoxTrading.Indicator import EMA
from ParadoxTrading.Performance import dailyReturn, FetchRecord

logging.basicConfig(level=logging.INFO)


class MAStrategy(StrategyAbstract):
    def __init__(self):
        super().__init__('ma_rb')

        self.addMarketRegister(RegisterInstrument('rb'))
        self.ema: EMA = EMA(20)
        self.last_status: int = SignalType.EMPTY
        self.empty_time: datetime = None

        self.addPickleKey('ema', 'last_status')

    def deal(self, _market_event: MarketEvent):
        data = _market_event.data
        if self.empty_time is None:
            self.empty_time = datetime.strptime(
                self.engine.getTradingDay(), '%Y%m%d'
            ) + timedelta(hours=14, minutes=45)

        if self.engine.getDatetime() > self.empty_time:
            if self.last_status != SignalType.EMPTY:
                self.addEvent(_market_event.symbol, SignalType.EMPTY)
                self.last_status = SignalType.EMPTY
            return

        closeprice = data['closeprice'][0]
        ema_value = self.ema.addOne(data).getLastData()['ema'][0]

        if len(self.ema) < 10:
            return

        if self.last_status == SignalType.EMPTY:
            if closeprice > ema_value:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                self.last_status = SignalType.LONG
            if closeprice < ema_value:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                self.last_status = SignalType.SHORT
        elif self.last_status == SignalType.LONG:
            if closeprice < ema_value:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                self.last_status = SignalType.SHORT
        elif self.last_status == SignalType.SHORT:
            if closeprice > ema_value:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                self.last_status = SignalType.LONG
        else:
            raise Exception('unknown last status')

    def settlement(self, _settlement_event: SettlementEvent):
        self.ema = EMA(20)
        self.last_status: int = SignalType.EMPTY
        self.empty_time: datetime = None


fetcher = FetchInstrumentMinData()
fetcher_day = FetchInstrumentDayData()
market_supply = BacktestMarketSupply(
    '20171015', '20171020', fetcher
)
portfolio = BarPortfolio(
    fetcher_day, 50_0000, 0.1
)
execution = BarBacktestExecution(5e-4)
engine = BacktestEngine(
    market_supply,
    execution,
    portfolio,
    MAStrategy()
)
engine.run()

portfolio.storeRecords('futures_bar_backtest')
daily_returns = dailyReturn('futures_bar_backtest')
print(daily_returns)
buy_records, sell_record = FetchRecord().fillToBuySell(
    'futures_bar_backtest'
)
day_data = fetcher.fetchDayData('20171015', '20171020', 'rb1801')

wizard = Wizard()

price_view = wizard.addView('price view')
price_view.addCandle(
    'price', day_data.index(), day_data.toRows([
        'openprice', 'highprice', 'lowprice', 'closeprice'
    ])[0]
)
price_view.addScatter('buy', buy_records.index(), buy_records['price'])
price_view.addScatter('sell', sell_record.index(), sell_record['price'])

wizard.show()
