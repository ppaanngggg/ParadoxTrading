import logging
from datetime import datetime, timedelta

from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Engine import MarketEvent, SettlementEvent, SignalType, \
    StrategyAbstract
from ParadoxTrading.EngineExt.Futures import TickBacktestExecution, TickPortfolio
from ParadoxTrading.EngineExt import BacktestEngine, BacktestMarketSupply
from ParadoxTrading.Fetch.ChineseFutures import FetchInstrumentDayData, \
    FetchInstrumentTickData, RegisterInstrument
from ParadoxTrading.Indicator import EMA
from ParadoxTrading.Performance import FetchRecord, dailyReturn

logging.basicConfig(level=logging.INFO)


class RangeStrategy(StrategyAbstract):
    def __init__(self):
        super().__init__('range_rb')

        self.addMarketRegister(RegisterInstrument('rb'))
        self.ask_ema: EMA = EMA(60, _use_key='askprice')
        self.bid_ema: EMA = EMA(60, _use_key='bidprice')
        self.last_status: int = SignalType.EMPTY
        self.empty_time: datetime = None

        self.addPickleKey('last_status')

    def deal(self, _market_event: MarketEvent):
        if self.empty_time is None:
            self.empty_time = datetime.strptime(
                self.engine.getTradingDay(), '%Y%m%d'
            ) + timedelta(hours=14, minutes=45)

        if self.engine.getDatetime() > self.empty_time:
            if self.last_status != SignalType.EMPTY:
                self.addEvent(_market_event.symbol, SignalType.EMPTY)
                self.last_status = SignalType.EMPTY
            return

        data = _market_event.data
        lastprice = data['lastprice'][0]
        ask_ema_value = self.ask_ema.addOne(data).getLastData()['ema'][0]
        bid_ema_value = self.bid_ema.addOne(data).getLastData()['ema'][0]

        if len(self.ask_ema) < 60:
            return

        if self.last_status == SignalType.EMPTY:
            if lastprice > ask_ema_value:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                self.last_status = SignalType.LONG
            if lastprice < bid_ema_value:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                self.last_status = SignalType.SHORT
        elif self.last_status == SignalType.LONG:
            if lastprice < bid_ema_value:
                self.addEvent(_market_event.symbol, SignalType.SHORT)
                self.last_status = SignalType.SHORT
        elif self.last_status == SignalType.SHORT:
            if lastprice > ask_ema_value:
                self.addEvent(_market_event.symbol, SignalType.LONG)
                self.last_status = SignalType.LONG
        else:
            raise Exception('unknown last status')

    def settlement(self, _settlement_event: SettlementEvent):
        # self.ask_ema = EMA(60, _use_key='askprice')
        # self.bid_ema = EMA(60, _use_key='bidprice')
        self.last_status: int = SignalType.EMPTY
        self.empty_time: datetime = None


fetcher_tick = FetchInstrumentTickData()
fetcher_day = FetchInstrumentDayData()
market_supply = BacktestMarketSupply(
    '20171016', '20171017', fetcher_tick
)
portfolio = TickPortfolio(
    fetcher_day, 50_0000, 0.15
)
execution = TickBacktestExecution(3e-4)
strategy = RangeStrategy()
engine = BacktestEngine(
    market_supply,
    execution,
    portfolio,
    strategy,
)
engine.run()

portfolio.storeRecords('futures_tick_backtest')
daily_returns = dailyReturn('futures_tick_backtest')
print(daily_returns)
buy_records, sell_record = FetchRecord().fillToBuySell(
    'futures_tick_backtest'
)
day_data = fetcher_tick.fetchDayData(
    '20171016', '20171017', 'rb1801'
)

wizard = Wizard()

price_view = wizard.addView('price view')
price_view.addLine(
    'price', day_data.index(), day_data['lastprice']
)
ask_ema_data = strategy.ask_ema.getAllData()
bid_ema_data = strategy.bid_ema.getAllData()
price_view.addLine(
    'ask ema', ask_ema_data.index(), ask_ema_data['ema']
)
price_view.addLine(
    'bid ema', bid_ema_data.index(), bid_ema_data['ema']
)
if len(buy_records):
    price_view.addScatter(
        'buy', buy_records.index(), buy_records['price']
    )
if len(sell_record):
    price_view.addScatter(
        'sell', sell_record.index(), sell_record['price']
    )

wizard.show()
