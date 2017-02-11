from ParadoxTrading.Engine import BacktestEngine, StrategyAbstract
from ParadoxTrading.Engine.Event import SignalEvent
from ParadoxTrading.Indicator import MA


class MAStrategy(StrategyAbstract):

    def init(self):
        self.rb_state = None

        self.ma_rb_5 = MA(5, 'lastprice')
        self.ma_rb_10 = MA(10, 'lastprice')

        self.ag_state = None

        self.ma_ag_5 = MA(5, 'lastprice')
        self.ma_ag_10 = MA(10, 'lastprice')

        self.addMarketRegister(_product='rb', _minute_skip=1)
        self.addMarketRegister(_product='ag', _minute_skip=1)

    def deal(self, _market_register_key: str):
        market_reg = self.getMarketRegister(_market_register_key)
        if market_reg.product == 'rb':
            self.ma_rb_5.addOne(
                market_reg.data.getCurBarBeginTime(),
                market_reg.data.getCurBar())
            self.ma_rb_10.addOne(
                market_reg.data.getCurBarBeginTime(),
                market_reg.data.getCurBar())
            rb_5 = self.ma_rb_5.getAllData().getColumn('ma')[-1]
            rb_10 = self.ma_rb_10.getAllData().getColumn('ma')[-1]
            if rb_5 > rb_10:
                if self.rb_state == SignalEvent.SHORT:
                    self.addEvent(market_reg.cur_data_inst, SignalEvent.LONG)
                self.rb_state = SignalEvent.LONG
            if rb_5 < rb_10:
                if self.rb_state == SignalEvent.LONG:
                    self.addEvent(market_reg.cur_data_inst, SignalEvent.SHORT)
                self.rb_state = SignalEvent.SHORT
        elif market_reg.product == 'ag':
            self.ma_ag_5.addOne(
                market_reg.data.getCurBarBeginTime(),
                market_reg.data.getCurBar())
            self.ma_ag_10.addOne(
                market_reg.data.getCurBarBeginTime(),
                market_reg.data.getCurBar())
            ag_5 = self.ma_ag_5.getAllData().getColumn('ma')[-1]
            ag_10 = self.ma_ag_10.getAllData().getColumn('ma')[-1]
            if ag_5 > ag_10:
                if self.ag_state == SignalEvent.SHORT:
                    self.addEvent(market_reg.cur_data_inst, SignalEvent.LONG)
                self.ag_state = SignalEvent.LONG
            if ag_5 < ag_10:
                if self.ag_state == SignalEvent.LONG:
                    self.addEvent(market_reg.cur_data_inst, SignalEvent.SHORT)
                self.ag_state = SignalEvent.SHORT
        else:
            raise Exception('unkonw product')

ma_strategy = MAStrategy('ma')

engine = BacktestEngine('20170119', '20170126')
engine.addStrategy(ma_strategy)

engine.run()

# while True:
#     ret = engine.market_supply.updateData()
#     if ret:
#         pass
#     else:
#         break
