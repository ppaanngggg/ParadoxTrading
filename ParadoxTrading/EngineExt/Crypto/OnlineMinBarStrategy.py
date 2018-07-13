from ParadoxTrading.Engine.Strategy import StrategyAbstract, MarketEvent, \
    SettlementEvent
from ParadoxTrading.Utils import DataStruct
from ParadoxTrading.Utils.Split import SplitIntoMinute


class OnlineMinBarStrategy(StrategyAbstract):
    def __init__(self, _name, _min_periods=(1,)):
        super().__init__(_name)

        self.min_periods = _min_periods
        self.split_dict = {}

    def deal(self, _market_event: MarketEvent):
        """
        recv ticker data from market supply,
        and split into min, the call do_deal(...)

        :param _market_event:
        :return:
        """
        symbol = _market_event.symbol
        data = _market_event.data

        for period in self.min_periods:
            key = symbol + (period,)
            try:
                spliter = self.split_dict[key]
            except KeyError:
                spliter = self.split_dict[key] = SplitIntoMinute(period)

            flag = False
            for d in data:
                flag = flag or spliter.addOne(d)
            if flag:  # gen new bar
                if len(spliter.getBarList()) > 1:  # has finished bar
                    last_bar = spliter.getBarList().pop(0)
                    last_begin_time = spliter.getBarBeginTimeList().pop(0)
                    last_end_time = spliter.getBarEndTimeList().pop(0)
                    price_list = last_bar['price']
                    volume_list = last_bar['amount']
                    bar_data = DataStruct([
                        'open', 'high', 'low', 'close', 'volume',
                        'begin_time', 'end_time'
                    ], 'begin_time')
                    bar_data.addDict({
                        'open': price_list[0],
                        'high': max(price_list),
                        'low': min(price_list),
                        'close': price_list[-1],
                        'volume': sum(volume_list),
                        'begin_time': last_begin_time,
                        'end_time': last_end_time
                    })
                    self.do_deal(MarketEvent(
                        _market_event.market_register_key,
                        _market_event.strategy,
                        _market_event.symbol,
                        bar_data
                    ), period)

    def do_deal(self, _market_event: MarketEvent, _period):
        """
        the true deal function for user, it will recv min data

        :param _market_event:
        :return:
        """
        raise NotImplementedError

    def settlement(self, _settlement_event: SettlementEvent):
        raise NotImplementedError
