import logging
import os
import typing

from ParadoxTrading.Engine import EngineAbstract, EventType, ReturnMarket, ReturnSettlement
from ParadoxTrading.Engine.Strategy import StrategyAbstract
from ParadoxTrading.EngineExt.Futures.InterDayOnlineExecution import InterDayOnlineExecution
from ParadoxTrading.EngineExt.Futures.InterDayOnlineMarketSupply import InterDayOnlineMarketSupply
from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import InterDayPortfolio


class InterDayOnlineEngine(EngineAbstract):
    def __init__(
            self,
            _market_supply: InterDayOnlineMarketSupply,
            _execution: InterDayOnlineExecution,
            _portfolio: InterDayPortfolio,
            _strategy: typing.Union[
                StrategyAbstract, typing.Iterable[StrategyAbstract]
            ],
            _dump_path: str = './save/'):
        """
        Engine used for backtest
        """
        super().__init__(_market_supply, _execution, _portfolio, _strategy)
        self.dump_path = _dump_path

    def load_history(self):
        if os.path.isdir(self.dump_path):
            self.load('{}/Engine'.format(self.dump_path))
            self.market_supply.load(
                '{}/MarketSupply'.format(self.dump_path)
            )
            self.execution.load(
                '{}/Execution'.format(self.dump_path)
            )
            self.portfolio.load(
                '{}/Portfolio'.format(self.dump_path)
            )
            for s in self.strategy_dict.values():
                s.load('{}/{}'.format(self.dump_path, s.name))
        else:
            logging.warning('{} not exists'.format(self.dump_path))

    def save_history(self):
        if not os.path.isdir(self.dump_path):
            os.mkdir(self.dump_path)
        self.save('{}/Engine'.format(self.dump_path))
        self.market_supply.save(
            '{}/MarketSupply'.format(self.dump_path)
        )
        self.execution.save(
            '{}/Execution'.format(self.dump_path)
        )
        self.portfolio.save(
            '{}/Portfolio'.format(self.dump_path)
        )
        for s in self.strategy_dict.values():
            s.save('{}/{}'.format(self.dump_path, s.name))

    def update_position(self):
        self.execution.loadCSV()
        while True:
            if len(self.event_queue):
                event = self.event_queue.popleft()
                if event.type == EventType.FILL:
                    self.portfolio.dealFill(event)
                else:
                    raise Exception('Except FILL event')
            else:
                break

    def update_market_info(self) -> typing.Union[None, str]:
        while True:
            ret = self.market_supply.updateData()
            if ret is None:
                return None

            # strategy receive from market
            # portfolio receive from strategy
            while True:
                if len(self.event_queue):  # deal all event at that moment
                    event = self.event_queue.popleft()
                    if event.type == EventType.MARKET:
                        self.strategy_dict[event.strategy].deal(event)
                    elif event.type == EventType.SIGNAL:
                        self.portfolio.dealSignal(event)
                    elif event.type == EventType.SETTLEMENT:
                        for s in self.strategy_dict.values():
                            s.settlement(event)
                    else:
                        logging.error(event)
                        raise Exception('unavailable event type!')
                else:
                    break

            # update portfolio status if necessary
            if isinstance(ret, ReturnMarket):
                self.portfolio.dealMarket(ret.symbol, ret.data)
            elif isinstance(ret, ReturnSettlement):
                return ret.tradingday
            else:
                raise Exception('unknown return by market supply')

    def update_orders(self, _tradingday):
        self.portfolio.dealSettlement(_tradingday)
        while True:
            if len(self.event_queue):
                event = self.event_queue.popleft()
                if event.type == EventType.ORDER:
                    self.execution.dealOrderEvent(event)
                else:
                    raise Exception('Except ORDER event')
            else:
                break
        self.execution.saveCSV()

    def run(self):
        """
        backtest until there is no market

        :return:
        """
        assert self.market_supply is not None
        assert self.portfolio is not None
        assert self.execution is not None

        assert len(self.event_queue) == 0
        self.update_position()
        assert len(self.event_queue) == 0
        ret = self.update_market_info()
        assert len(self.event_queue) == 0
        if isinstance(ret, str):
            self.update_orders(ret)
        else:
            logging.warning('market info is None')
        assert len(self.event_queue) == 0
