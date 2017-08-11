import logging
import os
import typing

from ParadoxTrading.Engine import EngineAbstract, EventType, ReturnMarket, \
    ReturnSettlement, MarketSupplyAbstract, ExecutionAbstract, PortfolioAbstract, StrategyAbstract


class CTAOnlineEngine(EngineAbstract):
    def __init__(
            self,
            _market_supply: MarketSupplyAbstract,
            _execution: ExecutionAbstract,
            _portfolio: PortfolioAbstract,
            _strategy: typing.Union[
                StrategyAbstract, typing.Iterable[StrategyAbstract]
            ],
            _dump_path: str = './save/'
    ):
        """
        Engine used for backtest
        """
        super().__init__(
            _market_supply, _execution, _portfolio, _strategy
        )
        self.dump_path = _dump_path
        if os.path.isdir(self.dump_path):
            self.load(self.dump_path)
            self.market_supply.load(self.dump_path)
            self.execution.load(self.dump_path)
            self.portfolio.load(self.dump_path)
            for s in self.strategy_dict.values():
                s.load(self.dump_path)
        else:
            os.mkdir(self.dump_path)

    def run(self):
        """
        backtest until there is no market

        :return:
        """
        assert self.market_supply is not None
        assert self.portfolio is not None
        assert self.execution is not None

        logging.info('Begin RUN!')
        while True:
            ret = self.market_supply.updateData()
            if ret is None:
                break

            # loop until finished all the events
            while True:
                if len(self.event_queue):  # deal all event at that moment
                    event = self.event_queue.popleft()
                    if event.type == EventType.MARKET:
                        self.strategy_dict[event.strategy].deal(event)
                    elif event.type == EventType.SIGNAL:
                        self.portfolio.dealSignal(event)
                    elif event.type == EventType.ORDER:
                        self.execution.dealOrderEvent(event)
                    elif event.type == EventType.FILL:
                        self.portfolio.dealFill(event)
                    elif event.type == EventType.SETTLEMENT:
                        for s in self.strategy_dict.values():
                            s.settlement(event)
                    else:
                        raise Exception('Unknown event type!')
                else:
                    break

            # deal something after all events if necessary
            if isinstance(ret, ReturnSettlement):
                self.portfolio.dealSettlement(
                    ret.tradingday
                )
            elif isinstance(ret, ReturnMarket):
                self.portfolio.dealMarket(ret.symbol, ret.data)
            else:
                raise Exception('unknown ret instance')

        logging.info('End RUN!')
        self.save(self.dump_path)
        self.market_supply.save(self.dump_path)
        self.execution.save(self.dump_path)
        self.portfolio.save(self.dump_path)
        for s in self.strategy_dict.values():
            s.save(self.dump_path)
