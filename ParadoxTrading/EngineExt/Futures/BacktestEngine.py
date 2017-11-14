import logging

import typing
from ParadoxTrading.Engine import EngineAbstract, EventType, \
    ExecutionAbstract, MarketSupplyAbstract, PortfolioAbstract, ReturnMarket, \
    ReturnSettlement, StrategyAbstract


class BacktestEngine(EngineAbstract):
    def __init__(
            self,
            _market_supply: MarketSupplyAbstract,
            _execution: ExecutionAbstract,
            _portfolio: PortfolioAbstract,
            _strategy: typing.Union[
                StrategyAbstract, typing.Iterable[StrategyAbstract]
            ]
    ):
        """
        Engine used for backtest
        """
        super().__init__(
            _market_supply, _execution, _portfolio, _strategy
        )

    def run(self):
        """
        backtest until there is no market tick

        :return:
        """
        assert self.market_supply is not None
        assert self.portfolio is not None
        assert self.execution is not None

        logging.info('Begin RUN!')
        while True:
            ret = self.market_supply.updateData()
            if ret is None:
                return

            # loop until finished all the events
            while True:
                if isinstance(ret, ReturnMarket):
                    # !!! the trigger must be ReturnMarket !!!
                    # match market for each tick,
                    # maybe there are orders to be filled.
                    # If filled, execution will add fill event into queue
                    # in fact, this is the simulation of exchange
                    self.execution.matchMarket(ret.symbol, ret.data)

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
