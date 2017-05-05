import logging
import typing

from ParadoxTrading.Engine import EngineAbstract, EventType, ReturnMarket, \
    ReturnSettlement, MarketSupplyAbstract, ExecutionAbstract, PortfolioAbstract, StrategyAbstract


class BacktestEngine(EngineAbstract):
    def __init__(
            self,
            _market_supply: MarketSupplyAbstract,
            _execution: ExecutionAbstract,
            _portfolio: PortfolioAbstract,
            _strategy: typing.Union[StrategyAbstract, typing.Iterable[StrategyAbstract]]
    ):
        """
        Engine used for backtest
        """
        super().__init__(_market_supply, _execution, _portfolio, _strategy)

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

            # loop until finished all the events
            while True:
                if isinstance(ret, ReturnMarket):
                    # !!! the trigger must be ReturnMarket !!!
                    # match market for each tick, maybe there are orders to be filled.
                    # If filled, execution will add fill event into queue
                    # in fact, this is the simulation of exchange
                    self.execution.matchMarket(ret.symbol, ret.data)

                if len(self.event_queue):  # deal all event at that moment
                    event = self.event_queue.popleft()
                    if event.type == EventType.MARKET:
                        self.strategy_dict[event.strategy_name].deal(event)
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

            if isinstance(ret, ReturnSettlement):
                if ret.tradingday:
                    self.portfolio.dealSettlement(
                        ret.tradingday, ret.next_tradingday
                    )
                if not ret.next_tradingday:
                    break
