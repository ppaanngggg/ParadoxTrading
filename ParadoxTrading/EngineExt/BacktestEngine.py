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
            if isinstance(ret, ReturnMarket):
                while True:
                    # match market for each tick, maybe there will be order to fill.
                    # If filled, execution will add fill event into queue
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
                            pass
                        else:
                            raise Exception('Unknown event type!')
                    else:
                        break
            elif isinstance(ret, ReturnSettlement):
                if ret.tradingday:
                    self.portfolio.dealSettlement(
                        ret.tradingday, ret.next_tradingday)
                if not ret.next_tradingday:
                    break
            else:
                raise Exception('Unknown ret type!')
