import logging

from ParadoxTrading.Engine import EngineAbstract
from ParadoxTrading.Engine import EventType


class BacktestEngine(EngineAbstract):
    def __init__(self):
        """
        Engine used for backtest
        """
        super().__init__()

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
            data = self.market_supply.updateData()
            if data is None:
                # if data is None, it means end
                break
            logging.debug('Data({}) {}'.format(
                data[0], data[1].toDict()
            ))
            while True:
                # match market for each tick, maybe there will be order to fill.
                # If filled, execution will add fill event into queue
                self.execution.matchMarket(*data)

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
                    else:
                        raise Exception('Unknow event type!')
                else:
                    break
