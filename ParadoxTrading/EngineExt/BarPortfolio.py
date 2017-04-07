import logging

from ParadoxTrading.Engine import OrderType, OrderEvent, SignalType, \
    ActionType, DirectionType, FillEvent, SignalEvent
from ParadoxTrading.Engine import PortfolioAbstract


class BarPortfolio(PortfolioAbstract):
    def __init__(self, _price_index: str = 'closeprice'):
        """
        :param _price_index: the index of price which will be used to deal
        """
        super().__init__()

        self.price_index = _price_index

    def setPriceIndex(self, _index: str):
        self.price_index = _index

    def dealSignal(self, _event: SignalEvent):
        logging.debug('Portfolio recv {}'.format(_event.toDict()))
        portfolio = self.getPortfolioByStrategy(_event.strategy_name)

        order_event = OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_event.symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.LIMIT,
        )
        if _event.signal_type == SignalType.LONG:
            if portfolio.getShortPosition(_event.symbol) - \
                    portfolio.getCloseBuyUnfilledOrder(_event.symbol) > 0:
                order_event.action = ActionType.CLOSE
            else:
                order_event.action = ActionType.OPEN
            order_event.direction = DirectionType.BUY
        elif _event.signal_type == SignalType.SHORT:
            if portfolio.getLongPosition(_event.symbol) - \
                    portfolio.getCloseSellUnfilledOrder(_event.symbol) > 0:
                order_event.action = ActionType.CLOSE
            else:
                order_event.action = ActionType.OPEN
            order_event.direction = DirectionType.SELL
        else:
            raise Exception('unknown signal')

        data = self.engine.getSymbolData(_event.symbol)
        order_event.price = data[self.price_index][-1]

        self.addEvent(order_event, _event.strategy_name)

        portfolio.dealSignalEvent(_event)
        portfolio.dealOrderEvent(order_event)

    def dealFill(self, _event: FillEvent):
        self.getPortfolioByIndex(_event.index).dealFillEvent(_event)
