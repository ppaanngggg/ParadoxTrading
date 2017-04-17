from ParadoxTrading.Engine import (ActionType, DirectionType, FillEvent,
                                   OrderEvent, OrderType, PortfolioAbstract,
                                   SignalEvent, SignalType)


class TickPortfolio(PortfolioAbstract):
    def __init__(self, _price_index='lastprice'):
        super().__init__()

        self.price_index = 'lastprice'

    def dealSignal(self, _event: SignalEvent):
        assert self.engine is not None

        portfolio = self.getPortfolioByStrategy(_event.strategy_name)

        # create order event
        order_event = OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_event.symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.MARKET, )
        if _event.signal_type == SignalType.LONG:
            # whether there is short position to close
            if portfolio.getShortPosition(_event.symbol) - \
                    portfolio.getCloseBuyUnfilledOrder(_event.symbol) > 0:
                order_event.action = ActionType.CLOSE
            else:
                order_event.action = ActionType.OPEN

            # buy because of long
            order_event.direction = DirectionType.BUY
            order_event.quantity = 1
        elif _event.signal_type == SignalType.SHORT:
            # whether there is long position to close
            if portfolio.getLongPosition(_event.symbol) - \
                    portfolio.getCloseSellUnfilledOrder(_event.symbol) > 0:
                order_event.action = ActionType.CLOSE
            else:
                order_event.action = ActionType.OPEN

            # sell because of short
            order_event.direction = DirectionType.SELL
            order_event.quantity = 1
        else:
            raise Exception('unknown signal')

        portfolio.dealSignalEvent(_event)
        portfolio.dealOrderEvent(order_event)

        self.addEvent(order_event, _event.strategy_name)

    def dealFill(self, _event: FillEvent):
        self.getPortfolioByIndex(_event.index).dealFillEvent(_event)

    def dealSettlement(self, _tradingday, _next_tradingday):
        assert _tradingday

        symbol_price_dict = {}
        for symbol in self.engine.getSymbolList():
            symbol_price_dict[symbol] = \
                self.engine.getSymbolData(symbol)[self.price_index][-1]

        for v in self.strategy_portfolio_dict.values():
            v.dealSettlement(
                _tradingday, _next_tradingday,
                symbol_price_dict
            )
