import logging

from ParadoxTrading.Engine import OrderType, OrderEvent, SignalType, \
    ActionType, DirectionType, FillEvent, SignalEvent
from ParadoxTrading.Engine import PortfolioAbstract
from ParadoxTrading.Utils import DataStruct


class BarPortfolio(PortfolioAbstract):
    """
    It is a simple portfolio management mod.
    When it receive a signal event, it will send a 1 hand limit order,
    and the limit price will be set as the latest closeprice. And it will
    check the portfolio of strategy which sends the event, it will close 
    positions if possible
    
    :param _price_index: the index of price which will be used to deal
    """

    def __init__(self, _price_index: str = 'closeprice', _use_market: bool = False):
        super().__init__()

        self.price_index = _price_index
        self.use_market = _use_market

    def setPriceIndex(self, _index: str):
        self.price_index = _index

    def dealSignal(self, _event: SignalEvent):
        """
        deal with the signal        
        
        :param _event: 
        :return: 
        """
        logging.debug('Portfolio recv {}'.format(_event.toDict()))
        portfolio = self.getPortfolioByStrategy(_event.strategy_name)

        order_event = OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_event.symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.MARKET if self.use_market else OrderType.LIMIT,
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

        if not self.use_market:
            data = self.engine.getSymbolData(_event.symbol)
            order_event.price = data[self.price_index][-1]

        self.addEvent(order_event, _event.strategy_name)

        portfolio.dealSignalEvent(_event)
        portfolio.dealOrderEvent(order_event)

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

    def dealMarket(self, _symbol: str, _data: DataStruct):
        pass
