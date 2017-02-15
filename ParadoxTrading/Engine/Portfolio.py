import typing

import tabulate

import ParadoxTrading.Engine
from ParadoxTrading.Engine.Event import (SignalEvent, OrderEvent, FillEvent)
from ParadoxTrading.Engine.Strategy import StrategyAbstract


class PortfolioPerStrategy:
    def __init__(self):
        self.SIGNAL_TYPE = ParadoxTrading.Engine.SignalType
        self.ORDER_TYPE = ParadoxTrading.Engine.OrderType
        self.ACTION_TYPE = ParadoxTrading.Engine.ActionType
        self.DIRECTION_TYPE = ParadoxTrading.Engine.DirectionType

        # records for signal, order and fill
        self.signal_record: typing.List[SignalEvent] = []
        self.order_record: typing.List[OrderEvent] = []
        self.fill_record: typing.List[FillEvent] = []

        # cur order and position state
        self.position: typing.Dict[str, typing.Dict[str, int]] = {}
        self.unfilled_order: typing.Dict[int, OrderEvent] = {}

    def incPosition(self, _instrument: str, _type: str, _quantity: int = 1):
        """
        inc position of instrument

        :param _instrument: instrument to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == self.SIGNAL_TYPE.LONG or _type == self.SIGNAL_TYPE.SHORT
        assert _quantity > 0
        try:
            # try to add directly
            self.position[_instrument][_type] += _quantity
        except KeyError:
            # create if failed
            self.position[_instrument] = {
                self.SIGNAL_TYPE.LONG: 0,
                self.SIGNAL_TYPE.SHORT: 0,
            }
            self.position[_instrument][_type] = _quantity

    def decPosition(self, _instrument: str, _type: str, _quantity: int = 1):
        """
        dec position of instrument

        :param _instrument: instrument to inc
        :param _type: long or short
        :param _quantity: how many
        :return:
        """
        assert _type == self.SIGNAL_TYPE.LONG or _type == self.SIGNAL_TYPE.SHORT
        assert _quantity > 0
        assert _instrument in self.position.keys()
        assert self.position[_instrument][_type] >= _quantity
        self.position[_instrument][_type] -= _quantity
        assert self.position[_instrument][_type] >= 0

    def getPosition(self, _instrument: str, _type: str) -> int:
        """
        get _type position of instrument

        :param _instrument: which instrument
        :param _type: long or short
        :return: number of position
        """
        assert _type == self.SIGNAL_TYPE.LONG or _type == self.SIGNAL_TYPE.SHORT
        if _instrument in self.position.keys():
            return self.position[_instrument][_type]
        return 0

    def getLongPosition(self, _instrument: str) -> int:
        """
        get long position of instrument

        :param _instrument: which instrument
        :return: number of position
        """
        return self.getPosition(_instrument, self.SIGNAL_TYPE.LONG)

    def getShortPosition(self, _instrument: str) -> int:
        """
        get short position of instrument

        :param _instrument: which instrument
        :return: number of position
        """
        return self.getPosition(_instrument, self.SIGNAL_TYPE.SHORT)

    def getUnfilledOrder(self, _instrument: str, _action: int, _direction: int) -> int:
        """
        get number of unfilled orders for _insturment

        :param _instrument: which instrument
        :param _action: open or close
        :param _direction: buy or sell
        :return: number of unfilled order
        """
        num = 0
        for order in self.unfilled_order.values():
            if order.instrument == _instrument and order.action == _action and order.direction == _direction:
                num += order.quantity

        return num

    def getOpenBuyUnfilledOrder(self, _instrument: str) -> int:
        """
        number of unfilled order which is OPEN and BUY

        :param _instrument:
        :return:
        """
        return self.getUnfilledOrder(_instrument, self.ACTION_TYPE.OPEN, self.DIRECTION_TYPE.BUY)

    def getOpenSellUnfilledOrder(self, _instrument: str) -> int:
        """
        number of unfilled order which is OPEN and SELL

        :param _instrument:
        :return:
        """
        return self.getUnfilledOrder(_instrument, self.ACTION_TYPE.OPEN, self.DIRECTION_TYPE.SELL)

    def getCloseBuyUnfilledOrder(self, _instrument: str) -> int:
        """
        number of unfilled order which is CLOSE and BUY

        :param _instrument:
        :return:
        """
        return self.getUnfilledOrder(_instrument, self.ACTION_TYPE.CLOSE, self.DIRECTION_TYPE.BUY)

    def getCloseSellUnfilledOrder(self, _instrument: str) -> int:
        """
        number of unfilled order which is CLOSE and SELL

        :param _instrument:
        :return:
        """
        return self.getUnfilledOrder(_instrument, self.ACTION_TYPE.CLOSE, self.DIRECTION_TYPE.SELL)

    def dealSignalEvent(self, _signal_event: SignalEvent):
        """
        deal signal event to set inner state

        :param _signal_event:
        :return:
        """
        self.signal_record.append(_signal_event)

    def dealOrderEvent(self, _order_event: OrderEvent):
        """
        deal order event to set inner state

        :param _order_event: order event generated by global portfolio
        :return:
        """
        assert _order_event.index not in self.unfilled_order.keys()
        self.order_record.append(_order_event)
        self.unfilled_order[_order_event.index] = _order_event

    def dealFillEvent(self, _fill_event: FillEvent):
        """
        deal fill event to set inner state

        :param _fill_event:
        :return:
        """
        assert _fill_event.index in self.unfilled_order.keys()
        self.fill_record.append(_fill_event)
        del self.unfilled_order[_fill_event.index]
        if _fill_event.action == self.ACTION_TYPE.OPEN:
            if _fill_event.direction == self.DIRECTION_TYPE.BUY:
                self.incPosition(_fill_event.instrument, self.SIGNAL_TYPE.LONG, _fill_event.quantity)
            elif _fill_event.direction == self.DIRECTION_TYPE.SELL:
                self.incPosition(_fill_event.instrument, self.SIGNAL_TYPE.SHORT, _fill_event.quantity)
            else:
                raise Exception('unknown direction')
        elif _fill_event.action == self.ACTION_TYPE.CLOSE:
            if _fill_event.direction == self.DIRECTION_TYPE.BUY:
                self.decPosition(_fill_event.instrument, self.SIGNAL_TYPE.SHORT, _fill_event.quantity)
            elif _fill_event.direction == self.DIRECTION_TYPE.SELL:
                self.decPosition(_fill_event.instrument, self.SIGNAL_TYPE.LONG, _fill_event.quantity)
            else:
                raise Exception('unknown direction')
        else:
            raise Exception('unknown action')

    def __repr__(self) -> str:
        def action2str(_action: int) -> str:
            if _action == self.ACTION_TYPE.OPEN:
                return 'open'
            elif _action == self.ACTION_TYPE.CLOSE:
                return 'close'
            else:
                raise Exception()

        def direction2str(_direction: int) -> str:
            if _direction == self.DIRECTION_TYPE.BUY:
                return 'buy'
            elif _direction == self.DIRECTION_TYPE.SELL:
                return 'sell'
            else:
                raise Exception()

        ret = '@@@ POSITION @@@\n'

        table = []
        for k, v in self.position.items():
            table.append([k, v[self.SIGNAL_TYPE.LONG], v[self.SIGNAL_TYPE.SHORT]])
        ret += tabulate.tabulate(table, ['instrument', 'LONG', 'SHORT'])

        ret += '\n\n@@@ ORDER @@@\n'

        table = []
        for k, v in self.unfilled_order.items():
            table.append([k, v.instrument, action2str(v.action), direction2str(v.direction), v.quantity])
        ret += tabulate.tabulate(table, ['index', 'instrument', 'ACTION', 'DIRECTION', 'QUANTITY'])

        ret += '\n\n@@@ RECORD @@@\n'
        ret += ' - Signal: ' + str(len(self.signal_record)) + '\n'
        ret += ' - Order: ' + str(len(self.order_record)) + '\n'
        ret += ' - Fill: ' + str(len(self.fill_record)) + '\n'

        return ret


class PortfolioAbstract:
    def __init__(self):
        # redirect to types
        self.SIGNAL_TYPE = ParadoxTrading.Engine.SignalType
        self.ORDER_TYPE = ParadoxTrading.Engine.OrderType
        self.ACTION_TYPE = ParadoxTrading.Engine.ActionType
        self.DIRECTION_TYPE = ParadoxTrading.Engine.DirectionType

        self.order_index: int = 0  # cur unused order index
        self.engine: ParadoxTrading.Engine.Engine.EngineAbstract = None

        self.strategy_portfolio_dict: typing.Dict[str, PortfolioPerStrategy] = {}

    def _setEngine(self, _engine: 'ParadoxTrading.Engine.Engine.EngineAbstract'):
        """
        PROTECTED !!!

        :param _engine: ref to engine
        :return:
        """
        self.engine = _engine

    def incOrderIndex(self) -> int:
        """
        return cur index and inc order index

        :return: cur unused order index
        """
        tmp = self.order_index
        self.order_index += 1
        return tmp

    def addEvent(self, _order_event: OrderEvent):
        """
        add event into engine's engine

        :param _order_event: order event object to be added
        :return:
        """

        # check if it is valid
        assert _order_event.order_type is not None
        assert _order_event.action is not None
        assert _order_event.direction is not None
        assert _order_event.quantity > 0
        if _order_event.order_type == self.ORDER_TYPE.LIMIT:
            assert _order_event.price is not None

        # add it
        self.engine.addEvent(_order_event)

    def dealSignal(self, _event: SignalEvent):
        """
        deal signal event from stategy

        :param _event: signal event to deal
        :return:
        """
        raise NotImplementedError('dealSignal not implemented')

    def dealFill(self, _event: FillEvent):
        """
        deal fill event from execute

        :param _event: fill event to deal
        :return:
        """
        raise NotImplementedError('dealFill not implemented')

    def addStrategy(self, _strategy: StrategyAbstract):
        """
        add strategy into portfolio manager

        :param _strategy: strategy to be added
        :return:
        """
        raise NotImplementedError('addStrategy not implemented')

    def getPortfolioByStrategy(self, _strategy_name: str) -> PortfolioPerStrategy:
        """
        get the individual portfolio manager of strategy

        :param _strategy_name: key
        :return:
        """
        return self.strategy_portfolio_dict[_strategy_name]


class SimplePortfolio(PortfolioAbstract):
    def __init__(self):
        super().__init__()

        self.order_strategy_dict: typing.Dict[int, str] = {}

    def dealSignal(self, _event: SignalEvent):
        assert self.engine is not None

        portfolio = self.getPortfolioByStrategy(_event.strategy_name)

        # create order event
        order_event = OrderEvent(
            _index=self.incOrderIndex(),
            _instrument=_event.instrument,
            _datetime=self.engine.getCurDatetime(),
            _order_type=self.ORDER_TYPE.MARKET,
        )
        if _event.signal_type == self.SIGNAL_TYPE.LONG:
            # whether there is short position to close
            if portfolio.getShortPosition(_event.instrument) - \
                    portfolio.getCloseBuyUnfilledOrder(_event.instrument) > 0:
                order_event.action = self.ACTION_TYPE.CLOSE
            else:
                order_event.action = self.ACTION_TYPE.OPEN

            # buy because of long
            order_event.direction = self.DIRECTION_TYPE.BUY
            order_event.quantity = 1
        elif _event.signal_type == self.SIGNAL_TYPE.SHORT:
            # whether there is long position to close
            if portfolio.getLongPosition(_event.instrument) - \
                    portfolio.getCloseSellUnfilledOrder(_event.instrument) > 0:
                order_event.action = self.ACTION_TYPE.CLOSE
            else:
                order_event.action = self.ACTION_TYPE.OPEN

            # sell because of short
            order_event.direction = self.DIRECTION_TYPE.SELL
            order_event.quantity = 1
        else:
            raise Exception('unknown signal')

        portfolio.dealSignalEvent(_event)
        portfolio.dealOrderEvent(order_event)

        self.order_strategy_dict[order_event.index] = _event.strategy_name
        self.addEvent(order_event)

    def dealFill(self, _event: FillEvent):
        portfolio = self.getPortfolioByStrategy(self.order_strategy_dict[_event.index])
        portfolio.dealFillEvent(_event)

    def addStrategy(self, _strategy: StrategyAbstract):
        assert _strategy.name not in self.strategy_portfolio_dict.keys()
        self.strategy_portfolio_dict[_strategy.name] = PortfolioPerStrategy()
