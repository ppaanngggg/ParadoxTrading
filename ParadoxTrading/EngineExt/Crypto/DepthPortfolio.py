import typing

from ParadoxTrading.Engine import PortfolioAbstract, SignalEvent, FillEvent, \
    OrderEvent, SignalType, OrderType, ActionType, DirectionType
from ParadoxTrading.Utils import DataStruct


class DepthPortfolio(PortfolioAbstract):

    def __init__(
            self, _init_fund: float = 0.0,
            _store_every_day=False,
            _store_path='./records'
    ):
        super().__init__(_init_fund)

        self.store_every_day = _store_every_day
        self.store_path = _store_path

        self.index_strategy_table: typing.Dict[int, str] = {}
        self.symbol_price_dict = {}

    def _gen_order(
            self, _symbol,
            _action: int, _direction: int, _quantity: int
    ) -> OrderEvent:
        return OrderEvent(
            _index=self.incOrderIndex(),
            _symbol=_symbol,
            _tradingday=self.engine.getTradingDay(),
            _datetime=self.engine.getDatetime(),
            _order_type=OrderType.MARKET,
            _action=_action,
            _direction=_direction,
            _quantity=_quantity,
        )

    def dealSignal(self, _event: SignalEvent):
        # add signal event to portfolio record
        self.portfolio_mgr.dealSignal(_event)

        order_qty = abs(_event.strength)

        order_list: typing.List[OrderEvent] = []
        short_qty = self.portfolio_mgr.getPosition(
            _event.symbol, SignalType.SHORT
        )
        long_qty = self.portfolio_mgr.getPosition(
            _event.symbol, SignalType.LONG
        )
        if _event.signal_type == SignalType.LONG:
            if short_qty > 0:  # close short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.BUY,
                    short_qty
                ))
            if long_qty < order_qty:  # open long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.OPEN, DirectionType.BUY,
                    order_qty - long_qty
                ))
        elif _event.signal_type == SignalType.SHORT:
            if long_qty > 0:  # close long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.SELL,
                    long_qty
                ))
            if short_qty < order_qty:  # open short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.OPEN, DirectionType.SELL,
                    order_qty - short_qty
                ))
        elif _event.signal_type == SignalType.EMPTY:
            if short_qty > 0:  # close short position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.BUY,
                    short_qty
                ))
            if long_qty > 0:  # close long position
                order_list.append(self._gen_order(
                    _event.symbol, ActionType.CLOSE, DirectionType.SELL,
                    long_qty
                ))
        else:
            raise Exception('unknown signal type')

        for order in order_list:
            self.index_strategy_table[order.index] = _event.strategy
            self.addEvent(order)
            self.portfolio_mgr.dealOrder(_event.strategy, order)

    def dealFill(self, _event: FillEvent):
        self.portfolio_mgr.dealFill(
            self.index_strategy_table[_event.index], _event
        )

    def dealSettlement(self, _tradingday: str):
        self.portfolio_mgr.dealSettlement(
            _tradingday, self.symbol_price_dict
        )
        # reset if necessary ?
        self.symbol_price_dict = {}

        if self.store_every_day:
            self.portfolio_mgr.getSignalData().toPandas().to_csv(
                '{}/{}_signal.csv'.format(self.store_path, _tradingday, )
            )
            self.portfolio_mgr.getOrderData().toPandas().to_csv(
                '{}/{}_order.csv'.format(self.store_path, _tradingday, )
            )
            self.portfolio_mgr.getFillData().toPandas().to_csv(
                '{}/{}_fill.csv'.format(self.store_path, _tradingday, )
            )
            self.portfolio_mgr.getSettlementData().toPandas().to_csv(
                '{}/{}_settlement.csv'.format(self.store_path, _tradingday, )
            )
            self.portfolio_mgr.resetRecords()

    def dealMarket(self, _symbol: str, _data: DataStruct):
        tmp = _data.toDict()
        self.symbol_price_dict[_symbol] = (tmp['askprice0'] + tmp['bidprice0']) / 2
