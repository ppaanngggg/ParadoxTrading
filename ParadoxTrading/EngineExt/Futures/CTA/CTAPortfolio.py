import typing

import tabulate

from ParadoxTrading.Engine import ActionType, DirectionType, FillEvent, \
    OrderEvent, OrderType, PortfolioAbstract, SignalEvent
from ParadoxTrading.Fetch.FetchAbstract import FetchAbstract
from ParadoxTrading.Utils import DataStruct




class CTAPortfolio(PortfolioAbstract):


class CTAWeightedPortfolio(CTAPortfolio):
    """
    a bit more complex allocation.
    It will keep all the current positions weighted by strength.
    (strength / total_strength * total_fund * margin_rate)
    Because it always adjusts the positions, it's a little
    ugly and expensive in commissions.
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage: float = 1.0,
            _alloc_limit: float = 0.1,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.leverage = _leverage
        self.alloc_limit = _alloc_limit
        self.total_fund: float = 0.0

    def _iter_update_next_position(self, _tradingday):
        total_strength = self._calc_total_strength()
        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:  # no position at all
                    p.next_quantity = 0
                else:
                    # next dominant instrument
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    try:
                        price = self.symbol_price_dict[p.next_instrument]
                    except KeyError:
                        price = self.fetcher.fetchData(
                            _tradingday, p.next_instrument
                        )[self.settlement_price_index][0]
                        self.symbol_price_dict[p.next_instrument] = price
                    p.next_quantity = int(
                        min(self.alloc_limit, p.strength / total_strength) *
                        self.total_fund * self.leverage /
                        price / POINT_VALUE[p.product]
                    ) * POINT_VALUE[p.product]


class CTAWeightedStablePortfolio(CTAPortfolio):
    """
    it also equally alloc the fund, however only triggered when
    signal adjusted. if the strategies, their products or strengths
    changes, it will adjust fund allocation according to their
    strengths
    """

    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage: float = 1.0,
            _alloc_limit: float = 0.1,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.leverage = _leverage
        self.alloc_limit = _alloc_limit
        self.total_fund: float = 0.0

    def _iter_update_next_position(self, _tradingday):
        total_strength = self._calc_total_strength()
        flag = self._detect_change()

        for s in self.strategy_table.values():
            for p in s:
                if p.strength == 0:
                    p.next_quantity = 0
                else:
                    p.next_instrument = self.fetcher.fetchSymbol(
                        _tradingday, _product=p.product
                    )
                    if flag or p.next_instrument != p.cur_instrument:
                        # if strength status changes or instrument changes
                        try:
                            price = self.symbol_price_dict[p.next_instrument]
                        except KeyError:
                            price = self.fetcher.fetchData(
                                _tradingday, p.next_instrument
                            )[self.settlement_price_index][0]
                            self.symbol_price_dict[p.next_instrument] = price
                        p.next_quantity = int(
                            min(self.alloc_limit, p.strength / total_strength) *
                            self.total_fund * self.leverage /
                            price / POINT_VALUE[p.product]
                        ) * POINT_VALUE[p.product]
                    else:
                        p.next_quantity = p.cur_quantity
