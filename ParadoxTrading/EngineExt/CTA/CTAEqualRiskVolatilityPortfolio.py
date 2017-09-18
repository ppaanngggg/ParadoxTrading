import typing

from ParadoxTrading.EngineExt.CTA.CTAPortfolio import CTAPortfolio, POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Indicator import Volatility
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskVolatilityPortfolio(CTAPortfolio):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _risk_rate: float = 0.6,
            _adjust_period: int = 5,
            _volatility_period: int = 30,
            _volatility_smooth: int = 12,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.risk_rate: float = _risk_rate
        self.total_fund: float = 0.0

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.volatility_period = _volatility_period
        self.volatility_smooth = _volatility_smooth
        self.volatility_table: typing.Dict[str, Volatility] = {}

        self.addPickleSet('adjust_count', 'volatility_table')

    def _iter_update_next_position(self, _tradingday):
        flag = self._detect_sign_change()
        # inc adjust count, adjust if count reach limit period
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True
        # reset count if adjust
        if flag:
            self.adjust_count = 0
        parts = self._calc_parts()

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
                        tmp_v = self.volatility_table[p.product].getAllData()[
                            'volatility'][-1]
                        try:
                            price = self.symbol_price_dict[p.next_instrument]
                        except KeyError:
                            price = self.fetcher.fetchData(
                                _tradingday, p.next_instrument
                            )[self.settlement_price_index][0]
                            self.symbol_price_dict[p.next_instrument] = price
                        if p.strength > 0:
                            p.next_quantity = int(
                                self.total_fund * self.risk_rate / parts / tmp_v
                                / price / POINT_VALUE[p.product]
                            ) * POINT_VALUE[p.product]
                        if p.strength < 0:
                            p.next_quantity = -int(
                                self.total_fund * self.risk_rate / parts / tmp_v
                                / price / POINT_VALUE[p.product]
                            ) * POINT_VALUE[p.product]
                    else:
                        p.next_quantity = p.cur_quantity

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        self._update_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._portfolio_settlement(
            _tradingday, self.symbol_price_dict
        )

        # 3 compute current total fund
        self.total_fund = self.portfolio.getStaticFund()

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()

        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()
        # 7. reset price table
        self.symbol_price_dict = {}

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.volatility_table[_symbol].addOne(_data)
        except KeyError:
            self.volatility_table[_symbol] = Volatility(
                _period=self.volatility_period,
                _factor=252, _smooth=self.volatility_smooth,
                _use_key=self.settlement_price_index,
            )
            self.volatility_table[_symbol].addOne(_data)
