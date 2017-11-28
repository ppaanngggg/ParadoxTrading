import typing

from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InstrumentMgr, InterDayPortfolio, ProductMgr
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Indicator import FastVolatility
from ParadoxTrading.Utils import DataStruct


class ArbitrageEqualFundVolatilityPortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _adjust_period: int = 5,
            _volatility_period: int = 30,
            _volatility_smooth: int = 12,
            _simulate_product_index: bool = False,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate,
            _simulate_product_index=_simulate_product_index,
            _settlement_price_index=_settlement_price_index,
        )

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.leverage_rate = _leverage_rate
        self.volatility_period = _volatility_period
        self.volatility_smooth = _volatility_smooth
        self.volatility_table: typing.Dict[str, FastVolatility] = {}

        self.addPickleKey('adjust_count', 'volatility_table')

    def _do_update_next_status(
            self, _tradingday: str,
            _p_mgr: ProductMgr, _strategy_fund: float
    ):
        # count product number
        num = 0
        tmp_table: typing.Dict[InstrumentMgr, float] = {}
        vol_sum = 0.0
        for i_mgr in _p_mgr:
            if i_mgr.strength == 0:
                continue
            num += 1
            vol = self.volatility_table[
                i_mgr.product
            ].getAllData()['volatility'][-1]
            vol = vol ** 2
            tmp_table[i_mgr] = 1. / vol
            vol_sum += 1. / vol
        if num == 0:  # all empty, remain all zero
            return

        for i_mgr, vol in tmp_table.items():
            fund = _strategy_fund * (vol / vol_sum)
            symbol = self.fetcher.fetchSymbol(
                _tradingday, i_mgr.product
            )
            price = self._fetch_buf_price(
                _tradingday, symbol
            )
            quantity = round(
                fund / price / POINT_VALUE[i_mgr.product]
            ) * POINT_VALUE[i_mgr.product]
            if i_mgr.strength > 0:
                i_mgr.next_instrument_dict[symbol] = quantity
            elif i_mgr.strength < 0:
                i_mgr.next_instrument_dict[symbol] = -quantity
            else:
                raise Exception('strength == 0 ???')

    def _iter_update_next_status(self, _tradingday: str):
        # detect any sign changes
        flag = self._detect_sign_change()
        # update instrument and detect changes
        if self._detect_instrument_change(_tradingday):
            flag = True
        # inc adjust count
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:  # to rebalance
            self.adjust_count = 0

            strategy_num = self._calc_available_strategy()
            if strategy_num == 0:  # all strategy empty
                return
            # available fund for each strategy
            strategy_fund = self.portfolio_mgr.getStaticFund() * (
                self.leverage_rate / strategy_num
            )
            for p_mgr in self.strategy_mgr:
                self._do_update_next_status(
                    _tradingday, p_mgr, strategy_fund
                )
        else:  # anything unchanged
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        continue
                    # copy current status
                    i_mgr.next_instrument_dict = i_mgr.cur_instrument_dict

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.volatility_table[_symbol].addOne(_data)
        except KeyError:
            self.volatility_table[_symbol] = FastVolatility(
                _period=self.volatility_period,
                _factor=252, _smooth=self.volatility_smooth,
                _use_key=self.settlement_price_index,
            )
            self.volatility_table[_symbol].addOne(_data)
