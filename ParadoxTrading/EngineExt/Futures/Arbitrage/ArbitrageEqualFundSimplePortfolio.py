from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio, ProductMgr, InstrumentMgr
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase


class ArbitrageEqualFundSimplePortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _adjust_period: int = 5,
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

        self.addPickleKey('adjust_count')

    def _update_dominant_status(
            self, _tradingday: str,
            _i_mgr: InstrumentMgr, _fund: float,
            _point_value: int,
    ):
        symbol = self.fetcher.fetchSymbol(
            _tradingday, _i_mgr.product
        )
        price = self._fetch_buf_price(
            _tradingday, symbol
        )
        quantity = round(
            _fund / price / _point_value
        ) * _point_value
        if quantity > 0:
            if _i_mgr.strength < 0:
                quantity = -quantity
            _i_mgr.next_instrument_dict[symbol] = quantity

    def _update_product_status(
            self, _tradingday: str,
            _i_mgr: InstrumentMgr, _fund: float,
            _point_value: int,
    ):
        # buf all data
        instrument_list = self.fetcher.fetchAvailableInstrument(
            _i_mgr.product, _tradingday
        )
        openinterest_dict = {}
        price_dict = {}
        volume_dict = {}
        for instrument in instrument_list:
            tmp = self.fetcher.fetchData(_tradingday, instrument)
            openinterest_dict[instrument] = tmp['openinterest'][0]
            price_dict[instrument] = tmp[self.settlement_price_index][0]
            volume_dict[instrument] = tmp['volume'][0]

        # product price
        total_openinterest = sum(openinterest_dict.values())
        assert total_openinterest > 0
        product_price = 0.0
        for instrument in instrument_list:
            product_price += price_dict[instrument] * (
                openinterest_dict[instrument] / total_openinterest
            )

        # find higher and lower instrument
        higher_dict = {}
        lower_dict = {}
        for instrument in instrument_list:
            price = price_dict[instrument]
            if price > product_price:
                higher_dict[instrument] = volume_dict[instrument]
            if price < product_price:
                lower_dict[instrument] = volume_dict[instrument]
        higher_instrument = sorted(
            higher_dict.items(), key=lambda x: x[1]
        )[-1][0]
        lower_instrument = sorted(
            lower_dict.items(), key=lambda x: x[1]
        )[-1][0]

        higher_price = price_dict[higher_instrument]
        lower_price = price_dict[lower_instrument]
        rate = (product_price - lower_price) / (higher_price - product_price)

        lower_quantity = _fund / _point_value / (
            lower_price + rate * higher_price
        )
        higher_quantity = rate * lower_quantity
        lower_quantity = round(lower_quantity) * _point_value
        higher_quantity = round(higher_quantity) * _point_value
        if _i_mgr.strength < 0:
            lower_quantity = -lower_quantity
            higher_quantity = -higher_quantity
        if lower_quantity != 0:
            _i_mgr.next_instrument_dict[lower_instrument] = lower_quantity
        if higher_quantity != 0:
            _i_mgr.next_instrument_dict[higher_instrument] = higher_quantity

    def _do_update_next_status(
            self, _tradingday: str,
            _p_mgr: ProductMgr, _strategy_fund: float
    ):
        # count product number
        abs_total = 0
        for i_mgr in _p_mgr:
            abs_total += abs(i_mgr.strength)
        if abs_total == 0:  # all empty, remain all zero
            return

        # set each product
        for i_mgr in _p_mgr:
            if i_mgr.strength == 0:
                continue
            fund = _strategy_fund / abs_total * abs(i_mgr.strength)
            point_value = POINT_VALUE[i_mgr.product]
            if not self.simulate_product_index:
                self._update_dominant_status(
                    _tradingday, i_mgr, fund, point_value
                )
            else:
                self._update_product_status(
                    _tradingday, i_mgr, fund, point_value
                )

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
