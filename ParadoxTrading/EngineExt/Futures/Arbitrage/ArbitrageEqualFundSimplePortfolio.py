from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio, ProductMgr
from ParadoxTrading.Fetch import FetchAbstract


class ArbitrageEqualFundSimplePortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _adjust_period: int = 5,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate,
            _settlement_price_index
        )

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.leverage_rate = _leverage_rate

        self.addPickleKey('adjust_count')

    def _do_update_next_status(
            self, _tradingday: str,
            _p_mgr: ProductMgr, _strategy_fund: float
    ):
        # count product number
        num = 0
        for i_mgr in _p_mgr:
            if i_mgr.strength == 0:
                continue
            num += 1
        if num == 0:  # all empty, remain all zero
            return

        fund = _strategy_fund / num
        for i_mgr in _p_mgr:
            if i_mgr.strength == 0:
                continue
            price = self._fetch_buf_price(
                _tradingday, i_mgr.next_instrument
            )
            quantity = round(
                fund / price / POINT_VALUE[i_mgr.product]
            ) * POINT_VALUE[i_mgr.product]
            if i_mgr.strength > 0:
                i_mgr.next_quantity = quantity
            elif i_mgr.strength < 0:
                i_mgr.next_quantity = -quantity
            else:
                raise Exception('strength == 0 ???')

    def _iter_update_next_status(self, _tradingday: str):
        # detect any sign changes
        flag = self._detect_sign_change()
        # update instrument and detect changes
        if self._detect_update_instrument(_tradingday):
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
                        i_mgr.next_quantity = 0
                    else:
                        i_mgr.next_quantity = i_mgr.cur_quantity
