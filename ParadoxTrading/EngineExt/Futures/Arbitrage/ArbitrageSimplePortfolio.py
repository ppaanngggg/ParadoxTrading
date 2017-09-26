from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import \
    InterDayPortfolio, POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract


class ArbitrageSimplePortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )
        self.leverage_rate = _leverage_rate

    def _iter_update_next_status(self, _tradingday):
        # update instrument
        self._detect_update_instrument(_tradingday)

        strategy_num = self._calc_available_strategy()
        if strategy_num == 0:  # all strategy empty, return
            return
        strategy_fund = self.total_fund * self.leverage_rate / strategy_num
        for p_mgr in self.strategy_mgr:
            product_num = 0
            for i_mgr in p_mgr:
                if i_mgr.strength != 0:
                    product_num += 1
            if product_num == 0:  # empty strategy, skip
                continue
            product_fund = strategy_fund / product_num
            for i_mgr in p_mgr:
                if i_mgr.strength == 0:  # empty product, skip
                    continue
                price = self._fetch_buf_price(_tradingday, i_mgr.next_instrument)
                i_mgr.next_quantity = int(
                    i_mgr.strength * product_fund / price /
                    POINT_VALUE[i_mgr.product]
                ) * POINT_VALUE[i_mgr.product]
