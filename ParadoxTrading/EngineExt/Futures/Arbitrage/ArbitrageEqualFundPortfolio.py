from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio
from ParadoxTrading.Fetch import FetchAbstract


class ArbitrageEqualFundPortfolio(InterDayPortfolio):
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
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.adjust_period = _adjust_period
        self.agjust_count = 0
        self.leverage_rate = _leverage_rate

    def _iter_update_next_status(self, _tradingday):
        flag = self._detect_change()
        # update instrument
        if self._detect_update_instrument(_tradingday):
            flag = True
        self.adjust_period += 1
        if self.adjust_period >= self.adjust_period:
            flag = True

        if flag:
            self.adjust_period = 0

            strategy_num = self._calc_available_strategy()
            if strategy_num == 0:  # all strategy empty
                strategy_fund = 0.0
            else:
                strategy_fund = self.portfolio_mgr.getStaticFund() \
                    * self.leverage_rate / strategy_num
            for p_mgr in self.strategy_mgr:
                product_num = 0
                for i_mgr in p_mgr:
                    if i_mgr.strength != 0:
                        product_num += 1
                if product_num == 0:  # all product empty
                    product_fund = 0.0
                else:
                    product_fund = strategy_fund / product_num
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:  # empty product, skip
                        i_mgr.next_quantity = 0
                    else:
                        price = self._fetch_buf_price(
                            _tradingday, i_mgr.next_instrument
                        )
                        i_mgr.next_quantity = round(
                            i_mgr.strength * product_fund / price /
                            POINT_VALUE[i_mgr.product]
                        ) * POINT_VALUE[i_mgr.product]
        else:
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        i_mgr.next_quantity = 0
                    else:
                        i_mgr.next_quantity = i_mgr.cur_quantity
