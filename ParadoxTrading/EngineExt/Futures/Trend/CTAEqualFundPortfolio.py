from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase


class CTAEqualFundPortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _adjust_period: int = 5,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.leverage_rate: float = _leverage_rate

        self.adjust_period = _adjust_period
        self.adjust_count = 0

        self.addPickleKey('adjust_count')

    def _iter_update_next_status(self, _tradingday):
        flag = self._detect_sign_change()
        if self._detect_update_instrument(_tradingday):
            flag = True
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:
            # reset count if adjust
            self.adjust_count = 0

            parts = self._calc_available_product()
            if parts == 0:
                return
            product_fund = self.portfolio_mgr.getStaticFund() \
                           * self.leverage_rate / parts

            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        continue
                    price = self._fetch_buf_price(
                        _tradingday, i_mgr.next_instrument
                    )
                    quantity = round(
                        product_fund / price / POINT_VALUE[i_mgr.product]
                    ) * POINT_VALUE[i_mgr.product]
                    if i_mgr.strength > 0:
                        i_mgr.next_quantity = quantity
                    elif i_mgr.strength < 0:
                        i_mgr.next_quantity = -quantity
                    else:
                        raise Exception('strength == 0 ???')

        else:
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        i_mgr.next_quantity = 0
                    else:
                        i_mgr.next_quantity = i_mgr.cur_quantity
