import math

from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio, InstrumentMgr
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase


class CTAEqualFundPortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _leverage_rate: float = 1.0,
            _adjust_period: int = 5,
            _simulate_product_index: bool = True,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate,
            _simulate_product_index=_simulate_product_index,
            _settlement_price_index=_settlement_price_index,
        )

        self.leverage_rate: float = _leverage_rate

        self.adjust_period = _adjust_period
        self.adjust_count = 0

        self.addPickleKey('adjust_count')

    def _get_dict(
            self, _i_mgr: InstrumentMgr,
            _tradingday: str, _part_fund_alloc: float
    ):
        point_value = POINT_VALUE[_i_mgr.product]
        instrument = self.fetcher.fetchSymbol(
            _tradingday, _product=_i_mgr.product
        )
        price = self._fetch_buf_price(
            _tradingday, instrument
        )
        per_fund = price * point_value
        real_q = _part_fund_alloc / per_fund
        return {
            'point_value': point_value,
            'instrument': instrument,
            'price': price,
            'real_q': real_q,
            'floor_q': math.floor(real_q),
            'ceil_q': math.floor(real_q),
            'per_fund': per_fund,
        }

    def _iter_update_next_status(self, _tradingday):
        flag = self._detect_sign_change()
        if self._detect_instrument_change(_tradingday):
            flag = True
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:
            self.adjust_count = 0  # reset count if adjust

            parts = self._calc_available_product()
            if parts == 0:
                return
            total_fund = \
                self.portfolio_mgr.getStaticFund() * self.leverage_rate
            part_fund_alloc = total_fund / parts

            tmp_dict = {}
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        continue
                    tmp_dict[i_mgr] = self._get_dict(
                        i_mgr, _tradingday, part_fund_alloc
                    )

            free_fund = total_fund
            for d in tmp_dict.values():
                free_fund -= d['floor_q'] * d['per_fund']
            # sort by per fund
            tmp_tuples = sorted(
                tmp_dict.items(), key=lambda x: x[1]['per_fund']
            )
            for i_mgr, tmp in tmp_tuples:
                if free_fund > tmp['per_fund']:
                    quantity = tmp['ceil_q'] * tmp['point_value']
                    if i_mgr.strength < 0:
                        quantity = -quantity
                    free_fund -= tmp['per_fund']
                else:
                    quantity = tmp['floor_q'] * tmp['point_value']
                    if i_mgr.strength < 0:
                        quantity = -quantity
                if quantity != 0:
                    i_mgr.next_instrument_dict[tmp['instrument']] = quantity
        else:
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        continue
                    # copy current status
                    i_mgr.next_instrument_dict = i_mgr.cur_instrument_dict
