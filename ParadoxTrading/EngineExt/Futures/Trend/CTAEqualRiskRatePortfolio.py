import math
import typing

from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio, InstrumentMgr
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Indicator import ReturnRate
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskRatePortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _risk_rate: float = 0.01,
            _adjust_period: int = 5,
            _rate_period: int = 50,
            _leverage_limit: int = 3,
            _simulate_product_index: bool = False,
            _settlement_price_index: str = 'closeprice',
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate,
            _simulate_product_index=_simulate_product_index,
            _settlement_price_index=_settlement_price_index,
        )

        self.risk_rate: float = _risk_rate

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.rate_period = _rate_period
        self.rate_table: typing.Dict[str, ReturnRate] = {}

        self.leverage_limit = _leverage_limit

        self.addPickleKey('adjust_count', 'rate_table')

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
        max_quantity = int(
            self.portfolio_mgr.getStaticFund()
            * self.leverage_limit / price / point_value
        )

        rate_abs = self.rate_table[
            _i_mgr.product
        ].getAllData()['returnrate'][-1]
        rate_abs /= abs(_i_mgr.strength)  # scale by strength

        # unlimit real weight and quantity
        real_w = self.risk_rate / rate_abs
        real_q = _part_fund_alloc * real_w / (price * point_value)

        # real quantity, weight and risk
        real_q = min(max_quantity, real_q)
        real_w = real_q * price * point_value / _part_fund_alloc
        real_v = real_w * rate_abs

        # floor quantity, weight and risk
        floor_q = math.floor(real_q)
        floor_w = floor_q * price * point_value / _part_fund_alloc
        floor_v = floor_w * rate_abs

        # ceil quantity, weight and risk
        ceil_q = math.ceil(real_q)
        ceil_w = ceil_q * price * point_value / _part_fund_alloc
        ceil_v = ceil_w * rate_abs

        return {
            'point_value': point_value, 'instrument': instrument,
            'real_w': real_w, 'real_q': real_q, 'real_v': real_v,
            'floor_w': floor_w, 'floor_q': floor_q, 'floor_v': floor_v,
            'ceil_w': ceil_w, 'ceil_q': ceil_q, 'ceil_v': ceil_v,
            'per_risk': ceil_v - floor_v,
            'diff_risk': ceil_v - real_v
        }

    def _iter_update_next_status(self, _tradingday):
        flag = self._detect_strength_change()
        if self._detect_instrument_change(_tradingday):
            flag = True
        # inc adjust count, adjust if count reach limit period
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:
            self.adjust_count = 0  # reset count if adjust

            parts = self._calc_available_product()
            if parts == 0:
                return
            part_fund_alloc = self.portfolio_mgr.getStaticFund() / parts

            tmp_dict = {}
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:  # remain 0
                        continue
                    # rate not ready
                    if len(self.rate_table[i_mgr.product]) < self.rate_period:
                        continue
                    tmp_dict[i_mgr] = self._get_dict(
                        i_mgr, _tradingday, part_fund_alloc
                    )

            # remove minimum risk
            free_risk_alloc = self.risk_rate * parts
            for d in tmp_dict.values():
                free_risk_alloc -= d['floor_v']
            # sort by risk per quantity
            tmp_tuples = sorted(
                tmp_dict.items(), key=lambda x: x[1]['per_risk']
            )
            for i_mgr, tmp in tmp_tuples:
                if free_risk_alloc > tmp['per_risk']:  # if risk remains
                    quantity = tmp['ceil_q'] * tmp['point_value']
                    if i_mgr.strength < 0:
                        quantity = -quantity
                    free_risk_alloc -= tmp['per_risk']
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

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.rate_table[_symbol].addOne(_data)
        except KeyError:
            self.rate_table[_symbol] = ReturnRate(
                _smooth_period=self.rate_period, _use_abs=True
            )
            self.rate_table[_symbol].addOne(_data)
