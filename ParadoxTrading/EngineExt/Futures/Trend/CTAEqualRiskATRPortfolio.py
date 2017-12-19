import math
import typing

from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE, \
    InterDayPortfolio, InstrumentMgr
from ParadoxTrading.Fetch.ChineseFutures.FetchBase import FetchBase
from ParadoxTrading.Indicator import ATR
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskATRPortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchBase,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _risk_rate: float = 0.01,
            _adjust_period: int = 5,
            _atr_period: int = 50,
            _leverage_limit: int = 3,
            _simulate_product_index: bool = False,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate,
            _simulate_product_index=_simulate_product_index,
            _settlement_price_index=_settlement_price_index
        )

        self.risk_rate: float = _risk_rate

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.atr_period = _atr_period
        self.atr_table: typing.Dict[str, ATR] = {}

        self.leverage_limit = _leverage_limit

        self.addPickleKey('adjust_count', 'atr_table')

    def _get_dict(
            self, _i_mgr: InstrumentMgr,
            _tradingday: str, _part_risk_alloc: float,
    ):
        # limit max quantity
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

        # if strength status changes or instrument changes
        atr = self.atr_table[
            _i_mgr.product
        ].getAllData()['atr'][-1]
        atr /= abs(_i_mgr.strength)  # scale by strength
        real = _part_risk_alloc / atr / point_value
        per_risk = point_value * atr
        return {
            'point_value': point_value, 'instrument': instrument,
            'atr': atr, 'real': min(real, max_quantity),
            'per_risk': per_risk,
            'diff_risk': per_risk * math.ceil(real) - real
        }

    def _iter_update_next_status(self, _tradingday):
        # 1. flag is true if sign change
        flag = self._detect_strength_change()
        # 2. flag is true if instrument change
        if self._detect_instrument_change(_tradingday):
            flag = True
        # 3. inc adjust count, adjust if count reach limit period
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:
            self.adjust_count = 0  # reset count if adjust

            parts = self._calc_available_product()
            if parts == 0:
                return

            total_fund = self.portfolio_mgr.getStaticFund()
            total_risk_alloc = total_fund * self.risk_rate
            part_risk_alloc = total_risk_alloc / parts

            tmp_dict = {}
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        continue
                    # atr not ready
                    if len(self.atr_table[i_mgr.product]) < self.atr_period:
                        continue
                    tmp_dict[i_mgr] = self._get_dict(
                        i_mgr, _tradingday, part_risk_alloc
                    )

            # reduce minimum risk
            free_risk_alloc = total_risk_alloc
            for d in tmp_dict.values():
                free_risk_alloc -= math.floor(d['real']) * d['per_risk']

            # greedy to contain more product
            tmp_tuples = sorted(
                tmp_dict.items(), key=lambda x: x[1]['per_risk']
            )
            for i_mgr, tmp in tmp_tuples:
                if free_risk_alloc > tmp['per_risk']:  # if risk available
                    quantity = math.ceil(tmp['real']) * tmp['point_value']
                    if i_mgr.strength < 0:
                        quantity = -quantity
                    free_risk_alloc -= tmp['per_risk']
                else:
                    quantity = math.floor(tmp['real']) * tmp['point_value']
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
            self.atr_table[_symbol].addOne(_data)
        except KeyError:
            self.atr_table[_symbol] = ATR(self.atr_period)
            self.atr_table[_symbol].addOne(_data)
