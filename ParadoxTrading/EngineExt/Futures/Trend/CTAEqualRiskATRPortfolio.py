import math
import typing

from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import \
    InterDayPortfolio, POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Indicator import ATR
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskATRPortfolio(InterDayPortfolio):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _risk_rate: float = 0.01,
            _adjust_period: int = 5,
            _atr_period: int = 50,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.risk_rate: float = _risk_rate

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.atr_period = _atr_period
        self.atr_table: typing.Dict[str, ATR] = {}

        self.addPickleKey('adjust_count', 'atr_table')

    def _iter_update_next_status(self, _tradingday):
        # 1. flag is true if sign change
        flag = self._detect_sign_change()
        # 2. flag is true if instrument change
        if self._detect_update_instrument(_tradingday):
            flag = True
        # 3. inc adjust count, adjust if count reach limit period
        self.adjust_count += 1
        if self.adjust_count >= self.adjust_period:
            flag = True

        if flag:
            # reset count if adjust
            self.adjust_count = 0

            parts = self._calc_parts()
            if parts == 0:
                return
            total_risk_alloc = self.total_fund * self.risk_rate
            part_risk_alloc = total_risk_alloc / parts

            tmp_dict = {}
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        i_mgr.next_quantity = 0
                    else:
                        # if strength status changes or instrument changes
                        atr = self.atr_table[i_mgr.product].getAllData()['atr'][-1]
                        real = part_risk_alloc / atr / POINT_VALUE[i_mgr.product]
                        per_risk = POINT_VALUE[i_mgr.product] * atr
                        tmp_dict[i_mgr] = {
                            'atr': atr, 'real': real, 'per_risk': per_risk,
                            'diff_risk': per_risk * math.ceil(real) - real
                        }

            # reduce minimum risk
            free_risk_alloc = total_risk_alloc
            for d in tmp_dict.values():
                free_risk_alloc -= math.floor(d['real']) * d['per_risk']

            # greedy to contain more product
            tmp_tuples = sorted(tmp_dict.items(), key=lambda x: x[1]['per_risk'])
            for i_mgr, tmp in tmp_tuples:
                if free_risk_alloc > tmp['per_risk']:  # if risk available
                    i_mgr.next_quantity = math.ceil(tmp['real']) * POINT_VALUE[i_mgr.product]
                    if i_mgr.strength < 0:
                        i_mgr.next_quantity = -i_mgr.next_quantity
                    free_risk_alloc -= tmp['per_risk']
                else:
                    i_mgr.next_quantity = math.floor(tmp['real']) * POINT_VALUE[i_mgr.product]
                    if i_mgr.strength < 0:
                        i_mgr.next_quantity = -i_mgr.next_quantity
        else:
            for p_mgr in self.strategy_mgr:
                for i_mgr in p_mgr:
                    if i_mgr.strength == 0:
                        i_mgr.next_quantity = 0
                    else:
                        i_mgr.next_quantity = i_mgr.cur_quantity

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.atr_table[_symbol].addOne(_data)
        except KeyError:
            self.atr_table[_symbol] = ATR(self.atr_period)
            self.atr_table[_symbol].addOne(_data)
