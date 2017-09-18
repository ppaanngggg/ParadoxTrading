import math
import typing

from ParadoxTrading.EngineExt.CTA.CTAPortfolio import CTAPortfolio, POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Indicator import ATR
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskATRPortfolio(CTAPortfolio):
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
        self.total_fund: float = 0.0

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.atr_period = _atr_period
        self.atr_table: typing.Dict[str, ATR] = {}

        self.addPickleSet('adjust_count', 'atr_table')

    def _iter_update_next_position(self, _tradingday):
        # 1. flag is true if sign change
        flag = self._detect_sign_change()
        # 2. flag is true if instrument change
        if self._iter_update_instrument(_tradingday):
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
            for s in self.strategy_table.values():
                for p in s:
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        # if strength status changes or instrument changes
                        atr = self.atr_table[p.product].getAllData()['atr'][-1]
                        real = part_risk_alloc / atr / POINT_VALUE[p.product]
                        per_risk = POINT_VALUE[p.product] * atr
                        tmp_dict[p] = {
                            'atr': atr, 'real': real, 'per_risk': per_risk,
                        }

            # reduce minimum risk
            free_risk_alloc = total_risk_alloc
            for d in tmp_dict.values():
                free_risk_alloc -= math.floor(d['real']) * d['per_risk']

            # greedy to contain more product
            tmp_tuples = sorted(tmp_dict.items(), key=lambda x: x[1]['per_risk'])
            for p, tmp in tmp_tuples:
                if free_risk_alloc > tmp['per_risk']:  # if risk available
                    p.next_quantity = math.ceil(tmp['real']) * POINT_VALUE[p.product]
                    if p.strength < 0:
                        p.next_quantity = -p.next_quantity
                    free_risk_alloc -= tmp['per_risk']
                else:
                    p.next_quantity = math.floor(tmp['real']) * POINT_VALUE[p.product]
                    if p.strength < 0:
                        p.next_quantity = -p.next_quantity
        else:
            for s in self.strategy_table.values():
                for p in s:
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        p.next_quantity = p.cur_quantity

    def dealSettlement(self, _tradingday):
        # check it's the end of prev tradingday
        assert _tradingday

        # 1. get the table map symbols to their price
        self._update_symbol_price_dict(_tradingday)
        # 2. set portfolio settlement
        self._portfolio_settlement(
            _tradingday, self.symbol_price_dict
        )

        # 3 compute current total fund
        self.total_fund = self.portfolio.getStaticFund()

        # 4. update each strategy's positions to current status
        self._iter_update_cur_position()

        # 5. update next status
        self._iter_update_next_position(_tradingday)
        # 6. send new orders
        self._iter_send_order()
        # 7. reset price table
        self.symbol_price_dict = {}

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.atr_table[_symbol].addOne(_data)
        except KeyError:
            self.atr_table[_symbol] = ATR(self.atr_period)
            self.atr_table[_symbol].addOne(_data)
