import math
import typing

from ParadoxTrading.EngineExt.CTA.CTAPortfolio import CTAPortfolio, POINT_VALUE
from ParadoxTrading.Fetch import FetchAbstract
from ParadoxTrading.Indicator import ReturnRate
from ParadoxTrading.Utils import DataStruct


class CTAEqualRiskRatePortfolio(CTAPortfolio):
    def __init__(
            self,
            _fetcher: FetchAbstract,
            _init_fund: float = 0.0,
            _margin_rate: float = 1.0,
            _risk_rate: float = 0.01,
            _adjust_period: int = 5,
            _rate_period: int = 50,
            _settlement_price_index: str = 'closeprice'
    ):
        super().__init__(
            _fetcher, _init_fund, _margin_rate, _settlement_price_index
        )

        self.risk_rate: float = _risk_rate
        self.total_fund: float = 0.0

        self.adjust_period = _adjust_period
        self.adjust_count = 0
        self.rate_period = _rate_period
        self.rate_table: typing.Dict[str, ReturnRate] = {}

        self.addPickleSet('adjust_count', 'rate_table')

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

            part_fund_alloc = self.total_fund / parts

            tmp_dict = {}
            for s in self.strategy_table.values():
                for p in s:
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        # if strength status changes or instrument changes
                        rate_abs = self.rate_table[p.product].getAllData()['returnrate'][-1]
                        real_w = self.risk_rate / rate_abs
                        real_v = real_w * rate_abs
                        try:
                            price = self.symbol_price_dict[p.next_instrument]
                        except KeyError:
                            price = self.fetcher.fetchData(
                                _tradingday, p.next_instrument
                            )[self.settlement_price_index][0]
                            self.symbol_price_dict[p.next_instrument] = price
                        real_q = part_fund_alloc * real_w / (price * POINT_VALUE[p.product])
                        floor_q = math.floor(real_q)
                        floor_w = floor_q * price * POINT_VALUE[p.product] / part_fund_alloc
                        floor_v = floor_w * rate_abs
                        ceil_q = math.ceil(real_q)
                        ceil_w = ceil_q * price * POINT_VALUE[p.product] / part_fund_alloc
                        ceil_v = ceil_w * rate_abs
                        tmp_dict[p] = {
                            'real_w': real_w, 'real_q': real_q, 'real_v': real_v,
                            'floor_w': floor_w, 'floor_q': floor_q, 'floor_v': floor_v,
                            'ceil_w': ceil_w, 'ceil_q': ceil_q, 'ceil_v': ceil_v,
                            'per_risk': ceil_v - floor_v, 'diff_risk': ceil_v - real_v
                        }

            free_risk_alloc = self.risk_rate * parts
            for d in tmp_dict.values():
                free_risk_alloc -= d['floor_v']

            tmp_tuples = sorted(tmp_dict.items(), key=lambda x: x[1]['per_risk'])
            for p, tmp in tmp_tuples:
                if free_risk_alloc > tmp['per_risk']:
                    p.next_quantity = tmp['ceil_q'] * POINT_VALUE[p.product]
                    if p.strength < 0:
                        p.next_quantity = -p.next_quantity
                    free_risk_alloc -= tmp['per_risk']
                else:
                    p.next_quantity = tmp['floor_q'] * POINT_VALUE[p.product]
                    if p.strength < 0:
                        p.next_quantity = -p.next_quantity
        else:
            for s in self.strategy_table.values():
                for p in s:
                    if p.strength == 0:
                        p.next_quantity = 0
                    else:
                        p.next_quantity = p.cur_quantity

    def dealMarket(self, _symbol: str, _data: DataStruct):
        try:
            self.rate_table[_symbol].addOne(_data)
        except KeyError:
            self.rate_table[_symbol] = ReturnRate(
                self.rate_period, True
            )
            self.rate_table[_symbol].addOne(_data)
