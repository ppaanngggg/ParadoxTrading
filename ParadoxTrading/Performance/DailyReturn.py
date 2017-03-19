import bisect
import typing

from ParadoxTrading.Engine import (ActionType, DirectionType, FillEvent,
                                   SignalType)
from ParadoxTrading.Utils import DataStruct


def _getInterDayDataDict(
        _begin_day: str, _end_day: str,
        _fill_event_list: typing.List[FillEvent],
        _fetch_func: typing.Callable[..., DataStruct]
) -> typing.Dict[str, DataStruct]:
    ret: typing.Dict[str, DataStruct] = {}

    tradingday_set: typing.Set[str] = set()
    symbol_set: typing.Set[str] = set()
    for d in _fill_event_list:
        tradingday_set.add(d.tradingday)
        symbol_set.add(d.symbol)

    tradingday_list = sorted(tradingday_set)
    begin_day = min(_begin_day, tradingday_list[0])
    end_day = max(_end_day, tradingday_list[-1])

    for symbol in symbol_set:
        ret[symbol] = _fetch_func(
            begin_day, end_day, symbol
        )
    return ret


def _calcDailyFund(
        _cur_fund: float,
        _tradingday: str,
        _dailydata_dict: typing.Dict[str, DataStruct],
        _position_dict: typing.Dict[str, typing.Dict[int, int]],
        _price_index: str = 'closeprice'
) -> float:
    fund = _cur_fund
    for k, v in _position_dict.items():
        i = bisect.bisect_right(_dailydata_dict[k].index(),
                                _tradingday) - 1
        closeprice = _dailydata_dict[k].getColumn(_price_index)[i]
        if v[SignalType.LONG] > 0:
            fund += closeprice * _position_dict[k][SignalType.LONG]
        if v[SignalType.SHORT] > 0:
            fund -= closeprice * _position_dict[k][SignalType.SHORT]

    return fund


def dailyReturn(
        _begin_day: str, _end_day: str,
        _fill_event_list: typing.List[FillEvent],
        _fetch_func: typing.Callable[..., DataStruct],
        _init_fund: float = 0.0,
        _price_index: str = 'closeprice'
) -> DataStruct:
    dailydata_dict: typing.Dict[str, DataStruct] = _getInterDayDataDict(
        _begin_day, _end_day, _fill_event_list, _fetch_func)
    position_dict: typing.Dict[str, typing.Dict[int, int]] = {}
    cur_fund: float = _init_fund
    cur_fill_idx = 0

    # use init fund to create DataStruct
    ret = DataStruct(['TradingDay', 'Fund'], 'TradingDay')
    ret.addRow(['', cur_fund], ['TradingDay', 'Fund'])

    tradingday_set = set()
    for v in dailydata_dict.values():
        tradingday_set |= set(v.index())

    for tradingday in sorted(tradingday_set):
        while cur_fill_idx < len(_fill_event_list) and \
                _fill_event_list[cur_fill_idx].tradingday <= tradingday:
            d = _fill_event_list[cur_fill_idx]
            if d.action == ActionType.OPEN:
                if d.direction == DirectionType.BUY:
                    try:
                        position_dict[d.symbol][
                            SignalType.LONG] += d.quantity
                    except KeyError:
                        position_dict[d.symbol] = {
                            SignalType.LONG: d.quantity, SignalType.SHORT: 0
                        }
                    cur_fund -= d.quantity * d.price + d.commission
                elif d.direction == DirectionType.SELL:
                    try:
                        position_dict[d.symbol][
                            SignalType.SHORT] += d.quantity
                    except KeyError:
                        position_dict[d.symbol] = {
                            SignalType.LONG: 0, SignalType.SHORT: d.quantity
                        }
                    cur_fund += d.quantity * d.price - d.commission
                else:
                    raise Exception('unknown direction')
            elif d.action == ActionType.CLOSE:
                if d.direction == DirectionType.BUY:
                    assert position_dict[d.symbol][
                        SignalType.SHORT] > d.quantity
                    position_dict[d.symbol][SignalType.SHORT] -= d.quantity
                    cur_fund -= d.quantity * d.price + d.commission
                elif d.direction == DirectionType.SELL:
                    assert position_dict[d.symbol][
                        SignalType.LONG] > d.quantity
                    position_dict[d.symbol][SignalType.LONG] -= d.quantity
                    cur_fund += d.quantity * d.price - d.commission
                else:
                    raise Exception('unknown direction')
            else:
                raise Exception('unknown action')

            cur_fill_idx += 1

        ret.addRow(
            [tradingday,
             _calcDailyFund(
                 cur_fund, tradingday,
                 dailydata_dict, position_dict,
                 _price_index
             )],
            ['TradingDay', 'Fund']
        )

    return ret
