import bisect
import typing

from ParadoxTrading.Engine import ActionType, DirectionType, SignalType, \
    FillEvent
from ParadoxTrading.Utils import DataStruct, Fetch


def _getInstrumentInterDayDataDict(
        _fill_event_list: typing.List[FillEvent]
) -> typing.Dict[str, DataStruct]:
    ret: typing.Dict[str, DataStruct] = {}

    tradingday_set: typing.Set[str] = set()
    instrument_set: typing.Set[str] = set()
    for d in _fill_event_list:
        tradingday_set.add(d.tradingday)
        instrument_set.add(d.instrument)

    tradingday_list = sorted(tradingday_set)
    begin_day = tradingday_list[0]
    end_day = tradingday_list[-1]

    for inst in instrument_set:
        ret[inst] = Fetch.fetchInterDayData(
            inst, begin_day, end_day
        )
    return ret


def _calcDailyFund(
        _cur_fund: float,
        _tradingday: str,
        _inst_dailydata_dict: typing.Dict[str, DataStruct],
        _inst_position_dict: typing.Dict[str, typing.Dict[int, int]]
) -> float:
    fund = _cur_fund
    for k, v in _inst_position_dict.items():
        i = bisect.bisect_right(_inst_dailydata_dict[k].index(),
                                _tradingday) - 1
        closeprice = _inst_dailydata_dict[k].getColumn('closeprice')[i]
        if v[SignalType.LONG] > 0:
            fund += closeprice * _inst_position_dict[k][SignalType.LONG]
        if v[SignalType.SHORT] > 0:
            fund -= closeprice * _inst_dailydata_dict[k][SignalType.SHORT]

    return fund


def dailyReturn(
        _fill_event_list: typing.List[FillEvent],
        _init_fund: float = 0.0
) -> DataStruct:
    inst_dailydata_dict: typing.Dict[
        str, DataStruct] = _getInstrumentInterDayDataDict(_fill_event_list)
    inst_position_dict: typing.Dict[str, typing.Dict[int, int]] = {}
    cur_fund: float = _init_fund
    cur_tradingday = None

    # use init fund to create DataStruct
    ret = DataStruct(['TradingDay', 'Fund'], 'TradingDay')
    ret.addRow(['', cur_fund], ['TradingDay', 'Fund'])

    for d in _fill_event_list:
        if cur_tradingday is not None and d.tradingday != cur_tradingday:
            # change tradingday now
            ret.addRow(
                [cur_tradingday,
                 _calcDailyFund(
                     cur_fund, cur_tradingday,
                     inst_dailydata_dict, inst_position_dict
                 )],
                ['TradingDay', 'Fund']
            )

        if d.action == ActionType.OPEN:
            if d.direction == DirectionType.BUY:
                try:
                    inst_position_dict[d.instrument][
                        SignalType.LONG] += d.quantity
                except KeyError:
                    inst_position_dict[d.instrument] = {
                        SignalType.LONG: d.quantity, SignalType.SHORT: 0
                    }
                cur_fund -= d.quantity * d.price + d.commission
            elif d.direction == DirectionType.SELL:
                try:
                    inst_position_dict[d.instrument][
                        SignalType.LONG] += d.quantity
                except KeyError:
                    inst_position_dict[d.instrument] = {
                        SignalType.LONG: d.quantity, SignalType.SHORT: 0
                    }
                cur_fund += d.quantity * d.price - d.commission
            else:
                raise Exception('unknown direction')
        elif d.action == ActionType.CLOSE:
            if d.direction == DirectionType.BUY:
                assert inst_position_dict[d.instrument][
                           SignalType.SHORT] > d.quantity
                inst_position_dict[d.instrument][SignalType.SHORT] -= d.quantity
                cur_fund -= d.quantity * d.price + d.commission
            elif d.direction == DirectionType.SELL:
                assert inst_position_dict[d.instrument][
                           SignalType.LONG] > d.quantity
                inst_position_dict[d.instrument][SignalType.LONG] -= d.quantity
                cur_fund += d.quantity * d.price - d.commission
            else:
                raise Exception('unknown direction')
        else:
            raise Exception('unknown action')

        cur_tradingday = d.tradingday

    # reach the end
    ret.addRow(
        [cur_tradingday,
         _calcDailyFund(
             cur_fund, cur_tradingday,
             inst_dailydata_dict, inst_position_dict
         )],
        ['TradingDay', 'Fund']
    )

    return ret
