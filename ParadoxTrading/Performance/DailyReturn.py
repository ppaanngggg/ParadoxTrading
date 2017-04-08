import typing

from ParadoxTrading.Engine import ActionType, DirectionType, SignalType
from ParadoxTrading.Performance.Utils import FetchRecord
from ParadoxTrading.Utils import DataStruct


def _get_fill_dict(_fill_records):
    fill_dict: typing.Dict[str, DataStruct] = {}
    for r in _fill_records:
        try:
            fill_dict[r['tradingday']].addDict(r)
        except KeyError:
            fill_dict[r['tradingday']] = DataStruct(
                r.keys(),
                'datetime',
                _dicts=[r]
            )
    return fill_dict


def _get_settlement_dict(_settlement_records):
    settlement_dict: typing.Dict[str, dict] = {}
    for r in _settlement_records:
        settlement_dict[r['tradingday']] = r
    return settlement_dict


def _daily_return(
        _fill_records: typing.List[typing.Dict[str, typing.Any]],
        _settlement_records: typing.List[typing.Dict[str, typing.Any]],
        _init_fund: float = 0.0,
) -> DataStruct:
    cur_fund = _init_fund

    # use init fund to create DataStruct
    ret = DataStruct(['tradingday', 'fund'], 'tradingday')
    ret.addDict({
        'tradingday': '',
        'fund': cur_fund,
    })

    fill_dict = _get_fill_dict(_fill_records)
    settlement_dict = _get_settlement_dict(_settlement_records)

    assert fill_dict.keys() <= settlement_dict.keys()

    for tradingday in sorted(settlement_dict.keys()):
        for d in fill_dict[tradingday]:
            cur_fund -= d['commission'][0]
            if d['direction'][0] == DirectionType.BUY:
                cur_fund -= d['price'][0] * d['quantity'][0]
            if d['direction'][0] == DirectionType.SELL:
                cur_fund += d['price'][0] * d['quantity'][0]
        ret.addDict({
            'tradingday': tradingday,
            'fund': cur_fund + settlement_dict[tradingday]['unfilled_fund'],
        })
    return ret


def dailyReturn(
    _backtest_key: str, _strategy_name: str, _init_fund: float=0.0,
    _mongo_host: str = 'localhost', _mongo_database='Backtest'
):
    fetcher = FetchRecord()
    fetcher.mongo_host = _mongo_host
    fetcher.mongo_database = _mongo_database

    return _daily_return(
        fetcher.fetchFillRecords(_backtest_key, _strategy_name),
        fetcher.fetchSettlementRecords(_backtest_key, _strategy_name),
        _init_fund
    )
