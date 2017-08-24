import math
import statistics

from tabulate import tabulate

from ParadoxTrading.Performance.Utils import FetchRecord
from ParadoxTrading.Utils import DataStruct


def dailyReturn(
        _backtest_key: str,
        _mongo_host: str = 'localhost',
        _mongo_database='Backtest'
) -> DataStruct:
    fetcher = FetchRecord(_mongo_host, _mongo_database)
    return fetcher.settlement(_backtest_key).clone(['fund'])


def marginRate(
        _backtest_key: str,
        _mongo_host: str = 'localhost',
        _mongo_database='Backtest'
) -> DataStruct:
    fetcher = FetchRecord(_mongo_host, _mongo_database)
    return fetcher.settlement(_backtest_key).clone(['margin'])


def avgYearReturn(
        _returns: DataStruct,
        _factor: int = 252,
        _fund_index: str = 'fund'
) -> float:
    fund = _returns[_fund_index]
    return (fund[-1] / fund[0]) ** (_factor / len(fund)) - 1


def sharpRatio(
        _returns: DataStruct,
        _factor: int = 252,
        _risk_free: float = 0.0,
        _fund_index: str = 'fund'
) -> float:
    fund = _returns[_fund_index]
    tmp_list = [
        a / b - 1.0 - _risk_free for a, b in zip(
            fund[1:], fund[:-1]
        )
    ]
    return statistics.mean(
        tmp_list
    ) / statistics.pstdev(
        tmp_list
    ) * math.sqrt(_factor)


def maxDrawdown(
        _returns: DataStruct,
        _fund_index: str = 'fund'
) -> float:
    fund_list = _returns[_fund_index]
    max_drawdown = 0.0
    max_fund = fund_list[0]
    for fund in fund_list[1:]:
        if fund > max_fund:
            max_fund = fund
        drawdown = (max_fund - fund) / max_fund
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def calmarRatio(
        _returns: DataStruct,
        _factor: int = 252,
        _fund_index: str = 'fund'
) -> float:
    return avgYearReturn(
        _returns, _factor, _fund_index
    ) / maxDrawdown(
        _returns, _fund_index
    )


def performance(
        _backtest_key: str,
        _mongo_host: str = 'localhost',
        _mongo_database='Backtest',
        _factor: int = 252,
        _risk_free: float = 0.0,
        _fund_index: str = 'fund'
):
    daily_return = dailyReturn(
        _backtest_key, _mongo_host, _mongo_database
    )
    tmp = [[
        'AvgYearRet',
        avgYearReturn(daily_return, _factor, _fund_index)
    ], [
        'SharpRatio',
        sharpRatio(daily_return, _factor, _risk_free, _fund_index)
    ], [
        'MaxDrawdown',
        maxDrawdown(daily_return, _fund_index)
    ], [
        'CalmarRatio',
        calmarRatio(daily_return, _factor, _fund_index)
    ]]
    print(tabulate(tmp))
