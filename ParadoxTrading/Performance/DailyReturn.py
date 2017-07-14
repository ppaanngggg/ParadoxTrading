from ParadoxTrading.Performance.Utils import FetchRecord
from ParadoxTrading.Utils import DataStruct


def dailyReturn(
        _backtest_key: str,
        _mongo_host: str = 'localhost',
        _mongo_database='Backtest'
) -> DataStruct:
    fetcher = FetchRecord(_mongo_host, _mongo_database)
    return fetcher.settlement(_backtest_key).clone(['total_fund'])
