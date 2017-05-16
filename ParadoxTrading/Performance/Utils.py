import pymongo
import typing

from ParadoxTrading.Engine import EventType, DirectionType
from ParadoxTrading.Utils import DataStruct

def fillRecordToBuySell(
        _fill_list: typing.Sequence[typing.Dict]
) -> (DataStruct, DataStruct):
    buy_ret = DataStruct(['datetime', 'price'], 'datetime')
    sell_ret = DataStruct(['datetime', 'price'], 'datetime')

    for d in _fill_list:
        if d['direction'] == DirectionType.BUY:
            buy_ret.addDict(d)
        if d['direction'] == DirectionType.SELL:
            sell_ret.addDict(d)

    return buy_ret, sell_ret

class FetchRecord:
    def __init__(self):
        self.mongo_host = 'localhost'
        self.mongo_database = 'Backtest'

    def _fetchRecords(
            self, _backtest_key: str, _strategy_name: str, _type: int,
    ) -> typing.List[dict]:
        client = pymongo.MongoClient(self.mongo_host)
        db = client[self.mongo_database]
        coll = db[_backtest_key]

        ret = list(coll.find({
            'strategy_name': _strategy_name,
            'type': _type,
        }).sort([
            ('tradingday', pymongo.ASCENDING),
            ('datetime', pymongo.ASCENDING)
        ]))

        client.close()
        return ret

    def fetchSignalRecords(
            self, _backtest_key: str, _strategy_name: str
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.SIGNAL
        )

    def fetchOrderRecords(
            self, _backtest_key: str, _strategy_name: str
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.ORDER
        )

    def fetchFillRecords(
            self, _backtest_key: str, _strategy_name: str
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.FILL
        )

    def fetchSettlementRecords(
            self, _backtest_key: str, _strategy_name: str
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.SETTLEMENT
        )
