import pymongo
import typing

from ParadoxTrading.Engine import EventType, DirectionType, SignalType
from ParadoxTrading.Utils import DataStruct

def fillRecordToBuySell(
        _fill_list: typing.Sequence[typing.Dict]
) -> (DataStruct, DataStruct):
    keys = [
        'type', 'index', 'symbol', 'tradingday',
        'datetime', 'quantity', 'action', 'direction',
        'price', 'commission', 'strategy_name'
    ]
    buy_ret = DataStruct(keys, 'datetime')
    sell_ret = DataStruct(keys, 'datetime')

    for d in _fill_list:
        if d['direction'] == DirectionType.BUY:
            buy_ret.addDict(d)
        if d['direction'] == DirectionType.SELL:
            sell_ret.addDict(d)

    return buy_ret, sell_ret

def signalRecordToLongShort(
        _signal_list: typing.Sequence[typing.Dict]
) -> (DataStruct, DataStruct):
    keys = [
        'type', 'symbol', 'strategy_name', 'signal_type',
        'tradingday', 'datetime', 'strength'
    ]
    long_ret = DataStruct(keys, 'datetime')
    short_ret = DataStruct(keys, 'datetime')

    for d in _signal_list:
        if d['signal_type'] == SignalType.LONG:
            long_ret.addDict(d)
        if d['signal_type'] == SignalType.SHORT:
            short_ret.addDict(d)

    return long_ret, short_ret

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
