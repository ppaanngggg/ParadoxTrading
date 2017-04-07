import typing

import pymongo

from ParadoxTrading.Engine import EventType
from ParadoxTrading.Engine import SignalEvent, OrderEvent, FillEvent


class FetchRecord:
    mongo_host = 'localhost'
    mongo_database = 'Backtest'

    @staticmethod
    def _fetchRecords(
            _backtest_key: str, _strategy_name: str, _type: int,
            _event_func: typing.Callable[
                [dict], typing.Union[SignalEvent, OrderEvent, FillEvent]]
    ) -> typing.List[typing.Union[SignalEvent, OrderEvent, FillEvent]]:
        client = pymongo.MongoClient(FetchRecord.mongo_host)
        db = client[FetchRecord.mongo_database]
        coll = db[_backtest_key]

        ret = []
        for d in coll.find({
            'strategy_name': _strategy_name,
            'type': _type,
        }).sort([('datetime', pymongo.ASCENDING)]):
            ret.append(_event_func(d))

        client.close()
        return ret

    @staticmethod
    def fetchSignalRecords(
            _backtest_key: str, _strategy_name: str
    ) -> typing.List[SignalEvent]:
        return FetchRecord._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.SIGNAL, SignalEvent.fromDict
        )

    @staticmethod
    def fetchOrderRecords(
            _backtest_key: str, _strategy_name: str
    ) -> typing.List[OrderEvent]:
        return FetchRecord._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.ORDER, OrderEvent.fromDict
        )

    @staticmethod
    def fetchFillRecords(
            _backtest_key: str, _strategy_name: str
    ) -> typing.List[FillEvent]:
        return FetchRecord._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.FILL, FillEvent.fromDict
        )
