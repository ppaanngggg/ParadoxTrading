import typing
import pymongo
from pymongo.collection import Collection

from ParadoxTrading.Engine import SignalEvent, OrderEvent, FillEvent
from ParadoxTrading.Engine import EventType, SignalType, ActionType, DirectionType


class Fetch:
    mongo_host = 'localhost'
    mongo_database = 'FutureBacktest'

    @staticmethod
    def _fetchRecords(
            _backtest_key: str, _strategy_name: str, _type: int,
            _event_func: typing.Callable[[dict], typing.Union[SignalEvent, OrderEvent, FillEvent]]
    ) -> typing.List[typing.Union[SignalEvent, OrderEvent, FillEvent]]:
        client = pymongo.MongoClient(Fetch.mongo_host)
        db = client[Fetch.mongo_database]
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
        return Fetch._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.SIGNAL, SignalEvent.fromDict
        )

    @staticmethod
    def fetchOrderRecords(
            _backtest_key: str, _strategy_name: str
    ) -> typing.List[OrderEvent]:
        return Fetch._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.ORDER, OrderEvent.fromDict
        )

    @staticmethod
    def fetchFillRecords(
            _backtest_key: str, _strategy_name: str
    ) -> typing.List[FillEvent]:
        return Fetch._fetchRecords(
            _backtest_key, _strategy_name,
            EventType.FILL, FillEvent.fromDict
        )
