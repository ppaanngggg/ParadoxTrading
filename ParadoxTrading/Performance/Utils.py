import typing

import pymongo

from ParadoxTrading.Engine import EventType, DirectionType, SignalType
from ParadoxTrading.Utils import DataStruct


class FetchRecord:
    def __init__(
            self, _mongo_host: str = 'localhost',
            _mongo_database: str = 'Backtest'
    ):
        self.mongo_host = _mongo_host
        self.mongo_database = _mongo_database

    def _fetchRecords(
            self, _backtest_key: str, _type: int,
            _strategy: typing.Union[str, None],
    ) -> typing.List[dict]:
        client = pymongo.MongoClient(self.mongo_host)
        db = client[self.mongo_database]
        coll = db[_backtest_key]

        query = {'type': _type}
        if _strategy is not None:
            query['strategy'] = _strategy

        ret = list(coll.find(query).sort([
            ('tradingday', pymongo.ASCENDING),
            ('datetime', pymongo.ASCENDING)
        ]))

        client.close()
        return ret

    def fetchSignalRecords(
            self, _backtest_key: str, _strategy: str = None
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, EventType.SIGNAL, _strategy,
        )

    def signalToLongShort(
            self, _backtest_key: str, _strategy: str = None
    ) -> typing.Tuple[DataStruct, DataStruct]:
        signal_list = self.fetchSignalRecords(_backtest_key, _strategy)
        keys = [
            'type', 'symbol', 'strategy', 'signal_type',
            'tradingday', 'datetime', 'strength'
        ]
        long_ret = DataStruct(keys, 'datetime')
        short_ret = DataStruct(keys, 'datetime')

        for d in signal_list:
            if d['signal_type'] == SignalType.LONG:
                long_ret.addDict(d)
            if d['signal_type'] == SignalType.SHORT:
                short_ret.addDict(d)

        return long_ret, short_ret

    def fetchOrderRecords(
            self, _backtest_key: str, _strategy: str = None
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, EventType.ORDER, _strategy,
        )

    def fetchFillRecords(
            self, _backtest_key: str, _strategy: str = None
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, EventType.FILL, _strategy,
        )

    def fillToBuySell(
            self, _backtest_key: str, _strategy: str = None
    ) -> typing.Tuple[DataStruct, DataStruct]:
        fill_list = self.fetchFillRecords(_backtest_key, _strategy)
        keys = [
            'type', 'index', 'symbol', 'tradingday',
            'datetime', 'quantity', 'action', 'direction',
            'price', 'commission', 'strategy'
        ]
        buy_ret = DataStruct(keys, 'datetime')
        sell_ret = DataStruct(keys, 'datetime')

        for d in fill_list:
            if d['direction'] == DirectionType.BUY:
                buy_ret.addDict(d)
            if d['direction'] == DirectionType.SELL:
                sell_ret.addDict(d)

        return buy_ret, sell_ret

    def fetchSettlementRecords(
            self, _backtest_key: str
    ) -> typing.List[dict]:
        return self._fetchRecords(
            _backtest_key, EventType.SETTLEMENT, None,
        )

    def settlement(self, _backtest_key) -> DataStruct:
        settlement_list = self.fetchSettlementRecords(_backtest_key)
        keys = [
            'tradingday', 'next_tradingday', 'type',
            'fund', 'unfilled_fund', 'total_fund'
        ]
        ret = DataStruct(keys, 'tradingday')
        for d in settlement_list:
            ret.addDict(d)

        return ret
