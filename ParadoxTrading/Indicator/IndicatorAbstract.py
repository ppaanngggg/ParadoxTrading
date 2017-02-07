import typing
from datetime import datetime

from ParadoxTrading.Utils import DataStruct


class IndicatorAbstract:

    def getLastData(self):
        return self.data.iloc[-1]

    def getAllData(self):
        return self.data

    def addOne(self, _index: datetime, _data: DataStruct) -> "IndicatorAbstract":
        self._addOne(_index, _data)
        return self

    def _addOne(self, _index: datetime, _data: DataStruct):
        raise NotImplementedError('You should implement addOne!')

    def addMany(
        self,
        _idx_list: typing.List[datetime],
        _data_list: typing.List[DataStruct]
    ) -> "IndicatorAbstract":
        for idx, data in zip(_idx_list, _data_list):
            self._addOne(idx, data)
        return self
