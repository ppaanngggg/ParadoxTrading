import typing

from ParadoxTrading.Utils import DataStruct


class IndicatorAbstract:
    def __init__(self):
        self.data: DataStruct = None

    def __len__(self):
        return len(self.data)

    def getLastData(self) -> DataStruct:
        return self.data.iloc[-1]

    def getAllData(self) -> DataStruct:
        return self.data

    def addOne(self, _data_struct: DataStruct) -> "IndicatorAbstract":
        assert len(_data_struct) == 1
        self._addOne(_data_struct)
        return self

    def _addOne(self, _data: DataStruct):
        raise NotImplementedError('You should implement addOne!')

    def addMany(
            self,
            _data_list: typing.Union[DataStruct, typing.List[DataStruct]],
    ) -> "IndicatorAbstract":
        for data in _data_list:
            self.addOne(data)
        return self
