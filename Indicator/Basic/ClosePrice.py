import typing

from ParadoxTrading.Utils import DataStruct


class ClosePrice():

    def __init__(self, _key='lastprice'):
        self.key = _key
        self.ret = []

    def addOne(self, _data: DataStruct) -> float:
        self.ret.append(_data.getColumn(self.key)[-1])
        return self.ret[-1]

    def addMany(
        self, _data_list: typing.List[DataStruct]
    ) -> typing.List[float]:
        return [self.addOne(data) for data in _data_list]
