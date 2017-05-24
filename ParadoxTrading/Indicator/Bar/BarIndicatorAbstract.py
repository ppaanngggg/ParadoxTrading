import typing
from datetime import datetime

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class BarIndicatorAbstract(IndicatorAbstract):
    def addOne(
            self, _data_struct: DataStruct,
            _idx: typing.Union[str, datetime] = None
    ) -> "BarIndicatorAbstract":
        self._addOne(_data_struct, _idx)
        return self

    def _addOne(
            self, _data_struct: DataStruct,
            _idx: typing.Union[str, datetime] = None
    ):
        raise NotImplementedError('You should implement addOne!')

    def addMany(
            self,
            _data_list: typing.List[DataStruct],
            _idx_list: typing.List[typing.Union[str, datetime]] = None,
    ) -> "BarIndicatorAbstract":
        for data, idx in zip(_data_list, _idx_list):
            self.addOne(data, idx)
        return self
