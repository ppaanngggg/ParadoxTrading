import typing

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class StopIndicatorAbstract(IndicatorAbstract):
    def __init__(self):
        super().__init__()

        self.is_stop = False

    def addOne(self, _data_struct: DataStruct) -> bool:
        """
        add one data into buf, and return isStop or not
        :param _data_struct: 
        :return: 
        """
        if not self.is_stop:
            assert len(_data_struct) == 1
            self._addOne(_data_struct)
            self._isStop(_data_struct)

        return self.is_stop

    def _addOne(self, _data_struct: DataStruct):
        raise NotImplementedError('You should implement addOne!')

    def _isStop(self, _data_struct: DataStruct) -> bool:
        raise NotImplementedError('You should implement isStop!')

    def addMany(
            self,
            _data_list: typing.Union[DataStruct, typing.List[DataStruct]],
    ):
        raise Exception('loss indicator has no addMany()')
