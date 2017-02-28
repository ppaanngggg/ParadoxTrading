from datetime import datetime

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class HighBar(IndicatorAbstract):
    def __init__(
            self, _use_key: str,
            _idx_key: str = 'time', _ret_key: str = 'high'
    ):
        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

    def _addOne(self, _index: datetime, _data: DataStruct):
        tmp_value = max(_data.getColumn(self.use_key))
        self.data.addRow([_index, tmp_value], [self.idx_key, self.ret_key])
