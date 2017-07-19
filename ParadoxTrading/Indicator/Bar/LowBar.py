import typing
from datetime import datetime

from ParadoxTrading.Indicator.Bar.BarIndicatorAbstract import BarIndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class LowBar(BarIndicatorAbstract):
    def __init__(
            self, _use_key: str,
            _idx_key: str = 'time', _ret_key: str = 'low'
    ):
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

    def _addOne(
            self, _data_struct: DataStruct,
            _idx: typing.Union[str, datetime] = None
    ):
        self.data.addDict({
            self.idx_key: _idx,
            self.ret_key: min(_data_struct[self.use_key])
        })
