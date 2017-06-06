from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class Diff(IndicatorAbstract):
    def __init__(
            self, _use_key: str, _init_value: float = None,
            _idx_key: str = 'time', _ret_key: str = 'diff'
    ):
        """


        :param _use_key:
        :param _init_value: if init_value set, the first ret will be complated, else 0
        :param _idx_key:
        :param _ret_key:
        """
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.last_value = _init_value

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        cur_value = _data_struct[self.use_key][0]
        diff_value = 0
        if self.last_value is not None:
            diff_value = cur_value - self.last_value
        self.last_value = cur_value
        self.data.addRow(
            (index_value, diff_value),
            (self.idx_key, self.ret_key)
        )
