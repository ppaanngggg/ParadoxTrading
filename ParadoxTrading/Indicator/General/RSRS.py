from collections import deque

import numpy as np
import typing
from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct
from scipy.stats import linregress


class RSRS(IndicatorAbstract):

    def __init__(
            self, _N: int=30,
            _use_key: typing.List[str] = ['highprice', 'lowprice'],
            _idx_key: str = 'time', _ret_key: str = 'rsrs'
    ):
        """
        _N: sample length
        """
        super().__init__()

        self.use_key = _use_key
        self.idx_key = _idx_key
        self.ret_key = _ret_key
        self.data = DataStruct(
            [self.idx_key, self.ret_key],
            self.idx_key
        )

        self.N = _N

        self.high_buf = deque(maxlen=self.N)
        self.low_buf = deque(maxlen=self.N)

    def _addOne(self, _data_struct: DataStruct):
        index_value = _data_struct.index()[0]
        self.high_buf.append(_data_struct[self.use_key[0]][-1])
        self.low_buf.append(_data_struct[self.use_key[1]][-1])

        if len(self.high_buf) >= self.N and len(self.low_buf) >= self.N:
            x = np.arange(self.N)
            high_beta = linregress(x, self.high_buf)[0]
            low_beta = linregress(x, self.low_buf)[0]

            self.data.addDict({
                self.idx_key: index_value,
                self.ret_key: high_beta - low_beta,
            })
