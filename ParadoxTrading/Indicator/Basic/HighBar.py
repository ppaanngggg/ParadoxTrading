import typing
from datetime import datetime

from ParadoxTrading.Indicator.IndicatorAbstract import IndicatorAbstract
from ParadoxTrading.Utils import DataStruct


class HighBar(IndicatorAbstract):

    def __init__(
        self, _use_key: str,
        _idx_key: str='time', _ret_key: str='high'
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


if __name__ == '__main__':
    from ParadoxTrading.Utils import Fetch, SplitIntoMinute
    data = Fetch.fetchIntraDayData('rb', '20170123')
    spliter = SplitIntoMinute(1)
    spliter.addMany(data)

    highprice = HighBar('lastprice')
    highprice.addMany(spliter.getBarBeginTimeList(), spliter.getBarList())
    print(highprice.getAllData())
