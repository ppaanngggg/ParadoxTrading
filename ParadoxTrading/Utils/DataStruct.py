import typing
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta

import numpy as np
import tabulate


class DataStruct:
    """

    ParadoxTrading的基本数据结构，仿照Pandas的DataFrame接口设计。
    剪裁了DataFrame的一些特性，并且加入了一些针对需求的特性。

    :param _keys:
    :param _index_name:
    :param _rows:
    :param _dicts:

    """

    EXPAND_STRICT = 'strict'

    def __init__(
            self,
            _keys: typing.Sequence[str],
            _index_name: str,
            _rows: typing.Sequence = None,
            _dicts: typing.Sequence[dict] = None
    ):
        assert _index_name in _keys

        self.index_name = _index_name
        self.data: typing.Dict[str, typing.List] = {}
        for key in _keys:
            self.data[key] = []

        self.loc: Loc = Loc(self)
        self.iloc: ILoc = ILoc(self)

        if _rows is not None:
            self.addRows(_rows, _keys)

        if _dicts is not None:
            self.addDicts(_dicts)

    def __getitem__(self, _item: str) -> typing.List[typing.Any]:
        assert type(_item) == str
        return self.data[_item]

    def __len__(self) -> int:
        return len(self.index())

    def __iter__(self):
        for i in range(len(self.index())):
            yield self.iloc[i]

    def __repr__(self):
        if len(self) > 20:
            tmp_rows, tmp_keys = self.iloc[:8].toRows()
            tmp_rows.append(['...' for _ in tmp_keys])
            tmp_rows += self.iloc[-8:].toRows()[0]
            return tabulate.tabulate(tmp_rows, headers=tmp_keys)
        tmp_rows, tmp_keys = self.toRows()
        return tabulate.tabulate(tmp_rows, headers=tmp_keys)

    def clone(self):
        return self.iloc[:]

    def merge(self, _struct: "DataStruct"):
        self.addRows(*_struct.toRows())

    def expand(self, _struct: "DataStruct", _type: str = 'strict'):
        if _type == self.EXPAND_STRICT:
            assert len(self) == len(_struct)
            for idx1, idx2 in zip(self.index(), _struct.index()):
                assert idx1 == idx2
            for name in _struct.getColumnNames(False):
                assert name not in self.getColumnNames()
            for name in _struct.getColumnNames(False):
                self.data[name] = _struct.getColumn(name)
        else:
            raise Exception('unknow type!')

    def addRow(
            self,
            _row: typing.Sequence[typing.Any],
            _keys: typing.Sequence[str]
    ):
        assert len(_row) == len(_keys)
        self.addDict(dict(zip(_keys, _row)))

    def addRows(
            self,
            _rows: typing.Sequence[typing.Sequence],
            _keys: typing.Sequence[str]
    ):
        for row in _rows:
            self.addRow(row, _keys)

    def addDict(self, _dict: typing.Dict[str, typing.Any]):
        index_value = _dict[self.index_name]
        insert_idx = bisect_right(self.index(), index_value)
        for k, v in _dict.items():
            self.data[k].insert(insert_idx, v)

    def addDicts(self, _dicts: typing.Sequence[dict]):
        for _dict in _dicts:
            self.addDict(_dict)

    def toRows(
            self, _keys=None
    ) -> (typing.List[typing.List[typing.Any]], typing.List[str]):
        """

        :param _keys: the columns to return
        :return: rows and keys
        """
        rows = []
        keys: typing.List[str] = _keys
        if keys is None:
            keys = self.getColumnNames()
        for i in range(len(self)):
            rows.append([self.data[k][i] for k in keys])
        return rows, keys

    def toRow(
            self, _index: int = 0, _keys=None
    ) -> (typing.List[typing.Any], typing.List[str]):
        keys: typing.List[str] = _keys
        if keys is None:
            keys = self.getColumnNames()
        row = [self.data[k][_index] for k in keys]
        return row, keys

    def toDicts(self) -> (typing.List[typing.Dict[str, typing.Any]]):
        dicts = []
        rows, keys = self.toRows()
        for d in rows:
            dicts.append(dict(zip(keys, d)))
        return dicts

    def toDict(self, _index: int = 0) -> (typing.Dict[str, typing.Any]):
        row, keys = self.toRow(_index)
        return dict(zip(keys, row))

    def toHDF5(self, _f_name: str):
        pass

    def index(self) -> list:
        return self.data[self.index_name]

    def getColumnNames(
            self, _include_index_name: bool = True
    ) -> typing.Sequence[str]:
        if _include_index_name:
            return sorted(self.data.keys())
        else:
            tmp = {self.index_name}
            return sorted(self.data.keys() - tmp)

    def changeIndex(self, _new_index: str) -> 'DataStruct':
        assert _new_index in self.data.keys()
        tmp = DataStruct(self.getColumnNames(), _new_index)
        for d in self:
            tmp.merge(d)
        return tmp

    def changeColumnName(self, _old_name: str, _new_name: str):
        assert _old_name != _new_name
        if self.index_name == _old_name:
            self.index_name = _new_name
        self.data[_new_name] = self.data[_old_name]
        del self.data[_old_name]

    def getColumn(self, _key: str) -> list:
        return self.data[_key]

    def dropColumn(self, _key: str):
        assert _key in self.data.keys()
        del self.data[_key]

    def createColumn(self, _key: str, _column: typing.Sequence[typing.Any]):
        assert _key not in self.data.keys()
        assert len(_column) == len(self)
        self.data[_key] = _column

    def any2str(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = [str(d) for d in self.data[k]]

    def datetime2float(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = [(d - datetime(1970, 1, 1)).total_seconds()
                        for d in self.data[k]]

    def float2datetime(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = [datetime(1970, 1, 1) + timedelta(seconds=d)
                        for d in self.data[k]]

    def str2float(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = np.array(self.data[k]).astype(np.float).tolist()

    def str2int(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = np.array(self.data[k]).astype(np.int).tolist()

    def str2datetime(self, _key: str = None):
        k = _key
        if k is None:
            k = self.index_name
        self.data[k] = [datetime.strptime(d, '%Y%m%d %H:%M:%S.%f')
                        for d in self.data[k]]


class Loc:
    def __init__(self, _struct: DataStruct):
        self.struct = _struct

    def __getitem__(self, _item: slice):
        if isinstance(_item, slice):
            new_start = None
            if _item.start is not None:
                new_start = bisect_left(self.struct.index(), _item.start)
            new_stop = None
            if _item.stop is not None:
                new_stop = bisect_left(self.struct.index(), _item.stop)
            new_item = slice(new_start, new_stop)
            return self.struct.iloc.__getitem__(new_item)
        else:
            n_i = bisect_left(self.struct.index(), _item)
            if n_i != len(self.struct) and _item == self.struct.index()[n_i]:
                return self.struct.iloc.__getitem__(n_i)
            else:
                return None


class ILoc:
    def __init__(self, _struct: DataStruct):
        self.struct = _struct

    def __getitem__(self, _item: typing.Union[int, slice]) -> DataStruct:
        ret = DataStruct(list(self.struct.data.keys()), self.struct.index_name)
        if isinstance(_item, slice):
            for k, v in self.struct.data.items():
                ret.data[k] = v.__getitem__(_item)
        else:
            for k, v in self.struct.data.items():
                ret.data[k] = [v[_item]]
        return ret
