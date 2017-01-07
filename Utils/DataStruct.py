from bisect import bisect_right, bisect_left


class DataStruct():

    def __init__(self, _keys: list, _index_name: str, _rows=None, _dicts=None):
        assert _index_name in _keys

        self.data = {}
        for key in _keys:
            self.data[key] = []
        self.index_name = _index_name

        self.loc = Loc(self)
        self.iloc = ILoc(self)

        if _rows is not None:
            assert isinstance(_rows, list)
            self.addRows(_rows, _keys)

        if _dicts is not None:
            assert isinstance(_dicts, list)
            self.addDicts(_dicts)

    def __getitem__(self, _item: slice):
        return self.loc.__getitem__(_item)

    def __len__(self):
        return len(self.index())

    def add(self, _struct):
        keys = _struct.data.keys()
        values = _struct.data.values()

        for i in range(len(_struct.index())):
            self.addRow([d[i] for d in values], keys)

    def addRow(self, _row: list, _keys: list):
        i = 0
        for key in _keys:
            if key == self.index_name:
                break
            i += 1
        index_value = _row[i]
        insert_idx = bisect_right(self.index(), index_value)
        for k, v in zip(_keys, _row):
            self.data[k].insert(insert_idx, v)

    def addRows(self, _rows: list, _keys: list):
        for row in _rows:
            self.addRow(row, _keys)

    def addDict(self, _dict: dict):
        self.addRow(_dict.values(), _dict.keys())

    def addDicts(self, _dicts: list):
        for _dict in _dicts:
            self.addDict(_dict)

    def index(self):
        return self.data[self.index_name]

    def iterrows(self):
        for i in range(len(self.index)):
            yield self.iloc[i]


class Loc():

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
            new_item = bisect_left(self.struct.index(), _item)
            return self.struct.iloc.__getitem__(new_item)


class ILoc():

    def __init__(self, _struct: DataStruct):
        self.struct = _struct

    def __getitem__(self, _item: slice):
        ret = DataStruct(self.struct.data.keys(), self.struct.index_name)
        if isinstance(_item, slice):
            for k, v in zip(self.struct.data.keys(), self.struct.data.values()):
                ret.data[k] = v.__getitem__(_item)
        else:
            for k, v in zip(self.struct.data.keys(), self.struct.data.values()):
                ret.data[k] = [v[_item]]
        return ret

if __name__ == '__main__':
    import datetime

    from ParadoxTrading.Utils import Fetch
    data = Fetch.fetchIntraDayData('rb', '20170104')
    tmp = data.iloc[100]
    print(tmp.index())
