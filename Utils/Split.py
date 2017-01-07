from datetime import datetime, timedelta
from DataStruct import DataStruct


class SplitIntoMinute():

    def __init__(self, _minute: int):

        self.skip_m = _minute

        self.cur_bar = None
        self.cur_bar_begin_time = None
        self.cur_bar_end_time = None

        self.bar_list = []
        self.bar_begin_time_list = []
        self.bar_end_time_list = []

    def _get_begin_end_time(self, _cur_time: datetime):
        base_m = _cur_time.minute // self.skip_m * self.skip_m
        begin_datetime = _cur_time.replace(minute=base_m, second=0)
        end_datetime = begin_datetime + timedelta(minutes=self.skip_m)
        return begin_datetime, end_datetime

    def _create_new_bar(self, _data: DataStruct, _cur_time: datetime):
        self.cur_bar = _data
        self.cur_bar_begin_time, self.cur_bar_end_time = \
            self._get_begin_end_time(_cur_time)
        self.bar_list.append(self.cur_bar)
        self.bar_begin_time_list.append(self.cur_bar_begin_time)
        self.bar_end_time_list.append(self.cur_bar_end_time)

    def addOne(self, _data: DataStruct) -> bool:
        """
        add one tick data into spliter

        Args:
            _data (DataStruct): one tick

        Returns:
            bool : whether created a new bar
        """

        assert len(_data) == 1
        cur_time = _data.index()[0]
        if self.cur_bar is None:
            self._create_new_bar(_data, cur_time)
            return True
        else:
            if cur_time < self.cur_bar_end_time:
                self.cur_bar.add(_data)
                return False
            else:
                self._create_new_bar(_data, cur_time)
                return True

    def addMany(self, _data: DataStruct):
        """
        add continue data into spliter

        Args:
            _data (DataStruct): continute data
        """
        for d in _data.iterrows():
            self.addOne(d)

if __name__ == '__main__':
    from Fetch import Fetch
    tmp = Fetch.fetchIntraDayData('rb', '20170105')
    spliter = SplitIntoMinute(1)
    spliter.addMany(tmp)
