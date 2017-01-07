from datetime import datetime, timedelta

import pandas as pd


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

    def _create_new_bar(self, _data: pd.Series, _cur_time: datetime):
        self.cur_bar = pd.DataFrame(
            _data, columns=_data.index, index=[_data.name])
        self.cur_bar_begin_time, self.cur_bar_end_time = \
            self._get_begin_end_time(_cur_time)
        self.bar_list.append(self.cur_bar)
        self.bar_begin_time_list.append(self.cur_bar_begin_time)
        self.bar_end_time_list.append(self.cur_bar_end_time)

    def addOne(self, _data: pd.Series):
        assert isinstance(_data, pd.Series)
        cur_time = _data.name.to_pydatetime()
        if self.cur_bar is None:
            self._create_new_bar(_data, cur_time)
        else:
            if cur_time < self.cur_bar_end_time:
                self.cur_bar.append(_data)
            else:
                self._create_new_bar(_data, cur_time)
        print(self.cur_bar)
        print(self.cur_bar_begin_time)
        print(self.cur_bar_end_time)
        input()

    def addMany(self, _data: pd.DataFrame):
        for _, d in _data.iterrows():
            self.addOne(d)

if __name__ == '__main__':
    from Fetch import Fetch
    tmp = Fetch.fetchIntraDayData('rb', '20170105')
    spliter = SplitIntoMinute(5)
    spliter.addMany(tmp)
