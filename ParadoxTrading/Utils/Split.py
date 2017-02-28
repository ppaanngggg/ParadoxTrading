import typing
from datetime import datetime, timedelta

from ParadoxTrading.Utils.DataStruct import DataStruct


class SplitAbstract:
    def __init__(self):
        self.cur_bar: DataStruct = None
        self.cur_bar_begin_time: datetime = None
        self.cur_bar_end_time: datetime = None

        self.bar_list: typing.List[DataStruct] = []
        self.bar_begin_time_list: typing.List[datetime] = []
        self.bar_end_time_list: typing.List[datetime] = []

    def getLastData(self) -> DataStruct:
        return self.cur_bar.iloc[-1]

    def getCurBar(self) -> DataStruct:
        return self.cur_bar

    def getCurBarBeginTime(self) -> datetime:
        return self.cur_bar_begin_time

    def getCurBarEndTime(self) -> datetime:
        return self.cur_bar_end_time

    def getBarList(self) -> typing.List[DataStruct]:
        return self.bar_list

    def getBarBeginTimeList(self) -> typing.List[datetime]:
        return self.bar_begin_time_list

    def getBarEndTimeList(self) -> typing.List[datetime]:
        return self.bar_end_time_list

    def _get_begin_end_time(self, _cur_time: datetime) -> (datetime, datetime):
        raise NotImplementedError('You need to implement _get_begin_end_time!')

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
                self.cur_bar.addDict(_data.toDict())
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
        for d in _data:
            self.addOne(d)


class SplitIntoSecond(SplitAbstract):
    def __init__(self, _second: int):
        super().__init__()
        self.skip_s = _second

    def _get_begin_end_time(self, _cur_time: datetime) -> (datetime, datetime):
        base_s = _cur_time.second // self.skip_s * self.skip_s
        begin_datetime = _cur_time.replace(second=base_s, microsecond=0)
        end_datetime = begin_datetime + timedelta(seconds=self.skip_s)
        return begin_datetime, end_datetime


class SplitIntoMinute(SplitAbstract):
    def __init__(self, _minute: int):
        super().__init__()
        self.skip_m = _minute

    def _get_begin_end_time(self, _cur_time: datetime) -> (datetime, datetime):
        base_m = _cur_time.minute // self.skip_m * self.skip_m
        begin_datetime = _cur_time.replace(
            minute=base_m, second=0, microsecond=0)
        end_datetime = begin_datetime + timedelta(minutes=self.skip_m)
        return begin_datetime, end_datetime


class SplitIntoHour(SplitAbstract):
    def __init__(self, _hour: int):
        super().__init__()
        self.skip_h = _hour

    def _get_begin_end_time(self, _cur_time: datetime) -> (datetime, datetime):
        base_h = _cur_time.hour // self.skip_h * self.skip_h
        begin_datetime = _cur_time.replace(
            hour=base_h, minute=0, second=0, microsecond=0)
        end_datetime = begin_datetime + timedelta(hours=self.skip_h)
        return begin_datetime, end_datetime
