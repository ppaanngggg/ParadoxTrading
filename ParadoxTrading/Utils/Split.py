import typing
from datetime import datetime, timedelta

import arrow

from ParadoxTrading.Utils.DataStruct import DataStruct

DATETIME_TYPE = typing.Union[str, datetime]


class SplitAbstract:
    def __init__(self):
        self.cur_bar: DataStruct = None
        self.cur_bar_begin_time: DATETIME_TYPE = None
        self.cur_bar_end_time: DATETIME_TYPE = None

        self.bar_list: typing.List[DataStruct] = []
        self.bar_begin_time_list: typing.List[DATETIME_TYPE] = []
        self.bar_end_time_list: typing.List[DATETIME_TYPE] = []

    def __len__(self) -> len:
        return len(self.getBarList())

    def getLastData(self) -> DataStruct:
        """
        get last

        :return:
        """
        return self.cur_bar.iloc[-1]

    def getCurBar(self) -> DataStruct:
        return self.cur_bar

    def getCurBarBeginTime(self) -> DATETIME_TYPE:
        return self.cur_bar_begin_time

    def getCurBarEndTime(self) -> DATETIME_TYPE:
        return self.cur_bar_end_time

    def getBarList(self) -> typing.List[DataStruct]:
        return self.bar_list

    def getBarBeginTimeList(self) -> typing.List[DATETIME_TYPE]:
        return self.bar_begin_time_list

    def getBarEndTimeList(self) -> typing.List[DATETIME_TYPE]:
        return self.bar_end_time_list

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        raise NotImplementedError('You need to implement _get_begin_end_time!')

    def _create_new_bar(self, _data: DataStruct, _cur_time: DATETIME_TYPE):
        self.cur_bar = _data.clone()
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

        return self


class SplitIntoSecond(SplitAbstract):
    def __init__(self, _second: int = 1):
        super().__init__()
        self.skip_s = _second

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        base_s = _cur_time.second // self.skip_s * self.skip_s
        begin_datetime = _cur_time.replace(second=base_s, microsecond=0)
        end_datetime = begin_datetime + timedelta(seconds=self.skip_s)
        return begin_datetime, end_datetime


class SplitIntoMinute(SplitAbstract):
    def __init__(self, _minute: int = 1):
        super().__init__()
        self.skip_m = _minute

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        base_m = _cur_time.minute // self.skip_m * self.skip_m
        begin_datetime = _cur_time.replace(
            minute=base_m, second=0, microsecond=0)
        end_datetime = begin_datetime + timedelta(minutes=self.skip_m)
        return begin_datetime, end_datetime


class SplitIntoHour(SplitAbstract):
    def __init__(self, _hour: int = 1):
        super().__init__()
        self.skip_h = _hour

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        base_h = _cur_time.hour // self.skip_h * self.skip_h
        begin_datetime = _cur_time.replace(
            hour=base_h, minute=0, second=0, microsecond=0)
        end_datetime = begin_datetime + timedelta(hours=self.skip_h)
        return begin_datetime, end_datetime


class SplitIntoWeek(SplitAbstract):
    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        cur_date = datetime.strptime(_cur_time, '%Y%m%d')
        weekday = cur_date.weekday()
        begin_datetime: datetime = cur_date - timedelta(days=weekday)
        end_datetime: datetime = begin_datetime + timedelta(weeks=1)
        return (
            begin_datetime.strftime('%Y%m%d'),
            end_datetime.strftime('%Y%m%d')
        )


class SplitIntoMonth(SplitAbstract):
    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        cur_date = arrow.get(_cur_time, 'YYYYMMDD')
        begin_datetime = cur_date.replace(day=1)
        end_datetime = begin_datetime.shift(months=1)
        return (
            begin_datetime.format('YYYYMMDD'),
            end_datetime.format('YYYYMMDD')
        )


class SplitIntoYear(SplitAbstract):
    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        cur_date = arrow.get(_cur_time, 'YYYYMMDD')
        begin_datetime = cur_date.replace(day=1)
        end_datetime = begin_datetime.shift(years=1)
        return (
            begin_datetime.format('YYYYMMDD'),
            end_datetime.format('YYYYMMDD')
        )


class SplitVolumeBars(SplitAbstract):
    def __init__(
            self, _use_key='volume', _volume_size: int = 1,
    ):
        """

        :param _use_key: use which index to split volume
        :param _volume_size: split ticks
        """
        super().__init__()

        self.use_key = _use_key
        self.volume_size = _volume_size

        self.total_volume = 0

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        return _cur_time, _cur_time

    def addOne(self, _data: DataStruct):
        assert len(_data) == 1
        cur_time = _data.index()[0]
        cur_volume = _data[self.use_key][0]
        if self.cur_bar is None:  # the first tick
            self._create_new_bar(_data, cur_time)
            self.total_volume = cur_volume
            return True

        if self.total_volume > self.volume_size:
            self._create_new_bar(_data, cur_time)
            self.total_volume = cur_volume
            return True

        self.cur_bar.addDict(_data.toDict())
        self.cur_bar_end_time = cur_time  # override end time
        self.bar_end_time_list[-1] = cur_time
        self.total_volume += cur_volume
        return False


class SplitTickImbalance(SplitAbstract):
    def __init__(
            self, _use_key='lastprice',
            _period=7, _init_T=1000
    ):
        """
        <Advances in Financial Machine Learning> - 2.3.2.1

        _use_key: use which index to calc bt
        _init_T: the length of first bar
        _period: period of EMA
        """
        super().__init__()

        self.use_key = _use_key
        self.last_value = None

        self.last_b = 1
        self.sum_b = 0  # sum of b
        self.num_b = 0  # total number of b

        self.T = _init_T  # len of Bar
        self.P = None  # probability of b == 1
        self.period = _period
        self.threshold = None

    def _get_begin_end_time(
            self, _cur_time: DATETIME_TYPE
    ) -> (DATETIME_TYPE, DATETIME_TYPE):
        return _cur_time, _cur_time

    def _update_b(self, _value):
        # update value, b and total_b
        if _value > self.last_value:
            self.last_b = 1
        elif _value < self.last_value:
            self.last_b = -1
        else:
            pass
        self.last_value = _value
        self.sum_b += self.last_b
        self.num_b += 1

    def _reset_b(self):
        self.sum_b = 0
        self.num_b = 0

    def _update_threshold(self):
        new_T = self.num_b
        new_P = (self.sum_b + self.num_b) / 2. / self.num_b
        self.T += (new_T - self.T) / self.period
        if self.P is None:  # init p
            self.P = new_P
        else:
            self.P += (new_P - self.P) / self.period
        self.threshold = self.T * abs(2 * self.P - 1)

    def addOne(self, _data: DataStruct) -> bool:
        # check data
        assert len(_data) == 1
        value = _data[self.use_key][0]
        cur_time = _data.index()[0]

        if self.cur_bar is None:  # init the first bar
            self.last_value = value
            self._create_new_bar(_data, cur_time)
            return True

        self._update_b(value)
        print(value, self.last_b, self.sum_b, self.num_b)

        flag = False
        if self.P is None:  # current is the first bar
            if self.num_b >= self.T:  # finish the first bar
                flag = True
        elif abs(self.sum_b) >= self.threshold:  # create new bar
            flag = True

        if flag:
            self._update_threshold()
            print(self.T, self.P, self.threshold)
            input()
            self._reset_b()
            self._create_new_bar(_data, cur_time)
            return True
        else:
            self.cur_bar.addDict(_data.toDict())
            self.cur_bar_end_time = cur_time  # override end time
            self.bar_end_time_list[-1] = cur_time
            return False
