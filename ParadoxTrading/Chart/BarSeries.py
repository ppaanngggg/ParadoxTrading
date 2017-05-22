import typing

from PyQt5.QtChart import QValueAxis, QChart, QBarSeries, QBarSet

from ParadoxTrading.Chart.SeriesAbstract import SeriesAbstract


class BarSeries(SeriesAbstract):
    def __init__(
            self, _name: str,
            _x_list: typing.Sequence,
            _y_list: typing.Sequence,
            _color: typing.Any = None,
            _show_value: bool = True,
    ):
        super().__init__(_name, _x_list, _y_list, _color, _show_value)

        self.type = SeriesAbstract.BAR

    def addSeries(
            self, _x2idx: typing.Dict, _idx2x: list, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
    ):
        bar_set = QBarSet(self.name)
        tmp_dict = dict(zip(self.x_list, self.y_list))
        for k in _idx2x:
            if k in tmp_dict.keys():
                bar_set.append(tmp_dict[k])
            else:
                bar_set.append(0)

        if self.color is not None:
            bar_set.setColor(self.color)

        bar_series = QBarSeries()
        bar_series.append(bar_set)
        _chart.addSeries(bar_series)
        _chart.setAxisX(_axis_x, bar_series)
        _chart.setAxisY(_axis_y, bar_series)

        if self.show_value:
            self.createShow()
