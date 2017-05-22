import typing

from PyQt5.QtChart import QChart, QValueAxis, QLineSeries

from ParadoxTrading.Chart.SeriesAbstract import SeriesAbstract


class LineSeries(SeriesAbstract):
    def __init__(
            self, _name: str,
            _x_list: typing.Sequence,
            _y_list: typing.Sequence,
            _color: typing.Any = None,
            _show_value: bool = True,
    ):
        super().__init__(_name, _x_list, _y_list, _color, _show_value)

        self.type = SeriesAbstract.LINE

    def addSeries(
            self, _x2idx: typing.Dict, _idx2x: list, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
    ):
        series = QLineSeries()
        series.setName(self.name)
        for x, y in zip(self.x_list, self.y_list):
            series.append(_x2idx[x], y)
        if self.color is not None:
            series.setColor(self.color)

        _chart.addSeries(series)
        _chart.setAxisX(_axis_x, series)
        _chart.setAxisY(_axis_y, series)

        if self.show_value:
            self.createShow()