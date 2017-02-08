import typing
from datetime import datetime

from PyQt5.Qt import QColor, QPainter
from PyQt5.QtChart import (QBarCategoryAxis, QBarSeries, QBarSet, QChart,
                           QChartView, QLineSeries, QValueAxis)

class CView:

    BAR = 'bar'
    LINE = 'line'

    def __init__(self, _name: str=None):

        self.name = _name

        self.raw_data_dict = {}

        self.axis_x = QBarCategoryAxis()
        self.axis_x.setVisible(False)
        self.axis_y = QValueAxis()
        if self.name is not None:
            self.axis_y.setTitleText(self.name)

    def _add(
        self, _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: QColor, _type: str
    ):
        assert _name not in self.raw_data_dict.keys()
        assert len(_x_list) == len(_y_list)
        self.raw_data_dict[_name] = {
            'x': _x_list, 'y': _y_list, 'name': _name,
            'color': _color, 'type': _type
        }

    def addBar(
        self, _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: QColor=None
    ):
        self._add(_x_list, _y_list, _name, _color, self.BAR)

    def addLine(
        self, _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: QColor=None
    ):
        self._add(_x_list, _y_list, _name, _color, self.LINE)

    def calcSetX(self) -> set:
        tmp = set()
        for v in self.raw_data_dict.values():
            tmp |= set(v['x'])
        return tmp

    def _addLineSeries(
        self, _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
        _axis_x: QBarCategoryAxis, _axis_y: QValueAxis
    ):
        series = QLineSeries()
        series.setName(_v['name'])
        for x, y in zip(_v['x'], _v['y']):
            series.append(_x2idx[x], y)
        if _v['color'] is not None:
            series.setColor(_v['color'])
        _chart.addSeries(series)
        _chart.setAxisX(_axis_x, series)
        _chart.setAxisY(_axis_y, series)

    def _addBarSeries(
        self, _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
        _axis_x: QBarCategoryAxis, _axis_y: QValueAxis
    ):
        barset = QBarSet(_v['name'])
        tmp_dict = dict(zip(_v['x'], _v['y']))
        for k in _idx2x:
            if k in tmp_dict.keys():
                barset.append(tmp_dict[k])
            else:
                barset.append(0)
        if _v['color'] is not None:
            barset.setColor(_v['color'])

        barseries = QBarSeries()
        barseries.append(barset)
        _chart.addSeries(barseries)
        _chart.setAxisX(_axis_x, barseries)
        _chart.setAxisY(_axis_y, barseries)

    def appendAxisX(self, _list: typing.List[str]):
        self.axis_x.append(_list)

    def setAxisX(self, _begin: str, _end: str):
        self.axis_x.setRange(_begin, _end)

    def createChartView(
            self, _x2idx: dict, _idx2x: list
    ) -> QChartView:
        chart = QChart()

        for k, v in self.raw_data_dict.items():
            if v['type'] == self.LINE:
                self._addLineSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.BAR:
                self._addBarSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            else:
                raise Exception('Unknow type!')

        chartview = QChartView(chart)
        chartview.setRenderHint(QPainter.Antialiasing)
        return chartview
