import typing
from datetime import datetime

from PyQt5.Qt import QColor, QMouseEvent, QPainter
from PyQt5.QtChart import (QBarCategoryAxis, QBarSeries, QBarSet, QChart,
                           QChartView, QLineSeries, QScatterSeries, QValueAxis)
from PyQt5.QtWidgets import QHBoxLayout, QFormLayout, QLabel, QLineEdit

import ParadoxTrading.Chart


class ChartView(QChartView):
    def setWizard(self, _wizard: "ParadoxTrading.Chart.Wizard"):
        self.wizard = _wizard

    def mouseMoveEvent(self, _event: QMouseEvent):
        point = self.chart().mapToValue(_event.pos())
        index = round(point.x())
        self.wizard.mouseMove(index)


class View:
    BAR = 'bar'
    LINE = 'line'
    SCATTER = 'scatter'

    def __init__(
            self, _name: str,
            _wizard: "ParadoxTrading.Chart.Wizard",
            _stretch: int = 10
    ):

        self.name = _name
        self.wizard = _wizard
        self.stretch = _stretch

        self.raw_data_dict = {}

        self.axis_x = QBarCategoryAxis()
        self.axis_x.setVisible(False)
        self.axis_y = QValueAxis()
        self.axis_y.setTitleText(self.name)

    def _add(
            self, _x_list: typing.List[datetime], _y_list: list,
            _name: str, _color: QColor, _type: str
    ):
        assert _name not in self.raw_data_dict.keys()
        assert len(_x_list) == len(_y_list)
        self.raw_data_dict[_name] = {
            'x': _x_list, 'y': _y_list, 'name': _name,
            'color': _color, 'type': _type,
            'x2y': dict(zip(_x_list, _y_list)),
        }

    def addBar(
            self, _x_list: typing.List[datetime], _y_list: list,
            _name: str, _color: QColor = None
    ):
        self._add(_x_list, _y_list, _name, _color, self.BAR)

    def addLine(
            self, _x_list: typing.List[datetime], _y_list: list,
            _name: str, _color: QColor = None
    ):
        self._add(_x_list, _y_list, _name, _color, self.LINE)

    def addScatter(
            self, _x_list: typing.List[datetime], _y_list: list,
            _name: str, _color: QColor = None
    ):
        self._add(_x_list, _y_list, _name, _color, self.SCATTER)

    def calcSetX(self) -> set:
        tmp = set()
        for v in self.raw_data_dict.values():
            tmp |= set(v['x'])
        return tmp

    def calcRangeY(self) -> (float, float):
        return (
            min([min(v['y']) for v in self.raw_data_dict.values()]),
            max([max(v['y']) for v in self.raw_data_dict.values()]),
        )

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

        edit = QLineEdit()
        edit.setDisabled(True)
        _v['edit'] = edit

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

        edit = QLineEdit()
        edit.setDisabled(True)
        _v['edit'] = edit

    def _addScatterSeries(
            self, _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
            _axis_x: QBarCategoryAxis, _axis_y: QValueAxis
    ):
        series = QScatterSeries()
        series.setName(_v['name'])
        for x, y in zip(_v['x'], _v['y']):
            series.append(_x2idx[x], y)
        if _v['color'] is not None:
            series.setColor(_v['color'])

        _chart.addSeries(series)
        _chart.setAxisX(_axis_x, series)
        _chart.setAxisY(_axis_y, series)

        edit = QLineEdit()
        edit.setDisabled(True)
        _v['edit'] = edit

    def appendAxisX(self, _list: typing.List[str]):
        self.axis_x.append(_list)

    def setAxisX(self, _begin: str, _end: str):
        self.axis_x.setRange(_begin, _end)

    def setAxisY(self, _begin: str, _end: str):
        self.axis_y.setRange(_begin, _end)

    def createChartView(
            self, _x2idx: dict, _idx2x: list
    ) -> QHBoxLayout:
        chart = QChart()

        self.x_edit = QLineEdit()
        self.x_edit.setDisabled(True)
        form_layout = QFormLayout()
        form_layout.addRow(QLabel('x:'), self.x_edit)

        self.setAxisY(*self.calcRangeY())
        for k, v in self.raw_data_dict.items():
            if v['type'] == self.LINE:
                self._addLineSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.BAR:
                self._addBarSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.SCATTER:
                self._addScatterSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            else:
                raise Exception('Unknow type!')
            form_layout.addRow(QLabel(k + ':'), v['edit'])

        chartview = ChartView(chart)
        chartview.setWizard(self.wizard)
        chartview.setRenderHint(QPainter.Antialiasing)

        global_layout = QHBoxLayout()
        global_layout.addWidget(chartview, self.stretch)
        global_layout.addLayout(form_layout)

        return global_layout

    def updateValue(self, _x):
        self.x_edit.setText(str(_x))
        for v in self.raw_data_dict.values():
            try:
                value = v['x2y'][_x]
                v['edit'].setText(str(value))
            except KeyError:
                pass
