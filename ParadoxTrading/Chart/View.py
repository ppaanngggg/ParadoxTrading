import typing
from datetime import datetime

import numpy as np
from PyQt5.Qt import QColor, QMouseEvent, QPainter
from PyQt5.QtChart import (QBarSeries, QBarSet, QChart, QCandlestickSeries, QCandlestickSet,
                           QChartView, QLineSeries, QScatterSeries, QValueAxis)
from PyQt5.QtWidgets import QHBoxLayout, QFormLayout, QLineEdit, QGroupBox, QLabel

import ParadoxTrading.Chart


class ChartView(QChartView):
    def __init__(
            self, _chart: QChart,
            _wizard: "ParadoxTrading.Chart.Wizard"
    ):
        super().__init__(_chart)
        self.wizard: ParadoxTrading.Chart.Wizard = _wizard

    def mouseMoveEvent(self, _event: QMouseEvent):
        point = self.chart().mapToValue(_event.pos())
        index = round(point.x())
        self.wizard.mouseMove(index)


class View:
    BAR = 'bar'
    LINE = 'line'
    SCATTER = 'scatter'
    CANDLE = 'candle'

    def __init__(
            self, _name: str,
            _wizard: "ParadoxTrading.Chart.Wizard",
            _stretch: int = 10
    ):
        """
        
        
        :param _name: name for this view, this will be shown as axis y
        :param _wizard: ref to wizard
        :param _stretch: screen rate for chart / value
        """

        self.name = _name
        self.wizard = _wizard
        self.stretch = _stretch

        self.raw_data_dict = {}

        self.axis_x = QValueAxis()
        self.axis_x.setVisible(False)
        self.axis_y = QValueAxis()
        # set name to axis_y, price for price, volume for volume, etc
        self.axis_y.setTitleText(self.name)

        self.x_edit: QLineEdit = None

    def _add(
            self, _x_list: typing.List[datetime],
            _y_list: typing.Sequence, _name: str,
            _color: typing.Union[QColor, None], _type: str
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

    def addCandle(
            self,
            _x_list: typing.List[typing.Any],
            _y_list: typing.Sequence[typing.Sequence],
            _name: str,
            _inc_color: QColor = None, _dec_color: QColor = None,
    ):
        self._add(_x_list, _y_list, _name, None, self.CANDLE)
        self.raw_data_dict[_name]['inc_color'] = _inc_color
        self.raw_data_dict[_name]['dec_color'] = _dec_color

    def calcSetX(self) -> typing.Set:
        """
        get the set of x, to get x range for wizard
        
        :return: the set of all x value
        """
        tmp = set()
        for v in self.raw_data_dict.values():
            tmp |= set(v['x'])
        return tmp

    def calcRangeY(self) -> typing.Tuple:
        """
        get range of y for this view
        
        :return: (min, max)
        """
        tmp_min_list = []
        tmp_max_list = []
        for v in self.raw_data_dict.values():
            if v['type'] == View.LINE or v['type'] == View.BAR or \
                            v['type'] == View.SCATTER:
                tmp_min_list.append(min(v['y']))
                tmp_max_list.append(max(v['y']))
            elif v['type'] == View.CANDLE:
                tmp_y = np.array(v['y'])
                tmp_min_list.append(tmp_y.min())
                tmp_max_list.append(tmp_y.max())
            else:
                raise Exception('unknown type')

        return min(tmp_min_list), max(tmp_max_list),

    @staticmethod
    def _addLineSeries(
            _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
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

        group = QGroupBox()
        group.setTitle(_v['name'])
        edit = QLineEdit()
        edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(edit)
        group.setLayout(layout)

        _v['group'] = group
        _v['edit'] = edit

    @staticmethod
    def _updateLineValue(_x: typing.Any, _v: dict):
        try:
            value = _v['x2y'][_x]
            _v['edit'].setText('{:f}'.format(value))
        except KeyError:
            _v['edit'].setText('')

    @staticmethod
    def _addBarSeries(
            _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
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

        group = QGroupBox()
        group.setTitle(_v['name'])
        edit = QLineEdit()
        edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(edit)
        group.setLayout(layout)

        _v['group'] = group
        _v['edit'] = edit

    @staticmethod
    def _updateBarValue(_x: typing.Any, _v: dict):
        try:
            value = _v['x2y'][_x]
            _v['edit'].setText('{:f}'.format(value))
        except KeyError:
            _v['edit'].setText('')

    @staticmethod
    def _addScatterSeries(
            _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
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

        group = QGroupBox()
        group.setTitle(_v['name'])
        edit = QLineEdit()
        edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(edit)
        group.setLayout(layout)

        _v['group'] = group
        _v['edit'] = edit

    @staticmethod
    def _updateScatterValue(_x: typing.Any, _v: dict):
        try:
            value = _v['x2y'][_x]
            _v['edit'].setText('{:f}'.format(value))
        except KeyError:
            _v['edit'].setText('')

    @staticmethod
    def _addCandleSeries(
            _x2idx: dict, _idx2x: list, _v: dict, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
    ):
        series = QCandlestickSeries()
        series.setName(_v['name'])
        for x, y in zip(_v['x'], _v['y']):
            series.append(QCandlestickSet(*y, _x2idx[x]))
        if _v['inc_color'] is not None:
            series.setIncreasingColor(_v['inc_color'])
        else:
            series.setIncreasingColor(QColor('#c41919'))
        if _v['dec_color'] is not None:
            series.setDecreasingColor(_v['dec_color'])
        else:
            series.setDecreasingColor(QColor('#009f13'))

        _chart.addSeries(series)
        _chart.setAxisX(_axis_x, series)
        _chart.setAxisY(_axis_y, series)

        group = QGroupBox()
        group.setTitle(_v['name'])
        open_edit = QLineEdit()
        open_edit.setDisabled(True)
        high_edit = QLineEdit()
        high_edit.setDisabled(True)
        low_edit = QLineEdit()
        low_edit.setDisabled(True)
        close_edit = QLineEdit()
        close_edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(QLabel('openprice'))
        layout.addWidget(open_edit)
        layout.addWidget(QLabel('highprice'))
        layout.addWidget(high_edit)
        layout.addWidget(QLabel('lowprice'))
        layout.addWidget(low_edit)
        layout.addWidget(QLabel('closeprice'))
        layout.addWidget(close_edit)
        group.setLayout(layout)

        _v['group'] = group
        _v['open_edit'] = open_edit
        _v['high_edit'] = high_edit
        _v['low_edit'] = low_edit
        _v['close_edit'] = close_edit

    @staticmethod
    def __updateCandleValue(_x: typing.Any, _v: dict):
        try:
            value = _v['x2y'][_x]
            _v['open_edit'].setText('{:f}'.format(value[0]))
            _v['high_edit'].setText('{:f}'.format(value[1]))
            _v['low_edit'].setText('{:f}'.format(value[2]))
            _v['close_edit'].setText('{:f}'.format(value[3]))
        except KeyError:
            _v['open_edit'].setText('')
            _v['high_edit'].setText('')
            _v['low_edit'].setText('')
            _v['close_edit'].setText('')

    def setAxisX(self, _begin: float, _end: float):
        self.axis_x.setRange(_begin, _end)

    def setAxisY(self, _begin: float, _end: float):
        self.axis_y.setRange(_begin, _end)

    def createChartView(
            self, _x2idx: dict, _idx2x: list
    ) -> QHBoxLayout:
        chart = QChart()

        # add value widget for x
        group = QGroupBox()
        group.setTitle('x')
        self.x_edit = QLineEdit()
        self.x_edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(self.x_edit)
        group.setLayout(layout)

        value_layout = QFormLayout()
        value_layout.addWidget(group)

        # assign y range
        self.setAxisY(*self.calcRangeY())

        # add each series
        for k, v in self.raw_data_dict.items():
            if v['type'] == self.LINE:
                View._addLineSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.BAR:
                View._addBarSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.SCATTER:
                View._addScatterSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            elif v['type'] == self.CANDLE:
                View._addCandleSeries(
                    _x2idx, _idx2x, v, chart, self.axis_x, self.axis_y)
            else:
                raise Exception('Unknown type!')

            # add value widget for each series
            value_layout.addWidget(v['group'])

        # create chartview and layout for view and value
        chartview = ChartView(chart, self.wizard)
        chartview.setRenderHint(QPainter.Antialiasing)

        global_layout = QHBoxLayout()
        global_layout.addWidget(chartview, self.stretch)
        global_layout.addLayout(value_layout)

        return global_layout

    def updateValue(self, _x):
        self.x_edit.setText('{}'.format(_x))
        for v in self.raw_data_dict.values():
            if v['type'] == View.LINE:
                View._updateLineValue(_x, v)
            elif v['type'] == View.BAR:
                View._updateBarValue(_x, v)
            elif v['type'] == View.SCATTER:
                View._updateScatterValue(_x, v)
            elif v['type'] == View.CANDLE:
                View.__updateCandleValue(_x, v)
            else:
                raise Exception('Unknown type!')
