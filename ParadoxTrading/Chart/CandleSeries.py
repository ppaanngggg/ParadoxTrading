import typing

import numpy as np
from PyQt5.Qt import QColor
from PyQt5.QtChart import QChart, QValueAxis, QCandlestickSet, QCandlestickSeries
from PyQt5.QtWidgets import QGroupBox, QLineEdit, QFormLayout, QLabel

from ParadoxTrading.Chart.SeriesAbstract import SeriesAbstract


class CandleSeries(SeriesAbstract):
    def __init__(
            self, _name: str,
            _x_list: typing.Sequence,
            _y_list: typing.Sequence,
            _inc_color: typing.Any = None,
            _dec_color: typing.Any = None,
            _show_value: bool = True,
    ):
        super().__init__(_name, _x_list, _y_list, None, _show_value)

        self.show_open_edit: QLineEdit = None
        self.show_high_edit: QLineEdit = None
        self.show_low_edit: QLineEdit = None
        self.show_close_edit: QLineEdit = None

        self.inc_color = _inc_color
        self.dec_color = _dec_color
        self.type = SeriesAbstract.CANDLE

    def calcRangeY(
            self, _begin_x=None, _end_x=None
    ) -> typing.Tuple:
        tmp_y = self.x2y.loc[_begin_x:_end_x]['y']
        if len(tmp_y) == 0:
            return None, None
        tmp_y = np.array(tmp_y)
        return tmp_y.min(), tmp_y.max()

    def addSeries(
            self, _x2idx: typing.Dict, _idx2x: list, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
    ):
        series = QCandlestickSeries()
        series.setName(self.name)

        for x, y in zip(self.x_list, self.y_list):
            series.append(QCandlestickSet(*y, _x2idx[x]))
        if self.inc_color is not None:
            series.setIncreasingColor(self.inc_color)
        else:
            series.setIncreasingColor(QColor('#c41919'))
        if self.dec_color is not None:
            series.setDecreasingColor(self.dec_color)
        else:
            series.setDecreasingColor(QColor('#009f9f'))

        _chart.addSeries(series)
        _chart.setAxisX(_axis_x, series)
        _chart.setAxisY(_axis_y, series)

        if self.show_value:
            self.createShow()

    def createShow(self):
        self.show_group = QGroupBox()
        self.show_group.setTitle(self.name)

        self.show_open_edit = QLineEdit()
        self.show_open_edit.setDisabled(True)
        self.show_high_edit = QLineEdit()
        self.show_high_edit.setDisabled(True)
        self.show_low_edit = QLineEdit()
        self.show_low_edit.setDisabled(True)
        self.show_close_edit = QLineEdit()
        self.show_close_edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(QLabel('open'))
        layout.addWidget(self.show_open_edit)
        layout.addWidget(QLabel('high'))
        layout.addWidget(self.show_high_edit)
        layout.addWidget(QLabel('low'))
        layout.addWidget(self.show_low_edit)
        layout.addWidget(QLabel('close'))
        layout.addWidget(self.show_close_edit)
        self.show_group.setLayout(layout)

    def updateValue(self, _x):
        value = self.x2y.loc[_x]
        if value is None:
            self.show_open_edit.setText('')
            self.show_high_edit.setText('')
            self.show_low_edit.setText('')
            self.show_close_edit.setText('')
        else:
            value = value['y'][0]
            self.show_open_edit.setText('{:f}'.format(value[0]))
            self.show_high_edit.setText('{:f}'.format(value[1]))
            self.show_low_edit.setText('{:f}'.format(value[2]))
            self.show_close_edit.setText('{:f}'.format(value[3]))
