import typing

from PyQt5.Qt import QColor
from PyQt5.QtChart import QChart, QValueAxis
from PyQt5.QtWidgets import QGroupBox, QLineEdit, QFormLayout

from ParadoxTrading.Utils import DataStruct


class SeriesAbstract:
    BAR = 'bar'
    LINE = 'line'
    SCATTER = 'scatter'
    CANDLE = 'candle'

    def __init__(
            self, _name: str,
            _x_list: typing.Sequence,
            _y_list: typing.Sequence,
            _color: typing.Any = None,
            _show_value: bool = True
    ):
        assert len(_x_list) == len(_y_list)
        assert len(_x_list) > 0

        self.name = _name
        self.x_list = _x_list
        self.y_list = _y_list
        self.show_value = _show_value
        self.show_group: QGroupBox = None
        self.show_edit: QLineEdit = None

        self.x2y = DataStruct(
            ['x', 'y'], 'x',
            list(zip(self.x_list, self.y_list))
        )
        self.color = None if _color is None else QColor(_color)

    def calcSetX(self) -> typing.Set:
        return set(self.x_list)

    def calcRangeY(
            self, _begin_x=None, _end_x=None
    ) -> typing.Tuple:
        tmp_y = self.x2y.loc[_begin_x:_end_x]['y']
        if len(tmp_y) == 0:
            return None, None
        return min(tmp_y), max(tmp_y)

    def addSeries(
            self, _x2idx: typing.Dict, _idx2x: list, _chart: QChart,
            _axis_x: QValueAxis, _axis_y: QValueAxis
    ):
        raise NotImplementedError('implement addSeries plz')

    def createShow(self):
        self.show_group = QGroupBox()
        self.show_group.setTitle(self.name)
        self.show_edit = QLineEdit()
        self.show_edit.setDisabled(True)
        layout = QFormLayout()
        layout.addWidget(self.show_edit)
        self.show_group.setLayout(layout)

    def updateValue(self, _x):
        value = self.x2y.loc[_x]
        if value is None:
            self.show_edit.setText('')
        else:
            value = value['y'][0]
            self.show_edit.setText('{:.5f}'.format(value))
