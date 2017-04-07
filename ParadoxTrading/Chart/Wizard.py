import sys
import typing
from datetime import datetime

from PyQt5.Qt import QColor, QPainter
from PyQt5.QtChart import QBarCategoryAxis
from PyQt5.QtWidgets import QApplication, QPushButton, QVBoxLayout

import ParadoxTrading.Chart.View
import ParadoxTrading.Chart.Window


class Wizard:

    ZOOM_STEP = 10.0
    SCROLL_STEP = 10.0

    def __init__(
        self, _title: str='', _width: int=1440, _height: int=960,
    ):
        self.title = _title
        self.width = _width
        self.height = _height

        self.view_list = []
        self.view_dict = {}  # typing.Dict

        self.begin_idx = 0
        self.end_idx = 0

    def addView(self, _name: str=None, _stretch: int=1):
        assert _name not in self.view_dict.keys()
        assert _stretch > 0
        self.view_list.append(_name)
        self.view_dict[_name] = {
            'view': ParadoxTrading.Chart.View.View(_name),
            'stretch': _stretch
        }

    def addLine(
        self, _view_name: str,
        _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: typing.Union[typing.Any, QColor]=None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addLine(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color))

    def addBar(
        self, _view_name: str,
        _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: typing.Union[typing.Any, QColor]=None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addBar(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color))

    def addScatter(
        self, _view_name:str,
        _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: typing.Union[typing.Any, QColor]=None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addScatter(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color))

    def _calcSetX(self) -> (dict, dict):
        tmp = set()
        for d in self.view_dict.values():
            tmp |= d['view'].calcSetX()
        tmp = sorted(list(tmp))
        return dict(zip(tmp, range(len(tmp)))), tmp

    def _calcAxisX(self, _list: list):
        tmp = [str(d) for d in _list]
        for d in self.view_dict.values():
            d['view'].appendAxisX(tmp)
        self.begin_idx = 0
        self.end_idx = len(_list) - 1

    def show(self):
        if not self.view_dict:
            return

        app = QApplication(sys.argv)

        self.x2idx, self.idx2x = self._calcSetX()
        self._calcAxisX(self.idx2x)

        layout = QVBoxLayout()

        for d in [self.view_dict[name] for name in self.view_list]:
            layout.addWidget(
                d['view'].createChartView(self.x2idx, self.idx2x),
                d['stretch']
            )

        window = ParadoxTrading.Chart.Window.Window()
        window.setWizard(self)
        window.setWindowTitle(self.title)
        window.resize(self.width, self.height)
        window.setLayout(layout)

        window.show()

        return app.exec()

    def _setAxisX(self, _begin: int, _end: int):
        tmp_begin = str(self.idx2x[_begin])
        tmp_end = str(self.idx2x[_end])

        for d in self.view_dict.values():
            d['view'].setAxisX(tmp_begin, tmp_end)

    def zoomIn(self):
        mid = (self.begin_idx + self.end_idx) / 2.0
        diff = max(
            int((self.end_idx - self.begin_idx) / 2.0 / self.ZOOM_STEP),
            1)
        if self.end_idx - self.begin_idx > 2 * diff:
            self.begin_idx += diff
            self.end_idx -= diff

        self._setAxisX(self.begin_idx, self.end_idx)

    def zoomOut(self):
        mid = (self.begin_idx + self.end_idx) / 2.0
        diff = max(
            int((self.end_idx - self.begin_idx) / 2.0 / self.ZOOM_STEP),
            1)
        self.begin_idx = max(self.begin_idx - diff, 0)
        self.end_idx = min(self.end_idx + diff, len(self.idx2x) - 1)

        self._setAxisX(self.begin_idx, self.end_idx)

    def scrollLeft(self):
        diff = max(
            int((self.end_idx - self.begin_idx) / self.SCROLL_STEP),
            1)
        self.begin_idx -= diff
        self.begin_idx = max(self.begin_idx, 0)
        self.end_idx -= diff
        self.end_idx = max(self.end_idx, 0)

        self._setAxisX(self.begin_idx, self.end_idx)

    def scrollRight(self):
        diff = max(
            int((self.end_idx - self.begin_idx) / self.SCROLL_STEP),
            1)
        self.begin_idx += diff
        self.begin_idx = min(self.begin_idx, len(self.idx2x) - 1)
        self.end_idx += diff
        self.end_idx = min(self.end_idx, len(self.idx2x) - 1)

        self._setAxisX(self.begin_idx, self.end_idx)
