import sys
import typing
from datetime import datetime

from PyQt5.Qt import QColor
from PyQt5.QtWidgets import QApplication, QVBoxLayout

import ParadoxTrading.Chart.View
import ParadoxTrading.Chart.Window


class Wizard:
    ZOOM_STEP = 10.0
    SCROLL_STEP = 10.0

    def __init__(
            self, _title: str = '', _width: int = 1440, _height: int = 960,
    ):
        self.title = _title
        self.width = _width
        self.height = _height

        self.view_list: typing.List[str] = []
        self.view_dict: typing.Dict = {}

        # map x to axis idx
        self.x2idx: typing.Dict[typing.Any, int] = None
        # map axis idx to x
        self.idx2x: typing.List[typing.Any] = None

        self.begin_idx: int = 0
        self.end_idx: int = 0

    def addView(
            self, _name: str, _stretch: int = 1,
            _view_stretch: int = 15
    ) -> str:
        assert _name not in self.view_dict.keys()
        assert _stretch > 0
        self.view_list.append(_name)
        self.view_dict[_name] = {
            'view': ParadoxTrading.Chart.View.View(
                _name, self, _view_stretch
            ),
            'stretch': _stretch
        }
        return _name

    def addLine(
            self, _view_name: str,
            _x_list: typing.List[datetime], _y_list: list,
            _name: str, _color: typing.Union[typing.Any, QColor] = None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addLine(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color)
        )

    def addBar(
            self, _view_name: str,
            _x_list: typing.List[typing.Any], _y_list: list,
            _name: str, _color: typing.Union[typing.Any, QColor] = None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addBar(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color)
        )

    def addScatter(
            self, _view_name: str,
            _x_list: typing.List[typing.Any], _y_list: list,
            _name: str, _color: typing.Union[typing.Any, QColor] = None
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addScatter(
            _x_list, _y_list, _name,
            None if _color is None else QColor(_color)
        )

    def addCandle(
            self, _view_name: str,
            _x_list: typing.List[typing.Any],
            _y_list: typing.Sequence[typing.Sequence],
            _name: str,
            _inc_color: typing.Union[typing.Any, QColor] = None,
            _dec_color: typing.Union[typing.Any, QColor] = None,
    ):
        assert _view_name in self.view_dict.keys()
        self.view_dict[_view_name]['view'].addCandle(
            _x_list, _y_list, _name,
            None if _inc_color is None else QColor(_inc_color),
            None if _dec_color is None else QColor(_dec_color),
        )

    def _calcSetX(self) -> (dict, list):
        # join all x
        tmp = set()
        for d in self.view_dict.values():
            tmp |= d['view'].calcSetX()
        tmp = sorted(list(tmp))
        # reset x range
        self.begin_idx = 0
        self.end_idx = len(tmp) - 1

        return dict(zip(tmp, range(len(tmp)))), tmp

    def _setAxisX(self, _begin: int, _end: int):
        tmp_begin = _begin - 1
        tmp_end = _end + 1

        for d in self.view_dict.values():
            d['view'].setAxisX(tmp_begin, tmp_end)

    def show(self):
        if not self.view_dict:
            return

        app = QApplication(sys.argv)

        # get the axis info, and reset it
        self.x2idx, self.idx2x = self._calcSetX()
        self._setAxisX(self.begin_idx, self.end_idx)

        layout = QVBoxLayout()
        # keep the sort when inserted
        for d in [self.view_dict[name] for name in self.view_list]:
            layout.addLayout(
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

    def mouseMove(self, _index):
        _index = max(0, _index)
        _index = min(len(self.idx2x) - 1, _index)
        x = self.idx2x[_index]
        for d in self.view_dict.values():
            d['view'].updateValue(x)
