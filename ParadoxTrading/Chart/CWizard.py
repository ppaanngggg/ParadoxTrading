import sys
import typing
from datetime import datetime

from PyQt5.Qt import QColor, QPainter
from PyQt5.QtChart import QBarCategoryAxis
from PyQt5.QtWidgets import QApplication, QPushButton, QVBoxLayout

import CView
import CWindow


class CWizard:

    BAR = 'bar'
    LINE = 'line'

    def __init__(
        self, _title: str='', _width: int=800, _height: int=600,
    ):
        self.title = _title
        self.width = _width
        self.height = _height

        self.view_list = []  # typing.List[CView.CView]

        self.begin_idx = 0
        self.end_idx = 0

    def addView(self, _stretch: int, _name: str=None):
        assert _stretch > 0
        self.view_list.append({
            'view': CView.CView(_name),
            'stretch': _stretch,
            'name': _name,
        })

    def addLine(
        self, _view_idx: int,
        _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: QColor=None
    ):
        assert _view_idx < len(self.view_list)
        self.view_list[_view_idx]['view'].addLine(
            _x_list, _y_list, _name, _color)

    def addBar(
        self, _view_idx: int,
        _x_list: typing.List[datetime], _y_list: list,
        _name: str, _color: QColor=None
    ):
        assert _view_idx < len(self.view_list)
        self.view_list[_view_idx]['view'].addBar(
            _x_list, _y_list, _name, _color)

    def _calcSetXDictList(self) -> (dict, dict):
        tmp = set()
        for d in self.view_list:
            tmp |= d['view'].calcSetX()
        tmp = sorted(list(tmp))
        return dict(zip(tmp, range(len(tmp)))), tmp

    def _calcAxisX(self, _list: list):
        tmp = [str(d) for d in _list]
        for d in self.view_list:
            d['view'].appendAxisX(tmp)
        self.begin_idx = 0
        self.end_idx = len(_list) - 1

    def show(self):
        if not self.view_list:
            return

        app = QApplication(sys.argv)

        self.x2idx, self.idx2x = self._calcSetXDictList()
        self._calcAxisX(self.idx2x)

        layout = QVBoxLayout()

        for d in self.view_list:
            layout.addWidget(
                d['view'].createChartView(self.x2idx, self.idx2x),
                d['stretch']
            )

        window = CWindow.CWindow()
        window.setWizard(self)
        window.setWindowTitle(self.title)
        window.resize(self.width, self.height)
        window.setLayout(layout)

        window.show()

        return app.exec()

    def _setAxisX(self, _begin: int, _end: int):
        tmp_begin = str(self.idx2x[_begin])
        tmp_end = str(self.idx2x[_end])

        for d in self.view_list:
            d['view'].setAxisX(tmp_begin, tmp_end)

    def zoomIn(self):
        mid = (self.begin_idx + self.end_idx) / 2.0
        diff = max(int((self.end_idx - self.begin_idx) / 2.0 / 10.0), 1)
        if self.end_idx - self.begin_idx > 2 * diff:
            self.begin_idx += diff
            self.end_idx -= diff

        self._setAxisX(self.begin_idx, self.end_idx)

    def zoomOut(self):
        mid = (self.begin_idx + self.end_idx) / 2.0
        diff = max(int((self.end_idx - self.begin_idx) / 2.0 / 10.0), 1)
        self.begin_idx = max(self.begin_idx - diff, 0)
        self.end_idx = min(self.end_idx + diff, len(self.idx2x) - 1)

        self._setAxisX(self.begin_idx, self.end_idx)

    def scrollLeft(self):
        diff = max(int((self.end_idx - self.begin_idx) / 10.0), 1)
        self.begin_idx -= diff
        self.begin_idx = max(self.begin_idx, 0)
        self.end_idx -= diff
        self.end_idx = max(self.end_idx, 0)

        self._setAxisX(self.begin_idx, self.end_idx)

    def scrollRight(self):
        diff = max(int((self.end_idx - self.begin_idx) / 10.0), 1)
        self.begin_idx += diff
        self.begin_idx = min(self.begin_idx, len(self.idx2x) - 1)
        self.end_idx += diff
        self.end_idx = min(self.end_idx, len(self.idx2x) - 1)

        self._setAxisX(self.begin_idx, self.end_idx)

if __name__ == '__main__':
    from ParadoxTrading.Utils import Fetch, SplitIntoMinute
    from ParadoxTrading.Indicator import CloseBar, Diff, MA

    from time import time

    begin_time = time()
    data = Fetch.fetchIntraDayData('20170123', 'rb')

    spliter = SplitIntoMinute(1)
    spliter.addMany(data)

    closeprice = CloseBar('lastprice').addMany(
        spliter.getBarBeginTimeList(),
        spliter.getBarList()).getAllData()

    maprice = MA(5, 'close').addMany(
        closeprice.index(), closeprice).getAllData()

    closevolume = CloseBar('volume').addMany(
        spliter.getBarBeginTimeList(),
        spliter.getBarList()).getAllData()

    volume = Diff('close').addMany(
        closevolume.index(), closevolume).getAllData()

    data = closeprice
    data.expand(volume)
    data.expand(maprice)
    data.changeColumnName('close', 'price')
    data.changeColumnName('diff', 'volume')

    print(data)

    wizard = CWizard('rb1705')

    wizard.addView(3, 'price')
    wizard.addLine(
        0, data.index(), data.getColumn('price'),
        'price', QColor('green'))
    wizard.addLine(
        0, data.index(), data.getColumn('ma'),
        'ma')

    wizard.addView(1, 'volume')
    wizard.addBar(
        1, data.index(), data.getColumn('volume'),
        'volume', QColor('red'))

    wizard.show()
