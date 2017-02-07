from PyQt5 import QtCore
from PyQt5.Qt import QKeyEvent
from PyQt5.QtWidgets import QWidget

import CWizard


class CWindow(QWidget):

    def setWizard(self, _wizard: "CWizard.CWizard"):
        self.wizard = _wizard

    def keyReleaseEvent(self, _event: QKeyEvent):
        key = _event.key()
        if key == QtCore.Qt.Key_Up:
            self.wizard.zoomIn()
        elif key == QtCore.Qt.Key_Down:
            self.wizard.zoomOut()
        elif key == QtCore.Qt.Key_Left:
            self.wizard.scrollLeft()
        elif key == QtCore.Qt.Key_Right:
            self.wizard.scrollRight()
        else:
            pass
