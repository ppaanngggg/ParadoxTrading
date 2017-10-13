import csv
import logging
import os
import re
import sys
import typing

from ParadoxTrading.Engine import ActionType, DirectionType, \
    ExecutionAbstract, FillEvent, OrderEvent
from ParadoxTrading.EngineExt.Futures.InterDayPortfolio import POINT_VALUE
from ParadoxTrading.Utils import DataStruct


class InterDayOnlineExecution(ExecutionAbstract):
    def __init__(self, _tradingday: str, _path: str = './csv/'):
        super().__init__()

        self.tradingday = _tradingday
        self.path = _path
        if not self.path.endswith('/'):
            self.path += '/'

        self.prog = re.compile(r'[a-z]+')
        self.order_buf: typing.List[OrderEvent] = []

    def matchMarket(self, _symbol: str, _data: DataStruct):
        pass

    def dealOrderEvent(self, _order_event: OrderEvent):
        self.order_dict[_order_event.index] = _order_event
        self.order_buf.append(_order_event)

    @staticmethod
    def sampleCSV(_path: str):
        f = open('{}.csv'.format(_path), 'w')
        writer = csv.writer(f)
        writer.writerow((
            'Index', 'Symbol', 'Quantity',
            'Action', 'Direction', 'Price', 'Commission'
        ))

    def loadCSV(self):
        try:
            f = open('{}{}_fill.csv'.format(self.path, self.tradingday))
            f.readline()
            reader = csv.reader(f)
            for row in reader:
                index = int(row[0])
                instrument = row[1].lower()
                product = self.prog.findall(instrument)[0]
                action = ActionType.fromStr(row[3])
                direction = DirectionType.fromStr(row[4])

                try:
                    assert index in self.order_dict.keys()
                    order = self.order_dict.pop(index)
                    assert action == order.action
                    assert direction == order.direction
                except AssertionError as e:
                    logging.error('Order and Fill not match, {}'.format(index))
                    ret = input('Continue?(y/n)')
                    if ret != 'y':
                        sys.exit(1)

                self.addEvent(FillEvent(
                    _index=index, _symbol=instrument,
                    _tradingday=self.tradingday,
                    _datetime=self.tradingday,
                    _quantity=int(row[2]) * POINT_VALUE[product],
                    _action=action,
                    _direction=direction,
                    _price=float(row[-2]),
                    _commission=float(row[-1]),
                ))
            f.close()
        except FileNotFoundError as e:
            logging.warning(e)

    def saveCSV(self):
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        f = open('{}{}_order.csv'.format(self.path, self.tradingday), 'w')
        writer = csv.writer(f)
        writer.writerow((
            'Index', 'Symbol',
            'Action', 'Direction', 'Quantity'
        ))
        for o in self.order_buf:
            product = self.prog.findall(o.symbol.lower())[0]
            writer.writerow((
                o.index, o.symbol,
                ActionType.toStr(o.action),
                DirectionType.toStr(o.direction),
                o.quantity / POINT_VALUE[product],
            ))
        self.order_buf = []  # clear it
        f.close()
