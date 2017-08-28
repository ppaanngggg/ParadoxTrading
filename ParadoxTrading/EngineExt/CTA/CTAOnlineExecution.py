import csv
import logging
import os
import re
import typing

from ParadoxTrading.Engine import ExecutionAbstract, OrderEvent, OrderType, \
    DirectionType, ActionType, FillEvent
from ParadoxTrading.EngineExt.CTA.CTAPortfolio import POINT_VALUE
from ParadoxTrading.Utils import DataStruct


class CTAOnlineExecution(ExecutionAbstract):
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

    def loadCSV(self):
        try:
            f = open('{}{}_fill.csv'.format(self.path, self.tradingday))
            f.readline()
            reader = csv.reader(f)
            for row in reader:
                index = int(row[0])
                instrument = row[1].lower()
                product = self.prog.findall(instrument)[0]
                assert index in self.order_dict.keys()
                del self.order_dict[index]
                self.addEvent(FillEvent(
                    _index=index, _symbol=instrument,
                    _tradingday=row[2], _datetime=row[3],
                    _quantity=int(row[4]) * POINT_VALUE[product],
                    _action=ActionType.fromStr(row[5]),
                    _direction=DirectionType.fromStr(row[6]),
                    _price=float(row[7]),
                    _commission=float(row[8]),
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
            'TradingDay', 'Datetime',
            'Type', 'Action', 'Direction',
            'Quantity', 'Price'
        ))
        for o in self.order_buf:
            product = self.prog.findall(o.symbol.lower())[0]
            writer.writerow((
                o.index, o.symbol,
                o.tradingday, o.datetime,
                OrderType.toStr(o.order_type),
                ActionType.toStr(o.action),
                DirectionType.toStr(o.direction),
                o.quantity / POINT_VALUE[product], o.price
            ))
        f.close()
