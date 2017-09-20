import configparser
import csv
import logging
from time import sleep

import schedule

from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi


class CTPFileTradeTool:
    def __init__(
            self, _config_path: str,
            _order_csv_path: str, _fill_csv_path: str
    ):
        self.config = configparser.ConfigParser()
        self.config.read(_config_path)

        self.order_csv_path = _order_csv_path
        self.fill_csv_path = _fill_csv_path

        self.trader: CTPTraderSpi = None

    def newTraderSpi(self):
        self.trader = CTPTraderSpi(
            self.config['TRADE']['ConPath'].encode(),
            self.config['TRADE']['Front'].encode(),
            self.config['TRADE']['BrokerID'].encode(),
            self.config['TRADE']['UserID'].encode(),
            self.config['TRADE']['Password'].encode(),
        )

    def delTraderSpi(self):
        del self.trader
        self.trader: CTPTraderSpi = None

    def getRemainOrderTable(self):
        ret = {}

        try:
            order_file = open(self.order_csv_path)
        except FileNotFoundError:
            logging.warning('NO order csv found')
            return ret

        try:
            fill_file = open(self.fill_csv_path)
        except FileNotFoundError:
            logging.warning('NO fill csv found')
            fill_file = open(self.fill_csv_path, 'w')
            fill_writer = csv.writer(fill_file)
            fill_writer.writerow((
                'Index', 'Symbol', 'Quantity',
                'Action', 'Direction', 'Price', 'Commission'
            ))
            fill_file = open(self.fill_csv_path)

        order_file.readline()
        fill_file.readline()
        order_reader = csv.reader(order_file)
        fill_reader = csv.reader(fill_file)

        for order in order_reader:
            index, instrument, action, direction, quantity = order
            ret[int(index)] = {
                'Symbol': instrument, 'Quantity': int(float(quantity)),
                'Action': action, 'Direction': direction
            }

        for fill in fill_reader:
            index = int(fill[0])
            del ret[index]

        return ret

    def tradeFunc(self):
        order_table = self.getRemainOrderTable()
        input(order_table)
        self.newTraderSpi()  # create ctp obj

        self.delTraderSpi()  # free ctp obj

    def run(self):
        schedule.every().day.at("21:00").do(self.tradeFunc)
        schedule.every().day.at("09:00").do(self.tradeFunc)

        schedule.run_pending()
        sleep(max(schedule.idle_seconds(), 0))
