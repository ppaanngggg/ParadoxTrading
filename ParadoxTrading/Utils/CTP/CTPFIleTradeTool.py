import configparser
import csv
import logging
from time import sleep

import schedule

from ParadoxTrading.Engine import DirectionType, ActionType
from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi


class CTPFileTradeTool:
    def __init__(
            self, _config_path: str,
            _order_csv_path: str, _fill_csv_path: str,
            _retry_time: int = 10, _price_rate: int = 1
    ):
        self.config = configparser.ConfigParser()
        self.config.read(_config_path)

        self.order_csv_path = _order_csv_path
        self.fill_csv_path = _fill_csv_path
        self.retry_time = _retry_time
        self.price_rate = _price_rate

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
            if direction == 'BUY':
                direction = DirectionType.BUY
            elif direction == 'SELL':
                direction = DirectionType.SELL
            else:
                raise Exception('unknown direction')
            if action == 'OPEN':
                action = ActionType.OPEN
            elif action == 'CLOSE':
                action = ActionType.CLOSE
            else:
                raise Exception('unknown action')
            ret[int(index)] = {
                'Symbol': instrument, 'Quantity': int(float(quantity)),
                'Action': action, 'Direction': direction
            }

        for fill in fill_reader:
            index = int(fill[0])
            del ret[index]

        return ret

    def tradeFunc(self):
        instrument_table = {}
        order_table = self.getRemainOrderTable()
        fill_table = {}

        for i in range(self.retry_time):
            self.newTraderSpi()  # create ctp obj

            logging.info('!!! TRY {}th TIME !!!'.format(i))
            if not self.trader.Connect():  # connect front
                continue
            if not self.trader.ReqUserLogin():  # login
                continue
            sleep(1)

            tmp = self.trader.ReqQryInstrument()  # all instrument
            if tmp is False:
                continue
            for d in tmp:
                instrument_table[d.index()[0].lower()] = d.toDict()
            sleep(1)

            indices = order_table.keys()
            for index in indices:
                # order info
                order_info = order_table[index]
                # instrument info
                inst_info = instrument_table[order_info['Symbol']]
                # encode the bytes instrument for ctp
                instrument = inst_info['InstrumentID'].encode()
                # commission rate
                comm_info = self.trader.ReqQryInstrumentCommissionRate(instrument)
                sleep(1)
                if comm_info is False:
                    continue
                # depth market data
                market_info = self.trader.ReqQryDepthMarketData(instrument)
                sleep(1)
                if market_info is False:
                    continue
                # limit price
                price_diff = self.price_rate * inst_info['PriceTick']
                if order_info['Direction'] == DirectionType.BUY:
                    price = market_info['AskPrice'] + price_diff
                elif order_info['Direction'] == DirectionType.SELL:
                    price = market_info['BidPrice'] - price_diff
                else:
                    raise Exception('unknown direction')
                # send order
                trade_info = self.trader.ReqOrderInsert(
                    instrument,
                    order_info['Direction'],
                    order_info['Action'],
                    order_info['Quantity'], price
                )
                sleep(1)
                if trade_info is False:
                    continue
                # !!! trade succeed !!!
                input(trade_info)

            self.trader.ReqUserLogout()  # logout
            sleep(1)
            self.delTraderSpi()  # free ctp obj

    def run(self):
        schedule.every().day.at("21:00").do(self.tradeFunc)
        schedule.every().day.at("09:00").do(self.tradeFunc)

        schedule.run_pending()
        sleep(max(schedule.idle_seconds(), 0))
