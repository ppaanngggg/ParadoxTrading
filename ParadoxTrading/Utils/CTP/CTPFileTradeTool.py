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
            _retry_time: int = 3, _price_rate: int = 1
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
        self.trader.Release()
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
            direction = DirectionType.fromStr(direction)
            action = ActionType.fromStr(action)
            ret[int(index)] = {
                'Symbol': instrument, 'Quantity': int(float(quantity)),
                'Action': action, 'Direction': direction
            }

        for fill in fill_reader:
            index = int(fill[0])
            del ret[index]

        order_file.close()
        fill_file.close()

        return ret

    def _trader_login(self):
        self.newTraderSpi()  # create ctp obj

        if not self.trader.Connect():  # connect front
            return False
        sleep(1)
        if not self.trader.ReqUserLogin():  # login
            return False
        sleep(1)
        if not self.trader.ReqSettlementInfoConfirm():  # settlement
            return False
        sleep(1)
        return True

    def _trader_all_instrument(self, _instrument_table: dict):
        tmp = self.trader.ReqQryInstrument()  # all instrument
        if tmp is False:
            return False
        for d in tmp:
            _instrument_table[d.index()[0].lower()] = d.toDict()
        sleep(1)
        return True

    def _trader_send_order(
            self, _index, _instrument_table, _order_table, _fill_table
    ):
        # order info
        order_info = _order_table[_index]
        # instrument info
        inst_info = _instrument_table[order_info['Symbol']]
        # encode the bytes instrument for ctp
        instrument = inst_info['InstrumentID'].encode()
        # commission rate
        comm_info = self.trader.ReqQryInstrumentCommissionRate(instrument)
        sleep(1)
        if comm_info is False:
            return False
        # depth market data
        market_info = self.trader.ReqQryDepthMarketData(instrument)
        sleep(1)
        if market_info is False:
            return False
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
            return False
        # !!! trade succeed !!!
        comm_value = 0
        if order_info['Action'] == ActionType.OPEN:
            comm_value += comm_info['OpenRatioByMoney'] * trade_info['Price'] * \
                          trade_info['Volume'] * inst_info['VolumeMultiple']
            comm_value += comm_info['OpenRatioByVolume'] * trade_info['Volume']
        elif order_info['Action'] == ActionType.CLOSE:
            comm_value += comm_info['CloseRatioByMoney'] * trade_info['Price'] * \
                          trade_info['Volume'] * inst_info['VolumeMultiple']
            comm_value += comm_info['CloseRatioByVolume'] * trade_info['Volume']
        else:
            raise Exception('unknown action')
        _fill_table[_index] = {
            'Symbol': order_info['Symbol'],
            'Quantity': trade_info['Volume'],
            'Action': ActionType.toStr(order_info['Action']),
            'Direction': DirectionType.toStr(order_info['Direction']),
            'Price': trade_info['Price'],
            'Commission': comm_value,
        }
        logging.info('!!! FILL {}: {} !!!'.format(
            _index, _fill_table[_index]
        ))
        return True

    def tradeFunc(self):
        instrument_table = {}
        order_table = self.getRemainOrderTable()
        fill_table = {}

        for i in range(self.retry_time):
            logging.info('!!! TRY ({})th TIME !!!'.format(i))
            logging.info('!!! REMAIN ORDERS !!!')
            for k, v in order_table.items():
                logging.info('!!! ORDER {}: {} !!!'.format(k, v))
            if len(order_table) == 0:
                # no order left
                break

            if not self._trader_login():
                continue
            if not self._trader_all_instrument(instrument_table):
                continue

            indices = tuple(order_table.keys())
            for index in indices:
                if not self._trader_send_order(
                        index, instrument_table, order_table, fill_table
                ):
                    continue

                del order_table[index]

            self.delTraderSpi()  # free ctp obj
            sleep(1)

        # write into fill csv
        fill_file = open(self.fill_csv_path, 'a')
        fill_writer = csv.writer(fill_file)
        for k, v in fill_table.items():
            fill_writer.writerow((
                k, v['Symbol'], v['Quantity'],
                v['Action'], v['Direction'],
                v['Price'], v['Commission'],
            ))
        fill_file.close()

    def run(self):
        schedule.every().day.at("21:00").do(self.tradeFunc)
        schedule.every().day.at("09:00").do(self.tradeFunc)

        while True:
            schedule.run_pending()
            logging.info('WAIT {} sec'.format(schedule.idle_seconds()))
            sleep(max(schedule.idle_seconds(), 1))
