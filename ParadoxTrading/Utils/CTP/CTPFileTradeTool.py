import configparser
import csv
import logging
import typing
from time import sleep

import schedule

from ParadoxTrading.Engine import ActionType, DirectionType
from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi


class OrderObj:
    def __init__(
            self, _index, _symbol, _quantity, _action, _direction,
    ):
        self.index = _index
        self.symbol = _symbol
        self.quantity = _quantity
        self.action = _action
        self.direction = _direction

    def __repr__(self):
        ret = 'INDEX: {}, SYMBOL: {}, QUANTITY: {}, ACTION: {}, DIRECTION: {}'
        return ret.format(
            self.index, self.symbol, self.quantity,
            self.action, self.direction
        )


class FillObj:
    def __init__(
            self, _index, _symbol, _action, _direction
    ):
        self.index = _index
        self.symbol = _symbol
        self.action = _action
        self.direction = _direction
        self.quantity = 0.0
        self.price = 0.0
        self.commission = 0.0

    def add(self, _quantity, _price, _commission):
        tmp = self.quantity * self.price + _quantity * _price
        self.quantity += _quantity
        self.price = tmp / self.quantity
        self.commission += _commission

    def __repr__(self):
        ret = 'INDEX: {}, SYMBOL: {}, ACTION: {}, DIRECTION: {}, ' \
              'QUANTITY: {}, PRICE: {}, COMMISSION: {}'
        return ret.format(
            self.index, self.symbol, self.action, self.direction,
            self.quantity, self.price, self.commission
        )


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
        """
        create a new trader obj
        """
        self.trader = CTPTraderSpi(
            self.config['TRADE']['ConPath'].encode(),
            self.config['TRADE']['Front'].encode(),
            self.config['TRADE']['BrokerID'].encode(),
            self.config['TRADE']['UserID'].encode(),
            self.config['TRADE']['Password'].encode(),
        )

    def delTraderSpi(self):
        """
        release trader obj and reset arg
        """
        self.trader.Release()
        del self.trader
        self.trader: CTPTraderSpi = None

    def getOrderAndFillTable(self):
        order_table: typing.Dict[int, OrderObj] = {}
        fill_table: typing.Dict[int, FillObj] = {}

        try:
            order_file = open(self.order_csv_path)
        except FileNotFoundError:
            logging.warning('NO order csv found')
            return order_table, fill_table

        order_file.readline()
        order_reader = csv.reader(order_file)

        for order_line in order_reader:
            index, instrument, action, direction, quantity = order_line
            index = int(index)
            direction = DirectionType.fromStr(direction)
            action = ActionType.fromStr(action)
            order_table[index] = OrderObj(
                _index=index, _symbol=instrument, _quantity=int(float(quantity)),
                _action=action, _direction=direction
            )
            fill_table[index] = FillObj(
                _index=index, _symbol=instrument,
                _action=action, _direction=direction,
            )
        # finish order csv

        try:
            fill_file = open(self.fill_csv_path)
        except FileNotFoundError:
            logging.warning('NO fill csv found')
            return order_table, fill_table

        fill_file.readline()
        fill_reader = csv.reader(fill_file)

        for fill_line in fill_reader:
            index, _, quantity, _, _, price, commission = fill_line
            index = int(index)
            fill_obj = fill_table[index]
            fill_obj.quantity = int(float(quantity))
            fill_obj.price = float(price)
            fill_obj.commission = float(commission)

        order_file.close()
        fill_file.close()

        return order_table, fill_table

    def traderLogin(self):
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

    def traderAllInstrument(self, _instrument_table: dict):
        tmp = self.trader.ReqQryInstrument()  # all instrument
        if tmp is False:
            return False
        for d in tmp:
            _instrument_table[d.index()[0].lower()] = d.toDict()
        sleep(1)
        return True

    def doTrade(
            self, _order_obj: OrderObj, _fill_obj: FillObj,
            _instrument_table, _commission_table
    ):
        quantity_diff = int(_order_obj.quantity - _fill_obj.quantity)
        if not quantity_diff > 0:  # order finished
            return

        inst_info = _instrument_table[_order_obj.symbol]
        # encode the bytes instrument for ctp
        instrument = inst_info['InstrumentID'].encode()
        # get commission from buffer
        try:
            comm_info = _commission_table[_order_obj.symbol]
        except KeyError:  # fetch it if failed
            comm_info = self.trader.ReqQryInstrumentCommissionRate(instrument)
            sleep(1)
            if comm_info is False:
                return
            _commission_table[_order_obj.symbol] = comm_info

        for _ in range(quantity_diff):  # order one by one
            # depth market data
            market_info = self.trader.ReqQryDepthMarketData(instrument)
            sleep(1)
            if market_info is False:
                continue
            # limit price
            price_diff = self.price_rate * inst_info['PriceTick']
            if _order_obj.direction == DirectionType.BUY:
                price = market_info['AskPrice'] + price_diff
            elif _order_obj.direction == DirectionType.SELL:
                price = market_info['BidPrice'] - price_diff
            else:
                raise Exception('unknown direction')
            # send order
            trade_info = self.trader.ReqOrderInsert(
                instrument,
                _order_obj.direction, _order_obj.action,
                1, price
            )
            sleep(1)
            if trade_info is False:
                continue
            # !!! trade succeed !!!
            comm_value = 0
            if _order_obj.action == ActionType.OPEN:
                comm_value += comm_info['OpenRatioByMoney'] * trade_info['Price'] * \
                              trade_info['Volume'] * inst_info['VolumeMultiple']
                comm_value += comm_info['OpenRatioByVolume'] * trade_info['Volume']
            elif _order_obj.action == ActionType.CLOSE:
                comm_value += comm_info['CloseRatioByMoney'] * trade_info['Price'] * \
                              trade_info['Volume'] * inst_info['VolumeMultiple']
                comm_value += comm_info['CloseRatioByVolume'] * \
                              trade_info['Volume']
            else:
                raise Exception('unknown action')
            _fill_obj.add(
                _quantity=trade_info['Volume'],
                _price=trade_info['Price'],
                _commission=comm_value,
            )
            logging.info('!!! FILL: {} !!!'.format(_fill_obj))

    def writeFillTable(self, _fill_table: typing.Dict[int, FillObj]):
        fill_file = open(self.fill_csv_path, 'w')
        fill_writer = csv.writer(fill_file)
        fill_writer.writerow((
            'Index', 'Symbol', 'Quantity',
            'Action', 'Direction', 'Price', 'Commission'
        ))
        for k, v in _fill_table.items():
            if v.quantity == 0:  # skip not filled
                continue
            fill_writer.writerow((
                k, v.symbol, int(v.quantity),
                ActionType.toStr(v.action),
                DirectionType.toStr(v.direction),
                v.price, v.commission
            ))
        fill_file.close()

    def checkOrderAndFill(self, _order_table, _fill_table):
        for index in _order_table:
            if _fill_table[index].quantity != _order_table[index].quantity:
                return True
        return False

    def tradeFunc(self):
        instrument_table = {}
        commission_table = {}
        order_table, fill_table = self.getOrderAndFillTable()

        if not self.checkOrderAndFill(order_table, fill_table):
            return

        # try to login
        for i in range(self.retry_time):
            logging.info('!!! TRY({}) to login !!!'.format(i))
            if self.traderLogin():
                break
        else:
            logging.warning('!!! LOGIN FAILED !!!')
            return
        # fetch all instruments
        for i in range(self.retry_time):
            logging.info('!!! TRY({}) to fetch all instruments !!!'.format(i))
            if self.traderAllInstrument(instrument_table):
                break
        else:
            logging.warning('!!! FETCH ALL INSTRUMENTS FAILED !!!')
            return

        for i in range(self.retry_time):
            logging.info('!!! TRY ({})th TIME !!!'.format(i))
            for k in order_table:
                self.doTrade(
                    order_table[k], fill_table[k],
                    instrument_table, commission_table
                )
            if not self.checkOrderAndFill(order_table, fill_table):
                break

        # release trader spi
        self.delTraderSpi()  # free ctp obj
        sleep(1)

        # writer fill table to file
        self.writeFillTable(fill_table)

    def run(self):
        schedule.every().day.at("21:00").do(self.tradeFunc)
        schedule.every().day.at("09:00").do(self.tradeFunc)

        while True:
            schedule.run_pending()
            logging.info('WAIT {} sec'.format(schedule.idle_seconds()))
            sleep(max(schedule.idle_seconds(), 1))
