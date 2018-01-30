import configparser
import csv
import logging
import typing
from time import sleep

import PyCTP
import schedule

from ParadoxTrading.Engine import ActionType, DirectionType
from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi
from ParadoxTrading.Utils.CTP.CTPMarketSpi import CTPMarketSpi


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
    SUB_SIZE = 100

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

        self.market: CTPMarketSpi = None
        self.trader: CTPTraderSpi = None

        # buf data
        self.order_table: typing.Dict[int, OrderObj] = {}
        self.fill_table: typing.Dict[int, FillObj] = {}
        self.data_table = {}
        self.instrument_table = {}
        self.commission_table = {}

    def delTraderSpi(self):
        self.trader.Release()
        del self.trader
        self.trader = None

    def delMarketSpi(self):
        self.market.Release()
        del self.market
        self.market = None

    def reset(self):
        if self.trader is not None:
            self.delTraderSpi()
        if self.market is not None:
            self.delMarketSpi()

        self.order_table = {}
        self.fill_table = {}
        self.data_table = {}
        self.instrument_table = {}
        self.commission_table = {}

    def getOrderAndFillTable(self):
        try:
            order_file = open(self.order_csv_path)
        except FileNotFoundError:
            logging.warning('NO order csv found')
            return

        order_file.readline()
        order_reader = csv.reader(order_file)

        for order_line in order_reader:
            index, instrument, action, direction, quantity = order_line
            index = int(index)
            direction = DirectionType.fromStr(direction)
            action = ActionType.fromStr(action)
            self.order_table[index] = OrderObj(
                _index=index, _symbol=instrument,
                _quantity=int(float(quantity)),
                _action=action, _direction=direction
            )
            self.fill_table[index] = FillObj(
                _index=index, _symbol=instrument,
                _action=action, _direction=direction,
            )
        # finish order csv

        try:
            fill_file = open(self.fill_csv_path)
        except FileNotFoundError:
            logging.warning('NO fill csv found')
            return

        fill_file.readline()
        fill_reader = csv.reader(fill_file)

        for fill_line in fill_reader:
            index, _, quantity, _, _, price, commission = fill_line
            index = int(index)
            fill_obj = self.fill_table[index]
            fill_obj.quantity = int(float(quantity))
            fill_obj.price = float(price)
            fill_obj.commission = float(commission)

        order_file.close()
        fill_file.close()

    def checkOrderAndFill(self):
        """
        if any instrument not equal, then return True, else False
        """
        for index in self.order_table:
            if self.fill_table[index].quantity \
                    != self.order_table[index].quantity:
                return True
        return False

    def dealMarket(
            self,
            _market_data: PyCTP.CThostFtdcDepthMarketDataField
    ):
        data = {
            'InstrumentID': _market_data.InstrumentID.decode('gb2312'),
            'TradingDay': _market_data.TradingDay.decode('gb2312'),
            'ActionDay': _market_data.ActionDay.decode('gb2312'),
            'UpdateTime': _market_data.UpdateTime.decode('gb2312'),
            'UpdateMillisec': _market_data.UpdateMillisec,
            'LastPrice': _market_data.LastPrice,
            'AskPrice': _market_data.AskPrice1,
            'BidPrice': _market_data.BidPrice1,
        }
        self.data_table[data['InstrumentID'].lower()] = data

    def traderLogin(self):
        self.trader = CTPTraderSpi(
            self.config['CTP']['ConPath'].encode(),
            self.config['CTP']['TraderFront'].encode(),
            self.config['CTP']['BrokerID'].encode(),
            self.config['CTP']['UserID'].encode(),
            self.config['CTP']['Password'].encode(),
        )
        if not self.trader.Connect():  # connect front
            return False
        if not self.trader.ReqUserLogin():  # login
            return False
        if not self.trader.ReqSettlementInfoConfirm():  # settlement
            return False
        sleep(1)
        return True

    def marketLogin(self) -> bool:
        self.market = CTPMarketSpi(
            self.config['CTP']['ConPath'].encode(),
            self.config['CTP']['MarketFront'].encode(),
            self.config['CTP']['BrokerID'].encode(),
            self.config['CTP']['UserID'].encode(),
            self.config['CTP']['Password'].encode(),
            self.dealMarket
        )
        if not self.market.Connect():
            return False
        if not self.market.ReqUserLogin():
            return False
        return True

    def traderAllInstrument(self):
        tmp = self.trader.ReqQryInstrument()  # all instrument
        if tmp is False:
            return False
        for d in tmp:
            self.instrument_table[
                d.index()[0].lower()
            ] = d.toDict()
        sleep(1)
        return True

    def subscribe(self):
        inst_list = []
        for v in self.order_table.values():
            inst_list.append(
                self.instrument_table[
                    v.symbol
                ]['InstrumentID'].encode('gb2312')
            )
        for start_idx in range(0, len(inst_list), self.SUB_SIZE):
            end_idx = start_idx + self.SUB_SIZE
            sub_inst_list = inst_list[start_idx: end_idx]
            for _ in range(self.retry_time):
                ret = self.market.SubscribeMarketData(sub_inst_list)
                sleep(1)
                if ret is not False:
                    break
            else:
                return False
        return True

    def doTrade(self, _index: int):
        order_obj = self.order_table[_index]
        fill_obj = self.fill_table[_index]
        quantity_diff = int(
            order_obj.quantity - fill_obj.quantity
        )
        if not quantity_diff > 0:  # order finished
            return

        inst_info = self.instrument_table[order_obj.symbol]
        # encode the bytes instrument for ctp
        instrument = inst_info['InstrumentID'].encode()
        # get commission from buffer
        try:
            comm_info = self.commission_table[order_obj.symbol]
        except KeyError:  # fetch it if failed
            comm_info = self.trader.ReqQryInstrumentCommissionRate(
                instrument
            )
            sleep(1)
            if comm_info is False:
                return
            self.commission_table[order_obj.symbol] = comm_info

        for _ in range(quantity_diff):  # order one by one
            # limit price
            try:
                market_info = self.data_table[order_obj.symbol]
            except KeyError:
                logging.warning('{} has not market data'.format(
                    order_obj.symbol
                ))
                sleep(1)
                continue
            price_diff = self.price_rate * inst_info['PriceTick']
            if order_obj.direction == DirectionType.BUY:
                price = market_info['AskPrice'] + price_diff
            elif order_obj.direction == DirectionType.SELL:
                price = market_info['BidPrice'] - price_diff
            else:
                raise Exception('unknown direction')
            # send order
            trade_info = self.trader.ReqOrderInsert(
                instrument,
                order_obj.direction, order_obj.action,
                1, price
            )
            sleep(1)
            if trade_info is False:
                continue
            # !!! trade succeed !!!
            comm_value = 0
            if order_obj.action == ActionType.OPEN:
                comm_value += comm_info['OpenRatioByMoney'] * \
                    trade_info['Price'] * trade_info['Volume'] * \
                    inst_info['VolumeMultiple']
                comm_value += comm_info['OpenRatioByVolume'] * \
                    trade_info['Volume']
            elif order_obj.action == ActionType.CLOSE:
                comm_value += comm_info['CloseRatioByMoney'] * \
                    trade_info['Price'] * trade_info['Volume'] * \
                    inst_info['VolumeMultiple']
                comm_value += comm_info['CloseRatioByVolume'] * \
                    trade_info['Volume']
            else:
                raise Exception('unknown action')
            fill_obj.add(
                _quantity=trade_info['Volume'],
                _price=trade_info['Price'],
                _commission=comm_value,
            )
            logging.info('FILL: {}'.format(fill_obj))

    def writeFillTable(self):
        fill_file = open(self.fill_csv_path, 'w')
        fill_writer = csv.writer(fill_file)
        fill_writer.writerow((
            'Index', 'Symbol', 'Quantity',
            'Action', 'Direction', 'Price', 'Commission'
        ))
        for k, v in self.fill_table.items():
            if v.quantity == 0:  # skip not filled
                continue
            fill_writer.writerow((
                k, v.symbol, int(v.quantity),
                ActionType.toStr(v.action),
                DirectionType.toStr(v.direction),
                v.price, v.commission
            ))
        fill_file.close()

    def tradeFunc(self):
        self.getOrderAndFillTable()
        if not self.checkOrderAndFill():
            self.reset()
            return

        # try to login
        for i in range(self.retry_time):
            logging.info('TRY ({}) times trader login'.format(i))
            if self.traderLogin():
                break
            logging.info('try login again')
            self.delTraderSpi()
        else:
            logging.error('trader login FAILED!')
            self.reset()
            return

        # fetch all instruments
        for i in range(self.retry_time):
            logging.info('TRY ({}) get instrument'.format(i))
            if self.traderAllInstrument():
                break
        else:
            logging.error('get instrument FAILED!')
            self.reset()
            return

        # create market spi
        for i in range(self.retry_time):
            logging.info('TRY ({}) times market login'.format(i))
            if self.marketLogin():
                break
            logging.info('try login again')
            self.delMarketSpi()
        else:
            logging.error('market login FAILED!')
            self.reset()
            return

        # sub market data
        if not self.subscribe():
            self.reset()
            return

        # trade each instrument
        for i in range(self.retry_time):
            logging.info('TRY ({}) times do trade !!!'.format(i))
            for k in self.order_table:
                self.doTrade(k)
            if not self.checkOrderAndFill():
                break

        self.writeFillTable()

        self.reset()

    def run(self):
        schedule.every().day.at("21:00").do(self.tradeFunc)
        schedule.every().day.at("09:00").do(self.tradeFunc)

        while True:
            schedule.run_pending()
            logging.info('WAIT {} sec'.format(schedule.idle_seconds()))
            sleep(max(schedule.idle_seconds(), 1))
