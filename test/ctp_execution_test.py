import cmd
import logging
from pprint import pprint

from ParadoxTrading.Engine import DirectionType, ActionType
from ParadoxTrading.Utils.CTPTraderSpi import CTPTraderSpi

logging.basicConfig(level=logging.INFO)


# trader.Connect()
# trader.ReqUserLogin()
# askprice = trader.ReqQryDepthMarketData(b'rb1801')['AskPrice'][0]
# print(askprice)
# # trader.ReqSettlementInfoConfirm()
# trader.ReqOrderInsert(
#     b'rb1801', DirectionType.BUY, ActionType.OPEN, 1, askprice
# )
# sleep(5)


class CTPCMD(cmd.Cmd):
    prompt = '>>> '

    def __init__(self):
        super().__init__()

        self.trader = CTPTraderSpi(
            b'./con/',
            # b'tcp://180.168.146.187:10030',
            b'tcp://180.168.146.187:10000',
            b'9999', b'079375', b'1994225123'
        )
        # self.trader = CTPTraderSpi(
        #     b'./con/',
        #     b'tcp://ctpdx1.zjfutures.cn:41205',
        #     b'7000', b'1700070', b'85856699'
        # )

    def emptyline(self):
        pass

    def do_connect(self, arg):
        'Connect to front'
        self.trader.Connect()

    def do_login(self, arg):
        'Log in'
        self.trader.ReqUserLogin()

    def do_instrument(self, arg):
        'Fetch all instrument info'
        print(self.trader.ReqQryInstrument())

    def do_position(self, arg):
        'Fetch all position info'
        print(self.trader.ReqQryInvestorPosition())

    def do_order(self, arg):
        print(self.trader.ReqQryOrder())

    def do_trade(self, arg):
        print(self.trader.ReqQryTrade())

    def do_market(self, arg):
        'Fetch instrument depth market data: market rb1801'
        pprint(self.trader.ReqQryDepthMarketData(arg.encode()))

    @staticmethod
    def _split_arg(_arg):
        instrument, volume, price = _arg.split()
        instrument = instrument.encode()
        volume = int(volume)
        price = float(price)
        return instrument, volume, price

    def do_open_buy(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.OPEN,
            volume, price
        )

    def do_open_sell(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.OPEN,
            volume, price
        )

    def do_close_buy(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.CLOSE,
            volume, price
        )

    def do_close_sell(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.CLOSE,
            volume, price
        )

    def do_close_buy_today(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.CLOSE,
            volume, price, _today=True
        )

    def do_close_sell_today(self, arg):
        instrument, volume, price = CTPCMD._split_arg(arg)
        self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.CLOSE,
            volume, price, _today=True
        )

    def do_quit(self, arg):
        return True


if __name__ == '__main__':
    CTPCMD().cmdloop()
