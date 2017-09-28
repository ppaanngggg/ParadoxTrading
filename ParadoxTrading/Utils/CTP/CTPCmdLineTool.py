import cmd
import configparser
from pprint import pprint

from ParadoxTrading.Engine import ActionType, DirectionType
from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi


class CTPCmdLineTool(cmd.Cmd):
    prompt = '>>> '

    def __init__(self, _config_path: str):
        super().__init__()

        config = configparser.ConfigParser()
        config.read(_config_path)
        self.trader = CTPTraderSpi(
            config['TRADE']['ConPath'].encode(),
            config['TRADE']['Front'].encode(),
            config['TRADE']['BrokerID'].encode(),
            config['TRADE']['UserID'].encode(),
            config['TRADE']['Password'].encode(),
        )

    def emptyline(self):
        pass

    def do_connect(self, arg):
        """Connect to front"""
        self.trader.Connect()

    def do_release(self, arg):
        """Release ctp object"""
        self.trader.Release()

    def do_login(self, arg):
        """Log in"""
        self.trader.ReqUserLogin()

    def do_logout(self, arg):
        """Log out"""
        self.trader.ReqUserLogout()

    def do_instrument(self, arg):
        """Fetch all instrument info"""
        print(self.trader.ReqQryInstrument())

    def do_position(self, arg):
        """Fetch all position record"""
        print(self.trader.ReqQryInvestorPosition())

    def do_order(self, arg):
        """Fetch all order record"""
        print(self.trader.ReqQryOrder())

    def do_trade(self, arg):
        """Fetch all trade record"""
        print(self.trader.ReqQryTrade())

    def do_market(self, arg):
        """Fetch instrument depth market data: market rb1801"""
        pprint(self.trader.ReqQryDepthMarketData(arg.encode()))

    @staticmethod
    def _split_arg(_arg):
        instrument, volume, price = _arg.split()
        instrument = instrument.encode()
        volume = int(volume)
        price = float(price)
        return instrument, volume, price

    def do_open_buy(self, arg):
        """Open Buy on instrument by volume at price: open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.OPEN,
            volume, price
        ))

    def do_open_sell(self, arg):
        """Open Sell on instrument by volume at price: open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.OPEN,
            volume, price
        ))

    def do_close_buy(self, arg):
        """Close Buy on instrument by volume at price, default(yesterday):
        open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.CLOSE,
            volume, price
        ))

    def do_close_sell(self, arg):
        """Close Sell on instrument by volume at price, default(yesterday):
        open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.CLOSE,
            volume, price
        ))

    def do_close_buy_today(self, arg):
        """Close Sell on instrument by volume at price, default(today):
        open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.BUY, ActionType.CLOSE,
            volume, price, _today=True
        ))

    def do_close_sell_today(self, arg):
        """Close Sell on instrument by volume at price, default(today):
        open_buy rb1801 1 3000"""
        instrument, volume, price = CTPCmdLineTool._split_arg(arg)
        pprint(self.trader.ReqOrderInsert(
            instrument, DirectionType.SELL, ActionType.CLOSE,
            volume, price, _today=True
        ))

    def do_account(self, arg):
        """Get account fund status"""
        pprint(self.trader.ReqQryTradingAccount())

    def do_commission_rate(self, arg):
        """Get commission rate of instrument: commission_rate rb1801"""
        pprint(self.trader.ReqQryInstrumentCommissionRate(arg.encode()))

    def do_settlement_info(self, arg):
        """Get settlement info of tradingday: settlement_info 20170101"""
        tmp = self.trader.ReqQrySettlementInfo(arg.encode())
        if tmp is not False:
            print(tmp.decode('gb2312'))

    def do_settlement_confirm(self, arg):
        """Confirm settlement info"""
        self.trader.ReqSettlementInfoConfirm()

    def do_quit(self, arg):
        """Quit"""
        return True
