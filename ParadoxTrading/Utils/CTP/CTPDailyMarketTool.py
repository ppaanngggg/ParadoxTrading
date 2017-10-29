import configparser
import logging
import pickle
import time

import arrow
import PyCTP
import schedule

from ParadoxTrading.Utils.CTP.CTPMarketSpi import CTPMarketSpi
from ParadoxTrading.Utils.CTP.CTPTraderSpi import CTPTraderSpi


class CTPDailyMarketTool:
    RETRY_TIME = 5
    SUB_SIZE = 100
    WAIT_MIN = 60

    def __init__(self, _config_path, _save_path):
        self.trader: CTPTraderSpi = None
        self.market: CTPMarketSpi = None

        self.today = None
        self.tradingday = None

        self.data_table = {}

        self.config = configparser.ConfigParser()
        self.config.read(_config_path)

        self.save_path = _save_path

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
            'PreClosePrice': _market_data.PreClosePrice,
            'OpenPrice': _market_data.OpenPrice,
            'HighestPrice': _market_data.HighestPrice,
            'LowestPrice': _market_data.LowestPrice,
            'ClosePrice': _market_data.ClosePrice,
            'PreSettlementPrice': _market_data.PreSettlementPrice,
            'SettlementPrice': _market_data.SettlementPrice,
            'Volume': _market_data.Volume,
            'Turnover': _market_data.Turnover,
            'PreOpenInterest': _market_data.PreOpenInterest,
            'OpenInterest': _market_data.OpenInterest,
        }
        self.data_table[data['InstrumentID']] = data

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
        self.today = None
        self.tradingday = None
        self.data_table = {}

    def traderLogin(self) -> bool:
        self.trader = CTPTraderSpi(
            self.config['CTP']['ConPath'].encode(),
            self.config['CTP']['TraderFront'].encode(),
            self.config['CTP']['BrokerID'].encode(),
            self.config['CTP']['UserID'].encode(),
            self.config['CTP']['Password'].encode(),
        )
        if not self.trader.Connect():
            return False
        if not self.trader.ReqUserLogin():
            return False
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

    def marketFunc(self):
        # create trader spi and fetch all instruments
        for i in range(self.RETRY_TIME):
            logging.info('TRY ({}) times trader login'.format(i))
            if self.traderLogin():
                break
            logging.info('try login again')
            self.delTraderSpi()
        else:
            logging.error('trader login FAILED!')
            self.reset()
            return

        # get all instrument
        for i in range(self.RETRY_TIME):
            logging.info('TRY ({}) get instrument'.format(i))
            instrument = self.trader.ReqQryInstrument()
            if instrument is not False:
                break
        else:
            logging.error('get instrument FAILED!')
            self.reset()
            return

        # check whether today is tradingday
        self.today = arrow.now().format('YYYYMMDD')
        self.tradingday = self.trader.GetTradingDay().decode('gb2312')
        logging.info('today: {}, tradingday: {}'.format(
            self.today, self.tradingday
        ))
        if self.today != self.tradingday:
            logging.info('today({}) is not tradingday({})!'.format(
                self.today, self.tradingday
            ))
            self.reset()
            return

        # create market spi
        for i in range(self.RETRY_TIME):
            logging.info('TRY ({}) times market login'.format(i))
            if self.marketLogin():
                break
            logging.info('try login again')
            self.delMarketSpi()
        else:
            logging.error('market login FAILED!')
            self.reset()
            return

        # sub instruments
        inst_list = instrument.index()
        for start_idx in range(0, len(inst_list), self.SUB_SIZE):
            end_idx = start_idx + self.SUB_SIZE
            sub_inst_list = inst_list[start_idx: end_idx]
            sub_inst_list = [d.encode('gb2312') for d in sub_inst_list]
            for _ in range(self.RETRY_TIME):
                ret = self.market.SubscribeMarketData(sub_inst_list)
                time.sleep(1)
                if ret is not False:
                    break
            else:
                logging.error('sub market FAILED!')
                self.reset()
                return

        # wait to collect data
        logging.info('wait {} mins!'.format(self.WAIT_MIN))
        time.sleep(self.WAIT_MIN * 60)
        logging.warning('not reveice instrument: {}'.format(
            set(inst_list) - self.data_table.keys()
        ))

        pickle.dump(
            self.data_table,
            open('{}/{}.pkl'.format(
                self.save_path, self.tradingday
            ), 'wb')
        )

        self.reset()

    def run(self):
        schedule.every().day.at("14:55").do(self.marketFunc)

        while True:
            schedule.run_pending()
            logging.info('WAIT {} sec'.format(schedule.idle_seconds()))
            time.sleep(max(schedule.idle_seconds(), 1))
