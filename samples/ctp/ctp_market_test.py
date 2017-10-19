import logging

from ParadoxTrading.Utils.CTP import CTPDailyMarketTool

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s[%(asctime)s] - %(message)s',
    # filename='market.log',
)

tool = CTPDailyMarketTool(
    './config.ini', './save/'
)
tool.marketFunc()
