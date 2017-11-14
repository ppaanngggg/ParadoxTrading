import logging

from ParadoxTrading.Utils.CTP import CTPFileTradeTool

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s[%(asctime)s] - %(message)s',
)

tool = CTPFileTradeTool(
    'config.ini', 'order.csv', 'fill.csv',
    _retry_time=3
)
tool.tradeFunc()
