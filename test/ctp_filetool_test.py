from ParadoxTrading.Utils.CTP.CTPFIleTradeTool import CTPFileTradeTool
import logging

logging.basicConfig(level=logging.INFO)

tool = CTPFileTradeTool(
    'config.ini', 'order.csv', 'fill.csv'
)
tool.tradeFunc()