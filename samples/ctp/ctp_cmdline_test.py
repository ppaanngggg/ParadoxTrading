import logging
from ParadoxTrading.Utils.CTP import CTPCmdLineTool

logging.basicConfig(level=logging.INFO)

CTPCmdLineTool('./config.ini').cmdloop()
