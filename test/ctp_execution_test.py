import logging
from time import sleep

from ParadoxTrading.EngineExt import CTPExecution

logging.basicConfig(level=logging.INFO)

execution = CTPExecution(
    './con/', 'tcp://180.168.146.187:10030',
    '9999', '079375', '1994225123'
)
