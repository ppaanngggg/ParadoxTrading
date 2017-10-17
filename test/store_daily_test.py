import logging

from ParadoxTrading.Database.ChineseFutures import ReceiveDailyCTP
from ParadoxTrading.Database.ChineseFutures import StoreDailyData

logging.basicConfig(level=logging.INFO)

recv = ReceiveDailyCTP('/home/pang/Workspace/ReceiverDailyCTP/save')
tmp = recv.fetchRaw('20171013')
tmp = recv.rawToDicts('20171013', tmp)

store = StoreDailyData()
store.store('20171013', *tmp)
