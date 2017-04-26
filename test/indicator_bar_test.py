from ParadoxTrading.Indicator import CloseBar, HighBar, LowBar, OpenBar, SumBar
from ParadoxTrading.Fetch import FetchGuoJinTick
from ParadoxTrading.Utils import SplitIntoMinute
from datetime import datetime

fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'

instrument = fetcher.fetchDominant('rb', '20160104')
data = fetcher.fetchData('20160104', instrument)
data = data.loc[:datetime(2016, 1, 4, 10)]
print(data)

spliter = SplitIntoMinute(5)
spliter.addMany(data)

open_bar = OpenBar('lastprice')
open_bar.addMany(spliter.getBarList(), spliter.getBarBeginTimeList())
bar_data = open_bar.getAllData()

close_bar = CloseBar('lastprice')
close_bar.addMany(spliter.getBarList(), spliter.getBarBeginTimeList())
bar_data.expand(close_bar.getAllData())

high_bar = HighBar('lastprice')
high_bar.addMany(spliter.getBarList(), spliter.getBarBeginTimeList())
bar_data.expand(high_bar.getAllData())

low_bar = LowBar('lastprice')
low_bar.addMany(spliter.getBarList(), spliter.getBarBeginTimeList())
bar_data.expand(low_bar.getAllData())

sum_bar = SumBar('askvolume', _ret_key='askvolume')
sum_bar.addMany(spliter.getBarList(), spliter.getBarBeginTimeList())
bar_data.expand(sum_bar.getAllData())

print(bar_data)