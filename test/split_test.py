from datetime import datetime

from ParadoxTrading.Fetch import FetchGuoJinTick, FetchSHFEDayIndex
from ParadoxTrading.Utils import SplitIntoSecond, SplitIntoMinute, SplitIntoHour, SplitIntoMonth, SplitIntoWeek

fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'

instrument = fetcher.fetchDominant('rb', '20160104')
data = fetcher.fetchData('20160104', instrument)
data = data.loc[datetime(2016, 1, 4, 9):datetime(2016, 1, 4, 11)]

spliter_sec = SplitIntoSecond(30)
spliter_min = SplitIntoMinute(5)
spliter_hour = SplitIntoHour(1)

spliter_sec.addMany(data)
print(spliter_sec.getBarBeginTimeList()[0])
print(spliter_sec.getBarEndTimeList()[0])
print(spliter_sec.getBarBeginTimeList()[-1])
print(spliter_sec.getBarEndTimeList()[-1])
print(len(spliter_sec.getBarList()[0]))
spliter_min.addMany(data)
print(spliter_min.getBarBeginTimeList()[0])
print(spliter_min.getBarEndTimeList()[0])
print(spliter_min.getBarBeginTimeList()[-1])
print(spliter_min.getBarEndTimeList()[-1])
print(len(spliter_min.getBarList()[0]))
spliter_hour.addMany(data)
print(spliter_hour.getBarBeginTimeList()[0])
print(spliter_hour.getBarEndTimeList()[0])
print(spliter_hour.getBarBeginTimeList()[-1])
print(spliter_hour.getBarEndTimeList()[-1])
print(len(spliter_hour.getBarList()[0]))

fetcher = FetchSHFEDayIndex()
data = fetcher.fetchDayData('20160101', '20170101', 'cu')

spliter_week = SplitIntoWeek()
spliter_month = SplitIntoMonth()

spliter_week.addMany(data)
print(spliter_week.getBarBeginTimeList()[0])
print(spliter_week.getBarEndTimeList()[0])
print(spliter_week.getBarBeginTimeList()[-1])
print(spliter_week.getBarEndTimeList()[-1])
print(len(spliter_week.getBarList()[0]))

spliter_month.addMany(data)
print(spliter_month.getBarBeginTimeList()[0])
print(spliter_month.getBarEndTimeList()[0])
print(spliter_month.getBarBeginTimeList()[-1])
print(spliter_month.getBarEndTimeList()[-1])
print(len(spliter_month.getBarList()[0]))