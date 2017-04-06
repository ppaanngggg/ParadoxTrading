from ParadoxTrading.Fetch import (FetchGuoJinDay, FetchGuoJinDayIndex,
                                  FetchGuoJinHour, FetchGuoJinHourIndex,
                                  FetchGuoJinMin, FetchGuoJinMinIndex,
                                  FetchGuoJinTick, FetchGuoJinTickIndex,
                                  RegisterGuoJinDay, RegisterGuoJinDayIndex,
                                  RegisterGuoJinHour, RegisterGuoJinHourIndex,
                                  RegisterGuoJinMin, RegisterGuoJinMinIndex,
                                  RegisterGuoJinTick, RegisterGuoJinTickIndex)

print('--- FetchGuoJinTick')
fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.productList())
# get dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinTick('rb').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinTick('rb', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('rb', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinTick(_instrument='rb1703').toKwargs()
))

print(len(fetcher.fetchData('20160506', 'rb1610', _cache=False)))
print(len(fetcher.fetchData('20160506', 'rb1701')))

print('--- FetchGuoJinTickIndex')
fetcher = FetchGuoJinTickIndex()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinTickIndex('cu').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'cu', _cache=False)))
print(len(fetcher.fetchData('20160509', 'cu')))

print('--- FetchGuoJinMin')
fetcher = FetchGuoJinMin()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinMin('au').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinMin('au', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('au', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinMin(_instrument='au1605').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'au1612', _cache=False)))
print(len(fetcher.fetchData('20160506', 'au1606')))

print('--- FetchGuoJinMinIndex')
fetcher = FetchGuoJinMinIndex()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinMinIndex('ag').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ag', _cache=False)))
print(len(fetcher.fetchData('20160509', 'ag')))

print('--- FetchGuoJinHour')
fetcher = FetchGuoJinHour()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinHour('m').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinHour('m', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('m', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinHour(_instrument='m1605').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'm1609', _cache=False)))
print(len(fetcher.fetchData('20160506', 'm1701')))

print('--- FetchGuoJinHourIndex')
fetcher = FetchGuoJinHourIndex()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinHourIndex('ma').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ma', _cache=False)))
print(len(fetcher.fetchData('20160509', 'ma')))

print('--- FetchGuoJinDay')
fetcher = FetchGuoJinDay()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinDay('pp').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinDay('pp', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('pp', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinDay(_instrument='pp1703').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'pp1609')))
print(len(fetcher.fetchDayData('20160101', '20160501', 'pp1609')))

print('--- FetchGuoJinDayIndex')
fetcher = FetchGuoJinDayIndex()
fetcher.psql_host = '192.168.4.102'
fetcher.mongo_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinDayIndex('oi').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'oi')))
print(len(fetcher.fetchDayData('20160101', '20160501', 'oi')))
