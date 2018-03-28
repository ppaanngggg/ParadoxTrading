from ParadoxTrading.Fetch.ChineseFutures import FetchInstrumentDayData, \
    FetchDominantIndex, FetchProductIndex, FetchInstrumentTickData, \
    FetchInstrumentMinData

import diskcache

# print('-- instrument tick data --')
# fetcher = FetchInstrumentTickData()
#
# print(fetcher.fetchData('20171016', 'rb1801'))
# print(fetcher.fetchDayData('20171016', '20171018', 'rb1801'))

print('-- instrument min data --')
fetcher = FetchInstrumentMinData()

print(fetcher.fetchData('20171016', 'rb1801'))
print(fetcher.fetchDayData('20171016', '20171018', 'rb1801'))

print('-- instrument day data --')
fetcher = FetchInstrumentDayData()

print(fetcher.isTradingDay('20171013'))
print(fetcher.isTradingDay('20171014'))

print(fetcher.fetchAvailableProduct('20171013'))
print(fetcher.fetchAvailableProduct('20171014'))

print(fetcher.productIsAvailable('rb', '20171013'))
print(fetcher.productIsAvailable('rb', '20171014'))

print(fetcher.productLastTradingDay('rb', '20171013'))
print(fetcher.productNextTradingDay('rb', '20171013'))

print(fetcher.fetchDominant('rb', '20171013'))
print(fetcher.fetchDominant('rb', '20171014'))

print(fetcher.fetchSubDominant('rb', '20171013'))
print(fetcher.fetchSubDominant('rb', '20171014'))

print(fetcher.fetchAvailableInstrument('rb', '20171013'))
print(fetcher.fetchAvailableInstrument('rb', '20171014'))

print(fetcher.instrumentIsAvailable('rb1801', '20171013'))
print(fetcher.instrumentIsAvailable('rb1801', '20171014'))

print(fetcher.instrumentLastTradingDay('rb1801', '20171013'))
print(fetcher.instrumentNextTradingDay('rb1801', '20171013'))

print(fetcher.fetchData('20171013', 'rb1801'))
print(fetcher.fetchData('20171014', 'rb1801'))

print(fetcher.fetchDayData('20171010', '20171017', 'rb1801'))

register = fetcher.register_type('rb')
print(register)
print(fetcher.fetchSymbol('20171013', **register.toKwargs()))

print('-- dominant index --')
fetcher = FetchDominantIndex()

print(fetcher.fetchData('20171013', 'rb'))
print(fetcher.fetchData('20171014', 'rb'))

print(fetcher.fetchDayData('20171010', '20171017', 'rb'))

register = fetcher.register_type('rb')
print(register)
print(fetcher.fetchSymbol('20171013', **register.toKwargs()))

print('-- product index --')
fetcher = FetchProductIndex()

print(fetcher.fetchData('20171013', 'rb'))
print(fetcher.fetchData('20171014', 'rb'))

print(fetcher.fetchDayData('20171010', '20171017', 'rb'))

print('-- cache keys --')
cache = diskcache.Cache('cache')
for k in cache.iterkeys():
    print(k)
