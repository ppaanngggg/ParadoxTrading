from ParadoxTrading.Fetch import \
    FetchFutureTick, RegisterFutureTick, \
    FetchFutureTickIndex, RegisterFutureTickIndex, \
    FetchFutureMin, RegisterFutureMin, \
    FetchFutureMinIndex, RegisterFutureMinIndex, \
    FetchFutureHour, RegisterFutureHour, \
    FetchFutureHourIndex, RegisterFutureHourIndex, \
    FetchFutureDay, RegisterFutureDay, \
    FetchFutureDayIndex, RegisterFutureDayIndex

print('--- FetchFutureTick')
fetcher = FetchFutureTick()
print(fetcher.productList())
# get dominant symbol of ag
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureTick('rb').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureTick('rb', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('rb', '20170123'))
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureTick(_instrument='rb1703').toKwargs()
))

print(len(fetcher.fetchData('20170123', 'rb1705', _cache=False)))
print(len(fetcher.fetchData('20170123', 'rb1710')))

print('--- FetchFutureTickIndex')
fetcher = FetchFutureTickIndex()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureTickIndex('cu').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'cu', _cache=False)))
print(len(fetcher.fetchData('20170124', 'cu')))

print('--- FetchFutureMin')
fetcher = FetchFutureMin()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureMin('au').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureMin('au', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('au', '20170123'))
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureMin(_instrument='au1703').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'au1706', _cache=False)))
print(len(fetcher.fetchData('20170123', 'au1710')))

print('--- FetchFutureMinIndex')
fetcher = FetchFutureMinIndex()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureMinIndex('ag').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'ag', _cache=False)))
print(len(fetcher.fetchData('20170124', 'ag')))

print('--- FetchFutureHour')
fetcher = FetchFutureHour()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureHour('m').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureHour('m', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('m', '20170123'))
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureHour(_instrument='m1705').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'm1705', _cache=False)))
print(len(fetcher.fetchData('20170123', 'm1709')))

print('--- FetchFutureHourIndex')
fetcher = FetchFutureHourIndex()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureHourIndex('ma').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'ma', _cache=False)))
print(len(fetcher.fetchData('20170124', 'ma')))

print('--- FetchFutureDay')
fetcher = FetchFutureDay()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureDay('pp').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureDay('pp', _sub_dominant=True).toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('pp', '20170123'))
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureDay(_instrument='pp1703').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'pp1705')))

print('--- FetchFutureDayIndex')
fetcher = FetchFutureDayIndex()
print(fetcher.fetchSymbol(
    '20170123', **RegisterFutureDayIndex('oi').toKwargs()
))
print(len(fetcher.fetchData('20170123', 'oi')))
