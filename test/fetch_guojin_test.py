from ParadoxTrading.Fetch import \
    FetchGuoJinTick, RegisterGuoJinTick, \
    FetchGuoJinTickIndex, RegisterGuoJinTickIndex, \
    FetchGuoJinMin, RegisterGuoJinMin, \
    FetchGuoJinMinIndex, RegisterGuoJinMinIndex, \
    FetchGuoJinHour, RegisterGuoJinHour, \
    FetchGuoJinHourIndex, RegisterGuoJinHourIndex, \
    FetchGuoJinDay, RegisterGuoJinDay, \
    FetchGuoJinDayIndex, RegisterGuoJinDayIndex

print('--- FetchGuoJinTick')
fetcher = FetchGuoJinTick()
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
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinTickIndex('cu').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'cu', _cache=False)))
print(len(fetcher.fetchData('20160509', 'cu')))

print('--- FetchGuoJinMin')
fetcher = FetchGuoJinMin()
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
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinMinIndex('ag').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ag', _cache=False)))
print(len(fetcher.fetchData('20160509', 'ag')))

print('--- FetchGuoJinHour')
fetcher = FetchGuoJinHour()
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
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinHourIndex('ma').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ma', _cache=False)))
print(len(fetcher.fetchData('20160509', 'ma')))

print('--- FetchGuoJinDay')
fetcher = FetchGuoJinDay()
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

print('--- FetchGuoJinDayIndex')
fetcher = FetchGuoJinDayIndex()
print(fetcher.fetchSymbol(
    '20160506', **RegisterGuoJinDayIndex('oi').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'oi')))
