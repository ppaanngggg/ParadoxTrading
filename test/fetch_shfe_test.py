from ParadoxTrading.Fetch import (FetchSHFEDay, FetchSHFEDayIndex,
                                  RegisterSHFEDay, RegisterSHFEDayIndex)

print('--- FetchSHFEDay')
fetcher = FetchSHFEDay()
print(fetcher.fetchSymbol(
    '20160506', **RegisterSHFEDay('rb').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterSHFEDay('rb', _sub_dominant=True).toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterSHFEDay(_instrument='rb1701').toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('rb', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterSHFEDay(_instrument='rb1703').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'rb1609')))
print(len(fetcher.fetchDayData('20170101', '20170601', 'rb1709')))

print('--- FetchSHFEDayIndex')
fetcher = FetchSHFEDayIndex()
print(fetcher.fetchSymbol(
    '20160506', **RegisterSHFEDayIndex('ag').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ag')))
print(len(fetcher.fetchDayData('20160101', '20170101', 'ag')))
