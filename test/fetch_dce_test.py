from ParadoxTrading.Fetch import (FetchDCEDay, FetchDCEDayIndex,
                                  RegisterDCEDay, RegisterDCEDayIndex)

print('--- FetchDCEDay')
fetcher = FetchDCEDay()
print(fetcher.fetchSymbol(
    '20160506', **RegisterDCEDay('c').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterDCEDay('c', _sub_dominant=True).toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterDCEDay(_instrument='c1701').toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('c', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterDCEDay(_instrument='c1605').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'c1701')))
print(len(fetcher.fetchDayData('20170101', '20170601', 'c1709')))

print('--- FetchSHFEDayIndex')
fetcher = FetchDCEDayIndex()
print(fetcher.fetchSymbol(
    '20160506', **RegisterDCEDayIndex('c').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'c')))
print(len(fetcher.fetchDayData('20160101', '20170101', 'c')))
