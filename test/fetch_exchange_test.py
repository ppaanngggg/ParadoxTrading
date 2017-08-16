from ParadoxTrading.Fetch import (FetchExchangeMarket, FetchExchangeMarketIndex,
                                  RegisterExchangeMarket, RegisterExchangeMarketIndex)

print('--- FetchSHFEDay')
fetcher = FetchExchangeMarket()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

print(fetcher.fetchSymbol(
    '20160506', **RegisterExchangeMarket('rb').toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterExchangeMarket('rb', _sub_dominant=True).toKwargs()
))
# get sub dominant symbol of ag
print(fetcher.fetchSymbol(
    '20160506', **RegisterExchangeMarket(_instrument='rb1701').toKwargs()
))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('rb', '20160506'))
print(fetcher.fetchSymbol(
    '20160506', **RegisterExchangeMarket(_instrument='rb1703').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'rb1609')))
print(len(fetcher.fetchDayData('20170101', '20170601', 'rb1709')))

print('--- FetchSHFEDayIndex')
fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

print(fetcher.fetchSymbol(
    '20160506', **RegisterExchangeMarketIndex('ag').toKwargs()
))
print(len(fetcher.fetchData('20160506', 'ag')))
print(len(fetcher.fetchDayData('20160101', '20170101', 'ag')))
print(fetcher.isTradingDay('20160506'))
print(fetcher.fetchTradeProduct('20160506'))