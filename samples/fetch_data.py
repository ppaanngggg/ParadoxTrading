from ParadoxTrading.Fetch import FetchGuoJinTick, FetchExchangeMarketIndex

fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

# get all product in mongo database
print(fetcher.productList())

# get symbol of config. This function is the most common interface
# 1. get the dominant symbol
print(fetcher.fetchSymbol('20160506', 'rb'))
# 2. get the subdominant symbol
print(fetcher.fetchSymbol('20160506', 'rb', _sub_dominant=True))
# 3. get the instrument symbol
print(fetcher.fetchSymbol('20160506', _instrument='rb1609'))

# get dominant symbol of ag
print(fetcher.fetchDominant('ag', '20160506'))
# get sub dominant symbol of ag
print(fetcher.fetchSubDominant('ag', '20160506'))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('ag', '20160506'))

# get tick data
print(fetcher.fetchData('20160506', fetcher.fetchSymbol('20160506', 'rb')))

# this is a product index fetcher
fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

# get the symbol of product
print(fetcher.fetchSymbol('20170123', 'rb'))

# get index data
print(fetcher.fetchData('20170123', 'rb'))

# get during data
print(fetcher.fetchDayData('20170101', '20170201', 'rb'))
