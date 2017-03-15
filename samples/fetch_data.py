from ParadoxTrading.Fetch import FetchGuoJinTick, FetchGuoJinMin, FetchGuoJinDay

fetcher = FetchGuoJinTick()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

# get all product in mongo database
print(fetcher.productList())
# get dominant symbol of ag
print(fetcher.fetchDominant('ag', '20160314'))
# get sub dominant symbol of ag
print(fetcher.fetchSubDominant('ag', '20160314'))
# get add instrument traded of this product
print(fetcher.fetchTradeInstrument('ag', '20160314'))
input()

# get dominant data of ag
print(fetcher.fetchData('20160314', _product='ag'))
input()
# get sub dominant data of ag
print(fetcher.fetchData('20160314', _product='ag', _sub_dominant=True))
input()
# get product index of ag
print(fetcher.fetchData('20160314', _product='ag', _product_index=True))
input()
# get insturment of ag1603
print(fetcher.fetchData('20160314', _instrument='ag1603'))

# fetch min data
fetcher = FetchGuoJinMin()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

# fetch min bar of ag, and use begin time as index
print(fetcher.fetchData('20160314', 'ag', _index='bartime'))
input()
# fetch min bar of ag, and use end time as index
print(fetcher.fetchData('20160314', 'ag', _index='barendtime'))
input()

# fetch day data
fetcher = Fetch = FetchGuoJinDay()
fetcher.psql_host = '192.168.4.102'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.102'

# fetch one day
print(fetcher.fetchData('20160314', 'ag'))
# fetch a range of days
print(fetcher.fetchDayData('20160301', '20160401', 'ag1606'))
input()