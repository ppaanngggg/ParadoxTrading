import diskcache

from ParadoxTrading.Fetch.Crypto import RegisterSymbol, FetchDepth, FetchTicker

print('--register symbol--')
register = RegisterSymbol('binance', 'BTC_USDT')
print(register)
json_str = register.toJson()
print(json_str)
new_register = RegisterSymbol.fromJson(json_str)
print(new_register)

print('--fetch depth--')
fetcher = FetchDepth(
    _psql_host='psql.local', _psql_dbname='cube_data', _psql_user='postgres'
)
symbol = fetcher.fetchSymbol('20180701', **register.toKwargs())
print(symbol)
data = fetcher.fetchData('20180701', symbol)
print(data)

print('--fetch ticker--')
fetcher = FetchTicker(
    _psql_host='psql.local', _psql_dbname='cube_data', _psql_user='postgres'
)
data = fetcher.fetchData('20180701', symbol)
print(data)

print('-- cache keys --')
cache = diskcache.Cache('cache')
for k in cache.iterkeys():
    print(k)
