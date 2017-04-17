from ParadoxTrading.Fetch import FetchOption50ETFDay
from ParadoxTrading.Fetch import FetchOption50ETFTick, RegisterOption50ETFTick

print('--- FetchOption50ETFTick')
register = RegisterOption50ETFTick('sh_10000001')
print(register)
print(register.toJson())
print(register.toKwargs())
print(RegisterOption50ETFTick.fromJson(register.toJson()))

fetcher = FetchOption50ETFTick()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

print(('--- fetch symbol'))
print(fetcher.fetchSymbol('20160101', 'sh_10000287'))
print(fetcher.fetchSymbol('20160104', 'sh_10000287'))
print(fetcher.fetchSymbol('20160104', 'sh_10000001'))

print(('--- fetch info'))
print(fetcher.fetchContractInfo('20160104'))

print(('--- fetch data'))
print(len(fetcher.fetchData('20160104', 'sh_10000287')))

print('--- FetchOption50ETFDay')
fetcher = FetchOption50ETFDay()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'
print(len(fetcher.fetchData('20160104', 'sh_10000287')))
print(len(fetcher.fetchDayData('20160104', '20160110', 'sh_10000287')))
