from ParadoxTrading.Fetch import FetchOption50ETFTick, RegisterOption50ETFTick
from ParadoxTrading.Fetch import FetchOption50ETFDay, RegisterOption50ETFDay
print('--- FetchOption50ETFTick')
register = RegisterOption50ETFTick('sh_10000001')
print(register)
print(register.toJson())
print(register.toKwargs())
print(RegisterOption50ETFTick.fromJson(register.toJson()))

fetcher = FetchOption50ETFTick()
print(len(fetcher.fetchData('20150209', 'sh_10000001')))

print('--- FetchOption50ETFDay')
fetcher = FetchOption50ETFDay()
print(len(fetcher.fetchData('20150209', 'sh_10000001')))
print(len(fetcher.fetchDayData('20150201', '20150301', 'sh_10000001')))