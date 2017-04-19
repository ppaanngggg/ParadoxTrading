from ParadoxTrading.Fetch import FetchLMEDay, RegisterLMEDay

print(RegisterLMEDay('cu'))
print(RegisterLMEDay('cu').toJson())
print(RegisterLMEDay('cu').toKwargs())

fetcher = FetchLMEDay()
print(fetcher.fetchSymbol('20170414', **RegisterLMEDay('cu').toKwargs()))
print(fetcher.fetchSymbol('20170413', **RegisterLMEDay('cu').toKwargs()))

print(fetcher.fetchData('20170414', 'cu'))
print(fetcher.fetchData('20170413', 'cu'))

print(fetcher.fetchDayData('20170101', '20170401', 'cu'))
