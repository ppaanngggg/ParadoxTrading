from ParadoxTrading.Fetch import FetchFutureTick, FetchFutureMin, \
    FetchFutureHour, FetchFutureDay

fetcher = FetchFutureTick()
inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _sub_dominant=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _sub_dominant=True
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _product_index=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _product_index=True
)
print(inst)
print(data)
input()

fetcher = FetchFutureMin()
inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _sub_dominant=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _sub_dominant=True
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _product_index=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _product_index=True
)
print(inst)
print(data)
input()

fetcher = FetchFutureHour()
inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _sub_dominant=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _sub_dominant=True
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _product_index=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _product_index=True
)
print(inst)
print(data)
input()

fetcher = FetchFutureDay()
inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _sub_dominant=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _sub_dominant=True
)
print(inst)
print(data)
input()

inst = fetcher.fetchSymbol(
    _tradingday='20170123',
    _product='rb',
    _product_index=True
)
data = fetcher.fetchData(
    _tradingday='20170203',
    _product='rb',
    _product_index=True
)
print(inst)
print(data)
input()
