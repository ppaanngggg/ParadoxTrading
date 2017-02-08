from ParadoxTrading.Utils import Fetch

data = Fetch.fetchIntraDayData('20170203', 'rb')
print(data)
