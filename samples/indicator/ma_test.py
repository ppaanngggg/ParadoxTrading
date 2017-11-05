from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import MA, FastMA
import time

fetcher = FetchDominantIndex()
data = fetcher.fetchDayData(
    '20100101', '20170101', 'rb'
)
print(time.clock())
ma = MA(30).addMany(data).getAllData()
print(time.clock())
fast_ma = FastMA(30).addMany(data).getAllData()
print(time.clock())

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addLine('price', data.index(), data['closeprice'])
price_view.addLine('ma', ma.index(), ma['ma'])
price_view.addLine('fast ma', fast_ma.index(), fast_ma['ma'])

wizard.show()
