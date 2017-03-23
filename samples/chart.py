from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchSHFEDayIndex
from ParadoxTrading.Indicator import MA

data = FetchSHFEDayIndex().fetchDayData('20160101', '20170301', 'rb')
ma_data = MA(10, 'averageprice').addMany(data.index(), data).getAllData()
data.expand(ma_data)

long_time = []
long_price = []

for d in data:
    if d['averageprice'][0] > d['ma'][0]:
        long_time.append(d['tradingday'][0])
        long_price.append(d['averageprice'][0])

wizard = Wizard('rb')

wizard.addView('price', 3)
wizard.addLine(
    'price', data.index(), data['averageprice'],
    'averageprice', 'blue'
)
wizard.addLine(
    'price', data.index(), data['ma'],
    'ma', 'grey'
)
wizard.addScatter(
    'price', long_time, long_price,
    'scatter'
)

wizard.addView('volume')
wizard.addBar(
    'volume', data.index(), data['volume'],
    'volume', 'red'
)

wizard.show()
