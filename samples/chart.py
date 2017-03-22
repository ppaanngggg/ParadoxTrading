from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchSHFEDayIndex
from ParadoxTrading.Indicator import MA

data = FetchSHFEDayIndex().fetchDayData('20160101', '20170301', 'rb')
ma_data = MA(10, 'averageprice').addMany(data.index(), data).getAllData()
data.expand(ma_data)

wizard = Wizard('rb')

wizard.addView('price', 3)

wizard.addLine(
    'price', data.index(), data.getColumn('averageprice'),
    'averageprice', 'blue'
)
wizard.addLine(
    'price', data.index(), data.getColumn('ma'),
    'ma', 'grey'
)

wizard.addView('volume')
wizard.addBar(
    'volume', data.index(), data.getColumn('volume'),
    'volume', 'red'
)

wizard.show()
