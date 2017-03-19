from ParadoxTrading.Chart import CWizard
from ParadoxTrading.Fetch import FetchFutureDay
from ParadoxTrading.Indicator import MA

data = FetchFutureDay().fetchDayData('20170101', '20170301', 'rb')
ma_data = MA(10, 'closeprice').addMany(data.index(), data).getAllData()
data.expand(ma_data)

wizard = CWizard('rb')

wizard.addView('price', 3)
wizard.addLine(
    'price', data.index(), data.getColumn('closeprice'),
    'closeprice', 'green'
)
wizard.addLine(
    'price', data.index(), data.getColumn('ma'),
    'ma'
)

wizard.addView('volume')
wizard.addBar(
    'volume', data.index(), data.getColumn('volume'),
    'volume', 'red'
)

wizard.show()
