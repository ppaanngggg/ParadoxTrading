from ParadoxTrading.Chart import CWizard
from ParadoxTrading.Indicator import MA, CloseBar, Diff
from ParadoxTrading.Utils import Fetch, SplitIntoMinute

data = Fetch.fetchIntraDayData('20170123', 'rb')

spliter = SplitIntoMinute(1)
spliter.addMany(data)

closeprice = CloseBar('lastprice').addMany(
    spliter.getBarBeginTimeList(),
    spliter.getBarList()
).getAllData()

maprice = MA(10, 'close').addMany(
    closeprice.index(), closeprice
).getAllData()

closevolume = CloseBar('volume').addMany(
    spliter.getBarBeginTimeList(),
    spliter.getBarList()
).getAllData()

volume = Diff('close').addMany(
    closevolume.index(), closevolume
).getAllData()

data = closeprice
data.expand(volume)
data.expand(maprice)
data.changeColumnName('close', 'price')
data.changeColumnName('diff', 'volume')

print(data)

wizard = CWizard('rb')

wizard.addView('price', 3)
wizard.addLine(
    'price', data.index(), data.getColumn('price'),
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
