from ParadoxTrading.Indicator import BBands, FastBBands
from ParadoxTrading.Fetch.ChineseFutures import FetchProductIndex
from ParadoxTrading.Chart import Wizard
import time

fetcher = FetchProductIndex()
data = fetcher.fetchDayData(
    '20100101', '20170101', 'rb'
)
print(time.clock())
bbands = BBands().addMany(data).getAllData()
print(time.clock())
fast_bbands = FastBBands().addMany(data).getAllData()
print(time.clock())

wizard = Wizard()

price_view = wizard.addView('price view')
price_view.addCandle(
    'candle', data.index(),
    data.toRows([
        'openprice', 'highprice', 'lowprice', 'closeprice'
    ])[0], _show_value=False
)
price_view.addLine(
    'bbands upband', bbands.index(), bbands['upband']
)
price_view.addLine(
    'bbands midband', bbands.index(), bbands['midband']
)
price_view.addLine(
    'bbands downband', bbands.index(), bbands['downband']
)
price_view.addLine(
    'fast upband', fast_bbands.index(), fast_bbands['upband']
)
price_view.addLine(
    'fast midband', fast_bbands.index(), fast_bbands['midband']
)
price_view.addLine(
    'fast downband', fast_bbands.index(), fast_bbands['downband']
)

wizard.show()
