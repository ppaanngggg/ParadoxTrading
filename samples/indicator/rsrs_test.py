from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import RSRS

fetcher = FetchDominantIndex()
data = fetcher.fetchDayData('20100101', '20170101', 'cu')

rsrs_data = RSRS().addMany(data).getAllData()

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addCandle(
    'candle', data.index(), data.toRows(
        ['openprice', 'highprice', 'lowprice', 'closeprice']
    )[0]
)

rsrs_view = wizard.addView('rsrs')
rsrs_view.addLine('rsrs', rsrs_data.index(), rsrs_data['rsrs'])

wizard.show()
