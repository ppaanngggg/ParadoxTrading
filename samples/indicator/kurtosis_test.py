from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import Kurtosis

fetcher = FetchDominantIndex()
data = fetcher.fetchDayData(
    '20100101', '20170101', 'rb'
)
k_30 = Kurtosis(30).addMany(data).getAllData()
k_60 = Kurtosis(60).addMany(data).getAllData()

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addLine('price', data.index(), data['closeprice'])

k_view = wizard.addView('k')
k_view.addLine('30', k_30.index(), k_30['kurtosis'])
k_view.addLine('60', k_60.index(), k_60['kurtosis'])

wizard.show()
