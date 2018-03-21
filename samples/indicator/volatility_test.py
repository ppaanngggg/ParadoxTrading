from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import FastVolatility

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100101', '20170101', 'rb')

fast_vol_1 = FastVolatility(30, _smooth=1).addMany(market).getAllData()
fast_vol_12 = FastVolatility(30, _smooth=12).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addCandle(
    'price', market.index(), market.toRows([
        'openprice', 'highprice', 'lowprice', 'closeprice'
    ])[0]
)

sub_view = wizard.addView('std')
sub_view.addLine('fast_vol_1', fast_vol_1.index(), fast_vol_1['volatility'])
sub_view.addLine('fast_vol_12', fast_vol_12.index(), fast_vol_12['volatility'])

wizard.show()
