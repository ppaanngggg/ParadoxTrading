from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import ReturnRate

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
rate_1 = ReturnRate().addMany(market).getAllData()
rate_12 = ReturnRate(12).addMany(market).getAllData()

rate_abs_1 = ReturnRate(_use_abs=True).addMany(market).getAllData()
rate_abs_12 = ReturnRate(12, _use_abs=True).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price', _view_stretch=3)
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('sub')
sub_view.addLine('rate_1', rate_1.index(), rate_1['returnrate'])
sub_view.addLine('rate_12', rate_12.index(), rate_12['returnrate'])

abs_view = wizard.addView('abs')
abs_view.addLine('rate_abs_1', rate_abs_1.index(), rate_abs_1['returnrate'])
abs_view.addLine('rate_abs_12', rate_abs_12.index(), rate_abs_12['returnrate'])

wizard.show()
