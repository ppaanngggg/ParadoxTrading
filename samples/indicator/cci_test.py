from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import CCI

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
cci = CCI(20).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price', _view_stretch=3)
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('sub')
sub_view.addLine('cci', cci.index(), cci['cci'])

wizard.show()
