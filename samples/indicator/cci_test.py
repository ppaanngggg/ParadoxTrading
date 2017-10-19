from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchExchangeMarketIndex
from ParadoxTrading.Indicator import CCI

fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
cci = CCI(20).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price', _view_stretch=3)
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('sub')
sub_view.addLine('cci', cci.index(), cci['cci'])

wizard.show()
