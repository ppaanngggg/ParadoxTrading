from ParadoxTrading.Fetch import FetchExchangeMarketIndex
from ParadoxTrading.Indicator import Plunge, EMA
from ParadoxTrading.Chart import Wizard

fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'
rb = fetcher.fetchDayData('20100101', '20170101', 'rb')

fast_ema = EMA(50).addMany(rb).getAllData()
slow_ema = EMA(100).addMany(rb).getAllData()
plunge = Plunge().addMany(rb).getAllData()

wizard = Wizard()

main_view = wizard.addView('main', _adaptive=True, _view_stretch=3)
main_view.addLine('price', rb.index(), rb['closeprice'])
main_view.addLine('fast_ema', fast_ema.index(), fast_ema['ema'])
main_view.addLine('slow_ema', slow_ema.index(), slow_ema['ema'])

sub_view = wizard.addView('sub')
sub_view.addLine('plunge', plunge.index(), plunge['plunge'])

wizard.show()