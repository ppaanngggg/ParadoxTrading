from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Performance import dailyReturn, sharpRatio

ret = dailyReturn('range_break', 'range_break')
print(ret)

wizard = Wizard('range_break_fund')

wizard.addView('fund')
wizard.addLine('fund', ret.index(), ret['fund'], 'fund change')

wizard.show()
