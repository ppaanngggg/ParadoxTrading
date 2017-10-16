# ParadoxTrading.Engine

This the backbone of the trading system. This is just a flexible framework, the implementation will be quite complex.

## parts

1. Engine: This is the core of the trading. It will connect all the other parts and send or redirect messages between parts.
2. MarketSupply: It will send events(MarketEvent) containing data to registered strategies. If a strategy need a serie of data, it have to register to market supply, and then it will receive continue data.
3. Strategy: First, It should registe data needed, then it will receive events(MarketEvent). You should send signal(SignalEvent) to portfolio according to your logic or model. Strategy can't operate orders directly, the portfolio will decide whether to send order(OrderEvent) and how much volume.
4. Portfolio: It will receive signal(SignalEvent) from strategies, and then it will turn signal to real order(OrderEvent) by mathematical magic according to current positions status and market info. And it will receive fill from execution, and update its positions status.
5. Execution: It will receive order(OrderEvent) from portfolio, and then send it to market. Of course, fill by itself when backtesting. Then it will send fill(FillEvent) back.
