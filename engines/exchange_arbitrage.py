import time
from time import strftime
import grequests
from exchanges.loader import EngineLoader
from mock_balance import mock_balance_kraken_parsed, mock_balance_bittrex_parsed

class CryptoEngineExArbitrage(object):
    def __init__(self, exParams, mock=False):
        self.exParams = exParams
        self.mock = mock
        self.minProfit = 0.00005 # This may not be accurate as coins have different value        
        self.hasOpenOrder = True # always assume there are open orders first
        self.openOrderCheckCount = 0

        self.engineA = EngineLoader.getEngine(self.exParams['exchangeA']['exchange'], self.exParams['exchangeA']['keyFile'])
        self.engineB = EngineLoader.getEngine(self.exParams['exchangeB']['exchange'], self.exParams['exchangeB']['keyFile'])

    def start_engine(self):
        print strftime('%Y-%m-%d %H:%M:%S') + ' starting Exchange Arbitrage Engine...'
        if self.mock:
            print '---------------------------- MOCK MODE ----------------------------'
        #Send the request asynchronously
        while True:
            try:
                if not self.mock and self.hasOpenOrder:
                    self.check_openOrder()
                else:
                    if self.check_balance():
                        bookStatus = self.check_orderBook()
                        print 'bookStatus: ' + str(bookStatus['status'])
                        if bookStatus['status']:
                            self.place_order(bookStatus['status'], bookStatus['ask'], bookStatus['bid'], bookStatus['maxAmount'])
                    else:
                        self.rebalance()
            except Exception, e:
                print e

            #time.sleep(self.engineA.sleepTime)
            time.sleep(self.engineA.sleepTime + 10)
            print '\n'
            
    def check_openOrder(self):
        if self.openOrderCheckCount >= 5:
            self.cancel_allOrders()
        else:
            print 'checking open orders...'
            rs = [self.engineA.get_open_order(),
                  self.engineB.get_open_order()]
            responses = self.send_request(rs)

            if not responses[0] or not responses[1]:
                print responses
                return False
            
            if responses[0].parsed or responses[1].parsed:
                self.engineA.openOrders = responses[0].parsed
                self.engineB.openOrders = responses[1].parsed
                print self.engineA.openOrders, self.engineB.openOrders
                self.openOrderCheckCount += 1
            else:
                self.hasOpenOrder = False
                print 'no open orders'
                print 'starting to check order book...'
    
    def cancel_allOrders(self):
        print 'cancelling all open orders...'
        rs = []
        print self.exParams['exchangeA']['exchange']
        for order in self.engineA.openOrders:
            print order
            rs.append(self.engineA.cancel_order(order['orderId']))

        print self.exParams['exchangeB']['exchange']
        for order in self.engineB.openOrders:
            print order
            rs.append(self.engineB.cancel_order(order['orderId']))

        responses = self.send_request(rs)
        
        self.engineA.openOrders = []
        self.engineB.openOrders = []
        self.hasOpenOrder = False
        

    #Check and set current balance
    def check_balance(self):
        rs = [self.engineA.get_balance([self.exParams['exchangeA']['tickerA'], self.exParams['exchangeA']['tickerB']]),
              self.engineB.get_balance([self.exParams['exchangeB']['tickerA'], self.exParams['exchangeB']['tickerB']])]

        responses = self.send_request(rs)

        self.engineA.balance = responses[0].parsed
        self.engineB.balance = responses[1].parsed
        if not self.engineA.balance:
            print 'using mock balance for ' + self.exParams['exchangeA']['exchange']
            self.engineA.balance = mock_balance_bittrex_parsed
        if not self.engineB.balance:
            print 'using mock balance for ' + self.exParams['exchangeB']['exchange']
            self.engineB.balance = mock_balance_kraken_parsed

        print self.exParams['exchangeA']['exchange'] + ' balance = ' + str(self.engineA.balance) +' '+self.exParams['exchangeB']['exchange'] + ' balance = ' + str(self.engineB.balance)
        
        if not self.mock:
            for res in responses:
                for ticker in res.parsed:
                    # This may not be accurate
                    if res.parsed[ticker] < 0.05:
                        print ticker, res.parsed[ticker], '- Not Enough'
                        return False
        return True
    
    def rebalance(self):
        print 'rebalancing...'

    def check_orderBook(self):
        rs = [self.engineA.get_ticker_orderBook_innermost(self.exParams['exchangeA']['tickerPair']),
              self.engineB.get_ticker_orderBook_innermost(self.exParams['exchangeB']['tickerPair'])]

        responses = self.send_request(rs)
        
        print "{0: <10} - {1}\n{2: <10} - {3}".format(
            self.exParams['exchangeA']['exchange'], responses[0].parsed,
            self.exParams['exchangeB']['exchange'], responses[1].parsed
            )

        diff_A = responses[0].parsed['ask']['price'] - responses[1].parsed['bid']['price']
        diff_B = responses[1].parsed['ask']['price'] - responses[0].parsed['bid']['price']

        if diff_A < 0 and diff_B < 0 and abs(diff_A) < abs(diff_B):
            diff_A = 0
            print 'diff_A = 0'

        # The highest price someone is buying a currency at Exchange B is higher than the lowest price someone is selling the same currency at Exchange A
        # --> Possible arbitrage
        # Buy from Exchange A, Sell to Exchange B
        if diff_A < 0:
            print 'ARBITRAGE: buy from '+self.exParams['exchangeA']['exchange'] +', sell to '+self.exParams['exchangeB']['exchange']
            maxAmount = self.getMaxAmount(responses[0].parsed['ask'], responses[1].parsed['bid'], 1)
            fee = self.engineA.feeRatio * maxAmount * responses[0].parsed['ask']['price'] + self.engineB.feeRatio * maxAmount * responses[1].parsed['bid']['price']
            print 'fee='+str(fee)

            if abs(diff_A * maxAmount) - fee > self.minProfit:
                print "{0}'s Ask {1} - {2}'s Bid {3} < 0".format(
                    self.exParams['exchangeA']['exchange'], 
                    responses[0].parsed['ask']['price'],
                    self.exParams['exchangeB']['exchange'], 
                    responses[1].parsed['bid']['price'])       
                print '{0} (diff) * {1} (amount) = {2}, commission fee: {3}'.format(diff_A, maxAmount, abs(diff_A * maxAmount), fee)            
                return {'status': 1, 'ask': responses[0].parsed['ask']['price'], 'bid': responses[1].parsed['bid']['price'], 'maxAmount': maxAmount}
            else:
                print 'Not profitable.'
                return {'status': 0}

        # Buy from Exchange B, Sell to Exchange A
        elif diff_B < 0:
            print 'ARBITRAGE: buy from '+self.exParams['exchangeB']['exchange'] +', sell to '+self.exParams['exchangeA']['exchange']
            maxAmount = self.getMaxAmount(responses[1].parsed['ask'], responses[0].parsed['bid'], 2)
            fee = self.engineB.feeRatio * maxAmount * responses[1].parsed['ask']['price'] + self.engineA.feeRatio * maxAmount * responses[0].parsed['bid']['price']
            print 'fee='+str(fee)

            if abs(diff_B * maxAmount) - fee > self.minProfit:
                print "{0}'s Ask {1} - {2}'s Bid {3} < 0".format(
                    self.exParams['exchangeB']['exchange'], 
                    responses[1].parsed['ask']['price'], 
                    self.exParams['exchangeA']['exchange'], 
                    responses[0].parsed['bid']['price'])             
                print '{0} (diff) * {1} (amount) = {2}, commission fee: {3}'.format(diff_B, maxAmount, abs(diff_B * maxAmount), fee)   
                return {'status': 2, 'ask': responses[1].parsed['ask']['price'], 'bid': responses[0].parsed['bid']['price'], 'maxAmount': maxAmount}
            else:
                print 'Not profitable.'
                return {'status': 0}
        
        print 'No arbitrage opportunity'
        return {'status': 0}

    def getMaxAmount(self, askOrder, bidOrder, type):
        amount = 0
        print 'getMaxAmount :: askOrder='+str(askOrder)+' bidOrder='+str(bidOrder)+' type='+str(type)
        # Buy from Exchange A, Sell to Exchange B
        if type == 1:
            maxOwnAmountA = self.engineA.balance[self.exParams['exchangeA']['tickerA']] / ((1 + self.engineA.feeRatio) * askOrder['price'])
            maxOwnAmountB = self.engineB.balance[self.exParams['exchangeB']['tickerB']]
            amount = min(maxOwnAmountA, maxOwnAmountB, askOrder['amount'], bidOrder['amount'])
        # Buy from Exchange B, Sell to Exchange A
        elif type == 2:
            maxOwnAmountA = self.engineA.balance[self.exParams['exchangeA']['tickerB']]
            maxOwnAmountB = self.engineB.balance[self.exParams['exchangeB']['tickerA']] / ((1 + self.engineB.feeRatio) * askOrder['price'])
            amount = min(maxOwnAmountA, maxOwnAmountB, askOrder['amount'], bidOrder['amount'])

        print 'maxAmount = ' + str(amount)
        return amount

    def place_order(self, status, ask, bid, amount):
        print 'placing order...'
        # Buy from Exchange A, Sell to Exchange B                
        if status == 1:
            print strftime('%Y-%m-%d %H:%M:%S') + ' Buy at {0} @ {1} & Sell at {2} @ {3} for {4}'.format(ask, self.exParams['exchangeA']['exchange'], bid, self.exParams['exchangeB']['exchange'], amount)
            rs = [
                self.engineA.place_order(self.exParams['exchangeA']['tickerPair'], 'bid', amount, ask),
                self.engineB.place_order(self.exParams['exchangeB']['tickerPair'], 'ask', amount, bid),                
            ]
        # Buy from Exchange B, Sell to Exchange A
        elif status == 2:
            print strftime('%Y-%m-%d %H:%M:%S') + ' Buy at {0} @ {1} & Sell at {2} @ {3} for {4}'.format(ask, self.exParams['exchangeB']['exchange'], bid, self.exParams['exchangeA']['exchange'], amount)
            rs = [
                self.engineB.place_order(self.exParams['exchangeB']['tickerPair'], 'bid', amount, ask),
                self.engineA.place_order(self.exParams['exchangeA']['tickerPair'], 'ask', amount, bid),                
            ]

        if not self.mock:
            responses = self.send_request(rs)
        self.hasOpenOrder = True
        self.openOrderCheckCount = 0

    def send_request(self, rs):
        responses = grequests.map(rs)
        for res in responses:
            if not res:
                print responses
                raise Exception
        return responses

    def run(self):
        self.start_engine()

if __name__ == '__main__':
    exParams = {
        'exchangeA': {
            'exchange': 'bittrex',
            'keyFile': '../keys/bittrex.key',
            'tickerPair': 'BTC-ETH',
            'tickerA': 'BTC',
            'tickerB': 'ETH'        
        },
        'exchangeB': {
            'exchange': 'bitstamp',
            'keyFile': '../keys/bitstamp.key',
            'tickerPair': 'ethbtc',
            'tickerA': 'btc',
            'tickerB': 'eth'         
        }
    }
    engine = CryptoEngineExArbitrage(exParams, True)
    #engine = CryptoEngineExArbitrage(exParams)
    engine.run()
