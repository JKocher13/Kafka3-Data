from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import statistics 

Base = declarative_base()
class Transaction(Base):
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.totalwithdraw = {}
        self.totaldeposit = {}
        self.depo_count = {}
        self.withdraw_count = {}
        self.log = []
        self.stats = {}
        self.engine = create_engine('mysql+pymysql://root:zipcoder@localhost/kafka_1')




        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            #print('{} received'.format(message))
            self.ledger[message['custid']] = message
            self.log.append(message)
            message_sql = Transaction(custid=message['custid'], type=message['type'], date=message['date'], amt = message['amt'])
            Session = sessionmaker(bind=self.engine)
            session = Session()
            session.add(message_sql)
            session.commit()
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
                self.stats[message['custid']] = {"deposit_stdev": 0, "deposit_average": 0, "withdraw_stdev": 0, "withdraw_average":0}
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                lst = self.stand_dev_dep(message)
                if len(lst) > 1:
                    stand_dev = statistics.stdev(lst)
                    self.stats[message['custid']]["deposit_stdev"]= stand_dev
                average = statistics.mean(lst)
                self.stats[message['custid']]["deposit_average"]= average
            else:
                self.custBalances[message['custid']] -= message['amt']
                lst = self.stand_dev_wth(message)
                if len(lst) > 1:
                    stand_dev = statistics.stdev(lst)
                    self.stats[message['custid']]["withdraw_stdev"]= stand_dev
                average = statistics.mean(lst)
                self.stats[message['custid']]["withdraw_average"]= average
            print(self.stats)
            #print(self.custBalances)

    def stand_dev_dep(self,message):
        amounts = []
        for trans in self.log:
            if trans["custid"] == message["custid"]:
                if trans["type"] == 'dep':
                    amounts.append(trans['amt'])
        return amounts

    def stand_dev_wth(self,message):
        amounts = []
        for trans in self.log:
            if trans["custid"] == message["custid"]:
                if trans["type"] == 'wth':
                    amounts.append(trans['amt'])
        return amounts




if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()