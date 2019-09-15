import sqlalchemy
import sqlalchemy.orm
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()


class Mysqlprocess():
    def __init__(self):
        self.engine = self.createDB()
        self.DB_Session = self.createSession(engine = self.engine)

    def packageData(self, Dates, Open, High, Low, Close, Object):
        item = {'Dates': Dates, 'Open': Open, 'High': High, 'Low': Low, 'Close': Close}
        if Object == 'ESA_index':
            u = ESA_index(Dates = item['Dates'], Data_Open = item['Open'], Data_Close = item['Close'], Data_High =
            item['High'], Data_Low = item['Low'])
        elif Object == 'AUDUSD_Curncy':
            u = AUDUSD_Curncy(Dates = item['Dates'], Data_Open = item['Open'], Data_Close = item['Close'], Data_High =
            item['High'], Data_Low = item['Low'])
        return u

    def addData(self, u):
        with self.DB_Session() as session:
            session.add(u)
            session.commit()

    def createDB(self):
        DB_CONNECT_STRING = 'mysql+pymysql://allanchan339:33715882aAB@localhost/data'
        engine = sqlalchemy.create_engine(DB_CONNECT_STRING, echo = False)
        return engine

    def createSession(self, engine):
        DB_Session = sqlalchemy.orm.sessionmaker(bind = engine)
        Base.metadata.create_all(engine)
        return DB_Session

    def insertData(self, items, tablename):
        with self.DB_Session() as session:
            for item in items:
                session.execute(f"""
                INSERT INTO {tablename} value (%s,%s,%s,%s,%s)""",
                                (
                                        item['Dates'],
                                        item['Open'],
                                        item['High'],
                                        item['Low'],
                                        item['Close']
                                        ))


class ESA_index(Base):
    __tablename__ = 'ESA_index'
    # id = sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key = True, autoincrement = True)
    Dates = sqlalchemy.Column(sqlalchemy.DateTime(timezone = True), primary_key = True)
    Data_Open = sqlalchemy.Column('Data_Open', sqlalchemy.Float)
    Data_Close = sqlalchemy.Column('Data_Close', sqlalchemy.Float)
    Data_High = sqlalchemy.Column('Data_High', sqlalchemy.Float)
    Data_Low = sqlalchemy.Column('Data_Low', sqlalchemy.Float)


class AUDUSD_Curncy(Base):
    __tablename__ = 'AUDUSD_Curncy'
    # id = sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key = True, autoincrement = True)
    Dates = sqlalchemy.Column(sqlalchemy.DateTime(timezone = True), primary_key = True)
    Data_Open = sqlalchemy.Column('Data_Open', sqlalchemy.FLOAT)
    Data_Close = sqlalchemy.Column('Data_Close', sqlalchemy.FLOAT)
    Data_High = sqlalchemy.Column('Data_High', sqlalchemy.FLOAT)
    Data_Low = sqlalchemy.Column('Data_Low', sqlalchemy.FLOAT)


class Excelprocess():
    def __init__(self):
        pass

    def readfile(self, filename):
        self.df = pd.read_excel(filename, skiprows = 3, sheet_name = None)
        self.xlsx = pd.ExcelFile(filename)
        return self.df, self.xlsx
    def readNews(self, filename):
        self.df = pd.read_excel(filename, sheet_name = None)
        self.xlsx = pd.ExcelFile(filename)
        return self.df, self.xlsx


def main():
    dataframe, xlsx = Excelprocess().readfile('Data.xlsx')
    print(xlsx.sheet_names)
    dataframe_sh1 = dataframe.get('Sheet1')
    dataframe_sh2 = dataframe.get('Sheet2')
    print(dataframe_sh1)
    print(dataframe_sh2)


if __name__ == '__main__':
    main()
