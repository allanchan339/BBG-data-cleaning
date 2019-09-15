import pandas
import sqlalchemy
import AllAboutDB
import datetime
import timestring
engine = AllAboutDB.Mysqlprocess().engine

def DateConvertorFromInterval(UserInput):
    return timestring.Range(UserInput).start.date, timestring.Range(UserInput).end.date
def DateConvertorFromExactDate(UserInput):
    return timestring.Date(UserInput).date

def getDataFromDateTime(tablename, Start, End):
    sql = f"""
            select * FROM {tablename} WHERE Dates >='{Start}' AND Dates <'{End}'
            """
    df = pandas.read_sql(sql = sql, con = engine)
    return df

def getNewsFromTicker(tablename, news, date = None):
    if date is None:
        sql = f'''
                select `Ticker`,`Date Time` from {tablename} WHERE Ticker = '{news}'
                '''
        df = pandas.read_sql(sql = sql, con = engine)

    else:
        sql = f'''
                        select * from {tablename} WHERE Ticker = '{news}' AND `Date Time` >= '{date}' AND `Date Time` <'
                        {date + datetime.timedelta(days=1)  }' 
                        '''
        df = pandas.read_sql(sql = sql, con = engine)
    return df

def getAllNewsTicker():
    sql = """
                select DISTINCT Ticker from News
                """
    df = pandas.read_sql(sql = sql, con = engine)
    return df

def io():
    print('find Data from table: ', engine.table_names(), '.\nSelect the table: ')
    TableName = input()
    if TableName != 'News':  # traditional data type
        print('Search from 1.date or 2.interval?')
        UserInput = input()
        if UserInput == '1':
            print('StartDate is? remark: input date as mm/dd/yyyy HH:MM:SS')
            start = input()
            start = DateConvertorFromExactDate(start)
            print('EndDate is? Remark: you can input "now"')
            end = input()
            end = DateConvertorFromExactDate(end)
        else:
            print('Input the interval:')
            print('eg : this week, 10 week, 10 week ago, next week, last 6 days , etc...')
            UserInput = input()
            start, end = DateConvertorFromInterval(UserInput)
        df = getDataFromDateTime(tablename = TableName, Start = start, End = end)
        print(df)

    else:
        pandas.set_option('display.max_columns', 500)
        Function = input('Function 1.Extract News info by ticker and date, 2. Extract all info by ticker ')
        print('News ticker is ? The followind DataFrame will show all Ticker from DB')
        print(getAllNewsTicker())
        ticker = input('Plz input ticker name: ')
        if Function == '1':
            print('Release Date is ? input date as mm/dd/yyyy HH:MM:SS')
            start = input()
            start = DateConvertorFromExactDate(start)
            df = getNewsFromTicker(tablename = TableName, news = ticker, date = start)
        else:
            df = getNewsFromTicker(tablename = TableName, news = ticker)
        print(df)

if __name__ == '__main__':
    io()
