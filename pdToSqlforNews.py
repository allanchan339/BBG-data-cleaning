import pandas
import numpy
import sqlalchemy
import AllAboutDB

engine = AllAboutDB.Mysqlprocess().createDB()


# dataframe_sh2 = dataframe.get('Sheet2')
# print(dataframe_sh1)
# print(dataframe_sh1['Surv(M)'])


def rowandcolumn(dataframe_sh1):
    dataframe_sh1['Date Time'] = dataframe_sh1['Date Time'].replace({'.+-.+': 'NaN'}, regex = True)
    dataframe_sh1 = dataframe_sh1[pandas.notnull(dataframe_sh1['Ticker'])]
    dataframe_sh1 = dataframe_sh1[pandas.notnull(dataframe_sh1['Date Time'])]
    indexNames = dataframe_sh1[dataframe_sh1['Date Time'] == 'NaN'].index
    dataframe_sh1.drop(indexNames, inplace = True)
    # print(dataframe_sh1['Date Time'])
    dataframe_sh1['Date Time'] = pandas.to_datetime(dataframe_sh1['Date Time'])
    dataframe_sh1['Date'] = pandas.to_datetime(dataframe_sh1['Date'])
    dataframe_sh1 = dataframe_sh1.drop(columns = ['A', 'M'])
    for i in range(4, 8):
        dataframe_sh1 = datacleaning(dataframe_sh1, list(dataframe_sh1)[i])
    for i in range(10, 14):
        dataframe_sh1 = datacleaning(dataframe_sh1, list(dataframe_sh1)[i])

    return dataframe_sh1


def datacleaning(dataframe_sh1,nameofcolumn):
    dataframe_sh1[nameofcolumn].replace({'\$': '', '[ÂA£¥]': '', '[bmkt]': '', '--': '0', 'NaN':'0'}, regex = True,
                                       inplace = True) #.map(pandas.eval)
    dataframe_sh1[nameofcolumn] = dataframe_sh1[nameofcolumn].map(pandas.eval)
    return dataframe_sh1

if __name__ == '__main__':
    DB_CONNECT_STRING = 'mysql+pymysql://allanchan339:33715882aAB@localhost/data'
    engine = sqlalchemy.create_engine(DB_CONNECT_STRING, echo = True)
    engine.execute("""DROP TABLE IF EXISTS News ;""")
    xlsxlist = [
            'Data/AUD Data.xlsx',
            'Data/CAD Data.xlsx',
            'Data/GBP Data.xlsx',
            'Data/JPY Data.xlsx',
            'Data/NZD Data.xlsx',
            'Data/USD Data.xlsx'
            ]
    for xlsxname in xlsxlist:
        dataframe, xlsx = AllAboutDB.Excelprocess().readNews(xlsxname)
        print(xlsx.sheet_names)
        dataframe_sh1 = dataframe.get(xlsx.sheet_names[0])
        print(dataframe_sh1['Surv(M)'])
        dataframe_sh1 = rowandcolumn(dataframe_sh1)

        print(dataframe_sh1['Surv(M)'])
        pandas.DataFrame.to_sql(dataframe_sh1, 'News', con = engine, if_exists = 'append', index = False)
    engine.dispose()
