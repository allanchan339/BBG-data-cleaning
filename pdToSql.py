import pandas as pd
from sqlalchemy import create_engine
import AllAboutDB
engine = AllAboutDB.Mysqlprocess().createDB()
dataframe, xlsx = AllAboutDB.Excelprocess().readfile('Data.xlsx')
# print(xlsx.sheet_names)
dataframe_sh1 = dataframe.get('Sheet1')
dataframe_sh2 = dataframe.get('Sheet2')
# print(dataframe_sh1)
# print(dataframe_sh2)
pd.DataFrame.to_sql(dataframe_sh1, 'AUDUSD_Curncy', con = engine, if_exists = 'replace', index = False)
pd.DataFrame.to_sql(dataframe_sh2, 'ESA_index', con = engine, if_exists = 'replace', index = False)

engine.dispose()
