import numpy as np
import pandas as pd


df = pd.read_csv('example')
df
df.to_csv('My_output',index=False) #index false prevents making duplicate index column
df
pd.read_excel('Excel_Sample.xlsx',sheet_name='Sheet1')
df.to_excel('Excel_Sample2.xlsx',sheet_name='NewSheet',index=False)
pd.read_excel('Excel_Sample2.xlsx')
#df.to_excel('Excel_Sample.xlsx',sheet_name='NewSheet',index=False)
data = pd.read_html('https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/')
data
type(data)
data[0]

#pandas is probably not the best way to read a sql database, because there are many versions of sql
from sqlalchemy import create_engine #allows us to import simple sql engine in memory
engine = create_engine('sqlite:///:memory:')
df.to_sql('my_table',engine) #writes df to the engine we have stored in memory
#you should use a specialized sql library built for whichever engine you're working in
sqldf = pd.read_sql('my_table',con=engine) #con is connection. We can't do index here, so it adds extra column?
sqldf
df
