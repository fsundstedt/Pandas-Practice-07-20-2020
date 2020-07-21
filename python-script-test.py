import psycopg2
from psycopg2 import Error
import numpy as np
import pandas as pd

read_file = './test_data.xlsx'

df = pd.read_excel(read_file)
count_row = df.shape[0]

def generate_data_queries(dataframe):
    statement = 'INSERT INTO itemstest (unit_id, unit, size, weight) VALUES '

    for index, row in dataframe.iterrows():
        values = '(' + str(row['id']) + ',' + '\'' + str(row['unit']) + '\'' + ',' + str(row['size']) + ',' + str(row['weight']) + ')'
        print(values)

        if index + 1 != count_row:
            values += ','
        
        statement += values

    statement += ';'

    print(statement)
    return statement

generate_data_queries(df)
