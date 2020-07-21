import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
load_dotenv()

import os
import numpy as np
import pandas as pd

# env info

env_host = os.getenv("DB_HOST")
env_name = os.getenv("DB_NAME")
env_user = os.getenv("DB_USER")
env_password = os.getenv("DB_PASSWORD")

read_file = './test_data.xlsx'

df = pd.read_excel(read_file)
count_row = df.shape[0]

print(df)

try:
    connection = psycopg2.connect(user = env_user,
                                  password = env_password,
                                  host = env_host,
                                  database = env_name)

    cursor = connection.cursor()

    create_table_query = '''
        CREATE TABLE itemstest
        (id SERIAL PRIMARY KEY,
        unit_id INT,
        unit VARCHAR,
        size INT,
        weight INT);
        '''
    
    def generate_data_queries(dataframe):
        statement = '''INSERT INTO itemstest (unit_id, unit, size, weight) VALUES '''

        for index, row in dataframe.iterrows():
            values = '(' + str(row['id']) + ',' + '\'' + str(row['unit']) + '\'' + ',' + str(row['size']) + ',' + str(row['weight']) + ')'

            if index + 1 != count_row:
                values += ','
            
            statement += values

        return statement

    generate_data_queries(df)
    
    add_dataframe_entries = generate_data_queries(df)
    
    cursor.execute(create_table_query)
    cursor.execute(add_dataframe_entries)
    connection.commit()
    print("Table created successfully in PostgreSQL ")

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")