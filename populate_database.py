import pandas as pd
import sqlite3

spreadsheet0 = pd.read_excel('shipping_data_0.csv')
spreadsheet1 = pd.read_excel('shipping_data_1.csv')
spreadsheet2 = pd.read_excel('shipping_data_2.csv')

db_conn = sqlite3.connect('shipment_database.db')


cursor = db_conn.cursor()


spreadsheet0.to_sql('table_name', db_conn, if_exists='replace', index=False)


for index, row in spreadsheet1.iterrows():
    # Combine data based on shipping identifier
    # Determine quantity and other required data
    
    # Insert data into the database
    cursor.execute("INSERT INTO table_name (column1, column2, ...) VALUES (?, ?, ...)", (value1, value2, ...))
    db_conn.commit()


for index, row in spreadsheet2.iterrows():
    # Extract origin and destination
    
    # Update corresponding records in the database
    cursor.execute("UPDATE table_name SET origin=?, destination=? WHERE shipping_id=?", (origin, destination, shipping_id))
    db_conn.commit()


db_conn.close()


