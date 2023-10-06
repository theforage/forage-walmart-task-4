import csv
import sqlite3

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipping_data_0 (
            origin_warehouse TEXT,
            destination_store TEXT,
            product TEXT,
            on_time TEXT,
            product_quantity INTEGER,
            driver_identifier TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipping_data_1 (
            shipment_identifier TEXT,
            product TEXT,
            on_time TEXT,
            origin_warehouse TEXT,
            destination_store TEXT
        )
    """)

def insert_shipping_data_0(cursor):
    with open('data/shipping_data_0.csv', 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier = row
            cursor.execute("INSERT INTO shipping_data_0 (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier) VALUES (?, ?, ?, ?, ?, ?)",
                           (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier))


# combining each row based on its shipping identifier, finding the quantity of goods in the shipment, and adding a new row to the database for each product in the shipment.

def insert_shipping_data_2(cursor):
    # Read and process shipping data from 'shipping_data_2.csv'
    with open('data/shipping_data_2.csv', 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row
        shipping_data_2_rows = [row for row in csv_reader]

    # Read and process shipping data from 'shipping_data_1.csv'
    with open('data/shipping_data_1.csv', 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row

        # Loop through rows in 'shipping_data_1.csv'
        for row in csv_reader:
            shipment_identifier, product, on_time = row

            # Find matching rows in 'shipping_data_2_rows' based on 'shipment_identifier'
            matching_rows = [r for r in shipping_data_2_rows if r[0] == shipment_identifier]

            # If a matching row is found in 'shipping_data_2_rows'
            if matching_rows:
                origin_warehouse, destination_store, driver_identifier = matching_rows[0][1], matching_rows[0][2], matching_rows[0][3]
                
                # Execute an SQL INSERT statement to insert data into the 'shipping_data_1' table
                cursor.execute("INSERT INTO shipping_data_1 (shipment_identifier, product, on_time, origin_warehouse, destination_store) VALUES (?, ?, ?, ?, ?)",
                               (shipment_identifier, product, on_time, origin_warehouse, destination_store))

if __name__ == "__main__":
    # Establishing connection to sqlite if the script is being run as the main program
    conn = sqlite3.connect('shipment_database.db')
    cursor = conn.cursor()

    create_tables(cursor)  # Creating required table

    insert_shipping_data_0(cursor)
    insert_shipping_data_2(cursor)

    # Commit changes and close the database connection
    conn.commit()
    conn.close()