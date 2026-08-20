import csv
import sqlite3

def process_data():
    conn = sqlite3.connect('shipment_database.db')
    cursor = conn.cursor()

    # Drop old table to reset the schema correctly
    cursor.execute('DROP TABLE IF EXISTS shipment')

    # Create table with all required columns
    cursor.execute('''
        CREATE TABLE shipment (
            shipment_identifier TEXT,
            origin_warehouse TEXT,
            destination_store TEXT,
            driver_identifier TEXT,
            product TEXT,
            product_quantity INTEGER,
            on_time BOOLEAN
        )
    ''')

    # 1. Process shipping_data_0.csv (Flat dataset)
    with open('data/shipping_data_0.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute('''
                INSERT INTO shipment (
                    origin_warehouse, destination_store, product, 
                    on_time, product_quantity, driver_identifier
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                row['origin_warehouse'],
                row['destination_store'],
                row['product'],
                1 if row['on_time'].lower() == 'true' else 0,
                int(row['product_quantity']),
                row['driver_identifier']
            ))

    # 2. Process shipping_data_2.csv (Shipment Metadata)
    shipments_meta = {}
    with open('data/shipping_data_2.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            shipments_meta[row['shipment_identifier']] = {
                'origin_warehouse': row['origin_warehouse'],
                'destination_store': row['destination_store'],
                'driver_identifier': row['driver_identifier']
            }

    # 3. Process shipping_data_1.csv (Line Items)
    shipment_products = {}
    with open('data/shipping_data_1.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            s_id = row['shipment_identifier']
            prod = row['product']
            on_time = 1 if row['on_time'].lower() == 'true' else 0

            key = (s_id, prod, on_time)
            shipment_products[key] = shipment_products.get(key, 0) + 1

    # Insert combined relational records
    for (s_id, prod, on_time), quantity in shipment_products.items():
        meta = shipments_meta.get(s_id, {})
        cursor.execute('''
            INSERT INTO shipment (
                shipment_identifier, origin_warehouse, destination_store,
                driver_identifier, product, product_quantity, on_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            s_id,
            meta.get('origin_warehouse'),
            meta.get('destination_store'),
            meta.get('driver_identifier'),
            prod,
            quantity,
            on_time
        ))

    conn.commit()
    conn.close()
    print("Database populated successfully.")

if __name__ == '__main__':
    process_data()