import os
import csv
import sqlite3

def insert_product(cursor, product_name):
    """
    Inserts a product into the product table if it doesn't exist,
    and returns its id.
    """
    cursor.execute("INSERT OR IGNORE INTO product (name) VALUES (?)", (product_name,))
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    return cursor.fetchone()[0]

def populate_database():
    # Connect to the SQLite database
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, 'shipment_database.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # --- Part 1: Process shipping_data_0.csv ---
    # This spreadsheet is self-contained.
    csv0_path = os.path.join(base_dir, 'data/shipping_data_0.csv')
    with open(csv0_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_name = row['product']
            origin = row['origin_warehouse']
            destination = row['destination_store']
            quantity = int(row['product_quantity'])
            
            product_id = insert_product(cursor, product_name)
            cursor.execute("""
                INSERT INTO shipment (product_id, quantity, origin, destination)
                VALUES (?, ?, ?, ?)
            """, (product_id, quantity, origin, destination))

    # --- Part 2: Process shipping_data_1.csv and shipping_data_2.csv ---
    # shipping_data_2.csv contains the origin and destination for each shipment
    shipment_info = {}
    csv2_path = os.path.join(base_dir, 'data/shipping_data_2.csv')
    with open(csv2_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            shipment_info[row['shipment_identifier']] = {
                'origin': row['origin_warehouse'],
                'destination': row['destination_store']
            }

    # shipping_data_1.csv contains a single product per row.
    # We must combine each row based on its shipping identifier to determine quantity.
    shipment_items = {}
    csv1_path = os.path.join(base_dir, 'data/shipping_data_1.csv')
    with open(csv1_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ship_id = row['shipment_identifier']
            product_name = row['product']
            
            if ship_id not in shipment_items:
                shipment_items[ship_id] = {}
            
            if product_name not in shipment_items[ship_id]:
                shipment_items[ship_id][product_name] = 0
                
            shipment_items[ship_id][product_name] += 1

    # Insert combined data into the database
    for ship_id, items in shipment_items.items():
        origin = shipment_info[ship_id]['origin']
        destination = shipment_info[ship_id]['destination']
        
        for product_name, quantity in items.items():
            product_id = insert_product(cursor, product_name)
            cursor.execute("""
                INSERT INTO shipment (product_id, quantity, origin, destination)
                VALUES (?, ?, ?, ?)
            """, (product_id, quantity, origin, destination))

    # Commit changes and close connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    populate_database()
