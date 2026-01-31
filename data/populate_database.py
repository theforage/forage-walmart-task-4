"""
Walmart Shipping Data Processing Script
Author: [Your Name]
Date: [Current Date]
Task: Combine multiple spreadsheets into SQLite database for shipping metrics analysis
"""

import pandas as pd
import sqlite3
from typing import Dict, Set
import os


class ShippingDataProcessor:
    """Main processor class for handling shipping data integration"""
    
    def __init__(self, db_path: str = 'shipment_database.db'):
        """
        Initialize the processor with database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect_db(self) -> None:
        """Establish connection to SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"Database connection established: {self.db_path}")
    
    def close_db(self) -> None:
        """Close database connection with proper cleanup"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            print("Database connection closed")
    
    def get_existing_products(self) -> Dict[str, int]:
        """
        Retrieve all existing products from database
        
        Returns:
            Dictionary mapping product names to their IDs
        """
        self.cursor.execute("SELECT id, name FROM product")
        return {name: id for id, name in self.cursor.fetchall()}
    
    def insert_products(self, products: Set[str]) -> Dict[str, int]:
        """
        Insert new products into database
        
        Args:
            products: Set of unique product names to insert
            
        Returns:
            Updated dictionary of all products with their IDs
        """
        existing_products = self.get_existing_products()
        new_products = products - set(existing_products.keys())
        
        if new_products:
            print(f"Adding {len(new_products)} new products to product table")
            for product in sorted(new_products):
                self.cursor.execute(
                    "INSERT INTO product (name) VALUES (?)",
                    (product,)
                )
            self.conn.commit()
        
        return self.get_existing_products()
    
    def process_spreadsheet_0(self, file_path: str) -> None:
        """
        Process self-contained shipping data (spreadsheet 0)
        
        Args:
            file_path: Path to shipping_data_0.csv file
        """
        print(f"\nProcessing Spreadsheet 0: {file_path}")
        
        # Load data from CSV
        df = pd.read_csv(file_path)
        print(f"  Rows loaded: {len(df)}")
        
        # Extract unique products
        unique_products = set(df['product'].unique())
        product_map = self.insert_products(unique_products)
        
        # Prepare shipment data for insertion
        shipments_data = []
        for _, row in df.iterrows():
            product_id = product_map[row['product']]
            shipments_data.append((
                product_id,
                row['product_quantity'],
                row['origin_warehouse'],
                row['destination_store']
            ))
        
        # Bulk insert into shipment table
        self.cursor.executemany(
            """INSERT INTO shipment (product_id, quantity, origin, destination) 
               VALUES (?, ?, ?, ?)""",
            shipments_data
        )
        
        print(f"  Shipments inserted: {len(shipments_data)}")
    
    def process_spreadsheets_1_and_2(self, products_file: str, routes_file: str) -> None:
        """
        Process interdependent shipping data (spreadsheets 1 and 2)
        
        Args:
            products_file: Path to shipping_data_1.csv (product listings)
            routes_file: Path to shipping_data_2.csv (shipment routes)
        """
        print(f"\nProcessing Spreadsheets 1 & 2:")
        print(f"  Products file: {products_file}")
        print(f"  Routes file: {routes_file}")
        
        # Load both datasets
        df_products = pd.read_csv(products_file)
        df_routes = pd.read_csv(routes_file)
        
        print(f"  Products data rows: {len(df_products)}")
        print(f"  Routes data rows: {len(df_routes)}")
        
        # Step 1: Calculate product quantities per shipment
        print("  Calculating product quantities...")
        product_counts = df_products.groupby(
            ['shipment_identifier', 'product']
        ).size().reset_index(name='quantity')
        
        # Step 2: Merge with route information
        merged_data = pd.merge(
            product_counts,
            df_routes,
            on='shipment_identifier',
            how='inner'
        )
        
        print(f"  Consolidated shipment records: {len(merged_data)}")
        
        # Step 3: Manage product references
        unique_products = set(merged_data['product'].unique())
        product_map = self.insert_products(unique_products)
        
        # Step 4: Prepare and insert shipment data
        shipments_data = []
        for _, row in merged_data.iterrows():
            product_id = product_map[row['product']]
            shipments_data.append((
                product_id,
                row['quantity'],
                row['origin_warehouse'],
                row['destination_store']
            ))
        
        # Bulk insert
        self.cursor.executemany(
            """INSERT INTO shipment (product_id, quantity, origin, destination) 
               VALUES (?, ?, ?, ?)""",
            shipments_data
        )
        
        print(f"  Shipments inserted: {len(shipments_data)}")
    
    def verify_results(self) -> None:
        """Validate and display processing results"""
        print("\n" + "=" * 60)
        print("PROCESSING VERIFICATION")
        print("=" * 60)
        
        # Count records in each table
        self.cursor.execute("SELECT COUNT(*) FROM product")
        product_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM shipment")
        shipment_count = self.cursor.fetchone()[0]
        
        print(f"\nDatabase Statistics:")
        print(f"  Products in database: {product_count}")
        print(f"  Shipments in database: {shipment_count}")
        
        # Display sample data
        print("\nSample Products (first 5):")
        self.cursor.execute("SELECT * FROM product LIMIT 5")
        for row in self.cursor.fetchall():
            print(f"  {row[0]:3} | {row[1]:20}")
        
        print("\nSample Shipments (first 3):")
        self.cursor.execute("""
            SELECT s.id, p.name, s.quantity, s.origin, s.destination 
            FROM shipment s 
            JOIN product p ON s.product_id = p.id 
            LIMIT 3
        """)
        for row in self.cursor.fetchall():
            print(f"\n  Shipment ID: {row[0]}")
            print(f"    Product: {row[1]}")
            print(f"    Quantity: {row[2]}")
            print(f"    Origin: {row[3][:30]}...")
            print(f"    Destination: {row[4][:30]}...")
    
    def run(self) -> None:
        """Execute the complete data processing pipeline"""
        print("=" * 60)
        print("WALMART SHIPPING DATA PROCESSING PIPELINE")
        print("=" * 60)
        
        try:
            # Initialize database connection
            self.connect_db()
            
            # Clear existing data for fresh import
            print("\nInitializing database...")
            self.cursor.execute("DELETE FROM shipment")
            self.cursor.execute("DELETE FROM product")
            self.conn.commit()
            
            # Process all data sources
            self.process_spreadsheet_0('data/shipping_data_0.csv')
            self.process_spreadsheets_1_and_2(
                'data/shipping_data_1.csv',
                'data/shipping_data_2.csv'
            )
            
            # Validate results
            self.verify_results()
            
            print("\n" + "=" * 60)
            print("PROCESSING COMPLETED SUCCESSFULLY")
            print("=" * 60)
            
        except Exception as e:
            print(f"\nError during processing: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_db()


def main() -> None:
    """Main execution function"""
    processor = ShippingDataProcessor()
    processor.run()


if __name__ == "__main__":
    main()