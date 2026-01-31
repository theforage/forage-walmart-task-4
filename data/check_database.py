# check_database.py
import sqlite3

print("=" * 60)
print("CHECKING DATABASE STRUCTURE")
print("=" * 60)

try:
    # Connect to database
    conn = sqlite3.connect('shipment_database.db')
    cursor = conn.cursor()
    
    print("✅ Connected to database: shipment_database.db")
    
    # Check if database has tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    if tables:
        print(f"\n📋 Found {len(tables)} table(s):")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table[0]}")
    else:
        print("\n📋 Database is empty (no tables)")
        conn.close()
        exit()
    
    # Show each table's structure
    print("\n" + "=" * 60)
    print("TABLE SCHEMAS")
    print("=" * 60)
    
    for table in tables:
        table_name = table[0]
        print(f"\n🔍 Table: {table_name}")
        print("-" * 40)
        
        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        if columns:
            print(f"{'Column':<20} {'Type':<15} {'Nullable':<10}")
            print("-" * 50)
            for col in columns:
                col_id, col_name, col_type, notnull, default_val, pk = col
                nullable = "NO" if notnull else "YES"
                print(f"{col_name:<20} {col_type:<15} {nullable:<10}")
        else:
            print("No columns found")
        
        # Count rows
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"\nTotal rows: {count}")
        
        # Show first few rows if table has data
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            print("\nSample data (first 3 rows):")
            for row in rows:
                print(f"  {row}")
    
    conn.close()
    print("\n✅ Database check completed!")
    
except Exception as e:
    print(f"❌ Error: {e}")