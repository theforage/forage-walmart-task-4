import csv
import sqlite3
import sys

DB_PATH       = "shipment_database.db"
SPREADSHEET_0 = "data/shipping_data_0.csv"
SPREADSHEET_1 = "data/shipping_data_1.csv"
SPREADSHEET_2 = "data/shipping_data_2.csv"

EXPECTED_PRODUCTS  = 45
EXPECTED_SHIPMENTS = 154


def get_or_create_product(cursor, product_name):
    """Return the product id for product_name, inserting it if missing."""
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    row = cursor.fetchone()
    if row is not None:
        return row[0]
    cursor.execute("INSERT INTO product (name) VALUES (?)", (product_name,))
    return cursor.lastrowid


def populate_spreadsheet_0(cursor):
    """Spreadsheet 0 is self-contained: each row is one shipment line."""
    with open(SPREADSHEET_0, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_name = row["product"].strip()
            quantity = int(row["product_quantity"])
            origin = row["origin_warehouse"].strip()
            destination = row["destination_store"].strip()

            product_id = get_or_create_product(cursor, product_name)

            cursor.execute(
                """
                INSERT INTO shipment (product_id, quantity, origin, destination)
                VALUES (?, ?, ?, ?)
                """,
                (product_id, quantity, origin, destination),
            )


def build_shipment_lookup():
    """Map shipment_identifier -> {origin, destination} using Spreadsheet 2."""
    lookup = {}
    with open(SPREADSHEET_2, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lookup[row["shipment_identifier"].strip()] = {
                "origin": row["origin_warehouse"].strip(),
                "destination": row["destination_store"].strip(),
            }
    return lookup


def aggregate_spreadsheet_1():
    """Aggregate Spreadsheet 1 by (shipment_identifier, product).

    Each row represents one unit of a product in that shipment,
    so quantity is the number of occurrences of the key.
    """
    aggregate = {}
    with open(SPREADSHEET_1, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row["shipment_identifier"].strip(),
                row["product"].strip(),
            )
            aggregate[key] = aggregate.get(key, 0) + 1
    return aggregate


def populate_spreadsheet_1(cursor):
    """Insert one shipment row per (shipment_identifier, product) pair."""
    shipment_lookup = build_shipment_lookup()
    aggregate       = aggregate_spreadsheet_1()

    for (shipment_id, product_name), qty in aggregate.items():
        info = shipment_lookup.get(shipment_id)
        if info is None:
            raise ValueError(
                f"Spreadsheet 1 references unknown shipment: {shipment_id!r}"
            )

        product_id = get_or_create_product(cursor, product_name)

        cursor.execute(
            """
            INSERT INTO shipment (product_id, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
            """,
            (product_id, qty, info["origin"], info["destination"]),
        )


def validate(cursor):
    """Ensure the DB matches the expected totals before committing."""
    cursor.execute("SELECT COUNT(*) FROM product")
    product_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM shipment")
    shipment_count = cursor.fetchone()[0]

    if product_count != EXPECTED_PRODUCTS:
        raise ValueError(
            f"Product count mismatch: expected {EXPECTED_PRODUCTS}, "
            f"got {product_count}"
        )
    if shipment_count != EXPECTED_SHIPMENTS:
        raise ValueError(
            f"Shipment count mismatch: expected {EXPECTED_SHIPMENTS}, "
            f"got {shipment_count}"
        )


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()

        conn.execute("BEGIN")

        # --- Full reload: child first, then parent ---
        cursor.execute("DELETE FROM shipment")
        cursor.execute("DELETE FROM product")

        # --- Load all spreadsheets ---
        populate_spreadsheet_0(cursor)
        populate_spreadsheet_1(cursor)

        # --- Validate before committing ---
        validate(cursor)

        conn.commit()
        print("Database populated and validated successfully.")

    except Exception as exc:
        conn.rollback()
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()