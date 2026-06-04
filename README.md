# Walmart Shipping Data Processor (Task 4)

This repository contains the solution for Walmart's data engineering task (Task 4) to combine disparate shipping datasets into a single, queryable SQLite database.

## Project Overview

The shipping data originated in several different formats and was impossible to query efficiently. This project extracts, cleans, and merges data from three different CSV spreadsheets and populates a normalized SQLite database schema.

### Spreadsheets Processed:
1. **`shipping_data_0.csv`**: Contains self-contained shipping data.
2. **`shipping_data_1.csv`**: Contains individual products mapped to a `shipment_identifier`.
3. **`shipping_data_2.csv`**: Contains routing information (origin and destination) for each `shipment_identifier`.

## Features
- **Data Merging**: Programmatically groups products by shipment ID and calculates aggregate quantities.
- **Relational Integrity**: Automatically builds out foreign-key relationships to the `product` table.
- **Robust Execution**: Utilizes absolute pathing via `os.path` to ensure the script executes flawlessly regardless of the current working directory.

## How to Run

Ensure you have Python 3 installed. No external packages are required as the script uses the standard library.

```bash
# Execute the data ingestion script
python3 populate_database.py
```

After running the script, the `shipment_database.db` SQLite database will be successfully populated with all the parsed shipment metrics. You can verify it by running:

```bash
sqlite3 shipment_database.db "SELECT count(*) FROM shipment;"
```
