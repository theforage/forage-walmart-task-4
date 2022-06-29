import csv, sqlite3

con = sqlite3.connect("shipment.db")
cur = con.cursor()

with open('data/shipping_data_0.csv', 'r')as csv_file:
    csv_reader = csv.reader(csv_file)

    for row in csv_reader:
        print(row)