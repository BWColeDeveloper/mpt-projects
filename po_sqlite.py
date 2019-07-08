import sqlite3
import csv


def database():
    # establish connection to database
    conn = sqlite3.connect('test4.db')

    # create cursor
    c = conn.cursor()

    # delete previous table
    c.execute("DROP TABLE IF EXISTS purchase_orders")

    # call cursor execute method to create new table
    c.execute("""CREATE TABLE IF NOT EXISTS purchase_orders
                        (id integer PRIMARY KEY AUTOINCREMENT,
                        po_number text,
                        item text,
                        item_description text, 
                        vendor text,
                        vendor_name text,
                        po_status text,
                        due_date text)""")


    path = r"C:\Users\bwcole\Documents\Programming\Source_Files\open_po_lines.csv"

    # pull out data extract from Mpower purchase order dump file and iterate so it can be added to sqlite3 database
    with open(path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for line in csvreader:
            # insert the data into the sqlite3 database
            c.execute("INSERT INTO purchase_orders(id, po_number, item, item_description, vendor, vendor_name, po_status, due_date) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?)", (line['PO'], line ['Item'], line['Item Description'], line['Vendor'], line['Vendor Name'], line['PO Status'], line['Due Date']))


    # save the changes and close the connection
    conn.commit()

    c.execute("""SELECT * FROM purchase_orders""")
    rows = c.fetchall()
    for row in rows:
        print(row)

    # close connection to the database
    conn.close()


if __name__ == '__main__':
    database()