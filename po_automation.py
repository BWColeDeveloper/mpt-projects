"""
------------------------
Summary of Purpose
------------------------
Script to automate some of the processes involved in requesting information from suppliers and updating PO dates.
Keeping Purchase Order dates updated has proven to be an extremely difficult task in our current environment.
------------------------
Sections
------------------------
1. Purchase Order evaluation -- parse PO data and Vendor Contracts data to determine which suppliers to communicate with
and what PO information to send (i.e. PO numbers, due dates, item numbers, etc.)
2. Supplier communication -- send/receive emails from suppliers and automatically pull key information from their responses
3. Document supplier response data -- document relevant data received from the supplier email and document in csv format
so it can be taken and entered into Mpower
"""
# import modules used in this script
from Email_API.emails import Email
import psutil
import subprocess
import csv
import codecs


class PO_parser():
    """evaluate PO data and determine which suppliers we need to contact"""
    def __init__(self, late_pos, vendor_contracts, vendor_contracts_utf8):
        self.purchase_orders = late_pos
        self.vendor_contracts = vendor_contracts
        self.vc_converted = vendor_contracts_utf8

    @property
    def parser(self):
        """return the results of the data comparison"""
        return 


    @parser.setter
    def parser(self, value1, value2):
        """setter"""
        self.purchase_orders = value1
        self.vendor_contracts = value2

    def parse_data(self):
        """read the data into csv format to compare PO and vendor contract data"""
        po_list = []
        # open the purchase orders file and read into csv object
        with codecs.open(self.purchase_orders, 'r', encoding='unicode') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for line in csv_reader:
                new_row = [line['PO'], line['Line'], line['Vendor'], line['Vendor Name'], line['PO Order Date'], line['Item'], line['Ordered'], line['Due Date']]
                po_list.append(new_row)
        

        vc_list = []
        with codecs.open(self.vendor_contracts, 'r', encoding='unicode') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for line in csv_reader:
                new_row = [line['Name'], line['Item']]
                vc_list.append(new_row)
            print(vc_list)
        
        
        # read in vendor contract file after encoding
        with open(self.vc_converted, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for i, e in csv_reader:
                print((i,e))

def main():
    path1 = r"C:\Users\bwcole\Documents\Programming\Source_Files\late_pos\late_purchase_orders.csv"
    path2 = r"C:\Users\bwcole\Documents\Programming\Source_Files\vendor_contracts.csv"
    path3 = r"C:\Users\bwcole\Documents\Programming\Source_Files\vendor_contracts_utf8.csv"
    po1 = PO_parser(path1, path2, path3)
    po1.parse_data()

if __name__ == "__main__":
    main()
