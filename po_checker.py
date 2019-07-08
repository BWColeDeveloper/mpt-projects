"""reads a purchase order dump file and determines which orders are late"""

import csv
from datetime import datetime
import subprocess
import psutil
from Email_API.emails import Email # self-created email package
import logging

# basic log functionality to track PO history by number of late PO's
logging.basicConfig(filename=r'Log_Files\po_checker.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def pull_data():
    """pull data out of ERP database and into csv file"""
    pass
    
def rw_csv_file():
    """iterate over contents of csv file, parse data, and write to new file(s)"""

    path = r'C:\Users\bwcole\Documents\prod_po_data.csv'
    
    global late_count
    global due_count

    late_list = []
    due_list = []

    try:
        # open the po_data.csv file and dumps the data into reader object
        with open(path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for line in csv_reader:
                file_str_date = line['date_required']
                file_dt_date = datetime.strptime(file_str_date, '%m/%d/%y') # converts str date to datetime object
                current_str_date = datetime.today().strftime('%m/%d/%y') # converts current date (datetime object) to desired str format
                current_dt_date = datetime.strptime(current_str_date, '%m/%d/%y') # converts str date to datetime object

                date_calc = file_dt_date - current_dt_date
                # if PO due date has passed append to late_list list
                if date_calc.days <= -1:
                    late_row = [line['order_no'], line['resource_no'], line['date_required'], line['description'], line['name'], line['fax'], line['attention']]
                    late_list.append(late_row)
                # if PO due date is between 0 and 5 append to due_list
                elif date_calc.days >= 0 and date_calc.days <= 5:
                    due_row = [line['order_no'], line['resource_no'], line['date_required'], line['description'], line['name'], line['fax'], line['attention']]
                    due_list.append(due_row)
        
        late_count = len(late_list)
        due_count = len(due_list)

    except ValueError as error_msg:
        # if error - print the error message to console
        print(error_msg)


    try:
        # open or create the late_po.csv file and writes the reader contents into it
        with open(r'Output_Files\late_po.csv', 'w', newline='') as writefile:
            csv_writer = csv.writer(writefile, 'excel')
            csv_writer.writerow(['order_no', 'resource_no', 'date_required', 'description', 'vendor', 'contact_method', 'contact_name'])
            csv_writer.writerows(late_list)
        
        with open(r'Output_Files\due_po.csv', 'w', newline='') as writefile:
            csv_writer = csv.writer(writefile, 'excel')
            csv_writer.writerow(['order_no', 'resource_no', 'date_required', 'description', 'vendor', 'contact_method', 'contact_name'])
            csv_writer.writerows(due_list)

    except PermissionError as error_msg:
        # if the write file is open an error is returned
        print(error_msg)

    logging.info((late_count, due_count))


def trigger_email():
    """check running PIDS for OUTLOOK.EXE and send emails"""
    path1 = r'C:\Users\bwcole\Documents\Programming\Output_Files\late_po.csv'
    path2 = r'C:\Users\bwcole\Documents\Programming\Output_Files\due_po.csv'

    # intercompany email information
    inter_receiver = 'sjcalo@master-pt.com'
    inter_copy = 'bwcole@master-pt.com'
    inter_subject = 'Purchase Order Reports'
    inter_message = """
    Morning,

    There are {} late orders and {} due within the next 5 days.
    """.format(late_count, due_count)

    # vendor email information
    vend_receiver = ''
    vend_copy = ''
    vend_subject = 'ACTION REQUIRED: Late Orders to Master Power Transmission'
    vend_message = """
    Dear Vendor,

    You are receiving this communication because we show the following orders are late.

    {}

    Please provide an update on the status of this order and associated Shipment Tracking Number at your earliest convenience.


    We look forward to making 2018 an excellent year for ourselves and our business partners.

    Thank you for working with us!


    Sincerely,

    Master Power Tranmission
    """

    purchasing_email = Email(inter_receiver, inter_subject, inter_message, path1, path2, inter_copy)

    for item in psutil.pids():
        p = psutil.Process(item)
        if p.name() == "OUTLOOK.EXE":
            flag = 1
            break
        else:
            flag = 0

    if flag == 1:
        purchasing_email.send_outlook_email()
    else:
        open_outlook()
        purchasing_email.send_outlook_email()


def open_outlook():
    """open outlook email application"""
    try:
        subprocess.run([r'C:\Program Files(x86)\Microsoft Office\Office12\OUTLOOK.EXE'])
    except:
        print("Error: Unable to open Outlook application")


if __name__ == "__main__":
    pull_data()
    rw_csv_file()
    trigger_email()