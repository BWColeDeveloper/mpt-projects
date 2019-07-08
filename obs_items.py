"""reconcile the quantity and value of obsolete items and report out"""

import csv
import pyodbc


def read_csv_file():
    """open excel file and read in csv reader object"""

    path = 'K:\\Accounting\\pm_extracts\\history\\stk_inv_detail_2_2018.csv'

    try:
        with open(path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            global current_list
            global future_list
            current_list = []
            future_list = []
            for line in csv_reader:
                if line['obsolete'] == 'Y':
                    current_row = [line['resource_no'], line['description'], line['product_class_description'], line['commodity'],
                                line['qty_onhand'], line['matl_dlrs'], line['labr_dlrs'], line['tot_cos']]
                    current_list.append(current_row)
                elif line['product_class_description'] == 'Reeves MAS' and line['obsolete'] == 'N':
                    future_row = [line['resource_no'], line['description'], line['product_class_description'], line['commodity'],
                                line['qty_onhand'], line['matl_dlrs'], line['labr_dlrs'], line['tot_cos']]
                    future_list.append(future_row)

    except csv.Error as error_msg:
        print('Master_Obs_Reader: ' + error_msg)


def write_csv_file():
    """write parsed data contents into csv file"""
    try:
        with open('Output_Files\\current_obs_item_list.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, 'excel')
            csv_writer.writerow(['resource_no', 'description', 'product_class_description', 'commodity', 'qty_onhand', 'matl_dirs',
                                    'labr_dlrs', 'tot_cos'])
            csv_writer.writerows(current_list)

    except csv.Error as error_msg:
        print('Current_Obs_Writer: ' + error_msg)
"""
    try:
        with open('Output_Files\\future_obs_item_list.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, 'excel')
            csv_writer.writerow(['resource_no', 'description', 'product_class_description', 'commodity', 'qty_onhand', 'matl_dirs',
                                    'labr_dlrs', 'tot_cos'])
            csv_writer.writerows(future_list)

    except csv.Error as error_msg:
        print('Future_Obs_Writer: ' + error_msg)
"""

if __name__ == "__main__":
    read_csv_file()
    write_csv_file()