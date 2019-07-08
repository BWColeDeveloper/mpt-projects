"""
Program summary: Determine which orders can be released to production based on component or material availability

Program Steps:
    1). Create SQLAlchemy database table objects
    2). Query database objects to retrieve order information
    3). Parse information to determine if each order is releasable, and -
    4). If not releasable, what the shortages are
    5). Output results to csv file

Dependencies (Google pip or pip3 if you don't understand what this is):
    1). Install SQLAlchemy by typing 'pip3 install sqlalchemy' in the console
    2.). Install pyodbc by typing 'pip3 install pyodbc' in the console
    3.) If you need pip install it by typing 'python -m --install pip3' in the console


Worth noting: The database objects are included in this file instead being imported from a separate package intentionally as to not create an unnecessary dependancy,
especially given the local dev environment and small file size.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, text, func, or_
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import pyodbc
import csv

"""
***************************
DATABASE OBJECTS: START 
--------------------------
"""
engine = create_engine('mssql+pyodbc://username:password') # engine for mssql using pyodbc (i.e., login credentials)
Base = declarative_base(engine)

class JobMaster(Base):
    """table for high level job details and order references"""
    __tablename__="job_mst"

    job = Column(String, primary_key=True) # job order number
    ord_type = Column(String) # type of order (e.g, customer or transfer order)
    ord_num = Column(String) # customer or transfer order number
    ord_line = Column(String) # customer or transfer order line
    item = Column(String) # item number
    description = Column(String) # item description
    qty_released = Column(Float) # job order quantity to fill
    qty_complete = Column(Float) # job order quantity complete
    stat = Column(String) # order status (e.g., Ordered, Filled, Complete)


class JobMaterials(Base):
    """table for job bill of materials"""
    __tablename__ = 'jobmatl_mst'

    job = Column(String, primary_key=True) # job order number
    oper_num = Column(String) # job operation number
    item = Column(String) # bill of material item number
    matl_qty = Column(Integer) # item quantity required
    description = Column(String) # item description


class Items(Base):
    """table for high level item details"""
    __tablename__ = 'item_mst'

    item = Column(String, primary_key=True) # item number
    description = Column(String) # item description
    qty_allocjob = Column(Float) # item quantity allocated
    lead_time= Column(Integer) # item standard lead time
    qty_used_ytd = Column(Float) # item quantity used year-to-date
    qty_mfg_ytd = Column(Float) # item quantity manufactured year-to-date
    p_m_t_code = Column(String) # item type (e.g., purchased, manufactured, or tooling)
    order_min = Column(Integer) # item order minimum (i.e., minimum to purchase or produce)
    order_mult = Column(Float) # item multiple (i.e., quantity to increase order by if min quantity isn't sufficient)
    comm_code = Column(String) # item commodity code


class ItemWhse(Base):
    """table for item warehouse details"""
    __tablename__ = 'itemwhse_mst'

    item = Column(String, primary_key=True) # item number
    qty_on_hand = Column(Float) # item quantity on hand
    qty_alloc_co = Column(Float) # item quantity allocated to customer orders
    alloc_trn = Column(Float) # item quantity allocated to transfer orders
    qty_ordered = Column(Float) # item quantity on order if purchased
    qty_reorder = Column(Float) # item safety stock value
    whse = Column(String) # item warehouse (i.e., manufacturing or distribution location)

"""
------------------------
DATABASE OBJECTS: END
*************************
"""
# required to load session (connection) to the database
def loadSession():
    """session manages the connection to the database"""
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()

    return session


class OrderReleaser():
    """"""
    def __init__(self):
        self.db_connection = loadSession()

    def queryReleases(self):
        """query MSSQL database for key job order data"""
        self.db_connection = loadSession()

        releasable_orders = self.db_connection.query(
                JobMaster.job,
                JobMaster.item,
                JobMaterials.item,
                JobMaterials.matl_qty * JobMaster.qty_released.label("required"),
                ItemWhse.qty_on_hand - Items.qty_allocjob.label("onhand"))\
            .join(JobMaterials, JobMaster.job == JobMaterials.job)\
            .join(ItemWhse, ItemWhse.item == JobMaterials.item)\
            .join(Items, Items.item == JobMaterials.item)\
            .filter(~JobMaster.job.contains("S"))\
            .filter(or_(JobMaster.stat == "F", JobMaster.stat == "S"))\
            .filter(ItemWhse.whse == "MCI")\
            .order_by(JobMaster.job)
        # Two actions:
        #   1. Identify jobs with shortages (required > on hand)
        #   2. Remove duplicate jobs
        self.short_orders = list(dict.fromkeys([x[0] for x in releasable_orders if x[3] > x[4]]))
        # convert query to nested list
        self.parsed_orders = list(dict.fromkeys([x[0] for x in releasable_orders if x[0] not in self.short_orders]))

        return self.parsed_orders

    def parseReleases(self, *queryReleases):
        """parse data passed in from the queryReleases method query"""
        #parsed_param = [x for x in queryReleases]
        print(queryReleases)
        with open(r'K:\Materials Team\Planning\RELEASABLE_ORDERS.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, 'excel')
            writer.writerow(['Job'])
            for line in queryReleases:
                writer.writerow(line)



if __name__ == "__main__":
    tester = OrderReleaser()
    tester.parseReleases(tester.queryReleases())

