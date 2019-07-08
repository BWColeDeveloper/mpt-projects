"""
Collection of scripts necessary for pulling data from SQLServer to complete various tasks,
some of which are daily recurring tasks.

Scripts include:
1. List of releasable orders
2. KANBAN list for production
3. List of late customer orders requiring reschedule
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, text, func, or_
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import pyodbc
from datetime import date, datetime, timedelta
import csv
import time

# set echo to 'False' in production
#engine = create_engine('sqlite:///test4.db', echo=False)
engine = create_engine('mssql+pyodbc://username:password') ### engine for mssql using pyodbc
Base = declarative_base(engine)

"""
******************
DATABASE TABLES
******************
"""
class COitem(Base):
    """create class map reference to coitem_mst table"""

    __tablename__ = 'coitem_mst'

    co_num = Column(String, primary_key=True)
    co_line = Column(String)
    item = Column(String)
    description = Column(String)
    qty_ordered = Column(Float)
    price = Column(Float)
    ref_num = Column(String)
    due_date = Column(DateTime)
    stat = Column(String)
    whse = Column(String)
    comm_code = Column(String)

class POitem(Base):
    """"""
    __tablename__='poitem_mst'

    po_num = Column(String, primary_key=True)
    po_line = Column(String)
    item = Column(String)
    description = Column(String)
    qty_ordered = Column(Float)
    qty_received = Column(Float)
    stat = Column(String)
    item_cost = Column(Float)
    po_vend_num = Column(String)
    due_date = Column(DateTime)
    promise_date = Column(DateTime)
    CreateDate = Column(DateTime)
    rcvd_date = Column(DateTime)

class Items(Base):
    """create class map reference to item_mst table"""
    __tablename__ = 'item_mst'

    item = Column(String, primary_key=True)
    description = Column(String)
    qty_allocjob = Column(Float)
    lead_time= Column(Integer)
    qty_used_ytd = Column(Float)
    qty_mfg_ytd = Column(Float)
    p_m_t_code = Column(String)
    order_min = Column(Integer)
    order_mult = Column(Float)
    comm_code = Column(String)


class Item_whse(Base):
    """create class map reference to itemwhse_mst table"""
    __tablename__ = 'itemwhse_mst'

    item = Column(String, primary_key=True)
    qty_on_hand = Column(Float)
    qty_alloc_co = Column(Float)
    alloc_trn = Column(Float)
    qty_ordered = Column(Float)
    qty_reorder = Column(Float)
    whse = Column(String)


class Workcenters(Base):
    """"""
    __tablename__ = 'wc_mst'

    wc = Column(String, primary_key=True)
    description = Column(String)


class Job_materials(Base):
    """"""
    __tablename__ = 'jobmatl_mst'

    job = Column(String, primary_key=True)
    oper_num = Column(String)
    item = Column(String)
    matl_qty = Column(Integer)
    description = Column(String)


class Job_routing(Base):
    """"""
    __tablename__ = 'jobroute_mst'

    id = Column(Integer, primary_key=True)
    job = Column(String)
    oper_num = Column(Integer)
    wc = Column(String)
    qty_scrapped = Column(Float)
    qty_received = Column(Float)
    qty_moved = Column(Float)


class Job_master(Base):
    """"""
    __tablename__="job_mst"

    job = Column(String, primary_key=True)
    ord_type = Column(String)
    ord_num = Column(String)
    ord_line = Column(String)
    item = Column(String)
    description = Column(String)
    qty_released = Column(Float)
    qty_complete = Column(Float)
    stat = Column(String)


class Item_stockroom(Base):
    """"""
    __tablename__='itemloc_mst'

    item = Column(String, primary_key=True)
    loc = Column(String)
    qty_on_hand = Column(Float)
    whse = Column(String)

class Commodity_codes(Base):
    """"""
    __tablename__='commodity_mst'

    comm_code = Column(String, primary_key=True)
    description = Column(String)

class APS(Base):
    """"""
    __tablename__='apsplandetail_mst'

    item = Column(String, primary_key=True)
    is_demand = Column(Integer)
    top_orderid = Column(String)
    topReference = Column(String)
    qty = Column(Float)
    parent_item = Column(String)
    due_date = Column(DateTime)

class Material_transactions(Base):
    """"""
    __tablename__='matltran_mst'

    trans_type = Column(String)
    trans_date = Column(DateTime)
    item = Column(String, primary_key=True)
    qty = Column(Float)
    whse = Column(String)
    loc = Column(String)
    ref_type = Column(String)
    cost = Column(Float)
    user_code = Column(String)

class Transfers(Base):
    """"""
    __tablename__='trnitem_mst'

    trn_num = Column(String, primary_key=True)
    trn_line = Column(String)
    stat = Column(String)
    item = Column(String)
    qty_req = Column(Float)
    qty_shipped = Column(Float)
    qty_received = Column(Float)
    frm_ref_type = Column(String)
    frm_ref_num = Column(String)
    from_whse = Column(String)
    to_whse = Column(String)
    rcpt_rqmt_q = Column(String)
    sch_ship_date = Column(DateTime)

class Picks(Base):
    """"""
    __tablename__='trp_item_mst'

    pack_num = Column(Integer, primary_key=True)
    trn_num = Column(String)
    trn_line = Column(String)
    qty_ordered = Column(Float)
    qty_packed = Column(Float)

class VendorContracts(Base):
    """"""
    __tablename__='itemvend_mst'

    item = Column(String, primary_key=True)
    vend_num = Column(String)
    rank = Column(Integer)

class Vendors(Base):
    """"""
    __tablename__='vendaddr_mst'

    vend_num = Column(String, primary_key=True)
    name = Column(String)

"""
**********************
END DATABASE TABLES
**********************
"""

def loadSession():
    """session manages the connection to the database"""
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def at_workcenter():
    """Identify the jobs/items at a particular workcenter and output to excel.
    Necessary for production managers to track material flow through the machine shop.

        KANBAN priority list (for future implementation):
        1. Customer order
        2. SSM replenishment
        3. Stock (MCT or MCI)
        ** Master > PTS
    """
    # create session for database access
    session = loadSession()

    # subquery for grouping APS planning demand by part number
    sub_jip = session.query(
        APS.item.label('item'),
        func.sum(APS.qty).label('total_demand'))\
    .group_by(APS.item)\
    .filter(APS.is_demand == 1)\
    .subquery()

    jobs_in_process = session.query(
            Job_routing.job,
            Job_master.item,
            Job_routing.oper_num,
            Job_routing.wc,
            Workcenters.description,
            Job_routing.qty_scrapped,
            Job_routing.qty_received,
            Job_routing.qty_moved,
            Item_whse.qty_on_hand,
            Item_whse.qty_reorder,
            sub_jip.c.total_demand)\
        .join(Workcenters, Job_routing.wc == Workcenters.wc)\
        .join(Job_master, Job_routing.job == Job_master.job)\
        .join(Item_whse, Item_whse.item == Job_master.item)\
        .join(Items, Items.item == Item_whse.item)\
        .outerjoin(sub_jip, sub_jip.c.item == Job_master.item)\
        .filter(Job_routing.qty_moved + Job_routing.qty_scrapped < Job_routing.qty_received)\
        .filter(Item_whse.whse == "MCI")\
        .filter(Job_master.stat != "C", Job_master.stat != "H")\
        .filter(~Workcenters.description.contains("Assembly"))\
        .filter(~Workcenters.description.contains("Bearing"))\
        .filter(~Workcenters.description.contains("Filling"))\
        .filter(~Workcenters.description.contains("MTD"))\
        .filter(~Workcenters.description.contains("MOTOR ADD"))\
        .filter(~Workcenters.description.contains("Kitting"))\
        .filter(~Workcenters.description.contains("Pick"))\
        .filter(~Workcenters.description.contains("Phantom"))\
        .filter(~Workcenters.description.contains("Nameplate"))\
        .order_by(Workcenters.description)

    # output query results to csv file
    with open(r'N:\Depts\Foremans\VB tools\KANBAN.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, "excel")
        writer.writerow(['job', 'item', 'op num', 'wc', 'description', 'qty scrapped', 'qty received', 'qty moved', 'on hand', 'safety stock', 'total required'])
        writer.writerows(jobs_in_process)

def releases():
    """file for expediting customer order and tranfer order releases"""
    session = loadSession()

    releasable_orders = session.query(
            Job_master.job,
            Job_master.stat,
            Job_master.item,
            Job_master.description,
            Job_master.qty_released,
            Job_master.ord_num,
            Job_master.ord_line,
            Job_materials.item,
            Job_materials.description,
            Items.p_m_t_code,
            Job_materials.matl_qty * Job_master.qty_released.label("required"),
            Item_whse.qty_on_hand - Items.qty_allocjob.label("onhand"),
            Commodity_codes.description)\
        .join(Job_materials, Job_master.job == Job_materials.job)\
        .join(Item_whse, Item_whse.item == Job_materials.item)\
        .join(Items, Items.item == Job_materials.item)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .filter(~Job_master.job.contains("S"))\
        .filter(or_(Job_master.stat == "F", Job_master.stat == "S"))\
        .filter(Item_whse.whse == "MCI")

        # output query results to csv file
    with open(r'K:\Materials Team\Planning\RELEASE_FILE.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Job', 'Status', 'Item', 'Description', 'Job Qty', 'Order', 'Order Line', 'BOM Item', 'BOM Item Desc.', 'Item Type', 'BOM Total Required', 'BOM Item OnHand', 'Comm Code'])
        writer.writerows(releasable_orders)

def item_locations():
    """checks item stockroom locations for quantities sitting in EPL, Packline, Partskitting"""
    session = loadSession()

    locations = session.query(Item_stockroom.item, Item_stockroom.loc, Item_stockroom.qty_on_hand)\
        .filter(or_(Item_stockroom.loc == "EPL", Item_stockroom.loc == "PARTSKITTING", Item_stockroom.loc == "PACKLINE"))\
        .filter(Item_stockroom.qty_on_hand != 0, Item_stockroom.whse == "MCI")

    with open("location_cleanup.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Location', 'Qty OnHand'])
        writer.writerows(locations)

def factory_order_cleanup():
    """pulls the factory order data to determine what is invalid"""
    session = loadSession()

    factory_orders = session.query(Job_master.job, Job_master.item, Job_master.stat, Item_whse.qty_reorder, Item_whse.qty_alloc_co + Items.qty_allocjob, Item_whse.qty_on_hand)\
        .join(Item_whse, Item_whse.item == Job_master.item)\
        .join(Items, Items.item == Item_whse.item)\
        .filter(Job_master.job.contains("W2"), Job_master.stat == "R")\
        .filter(Item_whse.qty_on_hand - (Item_whse.qty_alloc_co + Items.qty_allocjob) > Item_whse.qty_reorder)

    with open('W2s.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Job', 'Item', 'Status', 'Safety Stock', 'Allocations', 'On Hand'])
        writer.writerows(factory_orders)

def check_castings():
    """check castings for APS, XL, COMBOgear, and MAS parts to ensure we have castings on-hand or on-order"""
    session = loadSession()

    # MPT parts
    casting_query_MPT = session.query(
            Items.item,
            Items.description,
            Item_whse.qty_on_hand - Items.qty_allocjob,
            Item_whse.qty_reorder,
            Item_whse.qty_ordered,
            Items.comm_code,
            Commodity_codes.description)\
        .join(Item_whse, Items.item == Item_whse.item)\
        .join(Commodity_codes, Items.comm_code == Commodity_codes.comm_code)\
        .filter(or_(Items.comm_code == "022.012.002", Items.comm_code == "009.012.002", Items.comm_code == "004.012.002", Items.comm_code == "001.012.002"))\
        .filter(Items.p_m_t_code == "P")\
        .filter(Item_whse.qty_on_hand - Items.qty_allocjob <= 0)

    with open('casting_check_MPT.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Desc.', 'On Hand', 'Safety Stock', 'Qty Ordered', 'Comm Code', 'Comm Desc.'])
        writer.writerows(casting_query_MPT)

    # Regal parts
    casting_query_Regal = session.query(
            Items.item,
            Items.description,
            Item_whse.qty_on_hand - Items.qty_allocjob,
            Item_whse.qty_reorder,
            Item_whse.qty_ordered,
            Items.comm_code,
            Commodity_codes.description)\
        .join(Item_whse, Items.item == Item_whse.item)\
        .join(Commodity_codes, Items.comm_code == Commodity_codes.comm_code)\
        .filter(or_(Items.comm_code == "012.012.002", Items.comm_code == "013.012.002", Items.comm_code == "015.012.002", Items.comm_code == "017.012.002", Items.comm_code == "023.012.002"))\
        .filter(Items.p_m_t_code == "P")\
        .filter(Item_whse.qty_on_hand - Items.qty_allocjob <= 0)

    with open('casting_check_Regal.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Desc.', 'On Hand', 'Safety Stock', 'Qty Ordered', 'Comm Code', 'Comm Desc.'])
        writer.writerows(casting_query_Regal)

def customer_priority():
    """compile list of CO priorities"""
    session = loadSession()

    co_priority = session.query(
            COitem.co_num,
            COitem.item,
            COitem.description,
            COitem.stat,
            COitem.qty_ordered,
            COitem.price,
            COitem.due_date,
            )\
        .filter(COitem.whse == "MCI", COitem.description.contains("DURUS"), COitem.stat == "O")

    with open(r'C:\Users\bwcole\Desktop\co_priority.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['CO Num', 'Item', 'Desc', 'Status', 'Qty Ordered', 'Price', 'Due Date'])
        writer.writerows(co_priority)

def order_reschedule():
    """check orders to determine if rescheduling is necessary based on whether the job order is released within a certain window"""
    session = loadSession()

    order_query = session.query(
        COitem.co_num,
        COitem.co_line,
        COitem.item,
        COitem.ref_num,
        Job_master.stat,
        COitem.due_date)\
        .join(Job_master, Job_master.job == COitem.ref_num)\
        .filter(COitem.whse == "MCI", COitem.stat == "O", Job_master.stat == "F")\
        .filter(COitem.due_date <= date.today() + timedelta(days=7))

    transfer_query = session.query(
        Transfers.trn_num,
        Transfers.trn_line,
        Transfers.from_whse,
        Transfers.to_whse,
        Transfers.item,
        Transfers.frm_ref_num,
        Job_master.stat,
        Transfers.sch_ship_date)\
        .join(Job_master, Job_master.job == Transfers.frm_ref_num)\
        .filter(Transfers.stat == "O", Job_master.stat == "F")\
        .filter(Transfers.sch_ship_date <= date.today() + timedelta(days=7))

    with open(r'K:\Materials Team\Planning\customer_reschedules.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['CO', 'CO line', 'Item', 'Job', 'Job Status', 'CO Due Date'])
        writer.writerows(order_query)

    with open(r'K:\Materials Team\Planning\transfer_reschedules.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['TO', 'TO line', 'From Whse', 'To Whse', 'Item', 'Job', 'Job Status', 'Due Date'])
        writer.writerows(transfer_query)

def vendor_contracts():
    """pull vendor contract information and demand per part number for a given vendor"""
    session = loadSession()

    sub_jip = session.query(
        APS.item.label('item'),
        func.sum(APS.qty).label('total_demand'))\
    .group_by(APS.item)\
    .filter(APS.is_demand == 1)\
    .subquery()

    # currently pulling data for Interstate Castings
    vendor_contract_query = session.query(
        VendorContracts.item,
        Items.description,
        Item_whse.qty_on_hand,
        Items.qty_used_ytd,
        sub_jip.c.total_demand,
        VendorContracts.vend_num,
        Vendors.name,
        Items.comm_code,
        Commodity_codes.description)\
    .join(Items, Items.item == VendorContracts.item)\
    .join(Item_whse, Item_whse.item == VendorContracts.item)\
    .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
    .join(Vendors, Vendors.vend_num == VendorContracts.vend_num)\
    .outerjoin(sub_jip, sub_jip.c.item == VendorContracts.item)\
    .filter(Commodity_codes.description.contains("CASTING"), VendorContracts.rank == 1)\
    .order_by(Items.qty_used_ytd.desc())

    with open(r'C:\Users\bwcole\Desktop\casting_list.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'Qty On Hand', 'Qty Used YTD', 'Total Demand', 'Vend Number', 'Vendor', 'Comm Code', 'Comm Desc'])
        writer.writerows(vendor_contract_query)

def late_purchase_orders():
    """compile list of late PO's to expedite"""
    session = loadSession()

    late_po_query = session.query(
        POitem.po_num,
        POitem.po_line,
        POitem.item,
        POitem.description,
        POitem.qty_ordered,
        POitem.qty_received,
        POitem.item_cost * POitem.qty_ordered,
        POitem.due_date,
        POitem.promise_date,
        POitem.rcvd_date,
        POitem.stat,
        POitem.po_vend_num,
        Vendors.name)\
        .join(Vendors, Vendors.vend_num == POitem.po_vend_num)\
        .filter(POitem.due_date < datetime.today(), POitem.stat == "O")\
        .order_by(POitem.due_date)

    with open(r'K:\Materials Team\Purchasing\late_PO_list.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['PO Num', 'PO Line', 'Item', 'Description', 'Ordered', 'Received', 'PO Cost', 'Due Date', 'Promise Date', 'Last Received', 'Status', 'Vendor Num', 'Vendor'])
        writer.writerows(late_po_query)


if __name__ == "__main__":
    releases()
    at_workcenter()
    #build_schedule()
    item_locations()
    #check_castings()
    #customer_priority()
    order_reschedule()
    #vendor_contracts()
    late_purchase_orders()
    print("Last Update: {}".format(datetime.today()))

        # 1 hour = 3600 seconds
