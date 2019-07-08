"""
Collection of scripts necessary for pulling data from SQLServer to complete various tasks,
some of which are daily recurring tasks.

Scripts include:
1. List of late Purchase Orders
2. List of customer orders and due dates
3. Usage by item to determine damend forecast
4. List of negative inventory locations
5. Many other scripts used for 'one off' tasks
"""


from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, text, func, or_
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import pyodbc
from datetime import date, datetime, timedelta
import csv
from math import sqrt

# set echo to 'False' in production
#engine = create_engine('sqlite:///test4.db', echo=True)
engine = create_engine('mssql+pyodbc://username:password') ### engine for mssql using pyodbc
Base = declarative_base(engine)

"""
******************
DATABASE TABLES
******************
"""
class Cust(Base):
    """create class map reference to co_mst table"""
    __tablename__ = 'co_mst'

    co_num = Column(String, primary_key=True)
    cust_po = Column(String)


class COitem(Base):
    """create class map reference to coitem_mst table"""

    __tablename__ = 'coitem_mst'

    co_num = Column(String, primary_key=True)
    co_line = Column(String)
    item = Column(String)
    description = Column(String)
    qty_ordered = Column(Float)
    qty_shipped = Column(Float)
    qty_invoiced = Column(Float)
    price = Column(Float)
    ref_num = Column(String)
    due_date = Column(DateTime)
    stat = Column(String)
    whse = Column(String)
    co_cust_num = Column(String)
    comm_code = Column(String)
    Uf_First_Acknowledged_Date = Column(DateTime)


class POitem(Base):
    """create class map reference to poitem_mst table"""
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
    CreateDate = Column(DateTime)


class Items(Base):
    """create class map reference to item_mst table"""
    __tablename__ = 'item_mst'

    item = Column(String, primary_key=True)
    description = Column(String)
    qty_allocjob = Column(Float)
    lead_time= Column(Integer)
    qty_mfg_ytd = Column(Float)
    qty_used_ytd = Column(Float)
    p_m_t_code = Column(String)
    order_min = Column(Integer)
    order_mult = Column(Float)
    matl_cost = Column(Float)
    lbr_cost = Column(Float)
    unit_cost = Column(Float)
    product_code = Column(String)
    stat = Column(String)
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
    """create class map reference to wc_mst table"""
    __tablename__ = 'wc_mst'

    wc = Column(String, primary_key=True)
    description = Column(String)


class Job_materials(Base):
    """create class map reference to jobmatl_mst table"""
    __tablename__ = 'jobmatl_mst'

    job = Column(String, primary_key=True)
    oper_num = Column(String)
    item = Column(String)
    matl_qty = Column(Float)
    description = Column(String)


class Job_routing(Base):
    """create class map reference to jobroute_mst table"""
    __tablename__ = 'jobroute_mst'

    id = Column(Integer, primary_key=True)
    job = Column(String)
    oper_num = Column(Integer)
    wc = Column(String)
    qty_scrapped = Column(Float)
    qty_received = Column(Float)
    qty_moved = Column(Float)
    qty_complete = Column(Float)


class Job_master(Base):
    """create class map reference to job_mst table"""
    __tablename__="job_mst"

    job = Column(String, primary_key=True)
    lst_trx_date = Column(DateTime)
    ord_type = Column(String)
    ord_num = Column(String)
    ord_line = Column(String)
    item = Column(String)
    qty_released = Column(Float)
    qty_complete = Column(Float)
    stat = Column(String)


class Item_stockroom(Base):
    """create class map reference to itemloc_mst table"""
    __tablename__='itemloc_mst'

    item = Column(String, primary_key=True)
    loc = Column(String)
    qty_on_hand = Column(Float)
    whse = Column(String)

class Commodity_codes(Base):
    """create class map reference to commodity_mst table"""
    __tablename__='commodity_mst'

    comm_code = Column(String, primary_key=True)
    description = Column(String)

class APS(Base):
    """create class map reference to apsplan_mst table"""
    __tablename__='apsplan_mst'

    item = Column(String, primary_key=True)
    is_demand = Column(Integer)
    top_orderid = Column(String)
    qty = Column(Float)
    parent_item = Column(String)
    due_date = Column(DateTime)

class Material_transactions(Base):
    """create class map reference to matltran_mst table"""
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
    """create class map reference to trnitem_mst table"""
    __tablename__='trnitem_mst'

    trn_num = Column(String, primary_key=True)
    stat = Column(String)
    item = Column(String)
    qty_req = Column(Float)
    qty_shipped = Column(Float)
    qty_received = Column(Float)
    frm_ref_type = Column(String)
    frm_ref_num = Column(String)
    from_whse = Column(String)
    to_whse = Column(String)

class Picks(Base):
    """create class map reference to trp_item_mst table"""
    __tablename__='trp_item_mst'

    pack_num = Column(Integer, primary_key=True)
    trn_num = Column(String)
    trn_line = Column(String)
    qty_ordered = Column(Float)
    qty_packed = Column(Float)

class Vendors(Base):
    """create class map reference to vendaddr_mst table"""
    __tablename__='vendaddr_mst'

    vend_num = Column(String, primary_key=True)
    name = Column(String)

class Shipment_master(Base):
    """create class map reference to shipment_mst table"""
    __tablename__='shipment_mst'

    shipment_id = Column(String, primary_key=True)
    status = Column(String)
    whse = Column(String)
    ship_date = Column(DateTime)
    value = Column(Float)

class Shipment_lines(Base):
    """create class map reference to shipment_line_mst table"""
    __tablename__='shipment_line_mst'

    shipment_id = Column(String, primary_key=True)
    shipment_line = Column(String)
    ref_num = Column(String)
    ref_line_suf = Column(String)

class ItemVend(Base):
    """create class map reference to itemvend_mst table"""
    __tablename__='itemvend_mst'

    item = Column(String, primary_key=True)
    vend_num = Column(String)
    vend_item = Column(String)
    NewRank = Column(Integer)
    RecordDate = Column(DateTime)
    UpdatedBy = Column(String)

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

def puller():
    """extract list of late Purchase Orders"""
    session = loadSession()
    po_query = session.query(
        POitem.po_num,
        POitem.po_line,
        POitem.item,
        POitem.stat,
        POitem.po_vend_num,
        Vendors.name,
        POitem.due_date,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Vendors, Vendors.vend_num == POitem.po_vend_num)\
        .join(Items, Items.item == POitem.item)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .filter(POitem.due_date < date.today(), POitem.stat == "O")

    with open('POitems.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['PO Num', 'PO Line', 'Item', 'Status', 'Vendor Num', 'Vendor Name', 'Due Date', 'Commodity Code', 'Comm. Desc.'])
        writer.writerows(po_query)

def lead_time_pull():
    """extract list of items with a commodity code equal to undefined and compare lead times"""
    session = loadSession()

    lt_query = session.query(
        Items.item,
        Items.description,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .filter(Items.comm_code == "000.000.000")

    with open(r'C:\Users\bwcole\Desktop\lt_query.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Item', 'Desc', 'Comm Code', 'Comm Desc'])
        writer.writerows(lt_query)

def purchased():
    """extract list of purchased items to compare planning data"""
    session = loadSession()

    purchased_query = session.query(
        Items.item,
        Item_whse.qty_on_hand,
        Item_whse.qty_reorder,
        Items.order_min,
        Items.order_mult,
        Items.p_m_t_code,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Item_whse, Item_whse.item == Items.item)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)

    with open(r'C:\Users\bwcole\Desktop\item_pull.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Qty Onhand', 'Safety Stock', 'Order Min.', 'Order Mult.', 'Item Type', 'Commodity Code', 'Comm Desc.'])
        writer.writerows(purchased_query)

def mparts():
    """extract list of all manufactured parts"""
    session = loadSession()

    mquery = session.query(
        Items.item,
        Items.description,
        Item_whse.qty_reorder,
        Items.qty_used_ytd,
        Items.lead_time,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .join(Item_whse, Item_whse.item == Items.item)\
        .filter(Items.p_m_t_code == "M")

    with open(r'C:\Users\bwcole\Desktop\manufactured_items.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'Safety Stock', 'Qty Used YTD', 'Lead Time', 'Comm Code','Comm Code Desc'])
        writer.writerows(mquery)

def ytd_shipments():
    """extract YTD shipment information by day and compile information by item and commodity code"""

    session = loadSession()

    ship_query = session.query(
        Shipment_master.shipment_id,
        Shipment_master.status,
        Shipment_master.whse,
        Shipment_master.value,
        Shipment_master.ship_date,
        Shipment_lines.ref_num,
        Shipment_lines.ref_line_suf,
        COitem.item,
        COitem.comm_code,
        Commodity_codes.description)\
        .join(Shipment_lines, Shipment_master.shipment_id == Shipment_lines.shipment_id)\
        .join(COitem, COitem.co_num + Shipment_lines.ref_line_suf == Shipment_lines.ref_num + Shipment_lines.ref_line_suf)\
        .join(Commodity_codes, Commodity_codes.comm_code == COitem.comm_code)\
        .order_by(Shipment_master.ship_date).filter(Shipment_master.status == "S")

    with open(r'C:\Users\bwcole\Desktop\ytd_ships.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Ship ID', 'Status', 'Whse', 'Value', 'Ship Date', 'CO Num', 'CO Line', 'Item', 'Comm Code', 'Comm Desc.'])
        writer.writerows(ship_query)


def spend_categories():
    """pull category data to determine spend"""
    session = loadSession()

    spend_query = session.query(
        POitem.po_num,
        POitem.po_line,
        POitem.item,
        POitem.qty_ordered,
        POitem.item_cost,
        POitem.qty_ordered * POitem.item_cost,
        POitem.due_date,
        POitem.stat,
        POitem.po_vend_num,
        Vendors.name,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Vendors, Vendors.vend_num == POitem.po_vend_num)\
        .join(Items, Items.item == POitem.item)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .filter(or_(POitem.stat == "F", POitem.stat == "C"))

    with open(r'C:\Users\bwcole\Desktop\spend_categories.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['PO Num', 'PO Line', 'Item', 'Qty Ordered', 'Item Cost', 'Total PO Cost', 'Due Date', 'Status', 'PO Vendor Num', 'Vendor Name'])
        writer.writerows(spend_query)

def po_validation():
    """check planned PO quantities and price"""
    session = loadSession()

    po_validation_query = session.query(
        POitem.po_num,
        POitem.po_line,
        POitem.stat,
        POitem.item,
        POitem.qty_ordered,
         Items.unit_cost, # standard cost
        POitem.item_cost, # current cost
        POitem.qty_ordered * Items.unit_cost, # total standard cost
        POitem.qty_ordered * POitem.item_cost,
        (POitem.qty_ordered * POitem.item_cost) - (POitem.qty_ordered * Items.unit_cost))\
        .join(Items, Items.item == POitem.item)\
        .filter(POitem.stat == "P")

    with open(r'K:\Materials Team\Purchasing\po_validation.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['PO Num', 'PO Line', 'Status', 'Item', 'Qty Ordered', 'Std Cost', 'Current Cost', 'Total Std Cost', 'Total Current Cost', 'Variance'])
        writer.writerows(po_validation_query)

def customer_order_list():
    """pull list of current customer orders and due dates"""
    session = loadSession()

    sub_coitem = session.query(
        COitem.item.label('item'),
        func.sum(COitem.qty_ordered - COitem.qty_shipped).label('total_demand'))\
    .filter(COitem.stat == "O")\
    .group_by(COitem.item)\
    .subquery()

    co_query = session.query(
        COitem.co_num,
        COitem.item,
        COitem.description,
        COitem.qty_ordered,
        COitem.qty_shipped,
        COitem.qty_invoiced,
        COitem.qty_ordered - COitem.qty_shipped,
        COitem.price,
        COitem.price * (COitem.qty_ordered - COitem.qty_shipped),
        COitem.price * COitem.qty_ordered,
        COitem.due_date,
        COitem.ref_num,
        Job_master.stat,
        sub_coitem.c.total_demand,
        COitem.comm_code,
        Commodity_codes.description)\
    .join(Commodity_codes, Commodity_codes.comm_code == COitem.comm_code)\
    .join(sub_coitem, sub_coitem.c.item == COitem.item)\
    .outerjoin(Job_master, Job_master.job == COitem.ref_num)\
    .filter(COitem.whse == "MCI", COitem.stat == "O").order_by(COitem.due_date)
    
    with open(r'K:\Materials Team\Planning\co_by_date.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['CO Num', 'Item', 'Description', 'Qty Ordered', 'Qty Shipped', 'Qty Invoiced', 'Qty Open', 'PPU', 'Price Opportunity', 'Total Price', 'Due Date', 'Job', 'Job Status','Total Demand', 'Comm Code', 'Comm Desc.'])
        writer.writerows(co_query)


def po_checker():
    """pull open purchase orders and validate order quantities"""
    session = loadSession()

    sub_jip = session.query(
        APS.item.label('item'),
        func.sum(APS.qty).label('total_demand'))\
    .group_by(APS.item)\
    .filter(APS.is_demand == 1)\
    .subquery()

    checker_query = session.query(
        POitem.po_num,
        POitem.po_line,
        POitem.CreateDate,
        POitem.due_date,
        POitem.stat,
        POitem.item,
        POitem.description,
        POitem.po_vend_num,
        Vendors.name,
        Items.order_min,
        POitem.qty_ordered,
        POitem.qty_received,
        Item_whse.qty_on_hand,
        Item_whse.qty_reorder,
        POitem.item_cost,
        (POitem.qty_ordered - POitem.qty_received) * POitem.item_cost,
        POitem.item_cost * POitem.qty_ordered,
        sub_jip.c.total_demand)\
        .outerjoin(sub_jip, sub_jip.c.item == POitem.item)\
        .join(Item_whse, Item_whse.item == POitem.item)\
        .join(Items, Items.item == POitem.item)\
        .join(Vendors, Vendors.vend_num == POitem.po_vend_num)\
        .filter(POitem.stat == "O")

    with open(r'K:\Materials Team\Purchasing\po_checker.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['PO Num', 'PO Line', 'Created', 'Due Date', 'Status', 'Item', 'Description', 'Vendor Num', 'Vendor Name', 'Order Min', 'Qty Ordered', 'Qty Received', 'Qty Onhand', 'Safety Stock', 'Item Cost', 'Open Cost', 'Total Cost', 'Total Demand'])
        writer.writerows(checker_query)

def job_checker():
    """check jobs against demand to validate requirements"""
    session = loadSession()

    sub_jip = session.query(
        APS.item.label('item'),
        func.sum(APS.qty).label('total_demand'))\
    .group_by(APS.item)\
    .filter(APS.is_demand == 1)\
    .subquery()

    # group total qty ordered per item by pulling PO's in 'Ordered' status
    po_sub = session.query(
        POitem.item.label('po_item'),
        func.sum(POitem.qty_ordered).label('qty_ordered'))\
    .group_by(POitem.item)\
    .filter(POitem.stat == "O")\
    .subquery()

    job_query = session.query(
        Job_master.item,
        Job_materials.item,
        Job_materials.matl_qty,
        Item_whse.qty_reorder,
        Item_whse.qty_on_hand - (Job_materials.matl_qty * 8),
        po_sub.c.qty_ordered)\
        .join(Job_materials, Job_materials.job == Job_master.job)\
        .outerjoin(po_sub, po_sub.c.po_item == Job_materials.item)\
        .join(Item_whse, Item_whse.item == Job_materials.item)\
        .filter(Job_master.item == "PL0008", Item_whse.whse == "MCI", Job_master.job.contains("V4-0004057"))

    with open(r'C:\Users\bwcole\Desktop\job_cleanup.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Parent Item', 'BOM Item', 'Qty Required', 'Safety Stock', 'On Hand', 'Qty Ordered'])
        writer.writerows(job_query)

def bom_check():
    """compile item BOM and compare requirement vs onhand quantity"""
    session = loadSession()

    po_sub = session.query(
        POitem.item.label('po_item'),
        func.sum(POitem.qty_ordered).label('qty_ordered'))\
    .group_by(POitem.item)\
    .filter(POitem.stat == "O")\
    .subquery()

    bom_check_query = session.query(
        Job_master.item,
        Job_materials.item,
        Items.description,
        Job_materials.matl_qty,
        Item_whse.qty_on_hand,
        po_sub.c.qty_ordered)\
    .join(Job_materials, Job_materials.job == Job_master.job)\
    .join(Item_whse, Item_whse.item == Job_materials.item)\
    .join(Items, Items.item == Job_materials.item)\
    .join(po_sub, po_sub.c.po_item == Job_materials.item)\
    .filter(Job_master.item == "PL0008") # replace with desired part number

    with open(r'C:\Users\bwcole\Desktop\bom_check.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'BOM Item', 'Description', 'Qty Required', 'On Hand', 'Qty Ordered'])
        writer.writerows(bom_check_query)


def tt_stock():
    """query tt data for for new stocking program"""
    session = loadSession()

    tt_query = session.query(
        Items.item,
        Items.description,
        Items.qty_mfg_ytd * 1.25,
        (Items.qty_mfg_ytd * 1.25) / 12)\
    .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
    .filter(Commodity_codes.description.contains("Torq"), Items.item.startswith("P"), Items.qty_mfg_ytd > 0)

    with open(r'C:\Users\bwcole\Desktop\tt_stock.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'Mfg YTD', 'Qty to Stock'])
        writer.writerows(tt_query)

def bearing_forecast():
    """extract list of shaft mount bearings and usage for forecast purposes"""
    session = loadSession()

    bearing_query = session.query(
        Items.item,
        Job_materials.item,
        Job_materials.matl_qty,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Job_master, Job_master.item == Items.item)\
        .join(Job_materials, Job_materials.job == Job_master.job)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .filter(or_(Commodity_codes.description.contains("RAIDER"), Commodity_codes.description.contains("POWERGEAR"), Commodity_codes.description.contains("TORQ")))\
        .filter(Job_materials.item.startswith("FF"))

    with open(r'C:\Users\bwcole\Desktop\bearing_forecast.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'BOM Item', 'Qty Used Per Unit', 'Comm Code', 'Comm Desc'])
        writer.writerows(bearing_query)

def core_items():
    """pull all core items"""
    session = loadSession()

    sub_core = session.query(
        Material_transactions.item.label('mt_item'),
        func.sum(Material_transactions.qty * -1).label('qty_used'))\
    .group_by(Material_transactions.item)\
    .filter(Material_transactions.trans_date < datetime(2019, 1, 1))\
    .filter(or_(Material_transactions.trans_type == "I", Material_transactions.trans_type == "W"), Material_transactions.ref_type == "J")\
    .subquery()

    sub_core2 = session.query(
        Material_transactions.item.label('mt_item'),
        func.sum(Material_transactions.qty * -1).label('qty_used'))\
    .group_by(Material_transactions.item)\
    .filter(Material_transactions.trans_date < datetime(2019, 1, 1))\
    .filter(Material_transactions.trans_type == "F", Material_transactions.ref_type == "J")\
    .subquery()

    sub_core3 = session.query(
        Material_transactions.item.label('mt_item'),
        func.sum(Material_transactions.qty * -1).label('qty_used'))\
    .group_by(Material_transactions.item)\
    .filter(Material_transactions.trans_date > datetime(2019, 1, 1))\
    .filter(or_(Material_transactions.trans_type == "I", Material_transactions.trans_type == "W"), Material_transactions.ref_type == "J")\
    .subquery()

    sub_jip = session.query(
        APS.item.label('item'),
        func.sum(APS.qty).label('total_demand'))\
    .group_by(APS.item)\
    .filter(APS.is_demand == 1)\
    .subquery()


    core_query = session.query(
        Items.item,
        Items.description,
        Items.p_m_t_code,
        Items.lbr_cost,
        Items.matl_cost,
        Items.unit_cost,
        Item_whse.qty_on_hand,
        Item_whse.qty_ordered,
        sub_core.c.qty_used,
        sub_core3.c.qty_used,
        Items.order_min,
        Item_whse.qty_reorder,
        Vendors.name,
        Items.product_code,
        Items.stat,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .join(Item_whse, Item_whse.item == Items.item)\
        .outerjoin(sub_core, sub_core.c.mt_item == Items.item)\
        .outerjoin(sub_core3, sub_core3.c.mt_item == Items.item)\
        .join(ItemVend, ItemVend.item == Items.item)\
        .join(Vendors, Vendors.vend_num == ItemVend.vend_num)\
        .filter(Item_whse.whse == "MCI", Commodity_codes.description.contains("CASTING"), ItemVend.NewRank == 1)

    pg_query = session.query(
        Items.item,
        Items.description,
        Item_whse.qty_on_hand,
        sub_core.c.qty_used,
        sub_jip.c.total_demand,
        Items.p_m_t_code,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .join(Item_whse, Item_whse.item == Items.item)\
        .outerjoin(sub_core, sub_core.c.mt_item == Items.item)\
        .outerjoin(sub_jip, sub_jip.c.item == Items.item)\
        .filter(Commodity_codes.description.contains("Torq"), ~Commodity_codes.description.contains("complete"), Items.p_m_t_code == 'M')

    pg_query2 = session.query(
        Items.item,
        Items.description,
        Item_whse.qty_on_hand,
        sub_core2.c.qty_used,
        sub_jip.c.total_demand,
        Items.p_m_t_code,
        Items.comm_code,
        Commodity_codes.description)\
        .join(Commodity_codes, Commodity_codes.comm_code == Items.comm_code)\
        .join(Item_whse, Item_whse.item == Items.item)\
        .outerjoin(sub_core2, sub_core2.c.mt_item == Items.item)\
        .outerjoin(sub_jip, sub_jip.c.item == Items.item)\
        .filter(Commodity_codes.description.contains("Powergear"), Commodity_codes.description.contains("complete"), Items.p_m_t_code == 'M')

    with open(r'C:\Users\bwcole\Desktop\powergear_parts.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'On Hand', 'Qty Used', 'Demand', 'Type', 'Comm Code', 'Comm Desc'])
        writer.writerows(pg_query)

    with open(r'C:\Users\bwcole\Desktop\powergear_units.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'On Hand', 'Qty Used', 'Demand', 'Type', 'Comm Code', 'Comm Desc'])
        writer.writerows(pg_query2)

    with open(r'C:\Users\bwcole\Desktop\casting_project.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Description', 'Item Type', 'Labor Cost', 'Material Cost', 'Item Cost', 'Qty On Hand', 'Qty Ordered', '2018 Usage', '2019 Usage', 'Order Min', 'Safety Stock', 'Vendor', 'Product Code', 'Status', 'Comm Code', 'Comm Description', ])
        writer.writerows(core_query)

        ## EOQ = sqrt((2 * (Setup Costs * Qty Sold per Year))/Holding Costs)
        ## Holding Costs or Carrying Costs = Inventory Value per Item * .25
        ## Setup Costs = ??
        ## Demand Rate or Qty Sold per Year

def negative_locations():
    """extract list of all negative inventory locations"""
    session = loadSession()

    job_op_query = session.query(
        Job_master.job,
        Job_master.item,
        Job_master.qty_released,
        Job_routing.oper_num,
        Job_routing.qty_received,
        Job_routing.qty_complete,
        Job_routing.qty_moved,
        Job_routing.qty_scrapped)\
        .join(Job_routing, Job_master.job == Job_routing.job)\
        .filter(Job_routing.oper_num == 10, Job_master.job.contains("W2")).order_by(Job_master.item)

    with open(r'C:\Users\bwcole\Desktop\negative_locations.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Job', 'Item', 'Qty Released', 'Op num', 'Qty Received', 'Qty Complete', 'Qty Moved', 'Qty Scrapped'])
        writer.writerows(job_op_query)

def vendor_contracts():
    """extract list of recently changed vendor contracts"""
    session = loadSession()

    vendor_contract_query = session.query(
        ItemVend.item,
        Items.description,
        ItemVend.vend_item,
        ItemVend.vend_num,
        Vendors.name,
        ItemVend.NewRank,
        ItemVend.RecordDate,
        ItemVend.UpdatedBy)\
        .join(Vendors, Vendors.vend_num == ItemVend.vend_num)\
        .join(Items, Items.item == ItemVend.item)\
        .order_by(ItemVend.vend_num)

    with open(r'C:\Users\bwcole\Desktop\vend_contracts.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, 'excel')
        writer.writerow(['Item', 'Item Desc', 'Vend Item', 'Vendor', 'Vendor Name', 'New Vendor Rank', 'Record Date', 'Updated By'])
        writer.writerows(vendor_contract_query)

if __name__ == '__main__':
    #puller()
    #lead_time_pull()
    #purchased()
    #mparts()
    #ytd_shipments()
    #spend_categories()
    #po_validation()
    customer_order_list()
    po_checker()
    #job_checker()
    #bom_check()
    #tt_stock()
    #bearing_forecast()
    core_items()
    #negative_locations()
    #vendor_contracts()
    print("Last Update: {}".format(datetime.today()))
