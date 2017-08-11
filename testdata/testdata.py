#!/usr/bin/env python
"""
create test schema and insert some data
"""

import os
import psycopg2

from decimal import *
from datetime import datetime
from faker import Faker

SEED_VALUE = 2515049
NUM_CUSTOMERS = 65
NUM_PRODUCTS = 100
NUM_CAMPAIGNS = 10
NUM_ORDERLINES = 5000
NUM_INVEVENTS = 10000
MAX_UNIT_PRICE = 50000
MAX_UNIT_COST = 10000


def connect():
    return psycopg2.connect("dbname=warehouse user=postgres password=postgres")

def run_file(filename, conn):
    file = open(filename, 'r')
    data = file.read()
    print(data)
    cursor = conn.cursor()
    cursor.execute(data)
    conn.commit()


def load_schema():
    conn = connect()
    print(os.getcwd())
    run_file('./testdata/schema.sql', conn)
    run_file('./testdata/inserts.sql', conn)
    generate_fake_data(conn)
    run_file('./testdata/aggregation.sql', conn)


def random_price(fake):
    return Decimal(fake.random.randint(1, MAX_UNIT_PRICE)) / Decimal(10)

def random_cost(fake):
    return Decimal(fake.random.randint(1, MAX_UNIT_COST)) / Decimal(10)

def generate_fake_data(conn):
    cursor = conn.cursor()
    fake = Faker()
    # specify the seed so we always generate the same customers
    fake.random.seed(SEED_VALUE)
    for i in range(NUM_CUSTOMERS):
        params = {
            "name": fake.name(), 
            "address1": fake.street_address(),
            "address2": fake.secondary_address(),
            "state":  fake.state(),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "email": fake.safe_email()
        }
        cursor.execute("INSERT INTO reporting.DimCustomer (name, address1, address2, state, zip, phone, email) VALUES (%(name)s, %(address1)s, %(address2)s, %(state)s, %(zip)s, %(phone)s, %(email)s)", params)    
    # reset the seed so we generate the same products even if we add more customers
    fake.random.seed(SEED_VALUE)
    for i in range(NUM_PRODUCTS):
        params = {
            "name": fake.color_name() + ' ' + fake.bs(),
            "curr_price": random_price(fake)
        }
        cursor.execute("INSERT INTO reporting.DimProduct (name, curr_price) VALUES (%(name)s, %(curr_price)s)", params)
    fake.random.seed(SEED_VALUE)
    for i in range(NUM_CAMPAIGNS):
        time1 = fake.date_time_between_dates(datetime(2015,1,1), datetime(2018,1,1))
        time2 = fake.date_time_between_dates(datetime(2015,1,1), datetime(2018,1,1))
        start_time = time1 if time1 < time2 else time2
        end_time = time2 if time2 > time1 else time1
        
        params = {
            "name": fake.job(),
            "description": fake.sentence(),
            "start_time": start_time,
            "end_time": end_time
        }            
        cursor.execute("INSERT INTO reporting.DimCampaign(name, description, start_time, end_time) VALUES( %(name)s, %(description)s, %(start_time)s, %(end_time)s)", params)
    fake.random.seed(SEED_VALUE)
    for i in range(NUM_ORDERLINES):
        time1 = fake.date_time_between_dates(datetime(2015,1,1), datetime(2018,1,1))
        time2 = fake.date_time_between_dates(datetime(2015,1,1), datetime(2018,1,1))
        order_date = time1 if time1 < time2 else time2
        ship_date = None
        order_status = fake.random.randint(1,9)
        if order_status == 5:
            ship_date = time2 if time2 > time1 else time1
        campaign_id = None
        if fake.boolean():
            campaign_id = fake.random.randint(1, NUM_CAMPAIGNS)
        customer_id = fake.random.randint(1, NUM_CUSTOMERS)
        product_id = fake.random.randint(1, NUM_PRODUCTS)
        discount_percent = 0
        if fake.boolean() and campaign_id:
            discount_percent = Decimal(fake.random.randint(1,25))/Decimal(100)
        unit_price = random_price(fake)
        unit_cost = random_cost(fake)
        quantity = fake.random.randint(1,5)
        params = {
            "product_id": product_id,
            "order_date": order_date,
            "ship_date": ship_date,
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "order_status_id": order_status,
            "quantity": quantity,
            "total_discount": unit_price * quantity - unit_price * quantity * (1 - discount_percent),
            "total_price": unit_price * quantity,
            "total_cost": unit_cost * quantity,
        }            
        cursor.execute("INSERT INTO reporting.FactOrderLine(product_id, order_date, ship_date, customer_id, campaign_id, order_status_id, quantity, total_discount, total_price, total_cost) VALUES(%(product_id)s, %(order_date)s, %(ship_date)s, %(customer_id)s, %(campaign_id)s, %(order_status_id)s, %(quantity)s, %(total_discount)s, %(total_price)s, %(total_cost)s)", params)
    fake.random.seed(SEED_VALUE)
    for i in range(NUM_INVEVENTS):
        params = {
            "product_id" : fake.random.randint(1, NUM_PRODUCTS),
            "event_date" : fake.date_time_between_dates(datetime(2015,1,1), datetime(2018,1,1)),
            "quantity" : fake.random.randint(1,10),
            "unit_cost" : random_cost(fake)
        }            
        cursor.execute("INSERT INTO reporting.FactInventoryEvent (product_id, event_date, quantity, unit_cost) VALUES(%(product_id)s, %(event_date)s, %(quantity)s, %(unit_cost)s)", params)
    conn.commit()

if __name__ == "__main__":
    load_schema()
    