#!/usr/bin/env python
"""
create test schema and insert some data
"""
import os
import psycopg2

def connect():
    return psycopg2.connect("dbname=warehouse user=postgres password=postgres")


def load_schema():
    conn = connect()
    print(os.getcwd())
    file = open('./testdata/schema.sql', 'r')
    data = file.read()
    print(data)
    cursor = conn.cursor()
    cursor.execute(data)
    conn.commit()





if __name__ == "__main__":
    load_schema()
    