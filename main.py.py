import argparse
from sys import argv
import re
import sqlite3
import pandas as pd
import argparse
import inspect
from datetime import datetime
from string import Template
import re
import os
import csv


sql_dir = os.path.join(os.getcwd(), 'sql')
data_dir = os.path.join(os.getcwd(), 'data')
database_dir = os.path.join(os.getcwd(), 'database')

def return_connection_object(database_arg):
    """The function establishes connection with the database
    Returns:
        _type_: object
                it returns connection object
    """
    try:
        filename = os.path.join(database_dir, database_arg)
        conn = sqlite3.connect(filename)
        return conn
    except (Exception) as error:
        print("Error connecting to database ", error)


def extract(*args):
    print ("extract function")
    args = parser.parse_args()
    match = re.search(r'\d{4}-\d{2}-\d{2}', args.params)
    dates = datetime.strptime(match.group(), '%Y-%m-%d').date()
    print(dates)
    c = return_connection_object(args.database)
    filename = os.path.join(sql_dir, args.sql)
    file_handle = open(filename)
    sql = file_handle.read()

    query = Template(sql).substitute(
        date=dates
    )
    print(query)

    df = pd.read_sql_query(query,c)
    print("df is", df)
    df.to_csv(os.path.join(data_dir,f"{args.target}"),sep ='\t')

def load(*args):
    args = parser.parse_args()
    conn = return_connection_object(args.database)
    c = conn.cursor()
    c.execute('''CREATE TABLE {} (transaction_id  INTEGER ,invoice_id INTEGER,invoice_date datetime default current_timestamp,stock_code TEXT,
       description TEXT,quantity INTEGER,unit_price REAL,customer_id INTEGER,customer_country TEXT)'''.format(args.target))
    filename = os.path.join(data_dir, args.input)
    with open(filename, 'r') as input_table:
        dr = csv.DictReader(input_table, delimiter='\t')  # comma is default delimiter

        to_db = [(i['transaction_id'], i['invoice_id'], i['invoice_date'], i['stock_code'], i['description'],
                  i['quantity'], i['unit_price'], i['customer_id'], i['customer_country']) for i in dr]


    c.executemany('''INSERT INTO {} (transaction_id,invoice_id,invoice_date,stock_code,description,quantity,unit_price,
                        customer_id,customer_country) VALUES (?, ?, ?,?,?,?,?,?,?)'''.format(args.target), to_db)
    c.execute('''SELECT * FROM {}'''.format(args.target))

    rows = c.fetchall()

    for row in rows:
        print(row)



def transform(*args):
    args = parser.parse_args()

    match = re.search(r'\d{4}-\d{2}-\d{2}', args.params)
    dates = datetime.strptime(match.group(), '%Y-%m-%d').date()
    print(dates)
    c = return_connection_object(args.database)
    filename = os.path.join(sql_dir, args.sql)
    file_handle = open(filename)
    sql = file_handle.read()
    query = Template(sql).substitute(
        date=dates
    )
    print(query)
    df = pd.read_sql_query(query, c)
    print("df is", df)
    print(args.sql)
    if args.sql == "aggregation-user-per-day.sql":
        df.to_sql(f"{args.target}" + "_agg_per_day", con=c, if_exists='replace')
    #  transactions_2010-12-01.csvdf.to_csv("transactions.csv")
    else:
        df.to_sql(f"{args.target}" + "_product_per_day", con=c, if_exists='replace')

    # df.to_csv(f"{target_filename}", sep='\t')
    # c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    c.execute("SELECT * from transactions_agg_per_day")

    rows = c.fetchall()

    for row in rows:
        print(row)
    c.commit()

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(title="subcommand")

parser_sleep = subparsers.add_parser('extract')
parser_sleep.add_argument('--database', required=True)
parser_sleep.add_argument('--sql', required=True)
parser_sleep.add_argument('--params', required=True)
parser_sleep.add_argument('--target', required=True)
parser_sleep.set_defaults(func=extract)

parser_foo = subparsers.add_parser('load')
parser_foo.add_argument("--input", required=True)
parser_foo.add_argument("--database", required=True)
parser_foo.add_argument("--target", required=True)
parser_foo.set_defaults(func=load)


parser_transform = subparsers.add_parser('transform')
parser_transform.add_argument('--database', required=True)
parser_transform.add_argument('--sql', required=True)
parser_transform.add_argument('--params', required=True)
parser_transform.add_argument('--target', required=True)
parser_transform.set_defaults(func=extract)

args = parser.parse_args()

arg_spec = inspect.getargspec(args.func)
if arg_spec.keywords:
    ## convert args to a dictionary
    args_for_func = vars(args)
else:
    ## get a subset of the dictionary containing just the arguments of func
    args_for_func = {k:getattr(args, k) for k in arg_spec.args}

args.func(**args_for_func)