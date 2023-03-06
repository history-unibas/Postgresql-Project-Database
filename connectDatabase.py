#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
This file contains functions to interact with the project database.
"""


from sqlalchemy import create_engine
import psycopg2
import yaml
import logging


def populate_table(df, dbname, dbtable, user, password, host, port=5432, info=True):
    # Write a dataframe to a PostgreSQL data table
    
    url=f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(url)

    if info: 
        # Check if database table is empty
        table_empty = check_table_empty(dbname, dbtable, user, password, host, port)
        if not table_empty:
            logging.warning(f'The table {dbtable} of database {dbname} is not empty.')

    # Copy to avoid editing global variable
    df = df.copy()
    df.columns = df.columns.str.lower()

    # Write dataframe to database table
    df.to_sql(dbtable, con=engine, if_exists='append', index=False)


def read_table(dbname, dbtable, user, password, host, port=5432):
    # Read a PostgreSQL data table
    
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {dbtable}')
    result = cursor.fetchall()
    conn.close()
    return result


def check_database_exist(dbname, user, password, host, port=5432):
    # Check if the database exist

    conn = psycopg2.connect(user=user, host=host, password=password, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('SELECT datname FROM pg_database')
    db_list = cursor.fetchall()
    conn.close()
    if (dbname,) in db_list:
        return True
    else:
        return False


def check_table_empty(dbname, dbtable, user, password, host, port=5432):
    # Check if a database table is empty
    
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'SELECT CASE WHEN EXISTS(SELECT 1 FROM {dbtable}) THEN 0 ELSE 1 END AS IsEmpty')
    result = cursor.fetchall()
    conn.close()
    if result[0][0] == 0:
        return False
    elif result[0][0] == 1:
        return True
    else:
        return None


if __name__ == "__main__":
    # Load parameters
    config = yaml.safe_load(open('config.yaml'))
    dbname = config['dbname']
    db_user = config['db_user']
    db_password = input('PostgreSQL database superuser password:')
    db_host = config['db_host']
    db_port = input('PostgreSQL database port:')

    db_exist = check_database_exist(dbname, db_user, db_password, db_host, db_port)
    print(f'The database {dbname} does exist: {db_exist}.')
