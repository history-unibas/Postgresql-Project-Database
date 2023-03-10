#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
This file contains functions to create the project database and administrate it.
"""


import psycopg2
import yaml
import logging


def delete_database(dbname, user, password, host, port=5432):
    # Delete a database

    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute('DROP database ' + dbname)
        conn.close()
    except Exception as err:
        logging.error(f'Unexpected {err=}, {type(err)=}')
        raise


def create_database(dbname, user, password, host, port=5432):
    # Create a new database

    conn = psycopg2.connect(user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('CREATE database ' + dbname)
    conn.close()


def create_schema(dbname, user, password, host, port=5432):
    # Create database schema for given database

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()

    # Create tables for historical land registry Basel metadata
    cursor.execute('''CREATE TABLE StABS_Serie(serieId VARCHAR(10) PRIMARY KEY,
                                               stabsId VARCHAR(10) UNIQUE NOT NULL,
                                               title VARCHAR(100) NOT NULL,
                                               link VARCHAR(50) NOT NULL)
                   ''')
    cursor.execute('''CREATE TABLE StABS_Dossier(dossierId VARCHAR(15) PRIMARY KEY,
                                                 serieId VARCHAR(10) NOT NULL REFERENCES StABS_Serie(serieId),
                                                 stabsId VARCHAR(15) UNIQUE NOT NULL,
                                                 title VARCHAR(200) NOT NULL,
                                                 link VARCHAR(50) NOT NULL,
                                                 houseName VARCHAR(100),
                                                 oldHousenumber VARCHAR(100),
                                                 owner1862 VARCHAR(100),
                                                 descriptiveNote VARCHAR(600))
                   ''')

    # Create tables for the extract of the Transkribus database
    cursor.execute('''CREATE TABLE Transkribus_Collection(colId INTEGER PRIMARY KEY,
                                                          colName VARCHAR(10) NOT NULL,
                                                          nrOfDocuments SMALLINT NOT NULL)
                   ''')
    cursor.execute('''CREATE TABLE Transkribus_Document(docId INTEGER PRIMARY KEY,
                                                        colId INTEGER NOT NULL REFERENCES Transkribus_Collection(colId),
                                                        title VARCHAR(15) NOT NULL,
                                                        nrOfPages SMALLINT NOT NULL)
                   ''')
    cursor.execute('''CREATE TABLE Transkribus_Page(pageId INTEGER PRIMARY KEY,
                                                    key VARCHAR(30) UNIQUE NOT NULL,
                                                    docId INTEGER NOT NULL REFERENCES Transkribus_Document(docId),
                                                    pageNr SMALLINT NOT NULL,
                                                    urlImage VARCHAR(100) NOT NULL)
                   ''')
    cursor.execute('''CREATE TABLE Transkribus_Transcript(key VARCHAR(30) PRIMARY KEY,
                                                          tsId INTEGER UNIQUE NOT NULL,
                                                          pageId INTEGER NOT NULL REFERENCES Transkribus_Page(pageId),
                                                          parentTsId INTEGER NOT NULL,
                                                          urlPageXml VARCHAR(100) NOT NULL,
                                                          status VARCHAR(15) NOT NULL,
                                                          timestamp TIMESTAMP NOT NULL,
                                                          htrModel VARCHAR(1000))
                   ''')
    cursor.execute('''CREATE TABLE Transkribus_TextRegion(textRegionId VARCHAR(40) PRIMARY KEY,
                                                          key VARCHAR(30) NOT NULL REFERENCES Transkribus_Transcript(key),
                                                          index SMALLINT NOT NULL,
                                                          type VARCHAR(15),
                                                          textLine VARCHAR(200)[] NOT NULL,
                                                          text VARCHAR(10000) NOT NULL)
                   ''')

    # Load modules for full text search
    cursor.execute('''CREATE EXTENSION pg_trgm''')
    cursor.execute('''CREATE EXTENSION fuzzystrmatch''')
    cursor.execute('''CREATE EXTENSION dblink''')

    # Create index for transkribus_textregion.text
    cursor.execute('''CREATE INDEX text_idx ON transkribus_textregion USING GIST (text gist_trgm_ops)''')

    # Create read only user
    try:
        cursor.execute('''CREATE USER read_only WITH PASSWORD 'read_only' ''')
    except Exception as err:
        logging.warning(f'{err=}, {type(err)=}')
    cursor.execute('''GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only''')

    conn.close()


def rename_database(dbname_old, dbname_new, user, password, host, port=5432):
    # Rename an existing database

    conn = psycopg2.connect(user=user, host=host, password=password, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'ALTER DATABASE {dbname_old} RENAME TO {dbname_new}')
    conn.close()


def copy_database(dbname_source, dbname_destination, user, password, host, port=5432):
    # Copy a database

    conn = psycopg2.connect(user=user, host=host, password=password, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'CREATE DATABASE {dbname_destination} WITH TEMPLATE {dbname_source} OWNER {db_user}')
    conn.close()


if __name__ == "__main__":
    # Load parameters
    config = yaml.safe_load(open('config.yaml'))
    dbname = config['dbname']
    db_user = config['db_user']
    db_password = input('PostgreSQL database superuser password:')
    db_host = config['db_host']

    delete_database(dbname=dbname, user=db_user, password=db_password, host=db_host)

    create_database(dbname=dbname, user=db_user, password=db_password, host=db_host)

    create_schema(dbname=dbname, user=db_user, password=db_password, host=db_host)