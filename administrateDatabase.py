"""
This file contains functions to create the project database and administrate it.
"""


import psycopg2
import logging


def delete_database(dbname, user, password, host, port=5432):
    # Delete a database

    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()
        # Kill all other connections.
        cursor.execute(f"""
        SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <>
        pg_backend_pid() AND datname = '{dbname}'
        """)
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
    """Create project database schema.

    Args:
        dbname (str): Name of the database.
        user (str): Database user to connect with.
        password (str): User password.
        host (str): Host of the database connection.
        port (int or str): Port of the database connection.

    Returns:
        None.
    """
    conn = psycopg2.connect(dbname=dbname,
                            user=user, password=password,
                            host=host, port=port
                            )
    conn.autocommit = True
    cursor = conn.cursor()

    # Load module for creating UUID's.
    cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    # Load postgis module for spatial data.
    cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis')

    # Load module to query on other database.
    cursor.execute('CREATE EXTENSION IF NOT EXISTS dblink')

    # Load modules for full text search.
    cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
    cursor.execute('CREATE EXTENSION IF NOT EXISTS fuzzystrmatch')

    # Create tables for historical land registry Basel metadata.
    cursor.execute("""
    CREATE TABLE StABS_Serie(
        serieId VARCHAR(10) PRIMARY KEY,
        stabsId VARCHAR(10) UNIQUE NOT NULL,
        title VARCHAR(100) NOT NULL,
        link VARCHAR(50) NOT NULL)
    """
                   )
    cursor.execute("""
    CREATE TABLE StABS_Dossier(
        dossierId VARCHAR(15) PRIMARY KEY,
        serieId VARCHAR(10) NOT NULL REFERENCES StABS_Serie(serieId),
        stabsId VARCHAR(15) UNIQUE NOT NULL,
        title VARCHAR(200) NOT NULL,
        link VARCHAR(50) NOT NULL,
        houseName VARCHAR(100),
        oldHousenumber VARCHAR(100),
        owner1862 VARCHAR(100),
        descriptiveNote VARCHAR(600))
    """
                   )
    cursor.execute("""
    CREATE TABLE StABS_klingental_regest(
        link VARCHAR(50) PRIMARY KEY,
        identifier VARCHAR(100) NOT NULL,
        title VARCHAR(1000) NOT NULL,
        descriptiveNote VARCHAR(300),
        expressedDate VARCHAR(50) NOT NULL)
    """
                   )

    # Create tables for the extract of the Transkribus database.
    cursor.execute("""
    CREATE TABLE Transkribus_Collection(
        colId INTEGER PRIMARY KEY,
        colName VARCHAR(10) NOT NULL,
        nrOfDocuments SMALLINT NOT NULL)
    """
                   )
    cursor.execute("""
    CREATE TABLE Transkribus_Document(
        docId INTEGER PRIMARY KEY,
        colId INTEGER NOT NULL REFERENCES Transkribus_Collection(colId),
        title VARCHAR(15) NOT NULL,
        nrOfPages SMALLINT NOT NULL)
    """
                   )
    cursor.execute("""
    CREATE TABLE Transkribus_Page(
        pageId INTEGER PRIMARY KEY,
        key VARCHAR(30) UNIQUE NOT NULL,
        docId INTEGER NOT NULL REFERENCES Transkribus_Document(docId),
        pageNr SMALLINT NOT NULL,
        urlImage VARCHAR(100) NOT NULL)
    """
                   )
    cursor.execute("""
    CREATE TABLE Transkribus_Transcript(
        key VARCHAR(30) PRIMARY KEY,
        tsId INTEGER UNIQUE NOT NULL,
        pageId INTEGER NOT NULL REFERENCES Transkribus_Page(pageId),
        parentTsId INTEGER NOT NULL,
        urlPageXml VARCHAR(100) NOT NULL,
        status VARCHAR(15) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        htrModel VARCHAR(1000))
    """
                   )
    cursor.execute("""
    CREATE TABLE Transkribus_TextRegion(
        textRegionId VARCHAR(40) PRIMARY KEY,
        key VARCHAR(30) NOT NULL REFERENCES Transkribus_Transcript(key),
        index SMALLINT NOT NULL,
        type VARCHAR(15),
        textLine VARCHAR(200)[] NOT NULL,
        text VARCHAR(10000) NOT NULL)
    """
                   )

    # Create project specific tables.
    cursor.execute("""
    CREATE TABLE Project_Dossier(
        dossierId VARCHAR(15) PRIMARY KEY REFERENCES StABS_Dossier(dossierId),
        yearFrom1 SMALLINT,
        yearTo1 SMALLINT,
        yearFrom2 SMALLINT,
        yearTo2 SMALLINT,
        locationAccuracy VARCHAR(50),
        locationOrigin VARCHAR(100),
        location geometry(Point, 2056))
    """
                   )
    cursor.execute("""
    CREATE TABLE Project_Entry(
        entryId UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
        dossierId VARCHAR(15) NOT NULL REFERENCES Project_Dossier(dossierId),
        pageId INTEGER[] NOT NULL,
        year SMALLINT,
        yearSource VARCHAR(40) REFERENCES Transkribus_TextRegion(textRegionId),
        comment VARCHAR(100),
        manuallyCorrected BOOLEAN NOT NULL DEFAULT false,
        language VARCHAR(20))
    """
                   )

    # Create index for transkribus_textregion.text.
    cursor.execute("""
    CREATE INDEX text_idx
    ON transkribus_textregion
    USING GIST (text gist_trgm_ops)
    """
                   )

    # Create read only user.
    try:
        cursor.execute("CREATE USER read_only WITH PASSWORD 'read_only'")
    except Exception as err:
        logging.warning(f'{err=}, {type(err)=}')
    cursor.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only')
    cursor.execute('ALTER ROLE read_only SET '
                   'idle_in_transaction_session_timeout = "10min"'
                   )
    conn.close()


def rename_database(dbname_old, dbname_new, user, password, host, port=5432):
    # Rename an existing database

    conn = psycopg2.connect(user=user, host=host, password=password, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    # Kill all other connections.
    cursor.execute(f"""
    SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <>
    pg_backend_pid() AND datname = '{dbname_old}'
    """)
    cursor.execute(f'ALTER DATABASE {dbname_old} RENAME TO {dbname_new}')
    conn.close()


def copy_database(dbname_source, dbname_destination, user, password, host, port=5432):
    # Copy a database

    conn = psycopg2.connect(user=user, host=host, password=password, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    # Kill all other connections.
    cursor.execute(f"""
    SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <>
    pg_backend_pid() AND datname = '{dbname_source}'
    """)
    cursor.execute(f'CREATE DATABASE {dbname_destination} WITH TEMPLATE {dbname_source} OWNER {user}')
    conn.close()


def remove_privileges(dbname, user_revoke,
                      user_admin, password_admin,
                      host, port=5432):
    """Remove all privileges of a user within a particular database.

    Args:
        dbname (str): Name of the database.
        user_revoke (str): User to revoke all privileges.
        user_admin (str): Admin user to change the privileges.
        password_admin (str): Passwort for the admin user.
        host (str): Host of the database connection.
        port (int or str): Port of the database connection.

    Returns:
        None.
    """
    conn = psycopg2.connect(dbname=dbname,
                            user=user_admin, password=password_admin,
                            host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"""
    REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {user_revoke}
    """)
    conn.close()
