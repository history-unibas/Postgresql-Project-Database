"""
This file contains functions to interact with the project database.
"""


from sqlalchemy import create_engine
import psycopg2
import logging
import geopandas


def read_table(dbname, dbtable, user, password, host, port=5432):
    # Read a PostgreSQL data table
    
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {dbtable}')
    result = cursor.fetchall()
    conn.close()
    return result


def read_geotable(dbname, dbtable, geom_col, user, password, host, port=5432):
    """Read a database table containing a geometry column.

    Args:
        dbname (str): Name of the database.
        dbtable (str): Name of the database table.
        geom_col (str): Name of the geometry column.
        user (str): Database user.
        password (str): Database user password.
        host (str): Host of the database connection.
        port (int or str): Port of the database connection.

    Returns:
        GeoDataFrame: Content of database table inclusive geometry.
    """
    conn = psycopg2.connect(dbname=dbname, user=user, password=password,
                            host=host, port=port)
    conn.autocommit = True
    query = f'SELECT * FROM {dbtable}'
    result = geopandas.read_postgis(query, conn, geom_col=geom_col)
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


def check_dbtable_exist(dbname, dbtable, user, password, host, port=5432):
    # Check if the dbtable exist.

    conn = psycopg2.connect(dbname=dbname, user=user, password=password,
                            host=host, port=port
                            )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT EXISTS (SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name  = '{dbtable}')"""
                   )
    result = cursor.fetchall()
    conn.close()
    return result[0][0]


def populate_table(df, dbname, dbtable, user, password, host, port=5432, info=True):
    # Write a dataframe to a PostgreSQL data table
    
    url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
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


def populate_geotable(df, dbname, dbtable, user, password, host, port=5432,
                      info=True):
    """Write a geodataframe to a postgis geodatabase table.

    Args:
        df (GeoDataFrame): Table to be written to the database.
        dbname (str): Name of the database.
        dbtable (str): Name of the database table.
        user (str): Database user.
        password (str): Database user password.
        host (str): Host of the database connection.
        port (int or str): Port of the database connection.
        info (bool): Indicate if a information should be created.

    Returns:
        None.
    """
    # Define the connection.
    url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(url)

    if info:
        # Check if database table is empty
        table_empty = check_table_empty(dbname, dbtable, user, password,
                                        host, port)
        if not table_empty:
            logging.warning(f'The table {dbtable} of database {dbname} is not '
                            'empty.')

    # Copy the geodataframe to avoid editing global variable.
    df = df.copy()
    df.columns = df.columns.str.lower()

    # Write dataframe to database table.
    df.to_postgis(dbtable, con=engine, if_exists='append', index=False)
