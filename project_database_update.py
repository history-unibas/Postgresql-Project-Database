"""Script to create or update the project database.

This script creates or updates an SQL database according to a defined schema.
The following steps are performed:

1. A temporary database with a defined schema is created if it does not already
exist.

2. If desired, the metadata of the Historical Land Registry of the City of
Basel (HGB) will be read and stored in the temporary database. Otherwise,
metadata of the previous database will be copied to the temporary database (if
a previous database exists). Further information on metadata:
https://github.com/history-unibas/Metadata-Historical-Land-Registry-Basel.

3. If desired, data from the Transkribus platform is read and stored in the
temporary database. Otherwise, data originating from Transkribus will be
copied from the previous database to the temporary database (if a previous
database exists). For more information on the Transkribus platform:
https://github.com/history-unibas/Trankribus-API.

4. Previous database is deleted.

5. Temporary database is renamed.

6. A copy of the database is created with the date as postfix.
"""


import yaml
import logging
from datetime import datetime
import requests
import psycopg2
import pandas as pd
import xml.etree.ElementTree as et
import re

from administrateDatabase import (delete_database, create_database,
                                  create_schema, rename_database,
                                  copy_database)
from connectDatabase import (populate_table, read_table, check_database_exist,
                             check_table_empty)


# Set directory of logfile.
LOGFILE_DIR = './project_database_update.log'

# Set directory of the config file.
CONFIG_DIR = './config.yaml'

# Define url to necessary repositories.
URI_QUERY_METADATA = 'https://raw.githubusercontent.com/history-unibas/'\
    'Metadata-Historical-Land-Registry-Basel/main/queryMetadata.py'
URI_CONNECT_TRANSKRIBUS = 'https://raw.githubusercontent.com/history-unibas/'\
    'Trankribus-API/main/connect_transkribus.py'


def download_script(url):
    """Download a online file to current working directory.

    Args:
        url (str): Url of a file.

    Returns:
        None.

    Raises:
        ValueError: Request status code is not ok.
    """

    # TODO: Download a file given the url

    filename = url.split('/')[-1]
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        with open(filename, 'w') as f:
            f.write(r.text)
    else:
        logging.error(f'url invalid? {r}')
        raise ValueError(f'Request status code is not ok: {r}.')


# Download and import necessary functions from other github repositories.
download_script(URI_QUERY_METADATA)
download_script(URI_CONNECT_TRANSKRIBUS)
from queryMetadata import (query_series, get_series, get_serie_id,
                           get_dossiers, get_dossier_id)
from connect_transkribus import (get_sid, list_collections, list_documents,
                                 get_document_content, get_page_xml)


def processing_metadata(filepath_serie, filepath_dossier, dbname,
                        db_user, db_password,
                        db_host, db_port=5432):
    """Processes the metadata of the HGB.

    This function processes all series and dossiers of the HGB and write them
    to the project database. In addition, CSV files will be written.

    Args:
        filepath_serie (str): Filepath of destination csv containing series.
        filepath_dossier (str): Filepath of destination csv containing
        dossiers.
        dbname (str): Name of the destination database.
        db_user (str): User of the database connection.
        db_password (str): Password for the database connection.
        db_host (str): Host of the database connection.
        db_port (str): Port of the database connection.

    Returns:
        None.
    """
    # Query all series.
    logging.info('Query series...')
    series_data = query_series()
    logging.info('Series queried.')

    # Extract attributes of interest.
    series_data = get_series(series_data)

    # Generate the "project_id" of the series.
    series_data['serieId'] = series_data.apply(
        lambda row: get_serie_id(row['stabsId']), axis=1
        )

    # Get all dossiers from all series.
    all_dossiers = pd.DataFrame(
        columns=['dossierId', 'title', 'houseName', 'oldHousenumber',
                 'owner1862', 'descriptiveNote', 'link'
                 ])
    for row in series_data.iterrows():
        logging.info('Query dossier %s ...', row[1]['link'])
        dossiers = get_dossiers(row[1]['link'])

        # Case if serie does not have any dossier.
        if not isinstance(dossiers, pd.DataFrame):
            continue
        else:
            # Add series_id to dossiers.
            dossiers['serieId'] = row[1]['serieId']

            all_dossiers = pd.concat([all_dossiers, dossiers],
                                     ignore_index=True
                                     )

    # Generate the "project_id" of the dossiers.
    all_dossiers['dossierId'] = all_dossiers.apply(
        lambda row: get_dossier_id(row['stabsId']), axis=1)

    # Write data created to project database.
    populate_table(df=series_data, dbname=dbname, dbtable='stabs_serie',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port
                   )
    populate_table(df=all_dossiers, dbname=dbname, dbtable='stabs_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port)

    # Write data created to csv.
    series_data.to_csv(filepath_serie, index=False, header=True)
    all_dossiers.to_csv(filepath_dossier, index=False, header=True)


def processing_transkribus(series_data, dossiers_data, dbname,
                           db_user, db_password,
                           db_host, db_port=5432):
    """Processes the metadata of the HGB.

    This function processes all project database tables containing data from
    the Transkribus platform. Based on the HGB metadata, only Transkribus
    collections for which metadata exist are processed.

    Args:
        series_data (DataFrame): HGB metadata series created by
        processing_metadata().
        dossiers_data (DataFrame): HGB metadata dossiers created by
        processing_metadata().
        dbname (str): Name of the destination database.
        db_user (str): User of the database connection.
        db_password (str): Password for the database connection.
        db_host (str): Host of the database connection.
        db_port (str): Port of the database connection.

    Returns:
        None.
    """
    # Login to Transkribus.
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Check if collections already exist in project database.
    coll = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_collection',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['colId', 'colName', 'nrOfDocuments'])
    if len(coll) > 0:
        logging.warning('Collections already exist in the projct database. '
                        'Only those collections will be considered further.')
    else:
        # Read the transkribus collections and write those in project database.

        # Get all collections.
        coll = pd.DataFrame(list_collections(sid))

        # Analyse which collections where skipped.
        test = coll.merge(series_data, how='left',
                          left_on='colName', right_on='serieId',
                          indicator=True
                          )
        log_skipped = test.query("_merge == 'left_only'")['colName'].values
        logging.warning('The following Transkribus collection where skipped: '
                        f'{log_skipped}. They are not available in table '
                        'stabs_serie.'
                        )

        # Analyse which collections are missing.
        test = coll.merge(series_data, how='right',
                          left_on='colName', right_on='serieId',
                          indicator=True
                          )
        log_missing = test.query("_merge == 'right_only'")['title'].values
        logging.info('For the following series, no Transkribus collection are '
                     f'available: {log_missing}.')

        # Keep only collection features available in stabs_serie data.
        coll = pd.merge(coll, series_data, how='inner',
                        left_on='colName', right_on='serieId',
                        validate='one_to_one'
                        )

        # Keep columns according project database schema.
        coll = coll[['colId', 'colName', 'nrOfDocuments']]

        # Write collections to database.
        populate_table(df=coll, dbname=dbname,
                       dbtable='transkribus_collection',
                       user=db_user, password=db_password,
                       host=db_host, port=db_port
                       )

    # Get documents accoring project database schema for each collection
    # considered.
    all_doc = pd.DataFrame(columns=['docId', 'colId', 'title', 'nrOfPages'])
    for index, row in coll.iterrows():
        logging.info(f"Query documents of collection {row['colName']}...")
        doc_return = list_documents(sid, row['colId'])
        for doc in doc_return:
            all_doc = pd.concat(
                [all_doc,
                 pd.DataFrame([[doc['docId'],
                                doc['collectionList']['colList'][0]['colId'],
                                doc['title'], doc['nrOfPages']]],
                              columns=['docId', 'colId', 'title', 'nrOfPages']
                              )], ignore_index=True)
        n_documents = len(all_doc)

    # Analyse which documents where skipped.
    test = all_doc.merge(dossiers_data, how='left',
                         left_on='title', right_on='dossierId', indicator=True)
    log_skipped = test.query("_merge == 'left_only'")['title_x'].values
    logging.info('The following Transkribus document are not available in '
                 f'table stabs_dossier: {log_skipped}.')

    # Analyse which documents are missing.
    test = all_doc.merge(dossiers_data, how='right',
                         left_on='title', right_on='dossierId', indicator=True)
    log_missing = test.query("_merge == 'right_only'")['title_y'].values
    logging.info('The following Transkribus document where skipped: '
                 f'{log_missing}.')

    # Test if left join to stabs_dossier data returns one-to-one-connections.
    test = pd.merge(all_doc, dossiers_data, how='left',
                    left_on='title', right_on='dossierId',
                    suffixes=('', '_dossier'), validate='one_to_one')

    # Check if documents already exist in project database.
    transkribus_docs = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_document',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['docId', 'colId', 'title', 'nrOfPages'])
    if len(transkribus_docs) > 0:
        last_document = transkribus_docs.iloc[-1]['title']
        logging.info('Documents already exist in the projct database. '
                     'Processing the subsequent documents of document '
                     f'{last_document}.')

        # Skip documents that are already processed.
        last_document_index = all_doc[
            all_doc['title'] == last_document].index.item()
        all_doc = all_doc.iloc[last_document_index + 1:]

    # Iterate over documents.
    for index, row in all_doc.iterrows():
        # Get pages and transcripts accoring project database schema for each
        # dossier considered.
        logging.info('Query pages of document '
                     f"{row['title']} ({index + 1}/{n_documents})..."
                     )
        all_page = pd.DataFrame(
            columns=['pageId', 'key', 'docId', 'pageNr', 'urlImage'
                     ])
        all_transcript = pd.DataFrame(
            columns=['key', 'tsId', 'pageId', 'parentTsId', 'urlPageXml',
                     'status', 'timestamp', 'htrModel'
                     ])
        all_textregion = pd.DataFrame(
            columns=['textRegionId', 'key', 'index', 'type', 'textLine', 'text'
                     ])
        page_return = get_document_content(row['colId'], row['docId'], sid)

        # Iterate over pages.
        for page in page_return['pageList']['pages']:
            all_page = pd.concat(
                [all_page,
                 pd.DataFrame([
                     [page['pageId'], page['key'],
                      page['docId'], page['pageNr'], page['url']
                      ]],
                      columns=['pageId', 'key', 'docId', 'pageNr', 'urlImage'])
                 ], ignore_index=True)

            # Iterate over transcripts.
            for transcript in page['tsList']['transcripts']:
                key_transcript = transcript['key']
                url_page_xml = transcript['url']
                timestamp = datetime.fromtimestamp(
                    transcript['timestamp']/1000
                    )

                # Query the page xml and extract the data of interest.
                page_xml = et.fromstring(get_page_xml(url_page_xml, sid))
                creator_content = page_xml.find(
                    './/{http://schema.primaresearch.org/PAGE/gts/pagecontent/'
                    '2013-07-15}Creator').text
                htr_model = creator_content.split(':date=')[0]
                all_transcript = pd.concat(
                    [all_transcript,
                     pd.DataFrame([
                         [key_transcript, transcript['tsId'],
                          transcript['pageId'], transcript['parentTsId'],
                          url_page_xml, transcript['status'], timestamp,
                          htr_model]
                          ],
                          columns=['key', 'tsId', 'pageId', 'parentTsId',
                                   'urlPageXml', 'status', 'timestamp',
                                   'htrModel'])], ignore_index=True)

                # Iterate over text regions.
                for textregion in page_xml.iter(
                        '{http://schema.primaresearch.org/PAGE/gts/pagecontent'
                        '/2013-07-15}TextRegion'):
                    # Find all unicode tag childs.
                    unicode = textregion.findall(
                        './/{http://schema.primaresearch.org/PAGE/gts/'
                        'pagecontent/2013-07-15}Unicode')

                    # Extract all text lines (exclude last candidate,
                    # correspond to the whole text of the region).
                    text_line = [item.text for item in unicode[:-1]]
                    if not text_line:
                        # Skip empty textregions.
                        continue

                    # Create string of whole text region (not using last
                    # candidate because it might be empty because of
                    # Transkribus bug).
                    text = '\n'.join(text_line)

                    # Determine type of text region.
                    textregion_custom = textregion.get('custom')
                    index_textregion = int(
                        re.search(
                            r'index:[0-9]+;', textregion_custom).group()[6:-1]
                        )
                    match = re.search(r'type:[a-z]+;', textregion_custom)
                    if match:
                        type_textregion = match.group()[5:-1]
                    else:
                        type_textregion = None

                    # Get type of text region.
                    text_region_id = f'{key_transcript}_'\
                        f'{int(index_textregion):02}'

                    # Add text region to dataframe.
                    all_textregion = pd.concat(
                        [all_textregion,
                         pd.DataFrame([
                             [text_region_id, key_transcript,
                              index_textregion, type_textregion,
                              text_line, text]],
                              columns=['textRegionId', 'key', 'index', 'type',
                                       'textLine', 'text'])
                         ], ignore_index=True)

        # Write data for current document to project database.
        populate_table(df=pd.DataFrame([row.tolist()], columns=row.index),
                       dbname=dbname, dbtable='transkribus_document',
                       user=db_user, password=db_password,
                       host=db_host, port=db_port, info=False
                       )
        populate_table(df=all_page, dbname=dbname, dbtable='transkribus_page',
                       user=db_user, password=db_password,
                       host=db_host, port=db_port, info=False
                       )
        populate_table(df=all_transcript, dbname=dbname,
                       dbtable='transkribus_transcript',
                       user=db_user, password=db_password,
                       host=db_host, port=db_port, info=False
                       )
        populate_table(df=all_textregion, dbname=dbname,
                       dbtable='transkribus_textregion',
                       user=db_user, password=db_password,
                       host=db_host, port=db_port, info=False
                       )


def main():
    datetime_started = datetime.now()

    # Define logging environment.
    print(f'Consider the logfile {LOGFILE_DIR} for information about the run.')
    logging.basicConfig(filename=LOGFILE_DIR,
                        format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO,
                        encoding='utf-8'
                        )
    logging.info('Script started.')

    # Define which data will be processed.
    process_metadata = input('Do you want to (re)process the metadata? ')
    if process_metadata.lower() in ('true', 'yes', 'y', '1'):
        process_metadata = True
    elif process_metadata.lower() in ('false', 'no', 'n', '0'):
        process_metadata = False
    else:
        logging.error(f'Your answer is not True or False: {process_metadata}.')
        raise
    process_transkribus = input('Do you want to (re)process the Transkribus '
                                'data? ')
    if process_transkribus.lower() in ('true', 'yes', 'y', '1'):
        process_transkribus = True
    elif process_transkribus.lower() in ('false', 'no', 'n', '0'):
        process_transkribus = False
    else:
        logging.error('Your answer is not True or False: '
                      f'{process_transkribus}.')
        raise

    # Set parameters of the database.
    config = yaml.safe_load(open(CONFIG_DIR))
    dbname = config['dbname']
    db_user = config['db_user']
    db_password = input('PostgreSQL database superuser password:')
    db_host = config['db_host']
    db_port = input('PostgreSQL database port:')
    filepath_serie = config['filepath_serie']
    filepath_dossier = config['filepath_dossier']
    dblink_connname = f'host={db_host} user={db_user} '\
        f'password={db_password} dbname={dbname}'

    # Define name for temporary database in case the script breaks.
    dbname_temp = dbname + '_temp'

    # Check if temp database already exist.
    db_temp_exist = check_database_exist(dbname=dbname_temp,
                                         user=db_user, password=db_password,
                                         host=db_host, port=db_port
                                         )

    # Create new temp database and schema if not existent.
    if not db_temp_exist:
        create_database(dbname=dbname_temp,
                        user=db_user, password=db_password,
                        host=db_host, port=db_port
                        )
        create_schema(dbname=dbname_temp,
                      user=db_user, password=db_password,
                      host=db_host, port=db_port
                      )
        logging.info(f'New database {dbname_temp} created.')
    else:
        logging.warning(f'The database {dbname_temp} already exist.')

    # Check if database does exist.
    db_exist = check_database_exist(dbname=dbname,
                                    user=db_user, password=db_password,
                                    host=db_host, port=db_port
                                    )

    # Processing metadata.
    stabs_serie_empty = check_table_empty(
        dbname=dbname_temp, dbtable='stabs_serie',
        user=db_user, password=db_password,
        host=db_host, port=db_port
        )
    stabs_dossier_empty = check_table_empty(
        dbname=dbname_temp, dbtable='stabs_dossier',
        user=db_user, password=db_password,
        host=db_host, port=db_port
        )
    if not all((stabs_serie_empty, stabs_dossier_empty)):
        logging.warning(
            f'Metadata table(s) are not empty in database {dbname_temp}. '
            f'No metadata will be new processed or copied from {dbname}.'
            )
    # Case when all metadata tables are empty.
    else:
        if process_metadata:
            processing_metadata(filepath_serie=filepath_serie,
                                filepath_dossier=filepath_dossier,
                                dbname=dbname_temp,
                                db_user=db_user, db_password=db_password,
                                db_host=db_host, db_port=db_port
                                )
            logging.info('Metadata are processed.')
        elif db_exist:
            # Copy existing tables stabs_serie and stabs_dossier from database
            # hgb to database hgb_temp.
            conn = psycopg2.connect(dbname=dbname_temp,
                                    user=db_user, password=db_password,
                                    host=db_host, port=db_port
                                    )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"""
            INSERT INTO stabs_serie
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT serieid,stabsid,title,link FROM stabs_serie')
            AS t(serieid text, stabsid text, title text, link text)
            """)
            cursor.execute(f"""
            INSERT INTO stabs_dossier
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT dossierid,serieid,stabsid,title,link,housename,
            oldhousenumber,owner1862,descriptivenote FROM stabs_dossier')
            AS t(dossierid text, serieid text, stabsid text, title text,
            link text, housename text, oldhousenumber text, owner1862 text,
            descriptivenote text)
            """)
            conn.close()
            logging.info('Metadata are copied from current database.')
        else:
            logging.warning('No metadata will be available in database.')

    # Processing transkribus data.
    if process_transkribus:
        # Read series and dossiers created by processing_metadata() for
        # selecting transkribus features.
        series_data = pd.read_csv(filepath_serie)
        dossiers_data = pd.read_csv(filepath_dossier)
        processing_transkribus(series_data=series_data,
                               dossiers_data=dossiers_data,
                               dbname=dbname_temp,
                               db_user=db_user, db_password=db_password,
                               db_host=db_host, db_port=db_port
                               )
        logging.info('Transkribus data are processed.')
    elif db_exist:
        # Test if transkribus tables are empty.
        coll_empty = check_table_empty(dbname=dbname_temp,
                                       dbtable='transkribus_collection',
                                       user=db_user, password=db_password,
                                       host=db_host, port=db_port
                                       )
        doc_empty = check_table_empty(dbname=dbname_temp,
                                      dbtable='transkribus_document',
                                      user=db_user, password=db_password,
                                      host=db_host, port=db_port
                                      )
        page_empty = check_table_empty(dbname=dbname_temp,
                                       dbtable='transkribus_page',
                                       user=db_user, password=db_password,
                                       host=db_host, port=db_port
                                       )
        ts_empty = check_table_empty(dbname=dbname_temp,
                                     dbtable='transkribus_transcript',
                                     user=db_user, password=db_password,
                                     host=db_host, port=db_port
                                     )
        region_empty = check_table_empty(dbname=dbname_temp,
                                         dbtable='transkribus_textregion',
                                         user=db_user, password=db_password,
                                         host=db_host, port=db_port
                                         )
        if all((coll_empty, doc_empty, page_empty, ts_empty, region_empty)):
            # Copy existing transkribus tables from database hgb to database
            # hgb_temp.
            conn = psycopg2.connect(dbname=dbname_temp,
                                    user=db_user, password=db_password,
                                    host=db_host, port=db_port
                                    )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"""
            INSERT INTO transkribus_collection
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT colid,colname,nrofdocuments FROM transkribus_collection')
            AS t(colid integer, colname text, nrofdocuments integer)
            """)
            cursor.execute(f"""
            INSERT INTO transkribus_document
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT docid,colid,title,nrofpages FROM transkribus_document')
            AS t(docid integer, colid integer, title text, nrofpages integer)
            """)
            cursor.execute(f"""
            INSERT INTO transkribus_page
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT pageid,key,docid,pagenr,urlimage FROM transkribus_page')
            AS t(pageid integer, key text, docid integer, pagenr integer,
            urlimage text)
            """)
            cursor.execute(f"""
            INSERT INTO transkribus_transcript
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT key,tsid,pageid,parenttsid,urlpagexml,status,timestamp,
            htrmodel FROM transkribus_transcript')
            AS t(key text, tsid integer, pageid integer, parenttsid integer,
            urlpagexml text, status text, timestamp timestamp, htrmodel text)
            """)
            cursor.execute(f"""
            INSERT INTO transkribus_textregion
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT textregionid,key,index,type,textline,text
            FROM transkribus_textregion')
            AS t(textregionid text, key text, index integer, type text,
            textline text[], text text)
            """)
            conn.close()
            logging.info('Transkribus data are copied from current database.')
        else:
            logging.warning(
                f'Transkribus table(s) are not empty in database {dbname_temp}'
                f'. The data are not copied from {dbname}.')
    else:
        logging.warning('No transkribus data will be available in database.')

    # Delete existing database
    if db_exist:
        try:
            delete_database(dbname=dbname,
                            user=db_user, password=db_password,
                            host=db_host, port=db_port
                            )
            logging.info(f'Old database {dbname} was deleted.')
        except Exception as err:
            logging.error(f'The database {dbname} can\'t be deleted. {err=}, '
                          f'{type(err)=}')
            raise

    # Rename the database
    rename_database(dbname_old=dbname_temp, dbname_new=dbname,
                    user=db_user, password=db_password,
                    host=db_host, port=db_port)
    logging.info(f'Database {dbname_temp} was renamed to {dbname}.')

    # Generate a copy of the database with timestamp
    dbname_copy = dbname + '_' + str(datetime_started.date()).replace('-', '_')
    copy_database(dbname_source=dbname, dbname_destination=dbname_copy,
                  user=db_user, password=db_password,
                  host=db_host, port=db_port
                  )
    logging.info(f'New database {dbname} copied to {dbname_copy}.')

    datetime_ended = datetime.now()
    datetime_duration = datetime_ended - datetime_started
    logging.info('Duration of the run: '
                 f'{str(round(datetime_duration.seconds / 3600, 1))} hour(s).')
    logging.info('Script finished.')


if __name__ == "__main__":
    main()
