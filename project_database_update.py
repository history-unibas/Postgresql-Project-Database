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

4. If desired, project data tables are new created and filled. Otherwise, data
will be copied from the previous database (if existent).

5. Process the geodata. At the moment, the geodata will always be new created
and not copied from the previous database.

6. Previous database is deleted.

7. Create a copy of temporary database to new database.

8. Temporary database is renamed with date as postfix.
"""


import logging
from datetime import datetime
import requests
import psycopg2
import pandas as pd
import xml.etree.ElementTree as et
import re
import os

from administrateDatabase import (delete_database, create_database,
                                  create_schema, rename_database,
                                  copy_database)
from connectDatabase import (populate_table, read_table, check_database_exist,
                             check_table_empty, check_dbtable_exist)


# Set directory of logfile.
LOGFILE_DIR = './project_database_update.log'

# Set parameters for postgresql database
DB_NAME = 'hgb'
DB_USER = 'postgres'
DB_HOST = 'localhost'

# Set filepaths for HGB metadata.
FILEPATH_SERIE = './data/stabs_serie.csv'
FILEPATH_DOSSIER = './data/stabs_dossier.csv'

# Set parameter for geodata to be imported.
SHAPEFILE_PATH = 'data/HGB_Mappen_Liste_Staatsarchiv.shp'
SHAPEFILE_EPSG = 'EPSG:2056'

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


def do_process(prompt: str) -> bool:
    """Determine if a process should be executed based on user input.

    Args:
        prompt (str): Input message for the user.

    Returns:
        Bool: Indicator if a process should be executed.

    """
    r = input(prompt)
    if r.lower() in ('true', 'yes', 'y', '1'):
        return True
    elif r.lower() in ('false', 'no', 'n', '0'):
        return False
    else:
        return do_process(f'Your answer is not True or False: {r}. {prompt}')


def processing_stabs(filepath_serie, filepath_dossier, dbname,
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
        processing_stabs().
        dossiers_data (DataFrame): HGB metadata dossiers created by
        processing_stabs().
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

                    # Extract all text lines. Exclude last candidate,
                    # correspond to the whole text of the region as well as
                    # empty text lines.
                    text_line = [item.text for item in unicode[:-1]
                                 if bool(item.text)
                                 ]
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


def get_validity_range(remark: str):
    """Extract from StABS_Dossier.descriptiveNote the validity range of the
    Dossier.

    In the attribute descriptiveNote of the entity StABS_Dossier, the validity
    range of a dossier is partially documented. This function extracts the
    validity range from the note for the most frequent samples.

        Args:
            remark (str): Note according to the pattern of
            StABS_Dossier.descriptiveNote.

        Returns:
            Tuble: First element of the tuble correspond to the year from of
            the validity range, the second element to the year to of the
            validity range.
    """
    if not remark:
        return (None, None)
    else:
        year_from = None
        year_to = None

    # Search for year number from.
    match_from = re.search(r'^((Seit)|(Errichtet)|(Ab)) 1[0-9]{3}\.', remark)
    if match_from:
        year_from = match_from.group()[-5:-1]

    # Search for year number to.
    match_to = re.search(r'((Bis)|(Abgebrochen)) 1[0-9]{3}\.', remark)
    if match_to:
        year_to = match_to.group()[-5:-1]

    # Consider patterns like "1734-1819".
    if not year_from and not year_to:
        match = re.match(r'^1[0-9]{3}-1[0-9]{3}\.?$', remark)
        if match:
            year_from = match.group()[:4]
            year_to = match.group()[5:9]

    return (year_from, year_to)


def get_year(page_id, df_transcript, df_textregion):
    """Extract the first occurrence of a year in header text regions.

    Using the existing data in the project database, a year is extracted per
    entry of the database table project_entry in the text regions of type
    "header". The first occurrence of a year is taken into account.
    If there is no header text region, None will be returned.

        Args:
            page_id (list): List of page_id's of pages to be considered.
            df_transcript (DataFrame): Table of all transcript within the
            project database.
            df_textregion (DataFrame): Table of all text regions within the
            project database.

        Returns:
            Tuble: First element of the tuble correspond to the first
            occurrence of the year, the second element to the id of the text
            region, from which the year comes. If there is no year, None is
            returned.
    """
    # Iterate over all page_id's.
    for page in page_id:
        # Determine latest transcript of current page.
        ts = df_transcript[df_transcript['pageId'] == page]
        ts_sorted = ts.sort_values(by='timestamp', ascending=False)
        ts_latest = ts_sorted.iloc[0]

        # Get the header textregions of latest transcript.
        tr = df_textregion[df_textregion['key'] == ts_latest['key']]
        tr_header = tr[tr['type'] == 'header']
        if tr_header.empty:
            continue

        # Search for first year occurance in header textregions.
        for header in tr_header.iterrows():
            match = re.search(r'1[0-9]{3}', header[1]['text'])
            if match:
                return (int(match.group()), header[1]['textRegionId'])

    return (None, None)


def processing_project(dbname, db_password, db_user='postgres',
                       db_host='localhost', db_port=5432):
    """Processes the project data within the project database.

    This function processes all tables of the project database with the prefix
    "project_". In particular:
    - Determine the entries of the database table project_dossier.
        - Search entries for yearFrom and yearTo based on descriptiveNote.
    - Determine the entries of the database table project_entry.
        - Search the year per entry of the the table project_entry.

    Args:
        dbname (str): Name of the project database.
        db_password (str): Password for the database connection.
        db_user (str): User of the database connection.
        db_host (str): Host of the database connection.
        db_port (int,str): Port of the database connection.

    Returns:
        None.
    """
    # Read necessary database tables.
    stabs_dossier = pd.DataFrame(
        read_table(dbname=dbname, dbtable='stabs_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['dossierId', 'serieId', 'stabsId', 'title', 'link',
                 'houseName', 'oldHousenumber', 'owner1862', 'descriptiveNote'
                 ])
    page = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_page',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['pageId', 'key', 'docId', 'pageNr', 'urlImage'])
    transcript = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_transcript',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['key', 'tsId', 'pageId', 'parentTsId', 'urlPageXml', 'status',
                 'timestamp', 'htrModel'])
    textregion = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_textregion',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['textRegionId', 'key', 'index', 'type', 'textLine', 'text'])

    # Determine the data for the entity project_dossier.
    dossier = stabs_dossier[['dossierId', 'descriptiveNote']].copy()
    dossier[['yearFrom', 'yearTo']] = dossier.apply(
        lambda row: get_validity_range(row['descriptiveNote']),
        axis=1,
        result_type='expand'
        )
    dossier = dossier.drop('descriptiveNote', axis=1)

    # Generate entries of table project_entry. Currently the entries in
    # project_entry correspond to the entries in Transkribus_Page.
    entry = pd.DataFrame(columns=['pageId', 'year', 'yearSource'])
    for row in page.iterrows():
        entry = pd.concat(
            [entry,
             pd.DataFrame([[[row[1]['pageId']], None, None]],
                          columns=['pageId', 'year', 'yearSource'])
             ], ignore_index=True)

    # Search for first occurence in year in the header textregion of the
    # latest page version.
    entry[['year', 'yearSource']] = entry.apply(
        lambda row: get_year(page_id=row['pageId'],
                             df_transcript=transcript,
                             df_textregion=textregion),
        axis=1,
        result_type='expand'
        )

    # Write data created to project database.
    populate_table(df=dossier, dbname=dbname, dbtable='project_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port
                   )
    populate_table(df=entry, dbname=dbname, dbtable='project_entry',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port
                   )


def import_shapefile(dbname, dbtable,
                     shapefile_path, shapefile_epsg,
                     db_password, db_user='postgres',
                     db_host='localhost', db_port=5432):
    """Import a shapefile to a new database table.

    This function imports a shapefile with a defined coordinate system into a
    database table. The geometry and all attributes are taken from the objects
    in the shapefile.

    Args:
        dbname (str): Name of the project database.
        dbtable (str): Name of the destination database table.
        shapefile_path (str): Source path of the shapefile to be read.
        shapefile_epsg (str): EPSG code of the shapefile's coordinate system.
        db_password (str): Password for the database connection.
        db_user (str): User of the database connection.
        db_host (str): Host of the database connection.
        db_port (int,str): Port of the database connection.

    Returns:
        None.
    """
    # Test if dbtable already exist.
    dbtable_exist = check_dbtable_exist(dbname=dbname, dbtable=dbtable,
                                        user=db_user, password=db_password,
                                        host=db_host, port=db_port
                                        )
    if dbtable_exist:
        logging.error(f'Table {dbtable} already exist in database {dbname}. '
                      f'The shapefile {shapefile_path} will not be imported.')
    else:
        # Read the shapefile and write it in a new database table.
        connection = f'postgresql://{db_user}:{db_password}@'\
                     f'{db_host}:{db_port}/{dbname}'
        command = f"""
            shp2pgsql -D -I -s {shapefile_epsg} {shapefile_path} {dbtable} \
            | psql {connection}"""
        result = os.system(command)
        if result == 0:
            logging.info(f'Shapefile {shapefile_path} successfully imported '
                         f'into database {dbname}, table {dbtable}.'
                         )
        else:
            logging.error(f'The shapefile {shapefile_path} was not imported'
                          f'into database {dbname}, table {dbtable}: {result}.'
                          )


def processing_geodata(shapefile_path, shapefile_epsg,
                       dbname, db_password, db_user='postgres',
                       db_host='localhost', db_port=5432):
    """Processes the geodata within the project database.

    This function processes all tables of the project database with the prefix
    "geo_". In particular:
    - Imports the shapefile and creating the table geo_address.

    Args:
        shapefile_path (str): Source path of the shapefile to be read.
        shapefile_epsg (str): EPSG code of the shapefile's coordinate system.
        dbname (str): Name of the project database.
        db_password (str): Password for the database connection.
        db_user (str): User of the database connection.
        db_host (str): Host of the database connection.
        db_port (int,str): Port of the database connection.

    Returns:
        None.
    """
    # Import the shapefile to the project database.
    dbtable = 'geo_address'
    import_shapefile(
        dbname=dbname, dbtable=dbtable,
        shapefile_path=shapefile_path, shapefile_epsg=shapefile_epsg,
        db_password=db_password, db_user=db_user,
        db_host=db_host, db_port=db_port
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
    process_metadata = do_process('Do you want to (re)process the metadata?')
    logging.info(f'The metadata will be (re)processed: {process_metadata}.')
    process_transkribus = do_process('Do you want to (re)process the '
                                     'Transkribus data?')
    logging.info('The Transkribus data will be (re)processed: '
                 f'{process_transkribus}.')
    process_project = do_process('Do you want to (re)process the project data?'
                                 )
    logging.info(f'The project data will be (re)processed: {process_project}.')

    # Get parameters of the database.
    db_password = input('PostgreSQL database superuser password:')
    db_port = input('PostgreSQL database port:')
    dblink_connname = f'dbname={DB_NAME} '\
        f'user={DB_USER} password={db_password} '\
        f'host={DB_HOST} port={db_port}'

    # Define name for temporary database in case the script breaks.
    dbname_temp = DB_NAME + '_temp'

    # Check if temp database already exist.
    db_temp_exist = check_database_exist(dbname=dbname_temp,
                                         user=DB_USER, password=db_password,
                                         host=DB_HOST, port=db_port
                                         )

    # Create new temp database and schema if not existent.
    if not db_temp_exist:
        create_database(dbname=dbname_temp,
                        user=DB_USER, password=db_password,
                        host=DB_HOST, port=db_port
                        )
        create_schema(dbname=dbname_temp,
                      user=DB_USER, password=db_password,
                      host=DB_HOST, port=db_port
                      )
        logging.info(f'New database {dbname_temp} created.')
    else:
        logging.warning(f'The database {dbname_temp} already exist.')

    # Check if database does exist.
    db_exist = check_database_exist(dbname=DB_NAME,
                                    user=DB_USER, password=db_password,
                                    host=DB_HOST, port=db_port
                                    )

    # Processing metadata.
    stabs_serie_empty = check_table_empty(
        dbname=dbname_temp, dbtable='stabs_serie',
        user=DB_USER, password=db_password,
        host=DB_HOST, port=db_port
        )
    stabs_dossier_empty = check_table_empty(
        dbname=dbname_temp, dbtable='stabs_dossier',
        user=DB_USER, password=db_password,
        host=DB_HOST, port=db_port
        )
    if not all((stabs_serie_empty, stabs_dossier_empty)):
        logging.warning(
            f'Metadata table(s) are not empty in database {dbname_temp}. '
            f'No metadata will be new processed or copied from {DB_NAME}.'
            )
    # Case when all metadata tables are empty.
    else:
        if process_metadata:
            processing_stabs(filepath_serie=FILEPATH_SERIE,
                             filepath_dossier=FILEPATH_DOSSIER,
                             dbname=dbname_temp,
                             db_user=DB_USER, db_password=db_password,
                             db_host=DB_HOST, db_port=db_port
                             )
            logging.info('Metadata are processed.')
        elif db_exist:
            # Copy existing tables stabs_serie and stabs_dossier from database
            # hgb to database hgb_temp.
            conn = psycopg2.connect(dbname=dbname_temp,
                                    user=DB_USER, password=db_password,
                                    host=DB_HOST, port=db_port
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
        # Read series and dossiers created by processing_stabs() for
        # selecting transkribus features.
        series_data = pd.read_csv(FILEPATH_SERIE)
        dossiers_data = pd.read_csv(FILEPATH_DOSSIER)
        processing_transkribus(series_data=series_data,
                               dossiers_data=dossiers_data,
                               dbname=dbname_temp,
                               db_user=DB_USER, db_password=db_password,
                               db_host=DB_HOST, db_port=db_port
                               )
        logging.info('Transkribus data are processed.')
    elif db_exist:
        # Test if transkribus tables are empty.
        coll_empty = check_table_empty(dbname=dbname_temp,
                                       dbtable='transkribus_collection',
                                       user=DB_USER, password=db_password,
                                       host=DB_HOST, port=db_port
                                       )
        doc_empty = check_table_empty(dbname=dbname_temp,
                                      dbtable='transkribus_document',
                                      user=DB_USER, password=db_password,
                                      host=DB_HOST, port=db_port
                                      )
        page_empty = check_table_empty(dbname=dbname_temp,
                                       dbtable='transkribus_page',
                                       user=DB_USER, password=db_password,
                                       host=DB_HOST, port=db_port
                                       )
        ts_empty = check_table_empty(dbname=dbname_temp,
                                     dbtable='transkribus_transcript',
                                     user=DB_USER, password=db_password,
                                     host=DB_HOST, port=db_port
                                     )
        region_empty = check_table_empty(dbname=dbname_temp,
                                         dbtable='transkribus_textregion',
                                         user=DB_USER, password=db_password,
                                         host=DB_HOST, port=db_port
                                         )
        if all((coll_empty, doc_empty, page_empty, ts_empty, region_empty)):
            # Copy existing transkribus tables from database hgb to database
            # hgb_temp.
            conn = psycopg2.connect(dbname=dbname_temp,
                                    user=DB_USER, password=db_password,
                                    host=DB_HOST, port=db_port
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
                f'. The data are not copied from {DB_NAME}.')
    else:
        logging.warning('No transkribus data will be available in database.')

    # Processing project data.
    project_dossier_empty = check_table_empty(
        dbname=dbname_temp, dbtable='project_dossier',
        user=DB_USER, password=db_password,
        host=DB_HOST, port=db_port
        )
    project_entry_empty = check_table_empty(
        dbname=dbname_temp, dbtable='project_entry',
        user=DB_USER, password=db_password,
        host=DB_HOST, port=db_port
        )
    if not all((project_dossier_empty, project_entry_empty)):
        logging.warning(
            f'Project tables are not empty in database {dbname_temp}. '
            f'No project data will be new processed or copied from {DB_NAME}.'
            )
    # Case when all project tables are empty.
    else:
        if process_project:
            processing_project(dbname=dbname_temp,
                               db_password=db_password,
                               db_user=DB_USER,
                               db_host=DB_HOST,
                               db_port=db_port
                               )
            logging.info('Project data are processed.')
        elif db_exist:
            # Copy existing project table from database DB_NAME to dbname_temp.
            conn = psycopg2.connect(dbname=dbname_temp,
                                    user=DB_USER, password=db_password,
                                    host=DB_HOST, port=db_port
                                    )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"""
            INSERT INTO project_dossier
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT dossierid,yearfrom,yearto FROM project_dossier')
            AS t(dossierid text, yearfrom integer, yearto integer)
            """)
            cursor.execute(f"""
            INSERT INTO project_entry
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT entryid,pageid,year,yearsource FROM project_entry')
            AS t(entryid uuid, pageid integer[], year integer, yearsource text)
            """)
            conn.close()
            logging.info('Project data are copied from current database.')
        else:
            logging.warning('No project data will be available in database.')

    # Processing geodata. At the moment, the geodata will always be processed.
    processing_geodata(
        shapefile_path=SHAPEFILE_PATH, shapefile_epsg=SHAPEFILE_EPSG,
        dbname=dbname_temp, db_password=db_password, db_user=DB_USER,
        db_host=DB_HOST, db_port=db_port
        )
    logging.info('Geodata are processed.')

    # Delete existing database.
    if db_exist:
        try:
            delete_database(dbname=DB_NAME,
                            user=DB_USER, password=db_password,
                            host=DB_HOST, port=db_port
                            )
            logging.info(f'Old database {DB_NAME} was deleted.')
        except Exception as err:
            logging.error(f'The database {DB_NAME} can\'t be deleted. {err=}, '
                          f'{type(err)=}')
            raise

    # Copy the new created database.
    copy_database(dbname_source=dbname_temp, dbname_destination=DB_NAME,
                  user=DB_USER, password=db_password,
                  host=DB_HOST, port=db_port
                  )
    logging.info(f'New database {dbname_temp} copied to {DB_NAME}.')

    # Rename the database.
    dbname_copy = DB_NAME + '_' + \
        str(datetime_started.date()).replace('-', '_')
    rename_database(dbname_old=dbname_temp, dbname_new=dbname_copy,
                    user=DB_USER, password=db_password,
                    host=DB_HOST, port=db_port)
    logging.info(f'Database {dbname_temp} was renamed to {dbname_copy}.')

    datetime_ended = datetime.now()
    datetime_duration = datetime_ended - datetime_started
    logging.info('Duration of the run: '
                 f'{str(round(datetime_duration.seconds / 3600, 1))} hour(s).')
    logging.info('Script finished.')


if __name__ == "__main__":
    main()
