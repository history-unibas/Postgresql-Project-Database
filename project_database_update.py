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
database exists). Because the text line ordering within a given text region
may not always be correct, a text line ordering algorithmus can be applied set
the parameter CORRECT_LINE_ORDER to True. For more information on the
Transkribus platform: https://github.com/history-unibas/Trankribus-API.

4. If desired, project data tables are new created and filled. Otherwise, data
will be copied from the previous database (if existent).

5. Process the geodata. At the moment, the geodata will always be new created
and not copied from the previous database.

6. Create view for analyzing the data.

7. Previous database is deleted.

8. Create a copy of temporary database to new database.

9. Temporary database is renamed with date as postfix.

10. Create a backup file.
"""


import logging
from datetime import datetime
import requests
import psycopg2
import pandas as pd
import xml.etree.ElementTree as et
import re
import os
import statistics
import math
import geopandas


from administrateDatabase import (
    delete_database, create_database, create_schema, rename_database,
    copy_database, remove_privileges)
from connectDatabase import (populate_table, read_table, check_database_exist,
                             check_table_empty, check_dbtable_exist,
                             read_geotable, populate_geotable)


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

# Define if the text line order within the Transkribus text regions should be
# corrected.
CORRECT_LINE_ORDER = True

# Define if specific correction files for project_entry should be applied.
CORRECT_PROJECT_ENTRY = True

# Filepath of correction files for project_entry.
FILEPATH_PROJECT_ENTRY_CORR1 = './data/datetool_202312041016.csv'
FILEPATH_PROJECT_ENTRY_CORR2 = './data/chronotool_202312041016.csv'

# Define direction of the backup file.
BACKUP_DIR = '/mnt/research-storage/Projekt_HGB/DB_Dump/hgb'


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
                           get_dossiers, get_dossier_id,
                           query_documents, get_date)
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
    """Process metadata of the State Archives of Basel.

    This function processes all series and dossiers of the Historical Land
    Registry (HGB) and write them to the project database. In addition, CSV
    files thereof will be written.
    Furthermore, metadata of the documents containing in the Serie "Regesten
    Klingental" are queried and stored in the database.

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
    logging.info('Dossiers queried.')

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

    # Get and write documents of the serie "Regesten Klingental".
    logging.info('Query Klingental regest...')
    url_klingental_regest = 'https://ld.bs.ch/ais/Record/751516'
    klingental_regest = pd.DataFrame(query_documents(url_klingental_regest))
    klingental_regest['expresseddate'] = klingental_regest.apply(
        lambda row: get_date(row['isassociatedwithdate']), axis=1
        )
    klingental_regest = klingental_regest.drop(
        ['isassociatedwithdate', 'type'], axis=1
        )
    populate_table(df=klingental_regest, dbname=dbname,
                   dbtable='stabs_klingental_regest',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port
                   )
    logging.info('Klingental regest queried.')


def processing_transkribus(series_data, dossiers_data, dbname,
                           db_user, db_password,
                           db_host, db_port=5432,
                           correct_line_order=False):
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
        correct_line_order (bool): Define if text line order will be corrected.

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

    # Get documents according project database schema for each collection
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

                    if correct_line_order:
                        # Correct the text line order.
                        textregion_text = pd.DataFrame(
                            columns=['text_line',
                                     'min_x', 'max_x',
                                     'min_y', 'max_y'
                                     ])
                        for textline in textregion.findall(
                                './/{http://schema.primaresearch.org/PAGE/gts/'
                                'pagecontent/2013-07-15}TextLine'):
                            # Extract the transcripted text.
                            textline_unicode = textline.find(
                                './/{http://schema.primaresearch.org/PAGE/gts/'
                                'pagecontent/2013-07-15}Unicode')
                            if textline_unicode is not None:
                                textline_text = textline_unicode.text
                                if not textline_text:
                                    # Skip empty text lines.
                                    continue
                            else:
                                # Do not consider empty text lines.
                                continue

                            # Get the line coordinates.
                            coords_raw = textline.find(
                                './/{http://schema.primaresearch.org/PAGE/gts/'
                                'pagecontent/2013-07-15}Coords').get('points')
                            coords_list = coords_raw.split(' ')
                            coord_x = []
                            coord_y = []
                            for coord in coords_list:
                                coord_x.append(int(coord.split(',')[0]))
                                coord_y.append(int(coord.split(',')[1]))

                            # Determine the coordinate boarders for each text
                            # line.
                            min_y = (statistics.mean(coord_y)
                                     - (max(coord_y)
                                     - statistics.mean(coord_y))/3)
                            max_y = (statistics.mean(coord_y)
                                     + (max(coord_y)
                                     - statistics.mean(coord_y))/3)
                            textregion_text = pd.concat(
                                [textregion_text,
                                 pd.DataFrame([
                                    [textline_text,
                                     min(coord_x), max(coord_x),
                                     min_y, max_y]],
                                    columns=['text_line',
                                             'min_x', 'max_x',
                                             'min_y', 'max_y'])
                                 ], ignore_index=True)

                        # Get the number of text lines.
                        nlines = len(textregion_text)

                        if nlines == 0:
                            # Skip empty textregions.
                            continue

                        i = 1
                        while i < nlines:
                            # Analyse which text lines are on the same line.
                            index_shared_y = [i - 1]
                            while (
                                max(textregion_text.iloc[i - 1]['min_y'],
                                    textregion_text.iloc[i]['min_y'])
                                < min(textregion_text.iloc[i - 1]['max_y'],
                                      textregion_text.iloc[i]['max_y'])):
                                index_shared_y.append(i)
                                if i < nlines - 1:
                                    i += 1
                                else:
                                    break

                            if len(index_shared_y) > 1:
                                # Get the correct order for the text lines.
                                index_sorted_x = list(
                                    textregion_text.loc[index_shared_y].
                                    sort_values(by='min_x').index)

                                if index_shared_y == index_sorted_x:
                                    # Case text line order is correct.
                                    i += 1
                                    continue

                                # Test if text lines do not have an overlap
                                # horizontally.
                                no_overlap = True
                                index_line = 0
                                while index_line < len(index_sorted_x) - 1:
                                    if (textregion_text.iloc[
                                            index_sorted_x[index_line]]
                                            ['max_x']
                                        >= textregion_text.iloc[
                                            index_sorted_x[index_line + 1]]
                                            ['min_x']):
                                        no_overlap = False
                                        break
                                    else:
                                        index_line += 1

                                if no_overlap:
                                    # Correct order of the text lines.
                                    textregion_text.iloc[index_shared_y] = (
                                        textregion_text.iloc[index_sorted_x]
                                        .copy())
                                    text_line_nr_changed = [
                                        item + 1 for item in index_sorted_x]
                                    logging.info(
                                        'Correct text line order: '
                                        f"document {row['title']} "
                                        f"({row['docId']}), "
                                        f"page number {page['pageNr']} "
                                        f"({page['pageId']}), "
                                        f'transcript {key_transcript}, '
                                        f'text region type {type_textregion} '
                                        f'(index {index_textregion}), '
                                        'text line order changed for lines '
                                        f'{text_line_nr_changed}'
                                        )
                            i += 1
                        text_line = list(textregion_text['text_line'])

                    else:
                        # Do not correct the oder of the text lines.
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

                    # Create string of whole text region.
                    text = '\n'.join(text_line)

                    # Get id of text region.
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


def get_year(page_id, df_transcript, df_textregion, year_pattern=r'1[0-9]{3}'):
    """Extract the occurrence of a year in text regions of latest transcript.

    Using the existing data in the project database, a year of the pattern
    "1[0-9]{3}" is extracted per entry of the database table project_entry.
    - If a year of this pattern exist in the text regions "headers", the first
    occurrence is taken into account.
    - Otherwise, if a header contains the word like "Zins", any first
    occurrence of a year number of the same pattern will be returned. In this
    case, the page is assumed to be part of the so called "Zinsverzeichnis".
    - If there is no header text region, None will be returned.

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
            match = re.search(year_pattern, header[1]['text'])
            if match:
                return (int(match.group()), header[1]['textRegionId'])

        # Search for year in text region "paragraph" when header text region
        # contains a string like "Zins".
        for header in tr_header.iterrows():
            match_header = re.search(r'[Zz][iü]n[n]?s', header[1]['text'])
            if match_header:
                tr_paragraph = tr[tr['type'] == 'paragraph']
                for paragraph in tr_paragraph.iterrows():
                    match_paragraph = re.search(year_pattern,
                                                paragraph[1]['text']
                                                )
                    if match_paragraph:
                        return (int(match_paragraph.group()),
                                paragraph[1]['textRegionId']
                                )

    return (None, None)


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


def processing_project(dbname, db_password, db_user='postgres',
                       db_host='localhost', db_port=5432,
                       correct_entry=False,
                       filepath_corr1='',
                       filepath_corr2=''):
    """Processes the project data within the project database.

    This function processes all tables of the project database with the prefix
    "project_". In particular:

    1. Determine the entries of the database table project_entry. Based on the
    most recent transcript, an HGB entry is interpreted to be on multiple
    pages if the index card has no "header" and "marginalia" text region and
    the page before it has no "credit" text region and is in the same document.
    Pages without (non-empty) text regions and pages with status "DONE" are
    excluded. In particular:
    - Search the year per entry based on text regions.

    If correction CSV files are provided, the correction file 1 can override
    the usual logic based on the pageid:
    - If the column "omit" is True, the page will be omitted for the entity
    project_entry.
    - If the column "datum_neu" contains a value, so this year is taken as the
    date.
    - If the column "ist_folgeseite" is True, the page will be treated as same
    entry than on the previous page.
    - If the column "kommentar" contains a value, the value will be mapped to
    the column "comment".
    As the correction file 1, the correction file 2 override the usual logic
    as well as corrections provided in correction file 1 based on the pageid:
    - If the column "omit" is True, the page will be omitted for the entity
    project_entry.
    - If the column "datum_neu" contains a value, so this year is taken as
    date.
    - If the column "kommentar" contains the value "skipped: Folgeseite", the
    page will be treated as same entry than on the previous page.
    - If the column "kommentar" contains the value "skipped: undatiert ", the
    columns year and yearSource will be set to None and the column comment
    will get the value "undatiert".
    - If the column "kommentar" contains another value than considered above,
    the value will be mapped to the column "comment".
    The column project_entry.manuallyCorrected is set to True, if the date or
    the grouping of the pages is corrected by one of the correction files.

    2. Determine the entries of the database table project_dossier. Only
    dossiers are considered that are referenced by elements of the entity
    project_entry. In particular:
    - Search entries for yearFrom1 and yearTo1 based on
    stabs_dossier.descriptiveNote.
    - Determine the geographical localisation of the dossier if available in
    the entity geo_address.

    Args:
        dbname (str): Name of the project database.
        db_password (str): Password for the database connection.
        db_user (str): User of the database connection.
        db_host (str): Host of the database connection.
        db_port (int,str): Port of the database connection.
        correct_entry (bool): Defines whether a correction file should
        be applied.
        filepath_corr1 (str): Filepath of the first correction file.
        filepath_corr2 (str): Filepath of the second correction file.

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
    document = pd.DataFrame(
        read_table(dbname=dbname, dbtable='transkribus_document',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['docId', 'colId', 'title', 'nrOfPages'])
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
    geo_address = read_geotable(dbname=dbname, dbtable='geo_address',
                                geom_col='geom',
                                user=db_user, password=db_password,
                                host=db_host, port=db_port)

    # Read the entries to correct.
    if correct_entry:
        entry_correction1 = pd.read_csv(filepath_corr1)
        entry_correction2 = pd.read_csv(filepath_corr2)

    # Generate entries of table project_entry.
    entry = pd.DataFrame(columns=['dossierId', 'pageId',
                                  'year', 'yearSource',
                                  'comment',
                                  'manuallyCorrected'])
    entry['manuallyCorrected'] = entry['manuallyCorrected'].astype(bool)
    entry_prev_docid = None
    page_prev_has_credit = None
    page_prev_status = None
    for row in page.iterrows():
        dossierid = document[
                document['docId'] == row[1]['docId']
                ]['title'].values[0]

        # Determine corrections if requested.
        if correct_entry:
            page_corr1 = entry_correction1[
                row[1]['pageId'] == entry_correction1['pageid']]
            page_corr2 = entry_correction2[
                row[1]['pageId'] == entry_correction2['pageid']]

        # Determine latest transcript of current page.
        ts = transcript[transcript['pageId'] == row[1]['pageId']]
        ts_sorted = ts.sort_values(by='timestamp', ascending=False)
        ts_latest = ts_sorted.iloc[0]

        # Get the text regions of latest transcript.
        tr = textregion[textregion['key'] == ts_latest['key']]
        if tr.empty or ts_latest['status'] == 'DONE':
            # The content of current page is not considered to have a entry.
            pass
        elif (correct_entry
              and (not page_corr1.empty and page_corr1['omit'].values[0])
              or (not page_corr2.empty and page_corr2['omit'].values[0])):
            # The current page is omitted for entity project_entry.
            pass
        elif (correct_entry
              and (not page_corr1.empty
                   and page_corr1['ist_folgeseite'].values[0])
              or (not page_corr2.empty
                  and (page_corr2['kommentar'].values[0] ==
                       'skipped: Folgeseite'))):
            # The content of the current page is considered as same entry
            # than on the previous page.
            entry.at[entry.index[-1], 'pageId'] += [row[1]['pageId']]
            entry.at[entry.index[-1], 'manuallyCorrected'] = True
            if entry.at[entry.index[-1], 'dossierId'] != dossierid:
                logging.warning(
                    f"Page with pageId={row[1]['pageId']}, "
                    f'dossierId={dossierid} is manually defined as same entry '
                    'than page with pageId='
                    f"{entry.at[entry.index[-1], 'pageId'][0]}, "
                    f"dossierId={entry.at[entry.index[-1], 'dossierId']}. But "
                    'this pages belong not to same Dossier.'
                    )
        elif (page_prev_has_credit is False
              and entry_prev_docid == row[1]['docId']
              and page_prev_status != 'DONE'
              and not any(tr['type'].isin(['marginalia']))
              and not any(tr['type'].isin(['header']))):
            # The content of the current page is considered as same entry
            # than on the previous page.
            entry.at[entry.index[-1], 'pageId'] += [row[1]['pageId']]
        else:
            # The content of the current page is considered as new entry.
            entry = pd.concat(
                [entry,
                 pd.DataFrame([[dossierid, [row[1]['pageId']],
                                None, None,
                                None, False]],
                              columns=['dossierId', 'pageId',
                                       'year', 'yearSource',
                                       'comment', 'manuallyCorrected'])
                 ], ignore_index=True)
            entry_prev_docid = row[1]['docId']

        # Set parameters for the next iteration.
        if not tr.empty:
            page_prev_has_credit = any(tr['type'].isin(['credit']))
        else:
            page_prev_has_credit = None
        page_prev_status = ts_latest['status']

    # Search for occurence in year of the latest page version.
    entry[['year', 'yearSource']] = entry.apply(
        lambda row: get_year(page_id=row['pageId'],
                             df_transcript=transcript,
                             df_textregion=textregion),
        axis=1,
        result_type='expand'
        )

    # Correct years and add comments if requested.
    if correct_entry:
        for row in entry_correction1.iterrows():
            entry_match = entry[
                    entry['pageId'].apply(lambda x: row[1]['pageid'] in x)]
            if not math.isnan(row[1]['datum_neu']):
                # Update year and yearSource.
                entry.loc[entry_match.index,
                          ['year', 'yearSource', 'manuallyCorrected']
                          ] = [row[1]['datum_neu'], None, True]
            if isinstance(row[1]['kommentar'], str):
                # Copy the comment.
                entry.loc[entry_match.index, 'comment'] = row[1]['kommentar']

        for row in entry_correction2.iterrows():
            entry_match = entry[
                    entry['pageId'].apply(lambda x: row[1]['pageid'] in x)]
            if entry_match.empty:
                continue
            if not math.isnan(row[1]['datum_neu']):
                # Update year and yearSource.
                entry.loc[entry_match.index,
                          ['year', 'yearSource', 'manuallyCorrected']
                          ] = [row[1]['datum_neu'], None, True]
            if isinstance(row[1]['kommentar'], str):
                if row[1]['kommentar'] == 'skipped: undatiert ':
                    # Remove date and add comment.
                    entry.loc[entry_match.index,
                              ['year', 'yearSource',
                               'comment',
                               'manuallyCorrected']] = [
                                   None, None, 'undatiert', True]
                elif row[1]['kommentar'] == 'skipped: Folgeseite':
                    # Ignore the comment in this case.
                    pass
                else:
                    # Copy the comment.
                    entry.loc[entry_match.index, 'comment'] = row[1][
                        'kommentar']

    # Reduce the elements to the dossier referenced in project_entry.
    stabs_dossier_reduced = stabs_dossier[
        stabs_dossier['dossierId'].isin(entry['dossierId'])
        ].copy()
    dossier = stabs_dossier_reduced[['dossierId', 'descriptiveNote']].copy()

    # Determine the validity range based on the descriptive note.
    dossier[['yearFrom1', 'yearTo1']] = dossier.apply(
        lambda row: get_validity_range(row['descriptiveNote']),
        axis=1,
        result_type='expand'
        )

    # Adapt the dataframe to the destination database schema.
    dossier = dossier.drop('descriptiveNote', axis=1)
    dossier[['yearFrom2', 'yearTo2', 'location']] = [None, None, None]
    dossier = geopandas.GeoDataFrame(data=dossier, geometry='location',
                                     crs='EPSG:2056')

    for row in dossier.iterrows():
        # Copy the location if available in geo_address.
        stabsid = stabs_dossier[
            stabs_dossier['dossierId'] == row[1]['dossierId']
            ]['stabsId'].values[0]
        location = geo_address[
            geo_address['signatur'] == stabsid
            ]['geom'].copy()
        if not location.empty:
            dossier.at[row[0], 'location'] = location.values[0]

    # Write data created to project database.
    populate_geotable(df=dossier, dbname=dbname, dbtable='project_dossier',
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
        logging.warning(f'Table {dbtable} already exist in database {dbname}. '
                        f'The shapefile {shapefile_path} will not be imported.'
                        )
    else:
        # Read the shapefile and write it in a new database table.
        connection = f'postgresql://{db_user}:{db_password}@'\
                     f'{db_host}:{db_port}/{dbname}'
        command = f"""
            shp2pgsql -D -I -s {shapefile_epsg} {shapefile_path} {dbtable} \
            | psql {connection}"""
        result = os.system(command)
        if result == 0:
            # Grant read_only user to geodata table.
            conn = psycopg2.connect(dbname=dbname, user=db_user,
                                    password=db_password,
                                    host=db_host, port=db_port
                                    )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("""
            GRANT SELECT ON TABLE public.geo_address TO read_only
            """
                           )
            conn.close()
            logging.info(f'Shapefile {shapefile_path} successfully imported '
                         f'into database {dbname}, table {dbtable}.'
                         )
        else:
            logging.error(f'The shapefile {shapefile_path} was not imported '
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


def create_view(dbname, user, password, host, port=5432):
    """Create particular database view.

    Args:
        dbname (str): Name of the database.
        user (str): Database user.
        password (str): Passwort for database user.
        host (str): Host of the database connection.
        port (str): Port of the database connection.

    Returns:
        None.
    """
    view_name = 'transcript_date_geom'
    dbview_exist = check_dbtable_exist(dbname=dbname, dbtable=view_name,
                                       user=user, password=password,
                                       host=host, port=port
                                       )
    if dbview_exist:
        logging.warning(f'View {view_name} already exist in database {dbname}.'
                        f' The view will not be new created.')
    else:
        conn = psycopg2.connect(dbname=dbname,
                                user=user, password=password,
                                host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE VIEW {view_name} AS
        SELECT
            td.title AS hgb_dossier,
            tp.pagenr AS seite,
            tp.pageid,
            tt.text AS transkript,
            tt.type AS layout_typ,
            (SELECT pe.year AS jahr
                FROM project_entry pe
                WHERE tp.pageid = ANY (pe.pageid)
                ) AS jahr,
            ga.geom,
            tp.urlimage AS bild_link
        FROM transkribus_textregion tt
        JOIN transkribus_transcript tt2 ON tt.key::text = tt2.key::text
        JOIN transkribus_page tp ON tt2.pageid = tp.pageid
        JOIN transkribus_document td ON tp.docid = td.docid
        JOIN stabs_dossier sd ON td.title::text = sd.dossierid::text
        LEFT JOIN geo_address ga ON sd.stabsid::text = ga.signatur::text
        """
                       )
        cursor.execute("""GRANT SELECT ON transcript_date_geom TO read_only""")
        conn.close()


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
            cursor.execute(f"""
            INSERT INTO stabs_klingental_regest
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT link,identifier,title,descriptivenote,expresseddate
            FROM stabs_klingental_regest')
            AS t(link text, identifier text, title text, descriptivenote text,
            expresseddate text)
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
                               db_host=DB_HOST, db_port=db_port,
                               correct_line_order=CORRECT_LINE_ORDER
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

    # Processing geodata. At the moment, the geodata will always be processed.
    processing_geodata(
        shapefile_path=SHAPEFILE_PATH, shapefile_epsg=SHAPEFILE_EPSG,
        dbname=dbname_temp, db_password=db_password, db_user=DB_USER,
        db_host=DB_HOST, db_port=db_port
        )
    logging.info('Geodata are processed.')

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
                               db_port=db_port,
                               correct_entry=CORRECT_PROJECT_ENTRY,
                               filepath_corr1=FILEPATH_PROJECT_ENTRY_CORR1,
                               filepath_corr2=FILEPATH_PROJECT_ENTRY_CORR2
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
            'SELECT dossierid,yearfrom1,yearto1,yearfrom2,yearto2,location
            FROM project_dossier')
            AS t(dossierid text, yearfrom1 integer, yearto1 integer,
            yearfrom2 integer, yearto2 integer, location geometry)
            """)
            cursor.execute(f"""
            INSERT INTO project_entry
            SELECT * FROM dblink('{dblink_connname}',
            'SELECT entryid,dossierid,pageid,year,yearsource,comment,
            manuallycorrected FROM project_entry')
            AS t(entryid uuid, dossierid text, pageid integer[], year integer,
            yearsource text, comment text, manuallycorrected boolean)
            """)
            conn.close()
            logging.info('Project data are copied from current database.')
        else:
            logging.warning('No project data will be available in database.')

    # Create view.
    create_view(dbname=dbname_temp,
                user=DB_USER, password=db_password,
                host=DB_HOST, port=db_port
                )

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

    # Remove privileges for the read_only user for database with date postfix.
    remove_privileges(dbname=dbname_copy, user_revoke='read_only',
                      user_admin=DB_USER, password_admin=db_password,
                      host=DB_HOST, port=db_port)

    # Create backup of database.
    command = f'pg_dump -d {dbname_copy} -F p ' + \
        f'-f {BACKUP_DIR}/dump_{dbname_copy}.sql'
    result = os.system(command)
    if result == 0:
        logging.info(f'Backup of {dbname_copy} was created.')
    else:
        logging.error(f'Backup of {dbname_copy} failed: {result}.')

    datetime_ended = datetime.now()
    datetime_duration = datetime_ended - datetime_started
    logging.info(f'Duration of the run: {str(datetime_duration)}.')
    logging.info('Script finished.')


if __name__ == "__main__":
    main()
