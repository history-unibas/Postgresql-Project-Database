
"""Analyze the variable year of the table project_entry.

The attribute year in the table project_entry is extracted from transcribed
text. More information can be found in the module project_database_update.py,
especially in the function get_year().

This module analyses:
- No year was detected, but non-empty text regions exist.
- The year is smaller than the previous year on a previous page within the
same document.

An entry is made in the note column for the affected entries. The generated
table is exported as a file in csv format.
"""


import pandas as pd
import math
import warnings

from connectDatabase import read_table


# Set parameters for postgresql database
DB_NAME = 'hgb'

# Filepath for saving the results.
FILEPATH_ANALYSIS = './year_analysis.csv'


def main():
    # Disable FutureWarning.
    warnings.simplefilter(action='ignore', category=FutureWarning)

    # Get parameters of the database.
    db_user = input('PostgreSQL user:')
    db_password = input('PostgreSQL password:')
    db_host = input('PostgreSQL host:')
    db_port = input('PostgreSQL database port:')

    # Read necessary database tables.
    entry = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_entry',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['entryId', 'pageId', 'year', 'yearSource'])
    page = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='transkribus_page',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['pageId', 'key', 'docId', 'pageNr', 'urlImage'])
    transcript = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='transkribus_transcript',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['key', 'tsId', 'pageId', 'parentTsId', 'urlPageXml', 'status',
                 'timestamp', 'htrModel'])
    textregion = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='transkribus_textregion',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['textRegionId', 'key', 'index', 'type', 'textLine', 'text'])

    # Iterate over all elements in project_entry.
    entry_analysis = pd.DataFrame(
        columns=['docId', 'pageNr', 'pageId',
                 'year', 'yearSource', 'hasTextRegion',
                 'note']
        )
    for row in entry.iterrows():
        for page_id in row[1]['pageId']:
            has_tr = False
            note = None

            # Determine latest transcript of current page.
            ts = transcript[transcript['pageId'] == page_id]
            ts_sorted = ts.sort_values(by='timestamp', ascending=False)
            ts_latest = ts_sorted.iloc[0]

            # Determine if transkripted text is available in the latest
            # transcript.
            tr = textregion[textregion['key'] == ts_latest['key']]
            if len(tr) > 0:
                has_tr = True
                # Check if a text region is available but no year was detected.
                if math.isnan(row[1]['year']):
                    note = 'Has non-empty text region(s) but no year detected.'

            # Add new entry to analysis table.
            page_selected = page[page['pageId'] == page_id]
            new_entry = {'docId': page_selected['docId'],
                         'pageNr': page_selected['pageNr'],
                         'pageId': page_id,
                         'year': row[1]['year'],
                         'yearSource': row[1]['yearSource'],
                         'hasTextRegion': has_tr,
                         'note': note}
            entry_analysis = pd.concat([entry_analysis,
                                        pd.DataFrame(new_entry)
                                        ], ignore_index=True)

    # Analyse year ascending within the same Transkribus document.
    entry_analysis.sort_values(by=['docId', 'pageNr'], inplace=True)
    previous_doc_id = None
    previous_year = None
    for row in entry_analysis.iterrows():
        doc_id = row[1]['docId']
        year = row[1]['year']
        if doc_id == previous_doc_id:
            previous_doc_id = doc_id
            if math.isnan(year):
                continue
            elif year < previous_year:
                entry_analysis.at[row[0], 'note'] = 'Year number is smaller '\
                                                    'than previous year.'
                previous_year = year
            else:
                previous_year = year
        else:
            # Case having a new document.
            previous_doc_id = doc_id
            previous_year = year

    # Export the results.
    entry_analysis.to_csv(FILEPATH_ANALYSIS, index=False, header=True)


if __name__ == "__main__":
    main()
