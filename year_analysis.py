"""Analyze the variable year of the tables project_entry and project_dossier.

Step 1
The attribute year in the table project_entry is extracted from transcribed
text. More information can be found in the module project_database_update.py,
especially in the function get_year().

This module analyses:
- No year was detected, but non-empty text regions exist.
- The year is smaller than the year on a previous page or larger than the year
on the next page within the same document.

An entry is made in the note column for the affected entries. The generated
table is exported as a file year_analysis_entry.csv.

Step 2
The attribute year in the table project_dossier is extracted from the
descriptive note given in the metadata of the HGB. More information can be
found in the module project_database_update.py in the function
get_validity_range().

This module analyses:
- The minimal and maximal year number per dossier.
- The first and the last year number within a dossier.

The generated table is exported as a file year_analysis_dossier.csv.
"""


import pandas as pd
import math
import warnings

from connectDatabase import read_table


# Set parameters for postgresql database
DB_NAME = 'hgb'

# Filepath for saving the results.
FILEPATH_ANALYSIS = '.'


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
        columns=['entryId', 'dossierId', 'pageId',
                 'year', 'yearSource',
                 'comment', 'manuallyCorrected'])
    dossier = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['dossierId', 'yearFrom_stabs', 'yearTo_stabs',
                 'yearFrom2', 'yearTo2', 'location'])
    dossier = dossier.drop('location', axis=1)
    document = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='transkribus_document',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['docId', 'colId', 'title', 'nrOfPages'])
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

    # Analyze availability of year for every entry in project_entry.
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
                # Check if a text region is available but no year is available.
                if math.isnan(row[1]['year']):
                    note = 'Has non-empty text region(s) but no year '\
                           'available.'
            elif math.isnan(row[1]['year']):
                note = 'No year available.'

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
    entry_analysis.sort_values(by=['docId', 'pageNr'],
                               inplace=True, ignore_index=True
                               )
    year_previous = None
    for index, row in entry_analysis.iterrows():
        year_next = None
        docid_current = row['docId']
        year_current = row['year']

        # Determine the year of the previous entry within the same document.
        if index > 0:
            docid_previous = entry_analysis.iloc[index - 1]['docId']
            if docid_current == docid_previous:
                # Update year_previous only if not NaN, else take the last
                # available.
                year_candidate = entry_analysis.iloc[index - 1]['year']
                if not math.isnan(year_candidate):
                    year_previous = year_candidate
            else:
                year_previous = None

        # Skip current iteration when no year available.
        if math.isnan(year_current):
            continue

        # Determine the year of the next entry within the same document.
        if index + 1 < len(entry_analysis):
            docid_next = entry_analysis.iloc[index + 1]['docId']
            if docid_current == docid_next:
                year_next = entry_analysis.iloc[index + 1]['year']

        # Search for entries with no ascending year.
        if year_previous is not None and year_next is not None:
            if year_previous <= year_next and year_current > year_next:
                entry_analysis.at[index, 'note'] = 'Year number is larger '\
                    'than year from next entry.'
                continue
            elif year_previous <= year_next and year_current < year_previous:
                entry_analysis.at[index, 'note'] = 'Year number is smaller '\
                    'than year from previous entry.'
                continue
        if year_next is not None and year_current > year_next:
            entry_analysis.at[index, 'note'] = 'Year number is larger '\
                'than year from next entry.'
            continue
        if year_previous is not None and year_current < year_previous:
            entry_analysis.at[index, 'note'] = 'Year number is smaller '\
                'than year from previous entry.'

    # Export the results.
    entry_analysis.to_csv(FILEPATH_ANALYSIS + '/year_analysis_entry.csv',
                          index=False, header=True)

    # Analyze the time period for each HGB dossier.
    dossier['yearFrom_entryMin'] = None
    dossier['yearTo_entryMax'] = None
    dossier['yearFrom_entryFirst'] = None
    dossier['yearTo_entryLast'] = None
    for index, row in dossier.iterrows():
        dossier_id = document[
            document['title'] == row['dossierId']]['docId']
        if not dossier_id.empty:
            dossier_id = dossier_id.item()
        else:
            continue

        # Determine yearFrom and yearTo based on the minimal and maximal
        # value from project_entry.
        dossier_entry = entry_analysis[entry_analysis['docId'] == dossier_id]
        if dossier_entry.empty:
            continue
        if not math.isnan(dossier_entry['year'].min()):
            dossier.at[index,
                       'yearFrom_entryMin'
                       ] = int(dossier_entry['year'].min())
        if not math.isnan(dossier_entry['year'].max()):
            dossier.at[index,
                       'yearTo_entryMax'
                       ] = int(dossier_entry['year'].max())

        # Determine yearFrom and yearTo based on the first and last value from
        # project_entry.
        if not math.isnan(dossier_entry['year'].iloc[0]):
            dossier.at[index,
                       'yearFrom_entryFirst'
                       ] = int(dossier_entry['year'].iloc[0])
        if not math.isnan(dossier_entry['year'].iloc[-1]):
            dossier.at[index,
                       'yearTo_entryLast'
                       ] = int(dossier_entry['year'].iloc[-1])

    # Export the results.
    dossier.to_csv(FILEPATH_ANALYSIS + '/year_analysis_dossier.csv',
                   index=False, header=True)


if __name__ == "__main__":
    main()
