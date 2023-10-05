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
found in the module module project_database_update.py in the function
get_validity_range().

This module analyses:
- If the year from and year to differs between the source descriptive note
(metadata HGB) and the minimum / maximum value determined in project_entry
within the same Transkribus document.

An entry is made in the note column for the affected entries. The generated
table is exported as a file year_analysis_dossier.csv.
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
        columns=['entryId', 'pageId', 'year', 'yearSource'])
    dossier = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['dossierId', 'yearFrom_stabs', 'yearTo_stabs'])
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
    for index, row in entry_analysis.iterrows():
        year_previous = None
        year_next = None
        docid_current = row['docId']
        year_current = row['year']
        if math.isnan(year_current):
            continue

        # Determine the year of the previous entry within the same document.
        if index > 0:
            docid_previous = entry_analysis.iloc[index - 1]['docId']
            if docid_current == docid_previous:
                year_previous = entry_analysis.iloc[index - 1]['year']

        # Determine the year of the next entry within the same document.
        if index + 1 < len(entry_analysis):
            docid_next = entry_analysis.iloc[index + 1]['docId']
            if docid_current == docid_next:
                year_next = entry_analysis.iloc[index + 1]['year']

        # Search for entries with no ascending year.
        if year_previous and year_next:
            if year_previous < year_next and year_current > year_next:
                entry_analysis.at[index, 'note'] = 'Year number is larger '\
                    'than year from next entry.'

            elif year_previous < year_next and year_current < year_previous:
                entry_analysis.at[index, 'note'] = 'Year number is smaller '\
                    'than year from previous entry.'
        elif year_next:
            if year_current > year_next:
                entry_analysis.at[index, 'note'] = 'Year number is larger '\
                    'than year from next entry.'
        elif year_previous:
            if year_current < year_previous:
                entry_analysis.at[index, 'note'] = 'Year number is smaller '\
                    'than year from previous entry.'

    # Export the results.
    entry_analysis.to_csv(FILEPATH_ANALYSIS + '/year_analysis_entry.csv',
                          index=False, header=True)

    # Analyze the time period for each HGB dossier.
    dossier['yearFrom_entry'] = None
    dossier['yearTo_entry'] = None
    dossier['note'] = None
    for index, row in dossier.iterrows():
        dossier_id = document[
            document['title'] == row['dossierId']]['docId']
        if not dossier_id.empty:
            dossier_id = dossier_id.item()
        else:
            continue

        # Determine yearFrom and yearTo based on the values from project_entry.
        dossier_entry = entry_analysis[entry_analysis['docId'] == dossier_id]
        if dossier_entry.empty:
            continue
        if not math.isnan(dossier_entry['year'].min()):
            yearfrom_entry = int(dossier_entry['year'].min())
        if not math.isnan(dossier_entry['year'].max()):
            yearto_entry = int(dossier_entry['year'].max())
        dossier.at[index, 'yearFrom_entry'] = yearfrom_entry
        dossier.at[index, 'yearTo_entry'] = yearto_entry

        # Get contradictions between the sources metadata from stabs and
        # project_entry.
        if (not math.isnan(dossier.at[index, 'yearFrom_stabs'])
                and not math.isnan(dossier.at[index, 'yearFrom_entry'])
                and dossier.at[index, 'yearFrom_stabs'] != yearfrom_entry):
            dossier.at[index, 'note'] = 'YearFrom differs.'
        if (not math.isnan(dossier.at[index, 'yearTo_stabs'])
                and not math.isnan(dossier.at[index, 'yearTo_entry'])
                and dossier.at[index, 'yearTo_stabs'] != yearto_entry):
            if dossier.at[index, 'note']:
                dossier.at[index, 'note'] += ' YearTo differs.'
            else:
                dossier.at[index, 'note'] = 'YearTo differs.'

    # Export the results.
    dossier.to_csv(FILEPATH_ANALYSIS + '/year_analysis_dossier.csv',
                   index=False, header=True)


if __name__ == "__main__":
    main()
