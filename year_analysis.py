"""Analyze the variable year of the tables project_entry and project_dossier.

Step 1
The attribute year in the table project_entry is extracted from transcribed
text. More information can be found in the module project_database_update.py,
especially in the function get_year().

This module analyses:
- No year was detected, but non-empty text regions exist.
- The year could be incorrect due to the order of the years within the
document.

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

from connectDatabase import read_table


# Set parameters for postgresql database
DB_NAME = 'hgb'

# Filepath for saving the results.
FILEPATH_ANALYSIS = '.'


def main():
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
                 'yearFrom2', 'yearTo2',
                 'locationAccuracy', 'locationOrigin', 'location'])
    dossier = dossier.drop(['locationAccuracy', 'locationOrigin', 'location'],
                           axis=1)
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

    # Analyze the year numbers per document.
    entry_analysis = pd.DataFrame(
        columns=['docId', 'pageNr', 'pageId',
                 'year', 'yearSource', 'hasTextRegion',
                 'note']
        )
    for doc in document.iterrows():
        dossier_id = doc[1]['title']
        doc_entry = entry[entry['dossierId'] == dossier_id].copy()

        if doc_entry.empty:
            continue

        # Order the entries of this document by the page number.
        doc_entry['pageNr'] = doc_entry.apply(
            lambda row: page[
                page['pageId'] == row['pageId'][0]
                ]['pageNr'].values[0], axis=1
                )
        doc_entry.sort_values(by='pageNr', inplace=True)

        # Detect potential wrong years by pairwise comparisation.
        n_entries = len(doc_entry)
        score_minus = [0] * n_entries
        score_plus = [0] * n_entries
        index = 0
        for row in doc_entry.iterrows():
            for i in range(n_entries):
                if i < index and row[1]['year'] < doc_entry.iloc[i]['year']:
                    score_minus[index] += 1
                elif i > index and row[1]['year'] > doc_entry.iloc[i]['year']:
                    score_plus[index] += 1
            index += 1

        # Remove not relevant scores by pairwise comparisation.
        score_cum = [x + y for x, y in zip(score_minus, score_plus)]
        score = score_cum.copy()
        index = 0
        for row in doc_entry.iterrows():
            for i in range(n_entries):
                if i < index and row[1]['year'] < doc_entry.iloc[i]['year']:
                    if score_cum[i] < score_cum[index]:
                        score[i] -= 1
                    elif score_cum[i] > score_cum[index]:
                        score[index] -= 1
            index += 1

        # Detect cases when subsequent entry of wrong detected entry might be
        # wrong instead.
        for i in range(1, n_entries):
            if (score_plus[i - 1] > 0
                and score_minus[i] > score_minus[i - 1]
                and score_minus[i] >= score_plus[i - 1]):
                score[i] = score_minus[i]

        # Iterate over each page per entry.
        index = 0
        page_nr_previous = None
        for row in doc_entry.iterrows():
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
                    # Check if a text region is available but no year is
                    # available.
                    if math.isnan(row[1]['year']):
                        note = 'Has non-empty text region(s) but no year '\
                            'available.'
                elif math.isnan(row[1]['year']):
                    note = 'No year available.'

                # Create note if year might be wrong.
                if score[index] > 0:
                    note = 'Year may be wrong.'

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

                # Check if the pages are ordered.
                page_nr = page_selected['pageNr'].values[0]
                if (page_nr_previous is not None
                    and page_nr <= page_nr_previous):
                    print('Analysis may be wrong due to wrong page order: '
                          f'dossier_id={dossier_id}, '
                          f'docId={page_selected["docId"].values[0]}, '
                          f'pageNr={page_nr_previous}, {page_nr}'
                          )
                page_nr_previous = page_nr
            index += 1

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
