""" Determine validity period for dossier.

1. Step
Search for potential additional year from and year to based on
stabs_dossier.descriptiveNote. Reduce the descriptiveNote based on the
determination of the year from and year to based on
stabs_dossier.descriptiveNote as in function get_validity_range() in
project_database.update.py.

2. Step
Determine yearfrom and yearto based on the minimum and maximum year of the
entries belonging to the dossier, if the year could not be determined from the
metadata.
The source from which the year originates is documented in the yearfromSource
or yeartoSource column.

3. Step
Based on relations between dossiers, check the following conditions regarding
validity periods for each dossier.
- Dossier overlaps in time with previous or subsequent dossier,
- The difference to the previous or subsequent dossier is more than 20 years.
- If there is exactly one previous and one subsequent dossier, the programme
also checks whether the previous and subsequent dossiers are the same dossier.
Exception: Some dossiers contain a period of additional addresses/buildings
("including dossier"). Some dossiers were manually classified as such dossiers.
In these cases, a temporal overlap of the dossiers is to be expected. If only
one predecessor dossier exists, no analyses are carried out for this dossier
with the predecessor dossier.
Each fulfilled condition is documented in the note_postprocessing column.
"""


import pandas as pd
import re
import math
from datetime import datetime

from connectDatabase import read_table, read_geotable


# Set parameters for postgresql database
DB_NAME = 'hgb'

# Define if the descriptiveNote should be analyzed.
ANALYZE_DESCRIPTIVENOTE = False

# File name for file containing dossierId of dossier for which no time
# analysis with previous dossier is to be made.
FILENAME_INCLUDINGDOSSIER = './data/20240828_dossier_einschliesslich.csv'

# Filepath for saving the results.
FILEPATH_RESULT = './data/'

# Filenames of the output files.
FILENAME_NOTEANALYSIS = 'dossierFurterMatchesNote.csv'
FILENAME_VALIDITYRANGE = 'dossierValidityRange.csv'


def main():
    # Get current date for filenames.
    current_date = datetime.now().strftime('%Y%m%d')

    # Get parameters of the database.
    db_user = input('PostgreSQL user:')
    db_password = input('PostgreSQL password:')
    db_host = input('PostgreSQL host:')
    db_port = input('PostgreSQL database port:')

    # Read necessary database tables.
    stabs_dossier = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='stabs_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['dossierId', 'serieId', 'stabsId', 'title', 'link',
                 'houseName', 'oldHousenumber', 'owner1862', 'descriptiveNote'
                 ])
    project_dossier = read_geotable(
        dbname=DB_NAME,
        dbtable='project_dossier', geom_col='location',
        user=db_user, password=db_password,
        host=db_host, port=db_port
        )
    project_entry = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_entry',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['entryId', 'dossierId', 'pageId',
                 'year', 'yearSource',
                 'comment', 'manuallyCorrected',
                 'language'])
    project_relationship = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_relationship',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['sourceDossierId', 'targetDossierId'])

    # Merge the two tables.
    dossier = stabs_dossier.merge(
        project_dossier, left_on='dossierId', right_on='dossierid', how='right'
        )

    # Analyze the attribute descriptiveNote.
    if ANALYZE_DESCRIPTIVENOTE:
        dossier['furtherMatches'] = None
        for row in dossier.iterrows():
            note = row[1]['descriptiveNote']
            match = None
            has_match_from = False
            has_match_to = False

            if note:
                # Search for year number from.
                match = re.search(
                    r'^((Seit)|(Errichtet)|(Ab)) 1[0-9]{3}\.', note)
                if match:
                    note = note[:match.start()] + note[match.end():]
                    has_match_from = True

                # Search for year number to.
                match = re.search(r'((Bis)|(Abgebrochen)) 1[0-9]{3}\.', note)
                if match:
                    note = note[:match.start()] + note[match.end():]
                    has_match_to = True

                # Consider patterns like "1734-1819".
                if not match:
                    match = re.match(r'^1[0-9]{3}-1[0-9]{3}\.?$', note)
                    if match:
                        note = note[:match.start()] + note[match.end():]
                        has_match_from = True
                        has_match_to = True

                # Search for other matches.
                match = None
                if has_match_from and has_match_to:
                    match = re.findall(
                        r'(([Ss]eit)|([Ee]rrichtet)|([Aa]b)|([Bb]is)|'
                        r'([Aa]bgebrochen))', note)
                elif has_match_from:
                    match = re.findall(
                        r'(([Ss]eit)|([Ee]rrichtet)|([Aa]b)|([Bb]is)|'
                        r'([Aa]bgebrochen)|([Nn]achher))', note)
                elif has_match_to:
                    match = re.findall(
                        r'(([Ss]eit)|([Ee]rrichtet)|([Aa]b)|([Bb]is)|'
                        r'([Aa]bgebrochen)|([Vv]orher))', note)
                else:
                    match = re.findall(
                        r'(([Ss]eit)|([Ee]rrichtet)|([Aa]b)|([Bb]is)|'
                        r'([Aa]bgebrochen)|([Vv]orher)|([Nn]achher))', note)
                if match:
                    dossier.at[
                        row[0],
                        'furtherMatches'
                        ] = list(
                            set([m for group in match for m in group if m]))

        # Export result.
        dossier.to_csv(
            FILEPATH_RESULT + current_date + '_' + FILENAME_NOTEANALYSIS,
            index=False, header=True)

    # Determine year based on min/max year entry.
    dossier['yearfromSource'] = None
    dossier['yeartoSource'] = None
    for index, row in dossier.iterrows():
        entry_selected = project_entry[
            project_entry['dossierId'] == row['dossierId']]

        # Get minimal year if not already yearfrom determined from metadata.
        if math.isnan(row['yearfrom1']):
            if not math.isnan(entry_selected['year'].min()):
                dossier.at[index, 'yearfrom1'] = entry_selected['year'].min()
                dossier.at[index, 'yearfromSource'] = 'project_entry.year'
        else:
            dossier.at[
                index, 'yearfromSource'] = 'stabs_dossier.descriptiveNote'

        # Get maximal year if not already yearto determined from metadata.
        if math.isnan(row['yearto1']):
            if not math.isnan(entry_selected['year'].max()):
                dossier.at[index, 'yearto1'] = entry_selected['year'].max()
                dossier.at[index, 'yeartoSource'] = 'project_entry.year'
        else:
            dossier.at[index, 'yeartoSource'] = 'stabs_dossier.descriptiveNote'

    # Read including dossier.
    including_dossier = pd.read_csv(FILENAME_INCLUDINGDOSSIER)
    including_dossier = including_dossier['dossierId'].tolist()

    # Analysis of the years.
    dossier['note_postprocessing'] = ''
    for index, row in dossier.iterrows():
        if math.isnan(row['yearfrom1']) or math.isnan(row['yearto1']):
            dossier.at[index, 'note_postprocessing'] += 'Year is missing.\n'
        else:
            previous_dossier = project_relationship[
                project_relationship['targetDossierId'] == row['dossierId']]
            following_dossier = project_relationship[
                project_relationship['sourceDossierId'] == row['dossierId']]

            # Check if current dossier is classed as 'including dossier' and
            # only one previous dossier exist.
            overlap_expected = False
            if (row['dossierId'] in including_dossier
                    and previous_dossier.shape[0] == 1):
                # In this case, a temporal overlap of the dossiers with
                # previous dossier is expected.
                overlap_expected = True

            # Compare dossier with previous dossiers.
            if not previous_dossier.empty and not overlap_expected:
                for r in previous_dossier.iterrows():
                    yearto = dossier[
                        dossier['dossierId'] == r[1]['sourceDossierId']
                        ]['yearto1'].values[0]
                    dossierid_previous = r[1]['sourceDossierId']

                    # Check if yearto of previous dossier is larger than
                    # yearfrom1.
                    if yearto > row['yearfrom1']:
                        dossier.at[
                            index,
                            'note_postprocessing'
                            ] += (
                                f'yearto {int(yearto)} of preceding dossier '
                                f'{dossierid_previous} is larger than '
                                f"yearfrom1 {int(row['yearfrom1'])}.\n")

                    # Check if the difference of yearto of previous dossier
                    # and yearfrom1 is larger than 20 years.
                    elif yearto + 20 < row['yearfrom1']:
                        dossier.at[
                            index,
                            'note_postprocessing'
                            ] += (
                                f'The difference between yearto {int(yearto)} '
                                'of the preceding dossier '
                                f'{dossierid_previous} and yearfrom1 '
                                f"{int(row['yearfrom1'])} is more than 20 "
                                'years.\n')

            # Compare dossier with following dossiers.
            if not following_dossier.empty:
                for r in following_dossier.iterrows():
                    yearfrom = dossier[
                        dossier['dossierId'] == r[1]['targetDossierId']
                        ]['yearfrom1'].values[0]
                    dossierid_following = r[1]['targetDossierId']

                    # Check if yearfrom of following dossier is smaller than
                    # yearto1.
                    if yearfrom < row['yearto1']:
                        dossier.at[
                            index,
                            'note_postprocessing'
                            ] += (
                                f'yearfrom {int(yearfrom)} of following '
                                f'dossier {dossierid_following} is smaller '
                                f"than yearto1 {int(row['yearto1'])}.\n")

                    # Check if the difference of yearto1 and yearfrom of the
                    # following dossier is larger than 20 years.
                    elif row['yearto1'] + 20 < yearfrom:
                        dossier.at[
                            index,
                            'note_postprocessing'
                            ] += (
                                'The difference between yearto1 '
                                f"{int(row['yearto1'])} and yearfrom "
                                f'{int(yearfrom)} of the following dossier '
                                f'{dossierid_following} is more than 20 years.'
                                '\n')

            # Test whether previous dossier is also following dossier.
            if len(previous_dossier) == 1 and len(following_dossier) == 1:
                dossierid_previous = previous_dossier[
                    'sourceDossierId'].values[0]
                dossierid_following = following_dossier[
                    'targetDossierId'].values[0]
                if dossierid_previous == dossierid_following:
                    dossier.at[
                        index,
                        'note_postprocessing'
                        ] += (
                            f'The preceding dossier {dossierid_previous} is '
                            'also the subsequent dossier.\n')

    # Add additional columns for the postpocessing.
    dossier['yearfrom1_new'] = None
    dossier['yearto1_new'] = None
    dossier['yearfrom2_new'] = None
    dossier['yearto2_new'] = None
    dossier['remark'] = None

    # Export result.
    dossier.to_csv(
        FILEPATH_RESULT + current_date + '_' + FILENAME_VALIDITYRANGE,
        index=False, header=True)


if __name__ == "__main__":
    main()
