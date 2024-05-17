"""Determine relationships between dossiers.

This script is used to determine relationships between dossiers in the
historical land register. The following central steps are implemented in this
module.

1. based on metadata from the State Archives, the street and house number(s)
per dossier were determined based on the 1862 address. The values were partly
determined automatically, partly manually and imported as a correction file.

2. based on the addresses, dossiers were identified which were related to each
other, i.e. dossiers had the same address or the same street and house number
were mentioned together in a dossier. Clusters were formed from this. The
error rate was reduced with the help of a correction file.

3. relations were determined for dossiers that follow one another in time. On
the one hand based on the address and the descriptive note, on the other hand
using the clusters formed and the type of dossier.

As many relations cannot be generated automatically with this script,
relations are defined manually in the next step.
"""

import pandas as pd
import re
import itertools
import statistics
from datetime import datetime


from connectDatabase import read_table


# Set parameters for postgresql database.
DB_NAME = 'hgb'

# Define filepath of necessary data sources.
FILEPATH_DOSSIER = ('./data/dossier_relationship/'
                    '20240221 year_analysis_dossier.csv')
FILEPATH_HOUSENUMBER_CORRECTED = ('./data/dossier_relationship/'
                                  'Korrektur_Adressen.xlsx')
FILEPATH_ADDRESS_CORRECTED = ('./data/dossier_relationship/'
                              '20240420_DossierZwischenresultat_BH.xlsx')
FILEPATH_DOSSIER_TYPE = ('./data/dossier_relationship/'
                         '20240420 dossier_type.xlsx')

# Define output file names.
current_date = datetime.now().strftime('%Y%m%d')
FILEPATH_DOSSIER_RESULT = current_date + '_DossierZwischenresultat.csv'
FILEPATH_RELATIONSHIP = current_date + '_BeziehungenZwischenresultat.csv'
FILEPATH_CLUSTER = current_date + '_cluster.csv'


def main():
    # Get parameters of the database.
    db_user = input('PostgreSQL database user:')
    db_password = input('PostgreSQL database password:')
    db_host = input('PostgreSQL database host:')
    db_port = input('PostgreSQL database port:')

    # Read metadata of Staatsarchiv.
    stabs_dossier = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='stabs_dossier',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['dossierId', 'serieId', 'stabsId', 'title', 'link',
                 'houseName', 'oldHousenumber', 'owner1862', 'descriptiveNote'
                 ])
    stabs_dossier = stabs_dossier[['dossierId', 'title', 'oldHousenumber',
                                   'descriptiveNote']]

    # Read the dossier of interest.
    dossier = pd.read_csv(FILEPATH_DOSSIER)
    dossier = dossier[['dossierId']]

    # Merge the tables.
    dossier = pd.merge(dossier, stabs_dossier, how='left', on='dossierId')

    # Determine street name and house number.
    dossier['street'] = None
    dossier['numbers'] = None
    dossier['postfix'] = None
    for d in dossier.iterrows():
        address = d[1]['title']

        # Detect street name.
        street_match = re.match(r'^(?P<street>[a-zA-Zäöü\.\-\s]*[a-zA-Zäöü]+)',
                                address)
        street = street_match.group('street')

        # Correct for street names containing part of.
        partof_match = re.match(r'^(?P<street>[a-zA-Zäöü\.\-\s]*[a-zA-Zäöü]+)'
                                r'(?P<partof>(( Theil)|( Th.)|( Th)|( T.)|'
                                r'( Tv.))'
                                r'(( von)|( v))?)(?P<postfix>.*)$',
                                street)
        if partof_match:
            street = partof_match.group('street')
            # Correct special case "^Marktplatz Theil von Th. v..+$".
            if re.match(r'^(?P<street>Marktplatz) Theil von', street):
                street = 'Marktplatz'

        # Correct special cases with various postfix,
        # e.g. "Eisengasse Bank vor 26".
        special_match = re.search(r'( vor)|( unter)|( bei)|( alt)|'
                                  r'( abgebrochen)|( innerhalb)',
                                  street)
        if special_match:
            street = street.split()[0]

        # Store the street name.
        dossier.at[d[0], 'street'] = street

        # Determine the content after the street name.
        street_postfix = address.replace(street, '', 1).lstrip()

        # Determine house numbers following the pattern like
        # "64, 66" or "34/ 36".
        numbers_match = re.match(r'^(?P<numbers>(((, )|( ?/ ))?[0-9]+'
                                 r'( ?[a-z])?)+)$',
                                 street_postfix)
        if numbers_match:
            numbers = numbers_match.group('numbers')
            if re.search(r',', numbers):
                dossier.at[d[0], 'numbers'] = numbers.split(', ')
            elif re.search(r'/', numbers):
                numbers = numbers.replace(' ', '')
                dossier.at[d[0], 'numbers'] = numbers.split('/')
            else:
                dossier.at[d[0], 'numbers'] = numbers
            continue

        # Determine numbers that are part of a house number.
        partof_match = re.match(r'^(?P<partof>((Theil)|(Th.)|(Th)|(T.)|(Tv.)) '
                                r'((von )|(v. ))?)(?P<postfix>.*)$',
                                street_postfix)
        if partof_match:
            numbers_match = re.match(r'^(?P<numbers>(((, )|( / ))?[0-9]+'
                                     r'(( ?[a-zA-Z] )|( ?[a-zA-Z]$))?)+)'
                                     r'(?P<postfix>.*)$',
                                     partof_match.group('postfix'))
            if numbers_match:
                numbers = numbers_match.group('numbers')
                postfix = numbers_match.group('postfix')

                # Correct wrong detected house numbers.
                if re.match(r'^[.\w]', postfix):
                    numbers, _, to_postfix = numbers.rpartition(',')
                    postfix = ',' + to_postfix + postfix

                # Correct cases where additional house numbers are at the end.
                postfix_match = re.match(r'^(?P<postfix>.*) ((u.)|(und)) '
                                         r'(?P<numbers>((, )?[0-9]+)+)$',
                                         postfix)
                if postfix_match:
                    postfix_number = postfix_match.group('numbers')
                    numbers = numbers + ', ' + postfix_number
                    postfix = postfix_match.group('postfix')

                # Transform numbers to list and write to dataframe.
                if re.search(r',', numbers):
                    dossier.at[d[0], 'numbers'] = numbers.split(', ')
                elif re.search(r'/', numbers):
                    numbers = numbers.replace(' ', '')
                    dossier.at[d[0], 'numbers'] = numbers.split('/')
                else:
                    dossier.at[d[0], 'numbers'] = numbers
                dossier.at[d[0], 'postfix'] = postfix
            continue

        dossier.at[d[0], 'postfix'] = street_postfix

    # Read excel table containing manually corrected house numbers.
    dossier_correction = pd.read_excel(FILEPATH_HOUSENUMBER_CORRECTED)
    dossier_correction = dossier_correction[
        dossier_correction['Korrektur Nummer'].notna()]
    for correction in dossier_correction.iterrows():
        number_correction = str(correction[1]['Korrektur Nummer'])
        dossierid_correction = correction[1]['dossierId']
        dossier_index = dossier[
            dossier['dossierId'] == dossierid_correction].index.values[0]
        if number_correction == '-':
            # Case remove existing house number.
            dossier.at[dossier_index, 'numbers'] = None
        elif re.search(r', ', number_correction):
            # Create list if more than one number available.
            dossier.at[
                dossier_index, 'numbers'] = number_correction.split(', ')
        else:
            # Set manually defined house number.
            dossier.at[dossier_index, 'numbers'] = number_correction

    # Determine partof dossiers.
    dossier['number_partof'] = None
    for d in dossier.iterrows():
        street_postfix = d[1]['title'].replace(d[1]['street'], '', 1).lstrip()

        # Skrip, when no number is detected.
        if d[1].isna()['numbers']:
            continue

        # Handle specific cases.
        if d[1]['dossierId'] == 'HGB_1_074_075':
            dossier.at[d[0], 'number_partof'] = ['8', '10']
            continue
        elif d[1]['dossierId'] == 'HGB_1_122_026':
            dossier.at[d[0], 'number_partof'] = ['5', '6']
            continue
        elif d[1]['dossierId'] == 'HGB_1_136_012':
            dossier.at[d[0], 'number_partof'] = ['3', '5']
            continue
        elif d[1]['dossierId'] == 'HGB_1_136_013':
            dossier.at[d[0], 'number_partof'] = ['3', '5']
            continue
        elif d[1]['dossierId'] == 'HGB_1_159_054':
            dossier.at[d[0], 'number_partof'] = ['31', '33']
            continue
        elif d[1]['dossierId'] == 'HGB_1_229_020':
            dossier.at[d[0], 'number_partof'] = ['17', '21']
            continue
        elif d[1]['dossierId'] == 'HGB_1_154_027':
            dossier.at[d[0], 'number_partof'] = ['21', '19']
            continue
        elif d[1]['dossierId'] == 'HGB_1_154_031':
            dossier.at[d[0], 'number_partof'] = ['21', '23']
            continue
        elif d[1]['dossierId'] == 'HGB_1_154_028':
            dossier.at[d[0], 'number_partof'] = ['21', '19']
            continue
        elif d[1]['dossierId'] == 'HGB_1_154_032':
            dossier.at[d[0], 'number_partof'] = ['21', '23']
            continue
        elif d[1]['dossierId'] == 'HGB_1_154_029':
            dossier.at[d[0], 'number_partof'] = ['21', '19']
            continue
        elif d[1]['dossierId'] == 'HGB_1_147_026':
            dossier.at[d[0], 'number_partof'] = ['25', '23']
            continue
        elif d[1]['dossierId'] == 'HGB_1_091_056':
            dossier.at[d[0], 'number_partof'] = ['29', '31']
            continue
        elif d[1]['dossierId'] == 'HGB_1_024_096':
            dossier.at[d[0], 'number_partof'] = '10 A'
            continue
        elif d[1]['dossierId'] == 'HGB_1_024_097':
            dossier.at[d[0], 'number_partof'] = '10 B'
            continue
        elif d[1]['dossierId'] == 'HGB_1_024_099':
            dossier.at[d[0], 'number_partof'] = '10 D'
            continue
        elif d[1]['dossierId'] == 'HGB_1_091_020':
            dossier.at[d[0], 'number_partof'] = '61'
            continue

        # Determine dossiers following the structure
        # "street + Theil von [0-9]+ neben [0-9]+.*".
        partof_match = re.match(r'^((Theil )|(Th. )|(Th )|(T. )|(Tv. ))'
                                r'((von )|(v. ))?'
                                r'(?P<partof>[0-9]+)( neben | n. )'
                                r'(?P<nextto>[0-9]+)(?P<postfix>.*)?$',
                                street_postfix)
        if partof_match and partof_match.group('partof') in d[1]['numbers']:
            dossier.at[d[0], 'number_partof'] = partof_match.group('partof')
            dossier.at[d[0], 'postfix'] = partof_match.group('postfix')
            continue

        # Determine dossiers following the structure
        # "street + Theil von [0-9]+ *".
        partof_match = re.match(r'^((Theil )|(Th. )|(Th )|(T. )|(Tv. ))'
                                r'((von )|(v. ))?(?P<partof>[0-9]+a?)'
                                r'(?P<postfix>.*)?$',
                                street_postfix)
        if partof_match and partof_match.group('partof') in d[1]['numbers']:
            dossier.at[d[0], 'number_partof'] = partof_match.group('partof')
            dossier.at[d[0], 'postfix'] = partof_match.group('postfix')
            continue

        # Search dossiers containing the pattern "Theil von [0-9]+".
        partof_match = re.search(r'((Theil )|(Th. )|(Th )|(T. )|(Tv. ))'
                                 r'((von )|(v. ))?(?P<partof>[0-9]+)',
                                 street_postfix)
        if partof_match and partof_match.group('partof') in d[1]['numbers']:
            dossier.at[d[0], 'number_partof'] = partof_match.group('partof')
            continue

    dossier = dossier.drop(['postfix'], axis=1)

    # Remark dossiers without house numbers.
    dossier['note_postprocessing'] = ''

    # Read excel table containing manually corrected house numbers and
    # additional adresses for clustering.
    dossier_correction2 = pd.read_excel(FILEPATH_ADDRESS_CORRECTED)
    for correction in dossier_correction2.iterrows():
        if pd.isna(correction[1]['House Number']):
            continue
        number_correction = str(correction[1]['House Number'])
        dossierid_correction = correction[1]['dossierId']
        dossier_index = dossier[
            dossier['dossierId'] == dossierid_correction].index.values[0]
        if correction[1]['Remarks'] == 'additional structure':
            # Not consider special structured house numbers.
            continue
        elif number_correction == 'no housenumber available':
            # Create note for postprocessing.
            dossier.at[
                dossier_index,
                'note_postprocessing'] += 'No house number available. '
        elif re.search(r', ', number_correction):
            # Create list if more than one number available.
            dossier.at[
                dossier_index, 'numbers'] = number_correction.split(', ')
        else:
            # Set manually defined house number.
            dossier.at[dossier_index, 'numbers'] = number_correction

    # Determine relationships between dossiers.
    dossier['note'] = ''
    streets = dossier['street'].unique()
    relation = pd.DataFrame(columns=['origin',
                                     'source_dossierid',
                                     'target_dossierid'])
    dossier['connected_dossier'] = [[] for _ in range(len(dossier))]
    for street in streets:
        d = dossier[dossier['street'] == street]
        d = d.dropna(subset=['numbers'])
        for row in d.iterrows():
            if not isinstance(row[1]['numbers'], list):
                d.at[row[0], 'numbers'] = [row[1]['numbers']]

        # Get individual numbers of this street.
        numbers_all = []
        for row in d.iterrows():
            for value in row[1]['numbers']:
                numbers_all.append(value)
        numbers = list(set(numbers_all))

        # Search relationship for each individual house number.
        for n in numbers:
            dossier_n = d[d['numbers'].apply(lambda x: n in x)].copy()
            dossier_index = list(dossier_n.index)

            # Detect all dossier connected to this house number. Example:
            # "Eisengasse 21" is connected with house number 23 and 25.
            new_number = True
            n_connected = []
            for row in dossier_n.iterrows():
                for item in row[1]['numbers']:
                    n_connected.append(item)
            n_connected = list(set(n_connected))
            if len(n_connected) > 1:
                while new_number:
                    new_number = False
                    for i in n_connected:
                        new_row = d[
                            d['numbers'].apply(lambda x: i in x)].copy()
                        if not new_row.empty:
                            for r in new_row.iterrows():
                                if r[0] not in dossier_index:
                                    dossier_index.append(r[0])
                                    dossier_n = pd.concat([
                                        dossier_n,
                                        new_row.loc[[r[0]]]],
                                        ignore_index=True)
                                    for j in r[1]['numbers']:
                                        if j not in n_connected:
                                            n_connected.append(j)
                                            new_number = True
            if len(dossier_index) > 1:
                for k in dossier_index:
                    dossier.at[
                        k, 'connected_dossier'] = list(dossier_n['dossierId'])

            # Detect clear cases of occurence of house number in three dossier.
            # Examples: "Petersgraben 20", "Petersgraben Th. v. 20 neben 18",
            # "Petersgraben Th. v. 20 neben 22".
            if dossier_n.shape[0] == 3:
                dossier_n['prefix'] = None
                dossier_n['year_change'] = None
                for row in dossier_n.iterrows():
                    # Search for cases with documented relationship in note.
                    if row[1]['descriptiveNote']:
                        prefix_match = re.match(
                            r'^(?P<prefix>(Bis|Seit)) '
                            r'(?P<year_change>[0-9]{4}).*$',
                            row[1]['descriptiveNote'])
                        if prefix_match:
                            dossier_n.at[
                                row[0],
                                'prefix'] = prefix_match.group('prefix')
                            dossier_n.at[
                                row[0],
                                'year_change'] = prefix_match.group(
                                    'year_change')
                # Consider clear cases only.
                if (not dossier_n[['prefix',
                                   'year_change']].isnull().values.any()
                        and dossier_n.shape[0] == 3
                        and dossier_n['year_change'].nunique() == 1):
                    id_to = list(dossier_n[
                        dossier_n['prefix'] == 'Bis']['dossierId'])
                    id_from = list(dossier_n[
                        dossier_n['prefix'] == 'Seit']['dossierId'])
                    if len(id_to) == 2:
                        new_relation = pd.DataFrame(
                            {'origin': [[
                                id_to[0], id_to[1], id_from[0]],
                                [id_to[0], id_to[1], id_from[0]]],
                             'source_dossierid': [id_to[0], id_to[1]],
                             'target_dossierid': [id_from[0], id_from[0]]
                             })
                        relation = pd.concat([relation, new_relation],
                                             ignore_index=True)
                    elif len(id_from) == 2:
                        new_relation = pd.DataFrame(
                            {'origin': [[id_to[0], id_from[0], id_from[1]],
                                        [id_to[0], id_from[0], id_from[1]]],
                             'source_dossierid': [id_to[0], id_to[0]],
                             'target_dossierid': [id_from[0], id_from[1]]
                             })
                        relation = pd.concat([relation, new_relation],
                                             ignore_index=True)
                    dossier.loc[
                        dossier_index, 'note'] += 'Relation found on triple. '

    # Improve cluster detection.
    dossier_correction2 = dossier_correction2[
        dossier_correction2['Additional Address'].notna()]
    for row in dossier_correction2.iterrows():
        dossier_id = row[1]['dossierId']
        for address in row[1]['Additional Address'].split(', '):
            address_match = re.match(
                r'^(?P<street>[a-zA-Zäöü\.\-\s]*[a-zA-Zäöü]+) '
                r'(?P<number>[0-9]+)$',
                address)
            if address_match:
                street_match = address_match.group('street')
                number_match = address_match.group('number')
                dossier_match = dossier[
                    (dossier['street'] == street_match)
                    & (dossier['numbers'] == number_match)]
                if dossier_match.shape[0] > 0:
                    # Get the connected dossiers.
                    dossier_connected = dossier[
                        dossier['dossierId'] == dossier_id
                        ]['connected_dossier']
                    # Connect the clusters.
                    dossier_cluster = dossier_connected.values[0] \
                        + dossier_match['connected_dossier'].values[0] \
                        + [dossier_id] + [dossier_match['dossierId'].values[0]]
                    # Remove duplicates.
                    dossier_cluster = list(set(dossier_cluster))
                    # Update the cluster in all connected dossiers.
                    dossier.loc[
                        dossier['dossierId'].isin(dossier_cluster),
                        'connected_dossier'
                        ] = dossier.loc[
                            dossier['dossierId'].isin(dossier_cluster),
                            'connected_dossier'
                            ].apply(lambda x: dossier_cluster)
                    dossier.loc[
                        dossier['dossierId'].isin(dossier_cluster),
                        'note'
                        ] += 'Cluster enlarged based on additional address. '
                else:
                    print(f'For dossier {dossier_id} for {address} '
                          'no match found.')
            else:
                print(f'Additional address available for {dossier_id} '
                      'but no match.')

    # Determine relationships based on descriptiveNote.
    dossier['outside_match'] = dossier['descriptiveNote']
    for d in dossier.iterrows():
        note = d[1]['outside_match']
        if note:
            # Exclude house numbers with alphabetic character.
            # Example: "Blumenrain 11a".
            if d[1]['numbers']:
                if any(re.search(r'[A-Za-z]', s) for s in d[1]['numbers']):
                    # Add remark for postprocessing.
                    if dossier.loc[d[0], 'outside_match'] != '':
                        dossier.loc[
                            d[0], 'note_postprocessing'
                            ] += ('Not (all) content of descriptiveNote '
                                  'automatically processed. ')
                    continue

            dossier_street = dossier[dossier['street'] == d[1]['street']]
            # Search for following dossiers.
            after_match = re.search(r'(N|n)achher (siehe|s.|S.) '
                                    r'(?P<number1>[0-9]+)(\/ ?[0-9]+)?'
                                    r'(, | und | u. )(?P<number2>[0-9]+)'
                                    r'(\/ ?[0-9]+)?(, | und | u. )'
                                    r'(?P<number3>[0-9]+)',
                                    note)
            n_number = 3
            if not after_match:
                after_match = re.search(r'(N|n)achher (siehe|s.|S.) '
                                        r'(?P<number1>[0-9]+)(\/ ?[0-9]+)?'
                                        r'(, | und | u. )(?P<number2>[0-9]+)',
                                        note)
                n_number = 2
            if not after_match:
                after_match = re.search(r'(N|n)achher (siehe|s.|S.) '
                                        r'(?P<number1>[0-9]+)',
                                        note)
                n_number = 1
            if after_match:
                if n_number == 1:
                    number_following = [after_match.group('number1')]
                elif n_number == 2:
                    number_following = [after_match.group('number1'),
                                        after_match.group('number2')]
                elif n_number == 3:
                    number_following = [after_match.group('number1'),
                                        after_match.group('number2'),
                                        after_match.group('number3')]

                # Search for new house numbers after "/".
                # Example: "Bis 1478. Nachher siehe 10/ 12."
                note_red = re.search(
                    r'(N|n)achher (siehe|s.|S.) [A-Za-z0-9\/\s]+\.', note)
                if note_red:
                    number_candidate = re.findall(r'\/ ?(?P<number>\d+)',
                                                  note_red.group())
                    number_candidate = [
                        s for s in number_candidate if len(s) <= 2]
                    for i in number_candidate:
                        if i not in number_following:
                            number_following.append(i)

                # Search for the case if the house numbers are merged.
                # Example: "Bis 1593. Nachher siehe 45, 49 vereinigt."
                match_combined = re.search(
                    after_match.group() + r'(\/ ?[0-9]+)?,? vereinigt', note)
                if match_combined:
                    # Search for unique merged dossier.
                    # Consider also permutations of house numbers.
                    permutations = [
                        list(p) for p in list(
                            itertools.permutations(number_following))]
                    street_next = dossier_street[
                        dossier_street['numbers'].isin(permutations)]
                    # Remove current dossier itself.
                    if d[0] in street_next.index:
                        street_next = street_next.drop(d[0])
                    # Remove dossier containing a part of house number.
                    street_next = street_next[
                        street_next['number_partof'].isna()]
                    if street_next.shape[0] == 1:
                        new_relation = pd.DataFrame(
                            {'origin': [d[1]['dossierId']],
                             'source_dossierid': [d[1]['dossierId']],
                             'target_dossierid': [
                                 street_next['dossierId'].values[0]]
                             })
                        relation = pd.concat([relation, new_relation],
                                             ignore_index=True)
                        dossier.loc[
                            d[0],
                            'outside_match'
                            ] = re.sub(after_match.group()
                                       + r'(\/ ?[0-9]+)?,? vereinigt',
                                       '', dossier.loc[d[0], 'outside_match'])
                        dossier.loc[
                            d[0],
                            'note'] += 'Relation found on following united. '
                else:
                    # Search for unique separated dossier(s).
                    # Example: "Bis 1607. Nachher siehe 68/995 und 70/996."

                    # Test if no combined dossier are available.
                    # Example: "Seit 1735. Vorher siehe 7/1621, 9/1622."
                    dossier_combined = False
                    for r in range(1, len(number_following) + 1):
                        for subset in itertools.permutations(number_following,
                                                             r):
                            if len(subset) > 1:
                                test_combined = dossier_street[
                                    dossier_street['numbers'].isin([list(
                                        subset)])]
                                test_combined = test_combined[
                                    test_combined['dossierId'] != d[1][
                                        'dossierId']]
                                if test_combined.shape[0] > 0:
                                    dossier_combined = True
                    if not dossier_combined:
                        # Exclude combined dossier.
                        for n in number_following:
                            street_next = dossier_street[
                                dossier_street['numbers'] == n]
                            # Remove current dossier itself.
                            if d[0] in street_next.index:
                                street_next = street_next.drop(d[0])
                            # Remove dossier containing a part of house number.
                            street_next = street_next[
                                street_next['number_partof'].isna()]
                            if street_next.shape[0] == 1:
                                new_relation = pd.DataFrame(
                                    {'origin': [d[1]['dossierId']],
                                     'source_dossierid': [d[1]['dossierId']],
                                     'target_dossierid': [
                                         street_next['dossierId'].values[0]]
                                     })
                                relation = pd.concat([relation, new_relation],
                                                     ignore_index=True)
                                dossier.loc[
                                    d[0],
                                    'outside_match'
                                    ] = re.sub(
                                        after_match.group(), '',
                                        dossier.loc[d[0], 'outside_match'])
                                dossier.loc[
                                    d[0],
                                    'note'] += 'Relation found on following. '
                            else:
                                # Case no corresponding dossier could be found.
                                dossier.loc[
                                    d[0],
                                    'note_postprocessing'
                                    ] += 'No following relation found. '

            # Search for previous dossiers.
            before_match = re.search(r'(V|v)orher (siehe|s.|S.) '
                                     r'(?P<number1>[0-9]+)(\/ ?[0-9]+)?'
                                     r'(, | und | u. )'
                                     r'(?P<number2>[0-9]+)(\/ ?[0-9]+)?'
                                     r'(, | und | u. )(?P<number3>[0-9]+)',
                                     note)
            n_number = 3
            if not before_match:
                before_match = re.search(r'(V|v)orher (siehe|s.|S.) '
                                         r'(?P<number1>[0-9]+)(\/ ?[0-9]+)?'
                                         r'(, | und | u. )(?P<number2>[0-9]+)',
                                         note)
                n_number = 2
            if not before_match:
                before_match = re.search(r'(V|v)orher (siehe|s.|S.) '
                                         r'(?P<number1>[0-9]+)',
                                         note)
                n_number = 1
            if before_match:
                if n_number == 1:
                    number_previous = [before_match.group('number1')]
                elif n_number == 2:
                    number_previous = [before_match.group('number1'),
                                       before_match.group('number2')]
                elif n_number == 3:
                    number_previous = [before_match.group('number1'),
                                       before_match.group('number2'),
                                       before_match.group('number3')]

                # Search for new house numbers after "/".
                # Example: "Seit 1744. Vorher siehe 16/ 18."
                note_red = re.search(r'(V|v)orher (siehe|s.|S.) '
                                     r'[A-Za-z0-9\/\s-]+\.',
                                     note)
                if note_red:
                    number_candidate = re.findall(r'\/ ?(?P<number>\d+)',
                                                  note_red.group())
                    number_candidate = [
                        s for s in number_candidate if len(s) <= 2]
                    for i in number_candidate:
                        if i not in number_previous:
                            number_previous.append(i)

                # Search for the case if the house numbers are merged.
                # Example: "Seit 1537. Vorher siehe 38, 40 vereinigt."
                match_combined = re.search(before_match.group() +
                                           r'(\/ ?[0-9]+)?,? vereinigt',
                                           note)
                if match_combined:
                    # Search for unique merged dossier.
                    # Consider also permutations of house numbers.
                    permutations = [
                        list(p) for p in list(
                            itertools.permutations(number_previous))]
                    dossier_street_prev = dossier_street[
                        dossier_street['numbers'].isin(permutations)]
                    # Remove current dossier itself.
                    if d[0] in dossier_street_prev.index:
                        dossier_street_prev = dossier_street_prev.drop(d[0])
                    # Remove dossier containing a part of house number.
                    dossier_street_prev = dossier_street_prev[
                        dossier_street_prev['number_partof'].isna()]
                    if dossier_street_prev.shape[0] == 1:
                        new_relation = pd.DataFrame(
                            {'origin': [d[1]['dossierId']],
                             'source_dossierid': [
                                 dossier_street_prev['dossierId'].values[0]],
                             'target_dossierid': [d[1]['dossierId']]
                             })
                        relation = pd.concat([relation, new_relation],
                                             ignore_index=True)
                        dossier.loc[
                            d[0],
                            'outside_match'
                            ] = re.sub(before_match.group() +
                                       r'(\/ ?[0-9]+)?,? vereinigt', '',
                                       dossier.loc[d[0], 'outside_match'])
                        dossier.loc[
                            d[0],
                            'note'] += 'Relation found on before united. '
                else:
                    # Search for unique separated dossier(s).
                    # Example: "Seit 1542. Vorher siehe 31 u. 33 getrennt."

                    # Test if no combined dossier are available.
                    # Example: "Seit 1735. Vorher siehe 7/1621, 9/1622."
                    dossier_combined = False
                    for r in range(1, len(number_previous) + 1):
                        for subset in itertools.permutations(number_previous,
                                                             r):
                            if len(subset) > 1:
                                test_combined = dossier_street[
                                    dossier_street['numbers'].isin(
                                        [list(subset)])]
                                test_combined = test_combined[
                                    test_combined['dossierId'] != d[1][
                                        'dossierId']]
                                if test_combined.shape[0] > 0:
                                    dossier_combined = True
                    if not dossier_combined:
                        # Exclude combined dossier.
                        for n in number_previous:
                            dossier_street_prev = dossier_street[
                                dossier_street['numbers'] == n]
                            # Remove current dossier itself.
                            if d[0] in dossier_street_prev.index:
                                dossier_street_prev = dossier_street_prev.drop(
                                    d[0])
                            # Remove dossier containing a part of house number.
                            dossier_street_prev = dossier_street_prev[
                                dossier_street_prev['number_partof'].isna()]
                            if dossier_street_prev.shape[0] == 1:
                                new_relation = pd.DataFrame(
                                    {'origin': [d[1]['dossierId']],
                                     'source_dossierid': [
                                         dossier_street_prev[
                                             'dossierId'].values[0]],
                                     'target_dossierid': [d[1]['dossierId']]
                                     })
                                relation = pd.concat([relation, new_relation],
                                                     ignore_index=True)
                                dossier.loc[
                                    d[0],
                                    'outside_match'
                                    ] = re.sub(before_match.group(), '',
                                               dossier.loc[d[0],
                                                           'outside_match'])
                                dossier.loc[
                                    d[0],
                                    'note'] += 'Relation found on before. '
                            else:
                                # Case no corresponding dossier could be found.
                                dossier.loc[
                                    d[0],
                                    'note_postprocessing'
                                    ] += 'No before relation found. '

            # Analyze the column "outside_match" for the postprocessing.
            if (dossier.loc[d[0], 'outside_match'] != ''
                    and dossier.loc[d[0], 'note'] != ''):
                note_match = re.match(r'^(Bis|Seit|ganz|vereinigt|getrennt|'
                                      r'[0-9]{4}|/[0-9]{3,4}|[., ]|\[...\])+$',
                                      dossier.loc[d[0], 'outside_match'])
                if note_match:
                    dossier.loc[d[0], 'outside_match'] = ''

            # Add remark for postprocessing.
            if dossier.loc[d[0], 'outside_match'] != '':
                dossier.loc[
                    d[0],
                    'note_postprocessing'
                    ] += ('Not (all) content of descriptiveNote automatically '
                          'processed. ')

    # Remove duplicated relations.
    relation_red = relation.drop(columns=['origin'])
    relation_red = relation_red.drop_duplicates()

    # Determine ID, size and number of relationships for each cluster.
    dossier['cluster_id'] = pd.Series([], dtype='int')
    dossier['cluster_size'] = pd.Series([], dtype='int')
    dossier['cluster_nrelations'] = pd.Series([], dtype='int')
    lut_cluster = {}
    cluster_id = 1
    for row in dossier.iterrows():
        connected_dossier = row[1]['connected_dossier']
        if connected_dossier and str(connected_dossier) not in lut_cluster:
            # Cluster was not considered in a previous loop.
            lut_cluster[str(connected_dossier)] = cluster_id
            dossier.loc[
                dossier['dossierId'].isin(connected_dossier),
                'cluster_id'] = cluster_id
            dossier.loc[
                dossier['dossierId'].isin(connected_dossier),
                'cluster_size'] = len(connected_dossier)
            cluster_id += 1
            # Determine the number of realtionships.
            lut_relation = {}
            for d in connected_dossier:
                d_relation = relation_red[
                    (relation_red['source_dossierid'] == d) |
                    (relation_red['target_dossierid'] == d)]
                for r in d_relation.iterrows():
                    if r[0] not in lut_relation:
                        lut_relation[r[0]] = list(r[1])
            dossier.loc[
                dossier['dossierId'].isin(connected_dossier),
                'cluster_nrelations'] = len(lut_relation)

    # Determine relationships based on cluster and dossier type.
    dossier_type = pd.read_excel(FILEPATH_DOSSIER_TYPE)
    dossier = pd.merge(dossier, dossier_type, on='dossierId')
    entry = pd.DataFrame(
        read_table(dbname=DB_NAME, dbtable='project_entry',
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['entryId', 'dossierId', 'pageId', 'year', 'yearSource',
                 'comment', 'manuallyCorrected', 'language'])
    # Iterate over all clusters.
    cluster = dossier.groupby('cluster_id')
    for name, group in cluster:
        if len(group) == 3:
            # Determine the median year of the entries for each dossiers.
            for index, row in group.iterrows():
                group.at[index, 'year_median'] = statistics.median(
                    entry[entry['dossierId'] == row['dossierId']]['year'])
            counts = group['type'].value_counts()
            if counts.get('partOf') == 2 and counts.get('unchanged') == 1:
                if ((group[group['type'] == 'unchanged'][
                        'year_median'].values[0]
                    > group[group['type'] == 'partOf'][
                        'year_median'].values[0]) &
                    (group[group['type'] == 'unchanged'][
                        'year_median'].values[0]
                    > group[group['type'] == 'partOf'][
                        'year_median'].values[1])):
                    # Case dossier was united.
                    new_relation = pd.DataFrame(
                        {'source_dossierid': [
                            group[group['type'] == 'partOf'][
                                'dossierId'].values[0],
                            group[group['type'] == 'partOf'][
                                'dossierId'].values[1]],
                         'target_dossierid': [
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0],
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0]]
                         })
                    merged_outer = new_relation.merge(
                        relation_red, how='outer', indicator=True)
                    new_relation = merged_outer[
                        merged_outer['_merge'] == 'left_only']
                    if not new_relation.empty:
                        # Add new relations.
                        new_relation = new_relation.drop('_merge', axis=1)
                        relation_red = pd.concat([relation_red, new_relation],
                                                 ignore_index=True)
                        # Update cluster_nrelations.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'cluster_nrelations'] += new_relation.shape[0]
                        # Add note.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'note'] += 'Relation found on cluster. '
                elif ((group[group['type'] == 'unchanged'][
                        'year_median'].values[0]
                        < group[group['type'] == 'partOf'][
                            'year_median'].values[0]) &
                        (group[group['type'] == 'unchanged'][
                            'year_median'].values[0]
                            < group[group['type'] == 'partOf'][
                            'year_median'].values[1])):
                    # Case dossier was separated.
                    new_relation = pd.DataFrame(
                        {'source_dossierid': [
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0],
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0]],
                         'target_dossierid': [
                            group[group['type'] == 'partOf'][
                                'dossierId'].values[0],
                            group[group['type'] == 'partOf'][
                                'dossierId'].values[1]]})
                    merged_outer = new_relation.merge(
                        relation_red, how='outer', indicator=True)
                    new_relation = merged_outer[
                        merged_outer['_merge'] == 'left_only']
                    if not new_relation.empty:
                        # Add new relations.
                        new_relation = new_relation.drop('_merge', axis=1)
                        relation_red = pd.concat([relation_red, new_relation],
                                                 ignore_index=True)
                        # Update cluster_nrelations.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'cluster_nrelations'] += new_relation.shape[0]
                        # Add note.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'note'] += 'Relation found on cluster. '
            elif counts.get('unchanged') == 2 and counts.get('joined') == 1:
                if ((group[group['type'] == 'joined']['year_median'].values[0]
                        > group[group['type'] == 'unchanged'][
                            'year_median'].values[0]) &
                        (group[group['type'] == 'joined'][
                            'year_median'].values[0]
                            > group[group['type'] == 'unchanged'][
                            'year_median'].values[1])):
                    # Case dossier was united.
                    new_relation = pd.DataFrame(
                        {'source_dossierid': [
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0],
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[1]],
                         'target_dossierid': [
                            group[group['type'] == 'joined'][
                                 'dossierId'].values[0],
                            group[group['type'] == 'joined'][
                                'dossierId'].values[0]]
                         })
                    merged_outer = new_relation.merge(
                        relation_red, how='outer', indicator=True)
                    new_relation = merged_outer[
                        merged_outer['_merge'] == 'left_only']
                    if not new_relation.empty:
                        # Add new relations.
                        new_relation = new_relation.drop('_merge', axis=1)
                        relation_red = pd.concat([relation_red, new_relation],
                                                 ignore_index=True)
                        # Update cluster_nrelations.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'cluster_nrelations'] += new_relation.shape[0]
                        # Add note.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'note'] += 'Relation found on cluster. '
                elif ((group[group['type'] == 'joined'][
                        'year_median'].values[0]
                        < group[group['type'] == 'unchanged'][
                            'year_median'].values[0]) &
                        (group[group['type'] == 'joined'][
                            'year_median'].values[0]
                            < group[group['type'] == 'unchanged'][
                            'year_median'].values[1])):
                    # Case dossier was separated.
                    new_relation = pd.DataFrame(
                        {'source_dossierid': [
                            group[group['type'] == 'joined'][
                                'dossierId'].values[0],
                            group[group['type'] == 'joined'][
                                'dossierId'].values[0]],
                         'target_dossierid': [
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[0],
                            group[group['type'] == 'unchanged'][
                                'dossierId'].values[1]]})
                    merged_outer = new_relation.merge(
                        relation_red, how='outer', indicator=True)
                    new_relation = merged_outer[
                        merged_outer['_merge'] == 'left_only']
                    if not new_relation.empty:
                        # Add new relations.
                        new_relation = new_relation.drop('_merge', axis=1)
                        relation_red = pd.concat([relation_red, new_relation],
                                                 ignore_index=True)
                        # Update cluster_nrelations.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'cluster_nrelations'] += new_relation.shape[0]
                        # Add note.
                        dossier.loc[dossier['dossierId'].isin(
                            list(group['dossierId'])),
                            'note'] += 'Relation found on cluster. '

    # Summary statistics.
    print(dossier['note_postprocessing'].value_counts())
    print('Number of comments in note_postprocessing:',
          dossier['note_postprocessing'].str.len().gt(0).sum())
    print('Number of relations found:', relation_red.shape[0])
    print('Number of source dossier in table relation:',
          relation_red['source_dossierid'].nunique())
    print('Number of target dossier in table relation:',
          relation_red['target_dossierid'].nunique())

    # Export results.
    dossier.to_csv(FILEPATH_DOSSIER_RESULT, index=False, header=True)
    relation_red.to_csv(FILEPATH_RELATIONSHIP, index=False, header=True)
    cluster = dossier[['dossierId', 'cluster_id']]
    cluster.to_csv(FILEPATH_CLUSTER, index=False, header=True)


if __name__ == "__main__":
    main()
