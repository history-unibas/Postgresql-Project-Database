"""TODO: Description.


"""


import pandas as pd
import re

from connectDatabase import read_table


# constants


# functions



# Erstelle eine Funktion, welche die Jahreszahl ermittelt
# (so kann diese Funktion in projext_database_update.py eingebaut werden).

DB_NAME = 'hgb'



# TODO: testing
dbname = DB_NAME
db_password = 'EHWggZargJ4JLrVxoRKN'



def get_page_year(dbname, db_password,
                  dbtable_page='transkribus_page',
                  dbtable_ts='transkribus_transcript',
                  dbtable_tr='transkribus_textregion',
                  db_user='postgres', db_host='localhost', db_port=5432):
    """TODO: Description.

        Args:
            url (str): Url of a file. TODO

        Returns:
            int or None: Index of transkript version in case of keyword matching. TODO
    """
    # Read necessary tables.
    page = pd.DataFrame(
        read_table(dbname=dbname, dbtable=dbtable_page,
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['pageId', 'key', 'docId', 'pageNr', 'urlImage'])
    transcript = pd.DataFrame(
        read_table(dbname=dbname, dbtable=dbtable_ts,
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['key', 'tsId', 'pageId', 'parentTsId', 'urlPageXml', 'status',
                 'timestamp', 'htrModel'])
    textregion = pd.DataFrame(
        read_table(dbname=dbname, dbtable=dbtable_tr,
                   user=db_user, password=db_password,
                   host=db_host, port=db_port),
        columns=['textRegionId', 'key', 'index', 'type', 'textLine', 'text'])

    # Iterate over every Transkribus page.
    page_year = pd.DataFrame(columns=['colid', 'docid', 'pageid', 'pagenr',
                                        'tsid_reference', 'tsid_prediction',
                                        'url_reference', 'url_prediction',
                                        'textregionid', 'type',
                                        'text_reference', 'text_prediction',
                                        'is_valid', 'warning_message'])  # TODO
    for row in page.iterrows():
        # Determine latest transcript of current page.
        page_id = row[1]['pageId']
        ts = transcript[transcript['pageId'] == page_id]
        ts_sorted = ts.sort_values(by='timestamp', ascending=False)
        ts_latest = ts_sorted.iloc[0]

        # Get the header textregions of latest transcript.
        tr = textregion[textregion['key'] == ts_latest['key']]
        tr_header = tr[tr['type'] == 'header']
        if tr_header.empty:
            # TODO: Add entry to export file
            continue

        # Search for first year occurance in header textregions.
        for header in tr_header.iterrows():
            match = re.search(r'1[0-9]{3}', header[1]['text'])
            if match:
                # TODO: Add entry to export file
                continue



def main():

    db_password = input('PostgreSQL database superuser password:')
    db_port = input('PostgreSQL database port:')
    # Rufe erstellte Funktion aus

    # Analysiere: Auf welchen Seiten wurde keine Jahreszahl gefunden?

    # Analysiere: Wo innerhalb desselben Dokuments ist Jahreszahl kleiner als Jahreszahl der vorherigen Seiten?


if __name__ == "__main__":
    main()
