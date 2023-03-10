# Postgresql-Project-Database
This repository contains Python scripts to administrate the the project database.

## Requirements
- Python 3.10 or newer (only on Python 3.10 tested)
- PostgreSQL (only on PostgreSQL 15.1 tested)
- PostgreSQL extensions pg_trgm 1.6 and fuzzystrmatch 1.1
- Packages: see requirements.txt

## Notes
- These scripts were developed as part of the following research project: https://dg.philhist.unibas.ch/de/bereiche/mittelalter/forschung/oekonomien-des-raums/
- A Postgresql database (https://www.postgresql.org/) was used for the project. Details about...
    - data types: https://www.postgresql.org/docs/current/datatype.html
    - the pg_trgm module: https://www.postgresql.org/docs/current/pgtrgm.html

## Database Schema 
The following figure shows all entities defined in the project database and their links to each other. Reading example for the link between StABS_Series and StABS_Dossier: An entry of the entity StABS_Series is connected to one or more entries of the entity StABS_Dossier. Conversely, an entry of the StABS_Dossier entity is related to exactly one entry of the StABS_Series entity.

![entityRelations](entityRelations.svg)

The tables StABS_Series and StABS_Dossier containing selected metadata available as linked open data by the State Archives of Basel.

For our research project, the images of the register cards of the historical land registry are stored and processed on the platform Transkribus (https://readcoop.eu/transkribus/). Selected information from Transkribus is additionally written into the tables Transkribus_Collection, Transkribus_Document, Transkribus_Page, Transkribus_Page, and Transkribus_TextRegion respectively, for analyses.

### StABS_Serie
Elements of the StABS_Series entity represent streets. They are at the "Series" level at the State Archives. Series that do not have subordinate units at the State Archives are not represented in this table. Collections created on Transkribus for training models are also not represented in this and the following derived table.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| serieId | VARCHAR(10) | yes | PRIMARY KEY | Project identifier, derived from stabsId |
| stabsId | VARCHAR(10) | yes | UNIQUE | Identifier of the State Archives |
| title | VARCHAR(100) | yes |  | Street name |
| link | VARCHAR(50) | yes |  | URI of the linked open data entry of the State Archives |

### StABS_Dossier
Elements of the entity StABS_Dossier represent a building, address or further information of a street. The elements are at the "Dossier" level at the State Archives. Dossiers that do not have subordinate units at the State Archives are not represented in this table.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| dossierId | VARCHAR(15) | yes | PRIMARY KEY | Project identifier, derived from serieId |
| serieId | VARCHAR(10) | yes | FOREIGN KEY | Project identifier of the linked series |
| stabsId | VARCHAR(15) | yes | UNIQUE | Identifier of the State Archives |
| title | VARCHAR(200) | yes |  | Title of the dossier, often correspondend to the address according to the address book of 1862 |
| link | VARCHAR(50) | yes |  | URI of the linked open data entry of the State Archives |
| houseName | VARCHAR(100) | no |  | Name of the house |
| oldHousenumber | VARCHAR(100) | no |  | Old house number |
| owner1862 | VARCHAR(100) | no |  | Owner of the house in the year 1862 |
| descriptiveNote | VARCHAR(600) | no |  | Remarks |

### Transkribus_Collection
Elements of the Transkribus_Collection entity represent a street and are stored as collection on Transkribus.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| colId | INTEGER | yes | PRIMARY KEY | Identifier Transkribus collection (UUID) |
| colName | VARCHAR(10) | yes |  | Name of the collection, correspond to StABS_Serie.serieId |
| nrOfDocuments | SMALLINT | yes |  | Number of documents linked to the collection |

### Transkribus_Document
Elements of the Transkribus_Document entity represent a building or address. On Transkribus, they are stored as documents.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| docId | INTEGER | yes | PRIMARY KEY | Identifier Transkribus document (UUID) |
| colId | INTEGER | yes | FOREIGN KEY | Identifier to the linked collection |
| title | VARCHAR(15) | yes |  | Title of the Document, correspont to StABS_Dossier.dossierId |
| nrOfPages | SMALLINT | yes |  | Number of pages linked to the document |

### Transkribus_Page
Documents on Transkribus can contain several pages. Each element of the entity Transkribus_Page represent one page on Transkribus respectively a page of a file card of the historical land registry.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| pageId | INTEGER | yes | PRIMARY KEY | Identifier Transkribus page (UUID) |
| key | VARCHAR(30) | yes | UNIQUE | Key of the page (UUID) |
| docId | INTEGER | yes | FOREIGN KEY | Identifier to the linked document |
| pageNr | SMALLINT | yes |  | Page number in the document |
| urlImage | VARCHAR(100) | yes |  | URI of the image of the page stored in Transkribus |

### Transkribus_Transcript
Transcriptions of a page are saved as page xml on Transkribus. Each time a change is made on Transkribus, a new version is generated. Elements of the entity Transkribus_Transcript represent selected information of a page xml.

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| key | VARCHAR(30) | yes | PRIMARY KEY | Key of the transcription |
| tsId | INTEGER | yes | UNIQUE | Identifier Transkribus transcript (UUID) |
| pageId | INTEGER | yes | FOREIGN KEY | Identifier to the linked page |
| parentTsId | INTEGER | yes |  | Identifier of the previous transcription version |
| urlPageXml | VARCHAR(100) | yes |  | URI to the page xml of the transcription |
| status | VARCHAR(15) | yes |  | Defined status of the transcription. Possible values: NEW, IN_PROGRESS, DONE, FINAL, GROUND_TRUTH |
| timestamp | TIMESTAMP | yes |  | Time of transcription, Unix time stamp in milliseconds since 01.01.1970 UTC |
| htrModel | VARCHAR(1000) | no |  | Type of HTR model used for the transcription |

### Transkribus_TextRegion
Transcriptions of texts are stored on Transkribus within the page xmls in text regions. Each element of the Transkribus_TextRegion entity represents a non-empty text region of an element of the Transkribus_Transcript entity. 

| **Column name** | **Data type** | **Not NULL?** | **Additional Requirement** | **Description** |
|---------------|---------------|---------------|---------------|---------------|
| textRegionId | VARCHAR(40) | yes | PRIMARY KEY | Generated text region identifier according to the following structure: {key}_{int(index):02} (UUID) |
| key | VARCHAR(30) | yes | FOREIGN KEY | Key of the transcription |
| index | SMALLINT | yes |  | Index of the text region |
| type | VARCHAR(15) | no |  | Type assigned to the text region. Examples: marginalia, header, paragraph, credit, footer |
| textLine | VARCHAR(200)[] | yes |  | Transcribed text per line saved as a list |
| text | VARCHAR(10000) | yes |  | Entire transcribed text of the text region, indexed in the database |

## administrateDatabase.py
This script contains functions creating the project database and its schema and to administrate it using Python scripts.

## connectDatabase.py
Functions to read and write to database tables as well as functions to check if a database exist or a table is empty are defined in this script.

## updateProjectDatabase.py
This script allows to create and update the project database. The data sources used are metadata of the Historical Land Registry of the City of Basel and data from the Transkribus platform. The script uses functions from the following repositories:
- https://github.com/history-unibas/Metadata-Historical-Land-Registry-Basel
- https://github.com/history-unibas/Trankribus-API

## Contact
For questions please contact jonas.aeby@unibas.ch.