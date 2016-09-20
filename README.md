# ua2sql
Python program used to convert Unity Analytics raw data export into rows in PostgreSQL tables.

Usage:

    python ua2sql.py <path to config file>

This program does the following:

1. Collects `appStart`, `custom`, and `transaction` Unity Analytics feeds via Unity's Raw Data Export HTTP API.
2. Connects to a PostgreSQL database and inserts the collected data into database rows.
3. (Optional) Copies collected raw data into a backup location for long term storage. This location is specified via `backup_collection_path`.
4. Deletes the raw dumps stored locally to keep things clean.

The configuration file must be a valid `json` file containing these parameters:

    {
      "postgres_server": "<name or ip>",
      "database": "<db name>",
      "user": "<db user with insert permissions>",
      "password": "<db user pwd>",
      "local_collection_path": "<local temp file storage path>",
      "backup_collection_path": "<long term backup path>",
      "unity_project_id":  "<unity project id>",
      "unity_export_api_key": "<unity api key>"
    }

On the PostgreSQL side this program will create four tables. One table each for `appStart`, `custom`, and `transaction` data streams. These map one-to-one with the data Unity reports. Finally, the program makes a `jobId` table which is used to track the previous job GUID for each data stream type to continue from the last time the program was run.

The first time this program is run it will try to gather as much data from Unity as possible - 30 days. Subsequent runs using the same configuration file will continue exactly where it left off last time. Suggested use is to run this program once per day.

The program has been tested on both Python 2.7.5. and 3.5.2.

Python library dependencies and versions used during development:

1. requests 2.10.0
2. pyscopg2 2.6.2
3. SQLAlchemy 1.0.15
