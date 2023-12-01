from datetime import datetime
from os import path
import pandas as pd
import mysql.connector
import configparser
from sqlalchemy import create_engine

class DataStore:
    def __init__(self, config_file):
        self.reset()
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        db_username = self.config.get('Database', 'db_username')
        db_password = self.config.get('Database', 'db_password')
        db_host = self.config.get('Database', 'db_host')
        db_port = self.config.getint('Database', 'db_port', fallback=3306)
        db_name = self.config.get('Database', 'db_name')

        # Connect to MariaDB
        self.connection = mysql.connector.connect(
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name
        )

        # Create SQLAlchemy engine for MariaDB
        db_url = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.engine = create_engine(db_url)

    def reset(self):
        self.lastrow = {}
        self.data = pd.DataFrame()

    # Füge prefix und test_number hinzu
    def append(self, row):
        print(row)
        self.lastrow = row
        # Hinzufügen der Testnummer (hier als Beispiel die aktuelle Zeit in Sekunden)
        row['test_number'] = int(datetime.timestamp(datetime.now()))

        # Setze den Prefix für den aktuellen Datensatz
        row['prefix'] = self.get_write_prefix()

        # Hinzufügen des Labels anhand der variable "prefix" zu den Daten
        # Verwende concat anstelle von append
        self.data = pd.concat([self.data, pd.DataFrame([row])], ignore_index=True)

        # Schreibe Daten in Echtzeit nach MariaDB
        self.data.drop_duplicates().to_sql('E-Last_Messdaten', con=self.engine, if_exists='append', index=False)

    def write(self, basedir, user_input_prefix):
        self.prefix = user_input_prefix
        filename = "{}_raw_{}.csv".format(self.prefix, datetime.now().strftime("%Y%m%d_%H%M%S"))
        full_path = path.join(basedir, filename)
        export_rows = self.data.drop_duplicates()
        if export_rows.shape[0]:
            print("Write RAW data to {}".format(path.relpath(full_path)))
            export_rows.to_csv(full_path, index=False)
        else:
            print("no data")

    def get_write_prefix(self):
        return self.prefix if hasattr(self, 'prefix') and self.prefix else "default_prefix"

    def plot(self, **args):
        return self.data.plot(**args)

    def lastval(self, key):
        return self.lastrow[key]

    def setlastval(self, key, val):
        self.lastrow[key] = val