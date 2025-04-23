import datetime as datetime, csv, sys
import psycopg2
import psycopg2.extras as extras
import csv
from pathlib import Path
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os

from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
import logging
from pathlib import Path

import euc_reporting.config as config

logging.basicConfig(format="{asctime} - {levelname} - {message}",
                    style="{",     
                    datefmt="%Y-%m-%d %H:%M",
                    stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger(__name__)

class Store: 
    def __init__(self, connection:str, drop:bool) -> None:

        parts = connection.split(":")

        if len(parts) != 6:
            raise Exception("Invalid connection string")

        self.hostname = parts[0]
        logger.info('Hostname: %s', self.hostname)
        self.hostport = parts[1]
        logger.info('Port: %s', self.hostport)
        self.database = parts[2]
        logger.info('Database: %s', self.database)
        self.schema = parts[3]
        logger.info('Schema: %s', self.schema)
        self.username = parts[4]
        logger.info('Username: %s', self.username)
        self.password = parts[5]

        self.drop = drop
        logger.info(f'Drop Table: {self.drop}')
      
        try:
            self.connection = psycopg2.connect(database=self.database, user=self.username, 
                                               password=self.password, host=self.hostname, port=5432)
        except Exception as e:
            logger.exception(e)
            raise e
        
        logger.info('Connection Succesful')

    def create_table(self, tablename, columns):

        primary_key = tablename + "_pk"

        logger.info('Tablename: %s', tablename)
        logger.info('Primary Key: %s',  primary_key)
        date_set = frozenset(config.date_fields)
        
        drop_sql_statement = f"drop table IF EXISTS \"{self.schema}\".\"{tablename}\" " 

        create_sql_statement = f"create table IF NOT EXISTS \"{self.schema}\".\"{tablename}\" (\n" \
                        f"\"{primary_key}\" uuid NOT NULL DEFAULT gen_random_uuid(), "
        for column in columns:

            if column in date_set:
                create_sql_statement += f"\"{column}\" date NOT NULL,\n" 
            else:
                create_sql_statement += f"\"{column}\" text NOT NULL,\n" 
         
        create_sql_statement += f"\nCONSTRAINT \"{primary_key}_pkey\" PRIMARY KEY (\"{primary_key}\")\n"
        create_sql_statement += ") TABLESPACE pg_default;\n"

        logger.info(create_sql_statement)

        try: 
            with self.connection.cursor() as cur:

                if (self.drop):
                    cur.execute(drop_sql_statement)
                    logger.info(f"Drop table: \"{self.schema}\".\"{tablename}\" - successful") 
                    self.connection.commit()

                cur.execute(create_sql_statement)
                self.connection.commit()

                logger.info(f"Create table: \"{self.schema}\".\"{tablename}\" - successful") 
        except (psycopg2.DatabaseError, Exception) as error:
            logger.exception(error) 
            raise error    

        return tablename  
    
    def insert_rows(self, tablename, columns, rows):
        insert_columns = (', '.join('"' + column + '"' for column in columns))
        insert_values = (', '.join('%s' for column in columns))

        sql = f"""
        INSERT INTO \"{self.schema}\".\"{tablename}\" ({insert_columns}) 
	                        VALUES ({insert_values});
        """
        with self.connection.cursor() as cur:
           cur.executemany(sql, rows)
           self.connection.commit()


class Processor:
    def __init__(self, loader, database):
        self.database = database
        self.loader = loader

    def load(self):
        self.loader.load(self.database)

class Loader:
    def __init__(self, input:str, output:str, limit:int) -> None:
        self.input = input
        self.output = output
        self.limit = limit

        self.times = []

    def process(self, database, tablename, filename):
        with open(filename) as csvfile:
            dict_reader = csv.DictReader(csvfile)
            headers = dict_reader.fieldnames  
            logger.info(headers) 
            database.create_table(tablename, headers)    

            rows = csv.reader(csvfile)
            values = []

            x = []
            y = []

            batch_size = config.batch_size
            batch_counter = 0
            batch_total = 0

            for row in rows:

                if self.limit != 0 and batch_total >= self.limit:
                    logger.info(f"Limit - {self.limit} rows - {batch_total} - rows : reached")
                    break
                
                values.append(row)
                
                batch_total += 1
                
                if batch_counter == batch_size - 1 :
                    
                    commenced = datetime.datetime.now()

                    database.insert_rows(tablename, headers, values)
                    
                    completed = datetime.datetime.now()

                    delta = completed - commenced

                    logger.info(f"Committed - {int(delta.total_seconds() * 1000)} ms - {batch_total} - rows")

                    self.times.append([batch_counter, int(delta.total_seconds() * 1000)])

                    x.append(batch_total)
                    y.append(int(delta.total_seconds() * 1000))

                    values = []

                    batch_counter = 0
  
                else:
                    batch_counter += 1

            if len(values) != 0:
                database.insert_rows(tablename, headers, values)
                x.append(batch_total)
                y.append(int(delta.total_seconds() * 1000))
                
                logger.info(f"Committed - {int(delta.total_seconds() * 1000)} ms - {batch_total} - rows")

            # Plot each row as a separate line

            plt.figure(clear=True)
            plt.plot(x, y)
            plt.ylabel('Time Taken')
            plt.xlabel('Committed')

            plt.savefig(os.path.join(self.output, tablename + ".png"))
    
    def load(self, database):

        logger.info(f"Loading File: {self.input}")
        logger.info(f"Output File: {self.output}")

        path = Path(self.input)

        if path.is_dir():
            
            files = [file for file in os.listdir(self.input) if file.endswith('.csv')]

            for file in files:
                logger.info(f"Processing - File - '{file}'")
                self.process(database, Path(file).stem, os.path.join(self.input, file))

        elif path.is_file():
            self.process(database, Path(self.input).stem, self.input)

        else:
            raise Exception(f"File/Directory - '{self.input}' - does not exist")
"""
The container required for dependecy injection
"""   
class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    loader = providers.Singleton(Loader, input=config.input, output=config.output, limit=config.limit)

    database = providers.Singleton(
        Store,
        connection=config.connection,
        drop=config.drop
    )

    processor = providers.Factory(Processor, loader, database)

"""
Main class - it all starts here

Note: dependency injection is used throughout this program
"""
class Reporter:
    processor: Processor = Provide[Container.processor]

    def __init__(self, input, output, connection, limit, drop):
        """
        Setup the dependecy injection
        """
        container = Container()

        container.config.input.from_value(input)
        container.config.output.from_value(output)
        container.config.connection.from_value(connection)
        container.config.limit.from_value(limit)
        container.config.drop.from_value(drop)

        container.wire(modules=[__name__])

        logger.info(f"Input File: {input}")
        logger.info(f"Output File: {output}")
        
        self.input = input
        self.output = output

    def load(self):
        self.processor.load()
