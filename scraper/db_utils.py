import os
import csv
import datetime
import logging
import sqlite3
from scrapy.exporters import BaseItemExporter
from scrapy.exceptions import CloseSpider

class SQLiteExporter(BaseItemExporter):
    """ Item exporter for handling sqlite export """


    def __init__(self, db, **kwargs):
        self.db = db # filename for the database
        self.logger = logging.getLogger("SQLiteExporterLogger")

        # if connection fails, connection variable will remain None
        # check it, otherwise get unhandled errors
        self.connection = None
        self.cursor = None

        # create database if it doesn't exist already
        if not os.path.exists(self.db):
            try:
                self.connection = sqlite3.connect(self.db)

                # configure the newly opened database
                create_product_info_table_sql = """
                        CREATE TABLE product_info(
                            product_id INTEGER NOT NULL,
                            category TEXT NOT NULL,
                            name TEXT,
                            model TEXT,
                            brand TEXT,
                            supplier TEXT,
                            summary TEXT,
                            PRIMARY KEY(product_id, category)
                        );
                """
                create_product_status_table_sql = """
                        CREATE TABLE product_status(
                            product_id INTEGER NOT NULL,
                            category TEXT NOT NULL,
                            price REAL,
                            quantity INTEGER,
                            date TEXT,
                            FOREIGN KEY (product_id)
                                REFERENCES product_info (product_id),
                            FOREIGN KEY (category)
                                REFERENCES product_info (category),
                            PRIMARY KEY(product_id, category, date)
                        );
                """
                self.connection.execute(create_product_info_table_sql)
                self.connection.execute(create_product_status_table_sql)

                self.connection.commit()

            except sqlite3.Error as error:
                # shutdown crawling if we can't create database
                self.logger.error("Couldn't create database: %s", error)
                if self.connection:
                    self.connection.rollback()
                raise error
            finally:
                if self.connection:
                    self.connection.close()

    def start_exporting(self):
        try:
            # establish connection to database
            self.connection = sqlite3.connect(self.db)
            self.cursor = self.connection.cursor()
        except sqlite3.Error as error:
            # shutdown crawling if we can't access database
            self.logger.error("Error opening database: %s", error)
            raise error

    def finish_exporting(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def insert_items(self, sql_command, parameters):
        """ Helper function for inserting data into the database """
        try:
            self.cursor.execute(sql_command, parameters)
            self.connection.commit()
        except sqlite3.Error as error:
            self.logger.error("Error inserting items: %s", error)
            self.connection.rollback()

    def export_item(self, item):
        self.logger.debug("Inserting items: %s", item)
        # sql for inserting data
        insert_product_info_sql = """
            INSERT or IGNORE INTO  product_info(
                            product_id, category, 
                            name, model, brand, 
                            supplier, summary)

            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
        insert_product_status_sql = """
            INSERT or IGNORE INTO product_status(
                            product_id, category, 
                            price, quantity, 
                            date)

            VALUES (?, ?, ?, ?, ?)
            """
        # product info fields
        product_id = item['product_id']
        category = item['category']
        name = item['name']
        model = item['model']
        brand = item['brand']
        supplier = item['supplier']
        summary = item['summary']

        # product status fields
        date = datetime.datetime.now().strftime("%d-%m-%Y")
        price = item['price']
        quantity = item['quantity']

        # insert items into the two tables
        self.insert_items(insert_product_info_sql,
                (product_id, category, name, model, brand, supplier, summary)
            )
        self.insert_items(insert_product_status_sql,
                (product_id, category, price, quantity, date)
            )


def export_to_csv(database):
    """ Exports sqlite database as a csv file """

    
    # if connection fails, connection variable will remain None
    # check it, otherwise get unhandled errors
    connection = None
    if os.path.exists(database):
        try:
            # open database
            connection = sqlite3.connect(database)
            cursor = connection.cursor()

            #  query all data from database
            show_all_data_sql = """
                SELECT
                    i.product_id, i.category, i.name,
                    i.model, i.brand, i.supplier, i.summary,
                    s.price, s.quantity, s.date
                FROM
                    product_info i
                    INNER JOIN product_status s 
                        ON i.product_id = s.product_id
            """
            cursor.execute(show_all_data_sql)

            # write output to a csv file
            csv_filename = database.split(".")[0] + ".csv"

            with open(csv_filename, "w", encoding="utf-8", newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([header[0] for header in cursor.description])
                csv_writer.writerows(cursor) 

        except sqlite3.Error as error:
            logging.error("Error while exporting the database %s", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    else:
        logging.error("Couldn't find database file")