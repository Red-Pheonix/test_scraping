import os
import csv
import datetime
import logging
import sqlite3
from scrapy.exporters import BaseItemExporter

class SQLiteExporter(BaseItemExporter):
    """ Item exporter for handling sqlite export """


    def __init__(self, file, **kwargs):
        self.file = file
        self.logger = logging.getLogger("SQLiteExporterLogger")
        # create database if it doesn't exist already
        if not os.path.exists(self.file):
            try:
                connection = sqlite3.connect(self.file)
                cursor = connection.cursor()

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
                cursor.execute(create_product_info_table_sql)
                cursor.execute(create_product_status_table_sql)

                connection.commit()

            except sqlite3.Error as error:
                self.logger.error("Error creating tables: %s", error)
                connection.rollback()
            finally:
                cursor.close()
                connection.close()

    def start_exporting(self):
        self.connection = sqlite3.connect(self.file)
        self.cursor = self.connection.cursor()

    def finish_exporting(self):
        self.cursor.close()
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
            INSERT INTO product_info(
                            product_id, category, 
                            name, model, brand, 
                            supplier, summary)

            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
        insert_product_status_sql = """
            INSERT INTO product_status(
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

    if not os.path.exists(database):
        raise FileNotFoundError("Couldn't find database file")
    else:
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
            logging.error("Error while exporting the database")
        finally:
            cursor.close()
            connection.close()