import os
import csv
import datetime
import logging
import sqlite3
from scrapy.exporters import BaseItemExporter


class SQLiteExporter(BaseItemExporter):
    """ Item exporter for handling sqlite export """

    def __init__(self, db, **kwargs):
        self.db = db  # filename for the database
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
                          (product_id, category, name,
                           model, brand, supplier, summary)
                          )
        self.insert_items(insert_product_status_sql,
                          (product_id, category, price, quantity, date)
                          )


class RockySqliteItemExporter(BaseItemExporter):
    """ Not exactly mine. From here,
        https://github.com/RockyZ/Scrapy-sqlite-item-exporter
        Keep it for testing purposes
    """
    def __init__(self, file, **kwargs):
        self._configure(kwargs)
        self.conn = sqlite3.connect(file)
        self.conn.text_factory = str
        self.created_tables = []

    def export_item(self, item):
        item_class_name = type(item).__name__

        if item_class_name not in self.created_tables:
            self._create_table(item_class_name,
                               item.keys()
                               )
            self.created_tables.append(item_class_name)

        field_list = []
        value_list = []
        for field_name in item.keys():
            field_list.append('[%s]' % field_name)
            field = item.fields[field_name]
            value_list.append(
                self.serialize_field(field, field_name, str(item[field_name]))
                )

        sql = 'insert or ignore into [%s] (%s) values (%s)' % \
            (item_class_name,
                ', '.join(field_list),
                ', '.join(['?' for f in field_list])
             )
        self.conn.execute(sql, value_list)
        self.conn.commit()

    def _create_table(self, table_name, columns, keys=None):
        sql = 'create table if not exists [%s] ' % table_name

        column_define = ['[%s] text' % column for column in columns]
        print('type: %s' % type(keys))
        if keys:
            if len(keys) > 0:
                primary_key = 'primary key (%s)' % ', '.join(keys[0])
                column_define.append(primary_key)

            for key in keys[1:]:
                column_define.append('unique (%s)' % ', '.join(key))

        sql += '(%s)' % ', '.join(column_define)

        print('sql: %s' % sql)
        self.conn.execute(sql)
        self.conn.commit()

    def start_exporting(self):
        pass

    def finish_exporting(self):
        self.conn.close()


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
                csv_writer.writerow([header[0]
                                    for header in cursor.description])
                csv_writer.writerows(cursor)

        except sqlite3.Error as error:
            logging.error("Error while exporting the database %s", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    else:
        logging.error("Couldn't find database file")
