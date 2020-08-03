import os
import csv
import datetime
import logging
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from scrapy.exporters import BaseItemExporter
from scraper.db_orm import Base, ProductInfo, ProductStatus


class SQLExporter(BaseItemExporter):
    """ Item exporter for handling sqlite export """

    def __init__(self, db, **kwargs):
        self.db = db  # filename for the database
        self.logger = logging.getLogger("SQLExporter")

        # connect to database for use with sqlalchemy
        db_uri = 'sqlite:///' + self.db
        self.engine = create_engine(db_uri)
        self.session_maker = sessionmaker(bind=self.engine)

        # create database if it doesn't exist already
        if not os.path.exists(self.db):
            # create all the tables as defined in db_orm classes
            Base.metadata.create_all(self.engine)

    def _insert_item(self, session, item):
        try:
            session.add(item)
            session.commit()
        except SQLAlchemyError as e:
            self.logger.error("Error entering items into database: %s", e)
            session.rollback()

    def export_item(self, item):
        # make a session for inserting an item
        session = self.session_maker()

        # prepare items for database entry
        product_info_item = ProductInfo(
            product_id=item['product_id'],
            category=item['category'],
            name=item['name'],
            model=item['model'],
            brand=item['brand'],
            supplier=item['supplier'],
            summary=item['summary']
            )
        product_status_item = ProductStatus(
            product_id=item['product_id'],
            category=item['category'],
            date=datetime.datetime.now(),
            price=item['price'],
            quantity=item['quantity']
            )

        self._insert_item(session, product_info_item)
        self._insert_item(session, product_status_item)

        session.close()


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
