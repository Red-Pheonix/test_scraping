# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime
import logging
import pymongo
from scrapy.exceptions import DropItem
from scraper.db_utils import SQLiteExporter, export_to_csv


class SQLitePipeline:

    def __init__(self, sqlite_db):
        self.sqlite_db = sqlite_db  # sqlite database filename

    @classmethod
    def from_crawler(cls, crawler):
        return cls(sqlite_db=crawler.settings.get('SQLITE_DB'))

    def open_spider(self, spider):
        self.exporter = SQLiteExporter(self.sqlite_db)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        export_to_csv(self.sqlite_db)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class MongoPipeline:

    collection_name = 'techshop'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri  # mongo uri for connecting to mongodb
        self.mongo_db = mongo_db    # database we will be working with
        self.logger = logging.getLogger("MongoPipeLine")

        # if connection fails, client and db variable will remain None
        # check it, otherwise get unhandled errors
        self.client = None
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        try:
            # establish connection to database
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]

        except pymongo.errors.PyMongoError as error:
            # shutdown crawling if we can't access database
            self.logger.error("Couldn't connect to MongoDB: %s", error)
            raise error

    def close_spider(self, spider):
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        self.logger.debug("Inserting items: %s", item)

        # prepare item for database entry
        mongo_item = dict(item)
        mongo_item['date'] = datetime.datetime.now().strftime("%d-%m-%Y")

        # insert items into database
        try:
            self.db[self.collection_name].insert_one(mongo_item)

        except pymongo.errors.DuplicateKeyError:
            self.logger.warning("Database entry already exists")

        except pymongo.errors.PyMongoError as error:
            self.logger.error("Couldn't insert items into MongoDB: %s", error)

        return item
