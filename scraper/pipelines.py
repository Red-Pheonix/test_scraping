# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import datetime
import pymongo
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scraper.db_utils import SQLiteExporter

class SQLitePipeline:


    def __init__(self, sqlite_db):
        self.sqlite_db = sqlite_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(sqlite_db=crawler.settings.get('SQLITE_DB'))

    def open_spider(self, spider):
        self.exporter = SQLiteExporter(self.sqlite_db)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

class MongoPipeline:

    collection_name = 'techshop'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        mongo_item = ItemAdapter(item).asdict()
        mongo_item['date'] = datetime.datetime.now().strftime("%d-%m-%Y")
        self.db[self.collection_name].insert_one(mongo_item)
        return item
