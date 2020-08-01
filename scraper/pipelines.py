# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import datetime
from scrapy.exceptions import DropItem
from scraper.db_utils import SQLiteExporter

class SQLitePipeline:


    db_filename = "techshop.db"
    def open_spider(self, spider):
        self.exporter = SQLiteExporter(self.db_filename)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
