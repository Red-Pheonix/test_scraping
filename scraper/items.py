# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemadapter import ItemAdapter


class ProductItem(scrapy.Item):
    # define the fields for the product

    # fields that dont change
    name = scrapy.Field() # name of the product
    product_id = scrapy.Field() # product id in the url
    model = scrapy.Field() # product model number
    brand = scrapy.Field() # brand of the product
    supplier = scrapy.Field() # supplier of the product
    category = scrapy.Field() # product category
    summary = scrapy.Field() # summary of the product

    # dynamic fields
    price = scrapy.Field() # current price of the product
    quantity = scrapy.Field() # how many products are available

    # metadata
    current_date = scrapy.Field()
