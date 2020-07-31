import re
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from .. import items

class TechshopSpider(CrawlSpider):
    """ A CrawlSpider for scraping from techshopbd """

    # spider config for crawling techshop
    name = 'techshop'
    start_urls = ['https://www.techshopbd.com/']
    base_url = 'https://www.techshopbd.com/'

    # regex for processing urls
    product_regex = "product-categories/[^/]*/[\d]*"
    link_regex = "product-categories/"
    category_regex = "product-categories/([^/]*)/([0-9]*)"
    # regex for extracting info from page
    product_info_regex = ": *(.*)"

    # follow all links and scrape from pages with products
    rules = (
        Rule(LinkExtractor(allow=(product_regex)), callback='parse_product'),
        Rule(LinkExtractor(allow=(link_regex))) 
    )

    def extract_text(self, text):
        """ Helper function for scraping model, brand and supplier fields """

        # make sure the field exists before doing regex
        if text:
            return re.search(self.product_info_regex, text).group(1)
        else:
            return None
    
    def extract_quantity(self, quantity_text):
        """ Helper function for scraping quantity fields """

        quantity = re.findall('\d+', quantity_text)
        if quantity:
            return int(quantity[0])
        else:
            # for out of stock, meaning no items
            return 0
        

    def parse_product(self, response):
        """ Parse and extract info from the response given from the spider """


        self.logger.info(response.url)
        # extract product info from the page
        item = items.ProductItem()

        item['name'] =  response \
                        .xpath("//*[@class='product-intro']//span/text()") \
                        .get()

        model_text = response \
                            .xpath("//*[@class='product-intro'] \
                                //span[contains(text(),'Model')]/text()") \
                            .get()
        #print(type(model_text))
        item['model'] = self.extract_text(model_text)

        brand_text = response \
                            .xpath("//*[@class='product-intro'] \
                                //span[contains(text(),'Brand')]/text()") \
                            .get()
        item['brand'] = self.extract_text(brand_text)
        
        supplier_text = response \
                            .xpath("//*[@class='product-intro']//span \
                                [contains(text(),'Supplier')]/../text()") \
                            .get()
        item['supplier'] = self.extract_text(supplier_text)

        item['category'] = str(
                                re.search(self.category_regex, response.url) 
                                .group(1)
                            )
        item['product_id'] = int(
                                re.search(self.category_regex, response.url)
                                .group(2)
                            )

        item['summary'] = "".join( 
                                response.xpath("//*[@class='summary']//text()")
                                .getall()
                            ).strip()

        # dynamic fields
        item['price'] = float(response 
                            .css('.product_price_box') 
                            .css('.spacial_price::text')
                            .get()
                        ) 
        
        quantity_text = response \
                            .xpath("//*[@class='product_status_out_of_stock'] \
                                //span/text()") \
                            .get()
        item['quantity'] = self.extract_quantity(quantity_text)

        self.logger.info(str(item))

        return item

#response.xpath("//*[@class='product-intro']//span/text()").getall()