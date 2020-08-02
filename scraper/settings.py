# Scrapy settings for scraper project

BOT_NAME = 'scraper'

SPIDER_MODULES = ['scraper.spiders']
NEWSPIDER_MODULE = 'scraper.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Googlebot-News'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# MONGO URI for accessing MongoDB
MONGO_URI = ""
MONGO_DATABASE = ""

# sqlite database location
SQLITE_DB = ""

# pipelines are disabled by default
ITEM_PIPELINES = {
     #'scraper.pipelines.SQLitePipeline': 300,
     #'scraper.pipelines.MongoPipeline': 600,
}
