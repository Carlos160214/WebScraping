# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ColetaItem(scrapy.Item):
    # define the fields for your item here like:
    dsc_produto  = scrapy.Field()
    #preco_old    = scrapy.Field()
    #cent_old     = scrapy.Field()
    preco_new    = scrapy.Field()
    cent_new     = scrapy.Field()
    link         = scrapy.Field()
    categoria    = scrapy.Field()
    fonte        = scrapy.Field()
    data_int     = scrapy.Field()
   
