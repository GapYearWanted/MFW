# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MfwItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class MFW_MDD_COUNTRY_ITEM(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    tag = scrapy.Field()
    continent = scrapy.Field()
    country_id = scrapy.Field()


class MFW_MDD_CITY_ITEM(scrapy.Item):
    name = scrapy.Field()
    name_en = scrapy.Field()
    url = scrapy.Field()
    vistied_num = scrapy.Field()
    city_id = scrapy.Field()
    country_id = scrapy.Field()
    desc = scrapy.Field()
