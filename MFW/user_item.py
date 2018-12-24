# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class UserItem(scrapy.Item):
    user_id = scrapy.Field()
    name = scrapy.Field()
    city = scrapy.Field()
    vistied_country = scrapy.Field()
    visitied_mdd = scrapy.Field()
    comment_num = scrapy.Field()
    level = scrapy.Field()
    crawl_time = scrapy.Field()
    follow_list = scrapy.Field()
    page_visitied_num = scrapy.Field()
    sex = scrapy.Field()
    follow_num = scrapy.Field()
    fans_num = scrapy.Field()
    honey_num = scrapy.Field()