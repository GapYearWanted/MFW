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
    # 目的地国家
    name = scrapy.Field()
    url = scrapy.Field()
    tag = scrapy.Field()
    continent = scrapy.Field()
    country_id = scrapy.Field()


class MFW_MDD_CITY_ITEM(scrapy.Item):
    # 目的地城市
    name = scrapy.Field()
    name_en = scrapy.Field()
    url = scrapy.Field()
    vistied_num = scrapy.Field()
    city_id = scrapy.Field()
    country_id = scrapy.Field()
    desc = scrapy.Field()


class MFW_MDD_JD_ITEM(scrapy.Item):
    # 目的地景点
    name = scrapy.Field()
    score = scrapy.Field()
    comment_num = scrapy.Field()
    mention_num = scrapy.Field()
    address = scrapy.Field()
    recommend_reason = scrapy.Field()
    jd_type = scrapy.Field()
    url = scrapy.Field()
    jd_id = scrapy.Field()
    city_id = scrapy.Field()
    rank = scrapy.Field()
    date = scrapy.Field()
    crawl_time = scrapy.Field()


class MFW_MDD_MS_ITEM(scrapy.Item):
    # 目的地美食
    name = scrapy.Field()
    recommend_num = scrapy.Field()
    mention_num = scrapy.Field()
    rank = scrapy.Field()
    url = scrapy.Field()
    city_id = scrapy.Field()
    city_name = scrapy.Field()


def generate_names(names):
    print("    "+"\n    ".join([f"{name.strip()} = scrapy.Field()" for name in names.split(",")]))


if __name__ == "__main__":
    generate_names("name,comment_num,mention_num,rank,url,city_id")
