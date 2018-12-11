# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo.errors import DuplicateKeyError
from MFW.utils.mysql_client import connect, get_table
from MFW.utils.mongo_client import connect_table
from MFW.items import MFW_MDD_COUNTRY_ITEM, MFW_MDD_CITY_ITEM

DB = "crawler"

class MfwPipeline(object):
    relation = {
        MFW_MDD_CITY_ITEM: "mfw.mdd.city",
        MFW_MDD_COUNTRY_ITEM: "mfw.mdd.country"
    }

    def __init__(self):
        #self.cursor = connect(DB)
        #self.table = get_table(DB, "mfw_mdd_country")
        self.table = connect_table("mfw.mdd.country")
        self.tables = {
            i: connect_table(i) for i in self.relation.values()
        }

    def process_item(self, item, spider):
        for _class in self.relation.keys():
            if isinstance(item, _class):
                table = self.tables[self.relation[_class]]
                try:
                    table.insert(dict(item))
                except DuplicateKeyError:
                    pass
        #self.cursor.execute(f"insert into mfw_mdd_country (`name`,url, continent,tag) values ({item['name']}`,`{item['url']}`,`{item['continent']}`,{item['tag']})")
        return item
