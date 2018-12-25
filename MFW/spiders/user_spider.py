# __author__ = 'chendansi'

import uuid
import re
import scrapy
from MFW.user_item import UserItem
from datetime import datetime


HOST = "http://www.mafengwo.cn"


class UserSpider(scrapy.Spider):
    name = "user"

    start_urls = ["http://www.mafengwo.cn/"]
    user_regex = re.compile("http://www\.mafengwo\.cn/u/\d+\.html")
    int_list = ["level", "follow_num", "fans_num", "honey_num", "vistied_country", "visitied_mdd", "comment_num","user_id"]


    def parse(self, response):
        for url in response.css('a::attr(href)').extract():
            if 'javascript' in url:
                continue
            if 'mafengwo.cn' not in url:
                continue
            if not url.startswith("http"):
                url = HOST + url
            regex_result =  self.user_regex.findall(url)
            if regex_result:
                uid = str(uuid.uuid4())
                yield scrapy.Request(regex_result[0],
                                     cookies={
                                         "mfw_uuid": uid,
                                         "__mfwuuid": uid,
                                     },
                                     callback=self.user_parse)
            else:
                yield scrapy.Request(url)

    def user_parse(self, response):
        yield from self.parse(response)
        item = UserItem()
        item["user_id"] = response.url.split("/")[-1].split('.')[0]
        item["name"] = response.css(".MAvaName::text").extract_first().strip()
        item["sex"] = response.css(".MAvaName i::attr(class)").extract_first()
        item["level"] = response.css(".MAvaLevel a::text").extract_first().split(".")[-1]
        item["city"] = response.css(".MAvaPlace::attr(title)").extract_first()
        item["follow_num"],item["fans_num"],item["honey_num"] = response.css(".MAvaNums a::text").extract()
        page_visitied_num = response.css(".MUsersBehavior i::text").extract()
        item["page_visitied_num"] = page_visitied_num[0] if page_visitied_num else 0
        item["vistied_country"],item["visitied_mdd"],item["comment_num"] = response.css(".pin-topright .nums b::text").extract()
        item["crawl_time"] = str(datetime.now())[:19]
        for k in self.int_list:
            try:
                item[k] = int(item[k])
            except:
                item[k] = -1
        yield item

