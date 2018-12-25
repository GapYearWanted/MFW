# __author__ = 'chendansi'

import uuid
import re
import scrapy
from MFW.user_item import UserItem
from MFW.utils.mongo_client import connect_table
from datetime import datetime


HOST = "http://www.mafengwo.cn"


class UserSpider(scrapy.Spider):
    name = "user"
    mongo_table = "mfw.user"

    start_urls = ["http://www.mafengwo.cn/"]
    user_regex = re.compile("(http://www\.mafengwo\.cn/u/(\d+)\.html)")
    int_list = ["level", "follow_num", "fans_num", "honey_num", "vistied_country", "visitied_mdd", "comment_num","user_id"]


    def __init__(self, *args, **kwargs):
        super(UserSpider, self).__init__(*args, **kwargs)
        table = connect_table(self.mongo_table)
        self.crawled_user_id = set(table.distinct("user_id"))

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url)
        for user_id in self.crawled_user_id:
            uid = str(uuid.uuid4())
            yield scrapy.Request(f"http://www.mafengwo.cn/u/{user_id}.html",
                                cookies = {
                                              "mfw_uuid": uid,
                                              "__mfwuuid": uid,
                                          },
                                callback = self.user_parse,
                                meta = {
                                    "user_id": user_id
                                }
            )

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
                user_url, user_id = regex_result[0]
                if int(user_id) in self.crawled_user_id:
                    continue
                yield scrapy.Request(user_url,
                                     cookies={
                                         "mfw_uuid": uid,
                                         "__mfwuuid": uid,
                                     },
                                     callback=self.user_parse,
                                     meta={
                                         "user_id": user_id
                                     })

    def user_parse(self, response):
        yield from self.parse(response)
        item = UserItem()
        item["user_id"] = response.meta["user_id"]
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
        self.crawled_user_id.add(item["user_id"])
        yield item

