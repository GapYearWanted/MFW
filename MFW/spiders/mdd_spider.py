# __author__ = 'chendansi'

import json
import scrapy
import time
import uuid
from hot_redis import Set, HotClient
from scrapy.selector import Selector
from MFW.items import MFW_MDD_COUNTRY_ITEM, MFW_MDD_CITY_ITEM
from MFW.utils.mongo_client import connect_table


MONGO_COUNTRY_TABLE = "mfw.mdd.country"
MONGO_CITY_TABLE = "mfw.mdd.city"



class MddCountrySpider(scrapy.Spider):
    name = "mdd_country"

    def start_requests(self):
        yield scrapy.Request("http://www.mafengwo.cn/mdd/", callback=self.parse)

    def parse(self, response):
        for continent_tag in response.css(".row-list .bd .item"):
            continent_name = continent_tag.css(".sub-title::text").extract_first()
            for country_tag in continent_tag.css('li:not(.letter)'):
                country_name = country_tag.css("a::text").extract_first()
                country_href = country_tag.css("a::attr(href)").extract_first()
                tag = country_tag.css("i").extract_first()
                country_id = country_href.split('/')[-1].split('.')[0]
                item = MFW_MDD_COUNTRY_ITEM(
                    name = country_name,
                    url = f"http://www.mafengwo.cn{country_href}",
                    tag = 1 if tag else 0,
                    continent = continent_name,
                    country_id = int(country_id)
                )
                print(country_name, f"http://www.mafengwo.cn{country_href}",tag)
                yield item



class MddCitySpider(scrapy.Spider):
    """
    meta dict: {
        "country_id": 所属国家id,
        "page_num": 当前页码,
        "total_num": 总城市数量
    }
    """
    name = "mdd_city"
    redis_crawled_key = "mfw.mdd.crawled_city"

    CITY_PER_PAGE = 9

    def __init__(self):
        super(MddCitySpider, self).__init__()
        self.country_table = connect_table(MONGO_COUNTRY_TABLE)
        self.city_table = connect_table(MONGO_CITY_TABLE)
        cli = HotClient(host="127.0.0.1", port=6379)
        self.crawled_city = Set(client=cli, key=self.redis_crawled_key)

    def start_requests(self):
        for country_info in self.country_table.find():
            if country_info['country_id'] in self.crawled_city:
                #self.logger.info(f"{country_info['name']} is in redis! pass!")
                continue
            self.logger.info(f"crawling country {country_info['name']}")
            yield scrapy.Request(f"http://www.mafengwo.cn/mdd/citylist/{country_info['country_id']}.html",
                                 callback=self.parse,
                                 meta={"country_id": country_info["country_id"]})

    def _generate_post_data(self, mddid, page_num):
        # 拼凑翻页时的post data
        return {
            "mddid": str(mddid),
            "page": str(page_num),
            "_ts": str(int(time.time() * 1000)),
            "sn": str(uuid.uuid4())[-10:]
        }

    def _yield_post_page(self, meta):
        yield scrapy.FormRequest(
            "http://www.mafengwo.cn/mdd/base/list/pagedata_citylist",
            method="POST",
            callback=self.parse_post,
            formdata=self._generate_post_data(meta["country_id"], meta["page_num"]),
            meta=meta,
            dont_filter=True)

    def parse(self, response):
        # 解析各个国家的首页
        total_num = int(response.css("div.hd em::text").extract_first())
        country_id = response.meta["country_id"]
        mongo_num = self.city_table.count({"country_id": country_id})
        if total_num == mongo_num:
            self.logger.info(f"all city is crawled of country {country_id}")
            self.crawled_city.add(country_id)
            return
        yield from self.parse_city_selector(response.css("#citylistlist"), country_id)
        meta = response.meta
        meta["total_num"] = total_num
        meta["page_num"] = 2
        if total_num%9==0:
            meta["page_num"] = total_num//9+1
        if total_num > self.CITY_PER_PAGE:
            yield from self._yield_post_page(meta)

    def parse_city_selector(self, selector, country_id):
        for city_tag in selector.css(".item"):
            item = MFW_MDD_CITY_ITEM()
            item["name"] = city_tag.css(".title::text").extract_first().strip()
            item["name_en"] = city_tag.css(".title .enname::text").extract_first()
            item["vistied_num"] = city_tag.css(".nums b::text").extract_first()
            item["desc"] = city_tag.css(".detail::text").extract_first().strip()
            item["url"] = "http://www.mafengwo.cn" + city_tag.css("a::attr(href)").extract_first()
            item["city_id"] = int(item["url"].split('/')[-1].split('.')[0])
            item["country_id"] = country_id
            yield item

    def parse_post(self, response):
        self.logger.info(f"parsing post response of {response.meta}")
        meta = response.meta
        data = json.loads(response.text)
        yield from self.parse_city_selector(Selector(text=data["list"]), meta["country_id"])
        if meta["page_num"]*self.CITY_PER_PAGE < meta["total_num"]:
            meta["page_num"] += 1
            yield from self._yield_post_page(meta)

