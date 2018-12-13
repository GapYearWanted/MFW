# __author__ = 'chendansi'

import json
import scrapy
import time
import uuid
from datetime import datetime
from hot_redis import Set, HotClient, Dict
from scrapy.selector import Selector
from MFW.items import MFW_MDD_COUNTRY_ITEM, MFW_MDD_CITY_ITEM, MFW_MDD_JD_ITEM
from MFW.utils.mongo_client import connect_table
from MFW.utils.CONFIG import REDIS_HOST, REDIS_PASSWD


MONGO_COUNTRY_TABLE = "mfw.mdd.country"
MONGO_CITY_TABLE = "mfw.mdd.city"
MONGO_JD_TABLE = "mfw.mdd.jd"


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
    redis_crawled_key = "mfw:mdd:crawled_city"

    CITY_PER_PAGE = 9

    def __init__(self):
        super(MddCitySpider, self).__init__()
        self.country_table = connect_table(MONGO_COUNTRY_TABLE)
        self.city_table = connect_table(MONGO_CITY_TABLE)
        cli = HotClient(host=REDIS_HOST, port=6379, password=REDIS_PASSWD)
        self.crawled_city = Set(client=cli, key=self.redis_crawled_key)

    def start_requests(self):
        self.countryid2name = {}
        for country_info in self.country_table.find():
            if country_info['country_id'] in self.crawled_city:
                #self.logger.info(f"{country_info['name']} is in redis! pass!")
                continue
            self.logger.info(f"crawling country {country_info['name']}")
            self.countryid2name[country_info['country_id']] = country_info['name']
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
        self.logger.info(f"crawling page {meta['page_num']} of {self.countryid2name[meta['country_id']]}")
        meta["retry_times"] = 1
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
        if mongo_num%9==0:
            meta["page_num"] = mongo_num//9+1
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


class MddJdSpider(scrapy.Spider):

    name = "mdd_jd"
    redis_crawled_key = "mfw:mdd:crawled_jd"
    UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
    JD_NUM_PER_PAGE = 5


    def __init__(self):
        super(MddJdSpider, self).__init__()
        self.city_table = connect_table(MONGO_CITY_TABLE)
        self.jd_table = connect_table(MONGO_JD_TABLE)
        cli = HotClient(host=REDIS_HOST, port=6379, password=REDIS_PASSWD)
        self.crawled_city = Dict(client=cli, key=self.redis_crawled_key)

    def start_requests(self):
        for city_info in self.city_table.find().limit(17):
            city_id = city_info["city_id"]
            city_name = city_info["name"]
            if city_id not in self.crawled_city:
                self.logger.info(f"crawl page 1 of {city_name}({city_id})")
                yield scrapy.Request(f"https://m.mafengwo.cn/jd/{city_info['city_id']}/gonglve.html",
                                 headers={
                                     "User-Agent": self.UA,
                                     "Referer": f"https://m.mafengwo.cn/mdd/{city_info['city_id']}"
                                 },
                                 callback=self.parse_first_page,
                                 meta={
                                     "city_id": city_info["city_id"],
                                     "city_name": city_name,
                                     "page_num": 1
                                 })
            else:
                crawled_page_num = int(self.crawled_city[city_id])
                if crawled_page_num == -1:
                    self.logger.info(f"all jd of {city_name}({city_id}) is crawled.")
                    continue
                yield from self.crawl_ajax_page(meta={
                    "city_id": city_id,
                    "city_name": city_name,
                    "page_num": crawled_page_num + 1
                })

    def crawl_ajax_page(self, meta):
        meta["retry_times"] = 1
        self.logger.info(f"crawl page {meta['page_num']} of {meta['city_name']}({meta['city_id']})")
        yield scrapy.Request(
            f"https://m.mafengwo.cn/jd/{meta['city_id']}/gonglve.html?page={meta['page_num']}&is_ajax=1",
            headers={
                "Referer": f"https://m.mafengwo.cn/jd/{meta['city_id']}/gonglve.html",
                "User-Agent": self.UA
            },
            meta=meta,
            callback=self.parse_ajax_page,
        )


    def parse_first_page(self, response):
        meta = response.meta
        yield from self.parse_selector(response.css(".poi-list"),meta)
        more_tag = response.css("#btn_getmore")
        if more_tag:
            meta["page_num"] += 1
            yield from self.crawl_ajax_page(response.meta)
        else:
            self.crawled_city[response.meta["city_id"]] = -1

    def parse_ajax_page(self, response):
        meta = response.meta
        data = json.loads(response.text)
        yield from self.parse_selector(Selector(text=data["html"]), meta)
        if data["has_more"] == 1:
            meta["page_num"] += 1
            yield from self.crawl_ajax_page(meta)
        else:
            self.crawled_city[meta["city_id"]] = -1

    def parse_selector(self, selector, meta):
        item = MFW_MDD_JD_ITEM()
        for index, jd_tag in enumerate(selector.css("a.poi-li")):
            item["name"] = jd_tag.css(".hd::text").extract_first()
            item["score"] = jd_tag.css(".progress::attr(style)").extract_first().split(":")[1].split("%")[0]
            nums = jd_tag.css(".num::text").extract()
            if len(nums) == 2:
                item["comment_num"], item["mention_num"] = nums
            elif len(nums) == 1:
                item["comment_num"], item["mention_num"] = nums[0], 0
            else:
                item["comment_num"], item["mention_num"] = 0, 0
            item["jd_type"] = jd_tag.css(".m-t strong::text").extract_first().strip().split(" ")
            item["address"] = jd_tag.css("p:not(.m-t) strong::text").extract_first().strip()
            item["recommend_reason"] = jd_tag.css(".comment::text").extract_first()
            item["url"] = "https://m.mafengwo.cn"+jd_tag.css("::attr(href)").extract_first().split("?")[0]
            item["jd_id"] = item["url"].split("/")[-1].split(".")[0]
            item["city_id"] = meta["city_id"]
            item["rank"] = (meta["page_num"]-1)*self.JD_NUM_PER_PAGE + index + 1
            item["date"] = str(datetime.now().date())
            item["crawl_time"] = time.time()
            yield item
        self.crawled_city[meta["city_id"]] = meta["page_num"]