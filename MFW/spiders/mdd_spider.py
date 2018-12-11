# __author__ = 'chendansi'

import scrapy
from MFW.items import MFW_MDD_COUNTRY_ITEM, MFW_MDD_CITY_ITEM
from MFW.utils.mongo_client import connect_table


MONGO_COUNTRY_TABLE = "mfw.mdd.country"
MONGO_CITY_TABLE = "mfw.mdd.country"



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
    name = "mdd_city"

    def __init__(self):
        super(MddCitySpider, self).__init__()
        self.country_table = connect_table(MONGO_COUNTRY_TABLE)
        self.city_table = connect_table(MONGO_CITY_TABLE)

    def start_requests(self):
        for country_info in self.country_table.find():
            print(country_info)
            yield scrapy.Request(f"http://www.mafengwo.cn/mdd/citylist/{country_info['country_id']}.html", callback=self.parse)
            break

    def parse(self, response):
        total_num = response.css("div.hd em::text").extract_first()
        print(total_num)
        for city_tag in response.css("#citylistlist .item"):
            print(city_tag)
            item = MFW_MDD_CITY_ITEM()
            item["name"] = city_tag.css(".title::text").extract_first().strip()
            item["name_en"] = city_tag.css(".title .enname::text").extract_first()
            item["vistied_num"] = city_tag.css(".nums b::text").extract_first()
            item["desc"] = city_tag.css(".detail::text").extract_first().strip()
            item["url"] = "http://www.mafengwo.cn"+city_tag.css("a::attr(href)").extract_first()
            item["city_id"] = int(item["url"].split('/')[-1].split('.')[0])
            print(item)
            yield item


