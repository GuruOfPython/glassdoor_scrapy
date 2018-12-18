# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from urllib.parse import urljoin

from datetime import date
import datetime
from datetime import timedelta

CANADA_URL = "https://www.glassdoor.com/Reviews/canada-reviews-SRCH_IL.0,6_IN3_IP{}.htm"
DOMAIN_URL = "https://www.glassdoor.com/"
MONTHS_AGO = - 8
DAYS_MONTHLY = 30


class GlassdoorSpider(scrapy.Spider):
    name = 'glassdoor'
    allowed_domains = ['glassdoor.com']
    start_urls = []

    def __init__(self):
        print(
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        self.index = 0
        self.total_data = {}

    def create_result_file(self, result_file_name):
        self.result_file = open(result_file_name, "w", encoding="utf-8")

        heading = [
            "Company", "Website", "Headquarters", "Size", "Founded", "Type", "Industry", "Revenue ", "Competitors",
            "Rating", "1.0", "2.0", "3.0", "4.0", "5.0"
        ]

        import codecs
        self.result_file = codecs.open(result_file_name, "w", "utf-8")
        self.result_file.write(u'\ufeff')
        self.insert_row(heading)

    def insert_row(self, result_row):
        self.result_file.write('"' + '","'.join(result_row) + '"' + "\n")
        self.result_file.flush()

    def start_requests(self):
        self.create_result_file("glassdoor_result.csv")

        for i in range(1, 4301):
            start_url = CANADA_URL.format(i)

            request = FormRequest(
                url=start_url,
                method="GET",
                callback=self.parse,
            )
            yield request

    def parse(self, response):
        rows = response.xpath('//div[@class="header cell info"]')
        for i, row in enumerate(rows):
            url = urljoin(DOMAIN_URL, row.xpath('.//div[@class=" margBotXs"]/a/@href').extract_first())
            self.index += 1
            request = FormRequest(
                url=url,
                method="GET",
                callback=self.get_details,
                meta={
                    'index': self.index
                }
            )
            yield request

        try:
            next_url = urljoin(DOMAIN_URL, response.xpath('//li[@class="next"]/a/@href').extract_first())
            if next_url:
                request = FormRequest(
                    url=next_url,
                    method="GET",
                    callback=self.parse
                )
                yield request
        except:
            pass

    def get_details(self, response):
        index = response.meta['index']
        try:
            company = response.xpath('//div[@class="header cell info"]/h1/text()').extract_first().strip()
        except:
            company = ""
        try:
            website = response.xpath(
                '//label[text()="Website"]/following-sibling::span[1]/a/text()').extract_first().strip()
        except:
            website = ""
        try:
            headquarters = response.xpath(
                '//label[text()="Headquarters"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            headquarters = ""
        try:
            size = response.xpath('//label[text()="Size"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            size = ""
        try:
            founded = response.xpath(
                '//label[text()="Founded"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            founded = ""
        try:
            type = response.xpath('//label[text()="Type"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            type = ""
        try:
            industry = response.xpath(
                '//label[text()="Industry"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            industry = ""
        try:
            revenue = response.xpath(
                '//label[text()="Revenue"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            revenue = ""
        try:
            competitors = response.xpath(
                '//label[text()="Competitors"]/following-sibling::span[1]/text()').extract_first().strip()
        except:
            competitors = ""
        try:
            rating = response.xpath('//div[@class="ratingNum"]/text()').extract_first().strip()
        except:
            rating = ""

        self.total_data[str(index)] = {
            'company': company,
            'website': website,
            'headquarters': headquarters,
            'size': size,
            'founded': founded,
            'type': type,
            'industry': industry,
            'revenue': revenue,
            'competitors': competitors,
            'rating': rating,
            '1.0': [],
            '2.0': [],
            '3.0': [],
            '4.0': [],
            '5.0': [],
        }

        reviews_url = response.urljoin(response.xpath('//a[@data-label="Reviews"]/@href').extract_first())
        request = FormRequest(
            url=reviews_url,
            method="GET",
            callback=self.get_reviews,
            meta={
                "index": index
            }
        )

        yield request

    def get_reviews(self, response):
        index = response.meta['index']

        rows = response.xpath('//div[@class="hreview"]')

        for row in rows:
            review_date_str = row.xpath('.//div[@class="floatLt"]/time/@datetime').extract_first()
            review_date_str = review_date_str if review_date_str else ""
            rating = row.xpath('.//span[@class="value-title"]/@title').extract_first()
            rating = rating if rating else ""

            if compare(review_date_str=review_date_str):
                self.total_data[str(index)][rating].append(review_date_str)

        try:
            next_reviews_url_raw = response.xpath('//div[@id="FooterPageNav"]//li[@class="next"]/a/@href').extract_first()
            next_reviews_url = response.urljoin(next_reviews_url_raw) if next_reviews_url_raw else ""
        except:
            next_reviews_url = ""

        if next_reviews_url:
            print("Next reviews url is \t", next_reviews_url)
            request = FormRequest(
                url=next_reviews_url,
                method="GET",
                callback=self.get_reviews,
                meta={
                    "index": index
                }
            )

            yield request
        else:
            result_row = [
                self.total_data[str(index)]["company"],
                self.total_data[str(index)]["website"],
                self.total_data[str(index)]["headquarters"],
                self.total_data[str(index)]["size"],
                self.total_data[str(index)]["founded"],
                self.total_data[str(index)]["type"],
                self.total_data[str(index)]["industry"],
                self.total_data[str(index)]["revenue"],
                self.total_data[str(index)]["competitors"],
                self.total_data[str(index)]["rating"],
                str(len(self.total_data[str(index)]["1.0"])),
                str(len(self.total_data[str(index)]["2.0"])),
                str(len(self.total_data[str(index)]["3.0"])),
                str(len(self.total_data[str(index)]["4.0"])),
                str(len(self.total_data[str(index)]["5.0"])),
            ]
            self.insert_row(result_row=result_row)
            print("\t", result_row)


def compare(review_date_str):
    if review_date_str == "":
        return True
    deadline_date_str = str(date.today() + timedelta(days=MONTHS_AGO * DAYS_MONTHLY))
    datetimeFormat = '%Y-%m-%d'
    deadline_date = datetime.datetime.strptime(deadline_date_str, datetimeFormat)
    review_date = datetime.datetime.strptime(review_date_str, datetimeFormat)

    if review_date > deadline_date:
        return True
    else:
        return False

# scrapy crawl glassdoor -s LOG_ENABLED=False


if __name__ == '__main__':
    from scrapy import cmdline
    cmdline.execute("scrapy crawl glassdoor -o result.json".split())
