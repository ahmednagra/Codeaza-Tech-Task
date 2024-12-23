import os
import glob
import json
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

from scrapy.exceptions import CloseSpider
from scrapy import Request, signals, Spider


class AmazonSpider(Spider):
    name = "amazon"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        # Retry and concurrency settings
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        # 'CONCURRENT_REQUESTS': 3,
        'FEED_EXPORTERS': {
            'json': 'scrapy.exporters.JsonItemExporter',
        },

        # 'FEEDS': {
        #     f'output/Amazon Products Detail {current_dt}.json': {
        #         'format': 'json',
        #         'encoding': 'utf8',
        #         'fields': [
        #             'retailer_id', 'retailer_name', 'retailer_country', 'retailer_website',
        #             'product_id', 'product_title', 'product_description', 'promotion_type',
        #             'promotion_description', 'promotion_price', 'promotion_discount',
        #             'promotion_conditions', 'promotion_start_date', 'promotion_expiry',
        #             'promotion_badge_type', 'rich_content_displayed', 'rich_content_images',
        #             'timestamp'
        #         ]
        #     }
        # }
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'device-memory': '8',
        'downlink': '0.15',
        'dpr': '1.25',
        'ect': '3g',
        'priority': 'u=0, i',
        'referer': 'https://www.amazon.com',
        'rtt': '550',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1.25',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-viewport-width': '1536',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'viewport-width': '1536',
    }

    def __init__(self):
        super().__init__()
        self.items_scraped= 0
        self.all_items_scraped= 0
        self.queries_count= 0
        self.user_queries = self.read_input_user_queries()

        # Logs
        os.makedirs('output', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        self.current_dt = datetime.now().strftime("%d%m%Y%H%M")
        self.logs_filepath = f'logs/Amazon_logs_{self.current_dt}.txt'

        # Record script start time and write to log
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

    def parse_listingpage(self, response):
        try:
            query = response.meta.get('query', '').strip()

            products = response.css('[data-component-type="s-search-result"]')
            for product in products:
                item = OrderedDict()
                current_price = product.css('.a-price .a-offscreen ::text').get('').strip()
                was_price = product.css('.a-text-price .a-offscreen ::text').get('').strip()
                prod_img = product.css('.s-image[srcset] ::attr(src)').get('').strip()
                url = ''.join(product.css('[data-cy="title-recipe"] a::attr(href)').get('').split('ref=sr')[0:1])

                item['Title'] = product.css('h2 ::text').get('').strip()
                item['Stars Ranking'] = product.css('i ::text').get('').strip()
                item['Total reviews'] = product.css('.a-size-base.s-underline-text ::text').get('').strip()
                item['Last Month Sold'] = ''.join([text for text in product.css('span.a-size-base.a-color-secondary ::text').getall() if 'bought' in text])
                item['Discounted Price'] = current_price if was_price else ''
                item['Price'] = was_price if was_price else current_price
                item['Delivery'] = product.css('span:contains("Delivery") + span ::text').get('').strip()
                item['Image URL'] = prod_img.replace('_AC_UY218_.jpg', '_AC_UY654_FMwebp_QL65_.jpg') if prod_img else ''
                item['Product URL'] = urljoin(response.url, url) if url else ''
                item['Timestamps'] = datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')

                if not item['Title']:
                    self.write_logs(f'\n\n Item Not saved : {item}')
                    self.write_logs(f"Skipping incomplete item URL: {response.url}")
                    continue

                print(item)
                self.write_json(item, query)
        except Exception as e:
            self.write_logs(f'Error in listing Page URl: {response.url}')
            a=1

        # pagination
        # try:
        #     url = response.url
        #     for page_no in range(2, 21):
        #         next_page_url = f'{url}{page_no}'
        #         yield Request(next_page_url, callback=self.parse_listingpage,
        #                   dont_filter=True, headers=self.headers, meta=response.meta)
        # except Exception as e:
        #     self.write_logs(f"Pagination failed for {response.url}: {e}")


    def write_json(self, item, query):
        try:
            output_folder = 'output'
            file_name = os.path.join(output_folder, f'{query}.json')

            if os.path.exists(file_name):
                with open(file_name, "r", encoding="utf-8") as jsonfile:
                    data = json.load(jsonfile)

                data.append(item)

                with open(file_name, 'w') as jsonfile:
                    json.dump(data, jsonfile, indent=4, separators=(',', ': '))
            else:
                # File doesn't exist, create it with the new item as a list
                with open(file_name, mode='w', encoding="utf-8") as file:
                    json.dump([item], file, indent=4, separators=(',', ': '))

            self.items_scraped += 1
            self.all_items_scraped += 1

            print('Current Query Items Scraped', self.items_scraped)
            print('All Items Scraped', self.all_items_scraped)

        except Exception as e:
            self.write_logs(f'Error in json Writing File: {e}')
            a=1

    def read_input_user_queries(self):
        try:
            filename = glob.glob('input/user_queries.json')[0]

            with open(filename, 'r') as file:
                data = json.load(file)

            return data

        except Exception as e:
            self.write_logs(f"An error occurred: {e}")
            raise CloseSpider("An error occurred while reading the input file. Closing spider.")

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def close(spider, reason):
        spider.write_logs(f'Spider Started at:{spider.script_starting_datetime}')
        spider.write_logs(f'Spider Stopped at:{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
        spider.write_logs(f"Spider closed: {reason}")
        spider.write_logs(f"Total items scraped: {spider.all_items_scraped}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.user_queries:
            self.items_scraped = 0
            self.queries_count += 1
            query = self.user_queries.pop()
            self.write_logs(f"\n\n Search Keyword: {query} now start scraping")
            url = f'https://www.amazon.com/s?k={query}'

            req = Request(url, callback=self.parse_listingpage,
                          dont_filter=True, headers=self.headers,
                          meta={'handle_httpstatus_all': True, 'query': query})

            try:
                self.crawler.engine.crawl(req)
            except TypeError:
                self.crawler.engine.crawl(req, self)