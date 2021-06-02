import scrapy


class ServerErrorHandler(scrapy.Spider):
    name = 'test'
    start_urls = [
        'http://www.reuters.com/companies/IM21.L',
        'http://www.reuters.com/companies/AV.L'
    ]

    def start_requests(self):
        for u in self.start_urls:
            yield scrapy.Request(
                u,
                callback=self.parse,
                errback=self.handleErr,
                cb_kwargs=dict(url=u)
            )

    def parse(self, response, **kwargs):
        company_address = response.css("div.About-address-AiNm9 > p.About-value-3oDGk::text").extract()
        company_ph = response.css("p.About-phone-2No5Q::text").extract()
        company_primary_domain = response.css("a.website::attr(href)").get()
        yield{
            'company address': company_address,
            'company phone': company_ph,
            'primary domain': company_primary_domain
        }

    def handleErr(self, failure):
        if failure.value.response != 200:
            print("Server Error")
            print(failure.request.cb_kwargs['url'])


# class FinancialTableMissing(scrapy.Spider):
#     name = 'test'
#     start_urls = [
#         'https://www.reuters.com/companies/CAF.N/financials'
#     ]
#
#     def parse(self, response, **kwargs):
#         table_labels = response.css("th.FinanceTable-table-label-13vxs > span::text").extract()
#         if not table_labels:
#             print("No financials table")
