# use the CLI code: 'scrapy crawl idk -O filename.jl' to output data in JSON lines format

import scrapy
import pandas as pd
import json


class Scraper(scrapy.Spider):
    name = 'idk'
    baseurl = 'https://www.reuters.com'
    # using pandas dataframe to load list of company names
    company_list = pd.read_csv('C:\Dev\ScrapyProject\CompanyData.csv', usecols=[0])
    # Clean data as required to obtain company names in a format suitable for query strings in search url (aviva%20%plc, or royal%20mail%20%plc)
    new = company_list['companyname'].str.split(",", n=1, expand=True)
    company_list['companyname'] = new[0]
    company_list['companyname']=company_list['companyname'].str.replace('"', '')
    company_list['companyname']=company_list['companyname'].str.lower()
    company_list['companyname']=company_list['companyname'].str.replace(" ", "%20")
    # data cleaning used for a different csv file
    # company_list['Company Names'] = company_list['Company Names'].str.lower()
    # company_list['Company Names'] = company_list['Company Names'].str.replace(" ", "%20")
    scrape_urls = []
    company_page_link_list = []
    # Iterate over every company name in the list to form search urls to use as the scraper's start urls
    for company in company_list['companyname']:
        scrape_urls.append('https://www.reuters.com/finance/stocks/lookup?search=' + company + '&searchType=any')

    start_urls = scrape_urls

    # Parse is iterated over all start urls. Search for company names and pick first entry of search result
    def parse(self, response, **kwargs):
        company_page_link = response.css("tr.stripe::attr(onclick)").extract_first()
        # if search result entry exists, generate company page url
        if company_page_link is not None:
            next_url = self.baseurl + company_page_link.split("'")[1]
            yield response.follow(next_url, callback=self.parse_primary_details)  # start scraping company detail pages

    def parse_primary_details(self, response):
        company_name = response.css("h1.QuoteRibbon-name-3x_XE::text").extract()
        company_address = response.css("div.About-address-AiNm9 > p.About-value-3oDGk::text").extract()
        company_ph = response.css("p.About-phone-2No5Q::text").extract()
        company_primary_domain = response.css("a.website::attr(href)").get()
        details = {
            'company_name': company_name,
            'company_address': company_address,
            'company_ph': company_ph,
            'company_primary_domain': company_primary_domain
        }
        next_url = response.request.url + '/financials'
        # instead of returning(yielding) details data only, pass along scraped data to next function as meta data
        # the next function will scrape /financials page of each company to recover revenue and operating income
        # repeat the same until you reach the last static function that scrapes for employee data
        yield response.follow(next_url, meta={'details': details}, callback=self.parse_financials)

    def parse_financials(self, response):
        # retrieve meta data passed along the response request and store it in item variable
        item = response.meta['details']
        company_revenue = response.css("tr:nth-child(1) .FinanceTable-table-label-13vxs+ .digits::text").extract()
        operating_income = response.css("tr:nth-child(12) .FinanceTable-table-label-13vxs+ .digits::text").extract()
        net_income = response.css("tr:nth-child(20) .FinanceTable-table-label-13vxs+ .digits::text").extract()
        # add data scraped from current url to the item dictionary and pass it onto the next function in line
        item['company_revenue'] = company_revenue
        item['operating_income'] = operating_income
        item['net_income'] = net_income
        next_url = response.request.url.split("/financials")[0] + '/financials/balance-sheet-annual'
        yield response.follow(next_url, meta={'item': item}, callback=self.parse_totalassets)

    def parse_totalassets(self, response):
        item = response.meta['item']
        total_assets = response.css("tr:nth-child(19) .FinanceTable-table-label-13vxs+ .digits::text").extract()
        item['total_assets'] = total_assets
        next_url = response.request.url.split("/financials")[0] + '/people'
        yield response.follow(next_url, meta={'item': item}, callback=self.parse_employees)

    def parse_employees(self, response):
        item = response.meta['item']
        employees_list = response.css(".MarketsTable-officer_name-AAQuH .MarketsTable-name-1U4vs::text").extract()
        designations = response.css(".MarketsTable-officer_title-1Vc6L .MarketsTable-name-1U4vs::text").extract()
        employees_dict = {}

        for i in range(len(employees_list)):
            names = employees_list[i].split()
            employees_dict[i] = {
                'first_name': names[0:1],
                'middle_name': names[1:-1],
                'last_name': names[-1:],
                'designation': designations[i]
            }
        item['employees_list'] = employees_dict
        # all data has been collected in the items dictionary. Yield item and store it as necessary
        yield item
        print(json.dumps(item, indent=4))
