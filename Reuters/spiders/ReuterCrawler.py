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

    # data cleaning used for a different csv file
    # company_list['Company Names'] = company_list['Company Names'].str.lower()
    # company_list['Company Names'] = company_list['Company Names'].str.replace(" ", "%20")

    new = company_list['companyname'].str.split(",", n=1, expand=True)
    company_list['companyname'] = new[0]
    company_list['companyname'] = company_list['companyname'].str.replace('"', '')
    company_list['search'] = company_list['companyname'].str.lower()
    company_list['search'] = company_list['search'].str.replace(" ", "%20")
    scrape_urls = []
    # Iterate over every company name in the list to form search urls to use as the scraper's start urls
    for company in company_list['search']:
        scrape_urls.append('https://www.reuters.com/finance/stocks/lookup?search=' + company + '&searchType=any')

    start_urls = scrape_urls
    rows_list = []
    # Parse is iterated over all start urls. Search for company names and pick first entry of search result

    # function is executed after the spider closes. Scraped data stored in rows_list array is converted to
    # pandas dataframe and saved in CSV format with "|" delimiter
    def close(self, reason):
        df = pd.DataFrame(self.rows_list)
        df.reset_index().to_csv('Outputs.csv', index=False, sep="|")

    # Function takes start_urls as input. start_urls are search urls for company names. Should receive urls
    # from google scraper down the line to avoid using Reuters' search engine
    # If search results are present, the function scrapes for the URL of first SERP result and yields company's
    # detail page with parse_primary_details() function as callback function
    def parse(self, response, **kwargs):
        company_name = response.request.url.split("=")[1]
        company_name = company_name.split("&")[0]
        company_name = company_name.replace("%20", " ")
        company_name = company_name.upper()
        company_page_link = response.css("tr.stripe::attr(onclick)").extract_first()
        # if search result entry exists, generate company page url
        if company_page_link is not None:
            next_url = self.baseurl + company_page_link.split("'")[1]
            yield response.follow(
                next_url,
                callback= self.parse_primary_details,
                cb_kwargs=dict(cname=company_name),
                errback=self.parse_error
            )
        else:
            self.rows_list.append({'company_name': company_name})

    # Handles error if HTTP code 200 is not encountered

    def parse_error(self, failure):
        if failure.value.response != 200:
            self.rows_list.append({'company_name': failure.request.cb_kwargs['cname']})

    # Collects data from company primary page(address, phone number, and primary domain)
    # Yields /financials page to parse financials table with parse_income_statement() function as callback

    def parse_primary_details(self, response, cname):
        company_name = cname
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
        # Pass the scraped data along to next function as request meta data
        # the next function will scrape /financials page of each company to recover revenue and operating income
        # repeat the same until you reach the last function that scrapes for employee data
        yield response.follow(next_url, meta={'details': details}, callback=self.parse_income_statement)

    # Collects financials data from table(net income, company revenue, and operating income)
    # yields balance sheet page (/financials/balance-sheet-annual) with parse_balance_sheet() function as callback

    def parse_income_statement(self, response):
        # retrieve meta data passed along the response request and store it in item variable
        item = response.meta['details']
        table_labels = response.css("th.FinanceTable-table-label-13vxs > span::text").extract()
        if not table_labels:
            item['company_revenue'] = ''
            item['operating_income'] = ''
            item['net_income'] = ''
            item['net_income_trend'] = ''
            next_url = response.request.url.split("/financials")[0] + '/financials/balance-sheet-annual'
            yield response.follow(next_url, meta={'item': item}, callback=self.parse_balance_sheet)
        else:
            net_income_index, operating_income_index, company_revenue_index = 0, 0, 0
            for i, label in enumerate(table_labels):
                if label == "Net Income":
                    net_income_index = i + 1
                elif label == "Total Revenue":
                    company_revenue_index = i + 1
                elif label == "Operating Income":
                    operating_income_index = i + 1
                else:
                    pass
            company_revenue = response.css("tr:nth-child("+str(company_revenue_index)+") .FinanceTable-table-label-13vxs+ .digits::text").extract()
            operating_income = response.css("tr:nth-child("+str(operating_income_index)+") .FinanceTable-table-label-13vxs+ .digits::text").extract()
            net_incomes = response.css("tr:nth-child("+str(net_income_index)+") .digits::text").extract()
            # add data scraped from current url to the item dictionary and pass it onto the next function in line
            item['company_revenue'] = company_revenue
            item['operating_income'] = operating_income
            item['net_income'] = net_incomes[0]
            item['net_income_trend'] = net_incomes[:3]
            next_url = response.request.url.split("/financials")[0] + '/financials/balance-sheet-annual'
            yield response.follow(next_url, meta={'item': item}, callback=self.parse_balance_sheet)

    # collects retained earnings and total assets data from balance sheet table
    # yields /people page (to collect BOD members data) with parse_employees() function as callback

    def parse_balance_sheet(self, response):
        item = response.meta['item']
        table_labels = response.css("th.FinanceTable-table-label-13vxs > span::text").extract()
        if not table_labels:
            item['total_assets'] = ''
            item['retained_earnings'] = ''
            next_url = response.request.url.split("/financials")[0] + '/people'
            yield response.follow(next_url, meta={'item': item}, callback=self.parse_employees)
        else:
            total_assets_index, retained_earnings_index = 0, 0
            for i, label in enumerate(table_labels):
                if label == "Total Assets":
                    total_assets_index = i + 1
                elif label == "Retained Earnings (Accumulated Deficit)":
                    retained_earnings_index = i + 1
                else:
                    pass
            total_assets = response.css("tr:nth-child(" + str(total_assets_index) + ") .digits::text").extract()
            retained_earnings = response.css("tr:nth-child(" + str(retained_earnings_index) + ") .digits::text").extract()
            item['total_assets'] = total_assets[0]
            item['retained_earnings'] = retained_earnings[0]
            next_url = response.request.url.split("/financials")[0] + '/people'
            yield response.follow(next_url, meta={'item': item}, callback=self.parse_employees)

    # Parse /people to obtain employee data. Also appends item (python dictionary containing all the parsed data)
    # to the Scraper class' row_list array

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
        self.rows_list.append(item)
        # print(json.dumps(item, indent=4))
