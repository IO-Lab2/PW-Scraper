import scrapy
import asyncio
import logging
import json
from scrapy_playwright.page import PageMethod
from pw_scraper.items import ScientistItem, organizationItem, publicationItem

logging.getLogger('asyncio').setLevel(logging.CRITICAL)

def should_abort_request(request):
    return (
        request.resource_type in ["image", "stylesheet", "font", "media"]
        or ".jpg" in request.url
    )


class PwSpider(scrapy.Spider):
    name = "pw_spider"

    allowed_domains = ["repo.pw.edu.pl"]
    start_urls = ["https://repo.pw.edu.pl/index.seam"]

    custom_settings = {
        'PLAYWRIGHT_ABORT_REQUEST': should_abort_request,
        'AUTOTHROTTLE_ENABLED': True,
    }

    pw_url = 'https://repo.pw.edu.pl'

    def parse(self, response):
        categories_links = response.css('a.global-stats-link::attr(href)').getall()
        categories_names = response.css('span.global-stats-description::text').getall()
        categories = {name: self.pw_url + link for name, link in zip(categories_names, categories_links)}

        yield scrapy.Request(categories['People'], callback=self.parse_people_page,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[
                    PageMethod('wait_for_selector', 'a.authorNameLink'),
                    PageMethod('wait_for_selector', 'div#searchResultsFiltersInnerPanel'),
                    PageMethod('wait_for_selector', 'div#afftreemain div#groupingPanel ul.ui-tree-container'),
                    PageMethod("evaluate", """
                                async () => {
                                    const expandAllNodes = async () => {
                                        const buttons = Array.from(document.querySelectorAll('.ui-tree-toggler'));
                                        for (const button of buttons) {
                                            if (button.getAttribute('aria-expanded') === 'false') {
                                                await button.click();
                                                await new Promise(r => setTimeout(r, 300));
                                            }
                                        }
                                    };
                                    await expandAllNodes();
                                }
                            """)
                ],
                errback=self.errback
            ))

    async def parse_people_page(self, response):
        page = response.meta['playwright_page']

        organizations = response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li')
        university = response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>div.ui-treenode-content div.ui-treenode-label>span>span::text').get()
        for org in organizations:
            organization = organizationItem()
            organization['university'] = university

            institute = org.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['institute'] = institute

            cathedras = org.css('ul.ui-treenode-children li.ui-treenode-leaf div.ui-treenode-content div.ui-treenode-label span>span::text').getall()
            organization['cathedras'] = cathedras if cathedras else []

            yield organization

        total_pages=int(response.css('span.entitiesDataListTotalPages::text').get())

        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages + 1):

            page_url = f'https://repo.pw.edu.pl/globalResultList.seam?q=&oa=false&r=author&tab=PEOPLE&conversationPropagation=begin&lang=en&qp=openAccess%3Dfalse&p=xyz&pn=1'
            yield scrapy.Request(url=page_url,
                callback=self.parse_scientist_links, dont_filter=True,
                meta=dict(
                    playwright=True, 
                    playwright_include_page=True,
                    playwright_page_methods=[
                        PageMethod('wait_for_selector', 'a.authorNameLink', state='visible')
                        ],
                    errback=self.errback
            ))
        await page.close()

    async def parse_scientist_links(self, response):
        page = response.meta['playwright_page']

        authors_links = response.css('a.authorNameLink::attr(href)').getall()
        for author in authors_links:
            yield scrapy.Request(self.pw_url + author, callback=self.parse_scientist, dont_filter=True,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_context="people",
                    errback=self.errback
            ))
        await page.close()

    async def parse_scientist(self, response):
        page = response.meta['playwright_page']

        scientist = ScientistItem()

        try:
            personal_data=response.css('div.authorProfileBasicInfoPanel')

            name_title=personal_data.css('span.authorName::text').getall()
            scientist['first_name']= name_title[0]
            scientist['last_name']= name_title[1]
            academic_title=None
            if len(name_title)>2:
                academic_title=name_title[2]
                scientist['academic_title']= academic_title
            
            
            
            
            scientist['email']= personal_data.css('p.authorContactInfoEmailContainer>a::text').get() or None
            

            scientist['profile_url']= response.url
            scientist['position']=personal_data.css('p.possitionInfo span::text').get() or None

            scientist['h_index_scopus']=response.xpath('//li[@class="hIndexItem"][span[contains(text(), "Scopus")]]//a/text()').get() or 0
            
            scientist['h_index_wos']=response.xpath('//li[@class="hIndexItem"][span[contains(text(), "WoS")]]//a/text()').get() or 0
            
            pub_count=response.xpath('//li[contains(@class, "li-element-wcag")][span[@class="achievementName" and contains(text(), "Publications")]]//a/text()').get()
            scientist['publication_count']=pub_count or 0

            if response.css('ul.bibliometric-data-list li>span.indicatorName'):
                await page.wait_for_function(
                    """() => {
                        const element = document.querySelector('div#j_id_22_1_1_8_7_3_5b_a_2');
                        return element && element.textContent.trim().length > 0;
                    }"""
                )
                ministerial_score = await page.evaluate('document.querySelector("div#j_id_22_1_1_8_7_3_5b_a_2")?.textContent.trim()')

                scientist['ministerial_score'] = ministerial_score if '—' not in ministerial_score else 0
            else:
                scientist['ministerial_score'] = 0

            organization_scientist = personal_data.css('ul.authorAffilList li span a>span::text').getall() or None
            
            if organization_scientist:
                scientist['organization']=organization_scientist

            research_area = response.css('div.researchFieldsPanel ul.ul-element-wcag li span::text').getall()
            scientist['research_area'] = research_area or None

            if scientist['research_area'] and scientist['first_name']:
                yield scientist

        except Exception as e:
            self.logger.error(f'Error in parse_scientist, {e} {response.url}')
            yield scrapy.Request(response.url, callback=self.parse_scientist, dont_filter=True,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    errback=self.errback
            ))
        finally:
            await page.close()


    async def parse_publication_page(self, response):
        page = response.meta['playwright_page']
        total_pages = int(response.css('span.entitiesDataListTotalPages::text').get().replace(',', ''))
        
        await page.close()

    async def errback(self, failure):
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
