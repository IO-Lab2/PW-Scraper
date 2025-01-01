import scrapy
import logging
from scrapy_playwright.page import PageMethod
from pw_scraper.items import ScientistItem, organizationItem

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
        # Parse the main categories
        categories_links = response.css('a.global-stats-link::attr(href)').getall()
        categories_names = response.css('span.global-stats-description::text').getall()
        categories = {name: self.pw_url + link for name, link in zip(categories_names, categories_links)}

        # Go to the "People" category for scraping
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
        # Process the first page of scientist links
        page = response.meta['playwright_page']

        page_url = 'https://repo.pw.edu.pl/globalResultList.seam?q=&oa=false&r=author&tab=PEOPLE&conversationPropagation=begin&lang=en&qp=openAccess%3Dfalse&p=xyz&pn=1'
        yield scrapy.Request(url=page_url,
            callback=self.parse_scientist_links, dont_filter=True,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[PageMethod('wait_for_selector', 'a.authorNameLink', state='visible')],
                errback=self.errback
            ))

        await page.close()

    async def parse_scientist_links(self, response):
        # Scrape scientist profile links and visit their profiles
        page = response.meta['playwright_page']

        authors_links = response.css('a.authorNameLink::attr(href)').getall()
        for author_link in authors_links:
            yield scrapy.Request(self.pw_url + author_link, callback=self.parse_scientist,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_context="people",
                    errback=self.errback
                ))

        await page.close()

    async def parse_scientist(self, response):
        # Scrape personal data from the scientist's profile
        page = response.meta['playwright_page']
        scientist = ScientistItem()

        try:
            personal_data = response.css('div.authorProfileBasicInfoPanel')

                        # Extract name and academic title
            name_title = personal_data.css('p.author-profile__name-panel::text').getall()
            self.logger.info(f"Full name data: {name_title}")

            # Initialize the first and last name
            full_name = name_title[0] if len(name_title) > 0 else None

            if full_name:
                name_parts = full_name.split()
                
                # Assuming the first part is the first name and the last part is the last name
                scientist['first_name'] = name_parts[0] if len(name_parts) > 0 else None
                scientist['last_name'] = name_parts[-1] if len(name_parts) > 1 else None
            else:
                scientist['first_name'] = None
                scientist['last_name'] = None

            # Extract academic title if available
            scientist['academic_title'] = name_title[1] if len(name_title) > 1 else None
                        # Extract email
            scientist['email'] = personal_data.css('p.authorContactInfoEmailContainer>a::text').get() or None

            # Profile URL
            scientist['profile_url'] = response.url

            # Position
            scientist['position'] = personal_data.css('p.possitionInfo span::text').get() or None

            # H-indexes and publication count
            scientist['h_index_scopus'] = response.xpath('//li[@class="hIndexItem"][span[contains(text(), "Scopus")]]//a/text()').get() or 0
            scientist['h_index_wos'] = response.xpath('//li[@class="hIndexItem"][span[contains(text(), "WoS")]]//a/text()').get() or 0
            pub_count = response.xpath('//li[contains(@class, "li-element-wcag")][span[@class="achievementName" and contains(text(), "Publications")]]//a/text()').get()
            scientist['publication_count'] = pub_count or 0

            # Organization affiliations
            organization_scientist = personal_data.css('ul.authorAffilList li span a>span::text').getall() or None
            scientist['organization'] = organization_scientist

            # Research areas
            research_area = response.css('div.researchFieldsPanel ul.ul-element-wcag li span::text').getall()
            scientist['research_area'] = research_area or None

            # Debug scraped data
            self.logger.info(f"Scraped scientist data: {scientist}")

            yield scientist

        except Exception as e:
            self.logger.error(f"Error in parse_scientist: {e} {response.url}")

        finally:
            await page.close()

    async def errback(self, failure):
        # Handle errors gracefully
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
