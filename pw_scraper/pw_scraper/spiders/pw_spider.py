import scrapy
import logging
from scrapy_playwright.page import PageMethod
from pw_scraper.items import organizationItem

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
        # Scrape organization data
        page = response.meta['playwright_page']

        organizations = response.css(
            'div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li'
        )
        university = response.css(
            'div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>div.ui-treenode-content div.ui-treenode-label>span>span::text'
        ).get()

        for org in organizations:
            organization = organizationItem()
            organization['university'] = university

            institute = org.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['institute'] = institute

            cathedras = org.css(
                'ul.ui-treenode-children li.ui-treenode-leaf div.ui-treenode-content div.ui-treenode-label span>span::text'
            ).getall()
            organization['cathedras'] = cathedras if cathedras else []

            self.logger.info(f"Yielding organization: {organization}")  # Debugging
            yield organization

        # Scrape only the first page of "People" links
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
        # Scrape scientist profile links
        page = response.meta['playwright_page']

        authors_links = response.css('a.authorNameLink::attr(href)').getall()
        for author_link in authors_links:
            self.logger.info(f"Found scientist link: {self.pw_url + author_link}")  # Debugging
            yield {"profile_url": self.pw_url + author_link}

        await page.close()

    async def errback(self, failure):
        # Handle errors gracefully
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
