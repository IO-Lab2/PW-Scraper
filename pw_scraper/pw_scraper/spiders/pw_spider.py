import scrapy
from scrapy_playwright.page import PageMethod
from pw_scraper.items import ScientistItem

class PwSpider(scrapy.Spider):
    name = "pw_spider"
    allowed_domains = ["repo.pw.edu.pl"]
    start_urls = [
        "https://repo.pw.edu.pl/globalResultList.seam?r=author&tab=PEOPLE&lang=en&p=amj&qp=academicDegree%253Aterm%253D%2526member%253D%2526team%253D%2526otheractivity%253D%2526specialization%253Aterm%253D%2526activityDiscipline%253Aterm%253D%2526authorprofile_keywords%253D&pn=1"
    ]

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type in ["image", "stylesheet", "font", "media"],
        "AUTOTHROTTLE_ENABLED": True,
    }

   def parse(self, response):
    total_pages = 145  # Or dynamically extract this value
    for page_number in range(1, total_pages + 1):
        page_url = f"https://repo.pw.edu.pl/globalResultList.seam?pn={page_number}"
        yield scrapy.Request(
            url=page_url,
            callback=self.parse_page,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[
                    PageMethod("wait_for_selector", "div.authorGlobalSearchTemplateDescriptionPanel", state="visible", timeout=180000),
                ],
            ),
        )

    def parse_page(self, response):
    self.logger.info(f"Processing page: {response.url}")

    # Extract profile links
    profile_links = response.css("div.authorGlobalSearchTemplateDescriptionPanel a::attr(href)").getall()
    base_url = "https://repo.pw.edu.pl"

    for link in profile_links:
        absolute_url = response.urljoin(link)
        yield scrapy.Request(
            url=absolute_url,
            callback=self.parse_scientist,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[
                    PageMethod("wait_for_selector", "p", state="visible", timeout=180000),
                ],
            ),
        )

    def parse_scientist(self, response):
    self.logger.info(f"Parsing scientist profile: {response.url}")
    scientist = ScientistItem()

    try:
        # Extract relevant data
        name = response.css("p.author-profile__name-panel span.authorName::text").get()
        scientist["name"] = name.strip() if name else "Unknown"

        position = response.css("p.possitionInfo span.authorAffil::text").get()
        scientist["position"] = position.strip() if position else "Unknown"

        email = response.css("dd[property='email'] a::attr(href)").re_first(r"mailto:(.*)")
        scientist["email"] = email if email else "No email provided"

        yield scientist

    except Exception as e:
        self.logger.error(f"Error parsing scientist profile: {e}")
