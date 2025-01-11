import scrapy
import re
import ast
from bs4 import BeautifulSoup as bs
from lxml import etree
import logging
from scrapy_playwright.page import PageMethod
from pw_scraper.items import ScientistItem, OrganizationItem

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
        
        
    }

    headers = {
            "Accept": "application/xml, text/xml, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Faces-Request": "partial/ajax",
            "Host": "repo.pw.edu.pl",
            "Origin": "https://repo.pw.edu.pl",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
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

        
        organizations=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li')
        university=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>div.ui-treenode-content div.ui-treenode-label>span>span::text').get()
        for org in organizations:
            organization=OrganizationItem()
            organization['university']=university

            institute=org.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['institute']=institute

            cathedras = org.css('ul.ui-treenode-children li.ui-treenode-leaf div.ui-treenode-content div.ui-treenode-label span>span::text').getall()
            if cathedras:
                organization['cathedras']=cathedras
            else:
                organization['cathedras']=[]
                
            yield organization
            
            
        total_pages=int(response.css('span.entitiesDataListTotalPages::text').get())

        formdata_pages = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "resultTabsOutputPanel",
                "primefaces.ignoreautoupdate": "true",
                "javax.faces.partial.execute": "resultTabsOutputPanel",
                "javax.faces.partial.render": "resultTabsOutputPanel",
                "resultTabsOutputPanel": "resultTabsOutputPanel",
                "resultTabsOutputPanel_load": "true",
            }

        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages+1):
            page_url = f'https://repo.pw.edu.pl/globalResultList.seam?r=author&tab=PEOPLE&lang=en&p=bst&pn={page_number}'.format(page_number=page_number)
            yield scrapy.FormRequest(url=page_url,
                callback=self.parse_scientist_links, 
                headers=self.headers,
                formdata=formdata_pages)
            
            
        await page.close()

    def parse_scientist_links(self, response):
        response_bytes = response.body
        root = etree.fromstring(response_bytes)
        cdata_content = root.xpath('//update/text()')[0]
        soup = bs(cdata_content, 'html.parser')
        links_selectors = soup.find_all('a', class_='authorNameLink')
        if links_selectors:
            links=[link.get('href') for link in links_selectors]
            
        for link in links:
            yield scrapy.Request(self.pw_url+link, callback=self.parse_scientist)


    def parse_scientist(self, response):
        '''
            Scrapes scientist profile page
        '''
        

        def email_creator(datax):
            first=datax[0]
            second=datax[1]
            res=[None for i in range(0, len(second))]

            for i in range(0, len(second)):
                let=first[i]
                if let=='#':
                    let='@'
                res[second[i]]=let

            return ''.join(res)
            
        

        try:
            match = re.search(r"datax=(.*?\]\])", response.text)
            email=None
            if match:
                datax = ast.literal_eval(match.group(1))
                email=email_creator(datax)

            personal_data=response.css('div.authorProfileBasicInfoPanel')

            names=personal_data.css('p.author-profile__name-panel::text').get().strip()
            first_name=None
            last_name=None
            if names:
                # Split by comma to separate name from the academic title
                names = names.split(',')
                name_part = names[0].strip()  # The part before the comma (the actual name)
                
                name_parts = name_part.split()  # Split the name by spaces
                first_name = name_parts[0]  # First name is the first part
                last_name = name_parts[-1]  # Last name is the last part (even if there's a middle name)


            academic_title=response.css('div.careerAchievementListPanel ul.careerAchievementList li span.achievementName span::text').getall() or None

            profile_url= response.url
            position=personal_data.css('p.possitionInfo span::text').get() or None

            organization_scientist=personal_data.css('ul.authorAffilList li span a>span::text').getall()
            organization=organization_scientist if organization_scientist else None

            research_area=response.css('div.researchFieldsPanel ul.ul-element-wcag li span::text').getall()
            research_area=research_area if research_area else None


            formdata = {
                'javax.faces.partial.ajax': 'true',
                'javax.faces.source': 'j_id_22_1_1_8_7_3_4d',
                'primefaces.ignoreautoupdate': 'true',
                'javax.faces.partial.execute': 'j_id_22_1_1_8_7_3_4d',
                'javax.faces.partial.render': 'j_id_22_1_1_8_7_3_4d',
                'j_id_22_1_1_8_7_3_4d': 'j_id_22_1_1_8_7_3_4d',
                'j_id_22_1_1_8_7_3_4d_load': 'true',
            }
            

            
        except Exception as e:
            self.logger.error(f'Error in parse_scientist, {e} {response.url}')
        finally:
            if research_area and academic_title:    
                yield scrapy.FormRequest(url=response.url,
                    formdata=formdata,
                    headers=self.headers,
                    callback=self.bibliometric,
                    meta=dict(first_name=first_name, 
                                last_name=last_name, 
                                email=email, 
                                academic_title=academic_title, 
                                position=position, 
                                organization=organization, 
                                research_area=research_area, 
                                profile_url=profile_url))

            

    def bibliometric(self, response):
        
        try:
            response_bytes = response.body
            root = etree.fromstring(response_bytes)
            cdata_content = root.xpath('//update/text()')[0]
            soup = bs(cdata_content, 'html.parser')

            scientist=ScientistItem()

            scientist['first_name'] = response.meta['first_name']
            scientist['last_name'] = response.meta['last_name']
            scientist['academic_title'] = response.meta['academic_title']
            scientist['email'] = response.meta['email']
            scientist['profile_url'] = response.meta['profile_url']
            scientist['position'] = response.meta['position']

            h_index_scopus = soup.find(id="j_id_22_1_1_8_7_3_5b_2_1:1:j_id_22_1_1_8_7_3_5b_2_6")
            scientist['h_index_scopus']= h_index_scopus.find_all(string=True, recursive=False)[0].strip() if h_index_scopus else 0

            h_index_wos = soup.find(id="j_id_22_1_1_8_7_3_5b_2_1:2:j_id_22_1_1_8_7_3_5b_2_6")
            scientist['h_index_wos']= h_index_wos.find_all(string=True, recursive=False)[0].strip() if h_index_wos else 0

            publication_count = soup.find(id="j_id_22_1_1_8_7_3_56_9:0:j_id_22_1_1_8_7_3_56_o_1")
            scientist['publication_count']= publication_count.find_all(string=True, recursive=False)[0].strip() if publication_count else 0

            ministerial_score = soup.find(id="j_id_22_1_1_8_7_3_5b_a_2")
            if ministerial_score:
                scientist['ministerial_score']= ministerial_score.text.replace('\xa0','').strip() if ministerial_score and ('—' not in ministerial_score) else 0


            scientist['organization'] = response.meta['organization']
            scientist['research_area'] = response.meta['research_area']

        except Exception as e:
            self.logger.error(f'Error in bibliometric, {e} {response.url}')
        finally:
            yield scientist


        
    
            
    async def errback(self, failure):
        
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()