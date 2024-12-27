# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field
#from scrapy.loader import ItemLoader
#from itemloaders import processors
        
class ScientistItem(Item):
    first_name = Field()
    last_name = Field()
    academic_title = Field()
    position = Field()
    email = Field()
    organization_id = Field()
    profile_url = Field()
    identifier  = Field()

class BibliometricsItem(Item):
    identifier = Field()
    scientist_id = Field()
    h_index_wos = Field()
    h_index_scopus = Field()
    publication_count = Field()
    ministerial_score = Field()

class PublicationItem(Item):
    scientist_id = Field()
    identifier = Field()
    journal_type = Field()
    publication_year = Field()
    title = Field()
    journal = Field()
    #publisher = scrapy.Field()

class OrganizationItem(Item):
    organization_type = Field()
    name = Field()
    identifier = Field()
    parent_id = Field()