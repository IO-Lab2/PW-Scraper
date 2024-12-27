import json
from itemadapter import ItemAdapter
from pw_scraper.items import ScientistItem, PublicationItem, OrganizationItem  # Adjust imports as per your project

class PWScraperPipeline:
    """Pipeline to clean and process scraped items."""
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Cleaning and processing logic
        if isinstance(item, ScientistItem):
            self.process_scientist(adapter)
        elif isinstance(item, PublicationItem):
            self.process_publication(adapter)
        elif isinstance(item, OrganizationItem):
            self.process_organization(adapter)
        
        return item

    def process_scientist(self, adapter):
        """Process ScientistItem."""
        if 'academic_title' in adapter:
            adapter['academic_title'] = adapter['academic_title'].strip(', ')
        if 'email' in adapter:
            adapter['email'] = adapter['email'].strip().lower() if isinstance(adapter['email'], str) else None

    def process_publication(self, adapter):
        """Process PublicationItem."""
        if 'authors' in adapter:
            adapter['authors'] = [author.strip() for author in adapter['authors'] if isinstance(author, str)]
        if 'publication_date' in adapter and isinstance(adapter['publication_date'], str):
            adapter['publication_date'] += '-01-01'

    def process_organization(self, adapter):
        """Process OrganizationItem."""
        if 'name' in adapter:
            adapter['name'] = adapter['name'].strip()


class SaveToJsonPipeline:
    """Pipeline to save items to a JSON file."""
    
    def open_spider(self, spider):
        """Initialize the JSON file when the spider starts."""
        self.file = open('scraped_data.json', 'w', encoding='utf-8')
        self.file.write('[\n')  # Start the JSON array
        self.first_item = True

    def close_spider(self, spider):
        """Close the JSON file when the spider finishes."""
        self.file.write('\n]')  # End the JSON array
        self.file.close()

    def process_item(self, item, spider):
        """Write each item to the JSON file."""
        if not self.first_item:
            self.file.write(',\n')  # Add a comma before each new item
        else:
            self.first_item = False

        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False, indent=4)
        self.file.write(line)
        return item
