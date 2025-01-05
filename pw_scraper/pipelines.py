import json
import logging
from itemadapter import ItemAdapter
import psycopg
from dotenv import load_dotenv
import os
from pw_scraper.items import ScientistItem, PublicationItem, OrganizationItem
import re

class CleanItemsPipeline:
    # Clean items before saving to the database
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if isinstance(item, ScientistItem):
            self.clean_fields(adapter)

        elif isinstance(item, PublicationItem):
            self.clean_fields(adapter)

        elif isinstance(item, OrganizationItem):
            # Seemingly no fields of item need to be cleaned
            pass

        logging.info(f"Cleaned item: {adapter.get('')}")
        return item
    
    def clean_fields(self, adapter):
        # Clean the fields of ScientistItem
            for field_name in adapter.field_names():
                field_value = adapter.get(field_name)

                if isinstance(field_value, str):
                    field_value = field_value.strip()
                    field_value = re.sub(r'\s+', ' ', field_value) # Replace multiple spaces with a single space
                    adapter[field_name] = field_value


class pw_scraperPipeline:
    def open_spider(self, spider):
        # Initialize JSON files
        self.organisation_file_path = 'organisation.json'
        self.links_file_path = 'links.json'
        self.personal_data_file_path = 'personalData.json'

        # Create empty lists in the JSON files to initialize them
        self.initialize_json_file(self.organisation_file_path)
        self.initialize_json_file(self.links_file_path)
        self.initialize_json_file(self.personal_data_file_path)

        logging.info("Initialized JSON files")

    def close_spider(self, spider):
        logging.info("Spider finished")

    def process_item(self, item, spider):
        # Determine the file to save the item
        if "university" in item:  # Organization item
            file_path = self.organisation_file_path
        elif "profile_url" in item and "first_name" not in item:  # Scientist link
            file_path = self.links_file_path
        elif "first_name" in item:  # Scientist personal data
            file_path = self.personal_data_file_path
        else:
            logging.warning(f"Unknown item type: {item}")
            return item

        # Append the item to the appropriate file
        self.append_to_json_file(file_path, dict(item))

        logging.info(f"Saved item to {file_path}: {item}")
        return item

    def initialize_json_file(self, file_path):
        """Initialize an empty JSON file with an empty list."""
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump([], file, ensure_ascii=False)  # Initialize with an empty list

    def append_to_json_file(self, file_path, item):
        """Append an item to an existing JSON file."""
        # Read the existing data
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Append the new item
        data.append(item)

        # Write the updated data back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

class DatabasePipeline:
    def open_spider(self, spider):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=dotenv_path)

        self.hostname = os.getenv("PGHOST")
        self.scraper_username = os.getenv("PGUSERSCRAPER")
        self.scraper_password = os.getenv("PGPASSWORDSCRAPER")
        self.port = os.getenv("PGPORT")
        self.database = os.getenv("PGDATABASE")

        self.connection = psycopg.connect(host=self.hostname, user=self.scraper_username, 
                                          password=self.scraper_password, dbname=self.database, port=self.port)
        self.cur = self.connection.cursor()
        logging.info("Connected to database")

    def process_item(self, item, spider):
        # Insert the item into the appropriate table
        adapter = ItemAdapter(item)

        if isinstance(item, ScientistItem):
            pass
        elif isinstance(item, PublicationItem):
            pass
        elif isinstance(item, OrganizationItem):
            pass

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()
        logging.info("Database connection closed")
