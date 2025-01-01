import json
from itemadapter import ItemAdapter

class pw_scraperPipeline:
    def open_spider(self, spider):
        # Initialize both JSON files
        self.organisation_file_path = 'organisation.json'
        self.links_file_path = 'links.json'

        with open(self.organisation_file_path, 'w') as org_file:
            json.dump([], org_file)  # Initialize with an empty list

        with open(self.links_file_path, 'w') as links_file:
            json.dump([], links_file)  # Initialize with an empty list

        spider.logger.info("Initialized organisation.json and links.json")

    def close_spider(self, spider):
        spider.logger.info("Spider finished")

    def process_item(self, item, spider):
        if "university" in item:  # Organization item
            file_path = self.organisation_file_path
        elif "profile_url" in item:  # Scientist link item
            file_path = self.links_file_path
        else:
            self.logger.warning(f"Unknown item type: {item}")
            return item

        # Append the item to the appropriate file
        with open(file_path, 'r') as file:
            data = json.load(file)

        data.append(dict(item))

        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

        self.logger.info(f"Saved item to {file_path}: {item}")
        return item
