import json
import os

class SaveToJsonFile:
    def open_spider(self, spider):
        # Create or clear the JSON file when the spider starts
        self.file_path = 'scraped_dataa.json'
        with open(self.file_path, 'w') as file:
            json.dump([], file)  # Initialize with an empty list

        print(f'SPIDER: {spider.name} STARTED')

    def close_spider(self, spider):
        # Finalize actions when the spider closes
        print(f'SPIDER: {spider.name} FINISHED')

    def process_item(self, item, spider):
        # Read the existing JSON data
        with open(self.file_path, 'r') as file:
            data = json.load(file)
        
        # Append the new item (convert to a dict first)
        data.append(dict(item))
        
        # Write the updated data back to the file
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)
        
        return item
