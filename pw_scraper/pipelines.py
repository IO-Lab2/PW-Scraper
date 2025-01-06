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
                # Replace multiple spaces with a single space
                field_value = re.sub(r'\s+', ' ', field_value)
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
            # Initialize with an empty list
            json.dump([], file, ensure_ascii=False)

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
        logging.info(f'Spider: {spider.name} connected to database')

    def process_item(self, item, spider):
        # Insert the item into the appropriate table
        adapter = ItemAdapter(item)

        if isinstance(item, ScientistItem):
            scientist_fields = tuple(ItemAdapter(item).values())
            self.update_scientist(adapter, scientist_fields)
            return item

        elif isinstance(item, PublicationItem):
            authors = adapter.get('authors')
            if authors:
                for author_id in authors:
                    select_query = "SELECT id FROM scientists WHERE profile_url like %s;"
                    self.cur.execute(select_query, ('%' + author_id + '%',))
                    result = self.cur.fetchone()

                    if result:
                        scientist_id = result[0]
                        publication_id = self.update_publication(adapter['title'],
                                                                 adapter['publisher'],
                                                                 adapter['publication_date'],
                                                                 adapter['journal'],
                                                                 adapter['ministerial_score'])
                        self.update_author_publications(
                            scientist_id, publication_id)
                        logging.info(
                            f"Publication {adapter.get['title']} added to the scientist with id {author_id}")

        elif isinstance(item, OrganizationItem):
            university_id = self.update_organization(adapter.get('university'),
                                                     'university')
            self.update_organization_relationship(None, university_id)
            
            institute_id = self.update_organization(adapter.get('institute'),
                                                     'institute')
            self.update_organization_relationship(university_id, institute_id)

            cathedras = adapter.get('cathedras')
            if cathedras:
                for cathedra in cathedras:
                    cathedra_id = self.update_organization(cathedra, 'cathedra')
                    self.update_organization_relationship(institute_id, cathedra_id)
                    self.update_organization_relationship(cathedra_id, None)
            else:
                self.update_organization_relationship(institute_id, None)

        self.connection.commit()
        return item

    def close_spider(self, spider):

        self.cur.close()
        self.connection.close()
        logging.info(f'Spider: {spider.name}Database connection closed')

    def update_scientist(self, adapter, scientist_fields):
        email = adapter.get('email')

        search_query = """
            SELECT id, first_name, last_name, academic_title, email, profile_url, position FROM scientists WHERE email = %s;
        """
        self.cur.execute(search_query, (email,))
        scientist_db_check = self.cur.fetchone()

        if scientist_db_check:
            if scientist_db_check[1:] != scientist_fields[:6]:
                update_query = """
                                UPDATE scientists
                                SET
                                    first_name = %s,
                                    last_name = %s,
                                    academic_title = %s,
                                    email = %s,
                                    profile_url = %s,
                                    updated_at = CURRENT_TIMESTAMP,
                                    position=%s
                                WHERE email = %s;
                                """
                self.cur.execute(update_query, (email,))

                logging.info(
                    f"{adapter.get('first_name')} {adapter.get('last_name')} updated in the database")

            return scientist_db_check[0]
        else:
            add_query = """ 
                    INSERT INTO
                    scientists (
                        first_name,
                        last_name,
                        academic_title,
                        email,
                        profile_url,
                        position
                    )
                    VALUES (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                            )"""
            self.cur.execute(add_query, scientist_fields[:6])
            logging.info(
                f"{adapter.get('first_name')} {adapter.get('last_name')} added to the database")
            return self.cur.fetchone()[0]

    def update_publication(self, title, publisher, publication_date, journal, ministerial_score):
        select_query = "SELECT id, journal, ministerial_score FROM publications WHERE title = %s AND (publication_date = %s OR publication_date IS NULL);"
        self.cur.execute(select_query, (title, publication_date))

        result = self.cur.fetchone()
        if result:

            if result[1:3] != (journal, ministerial_score,):
                update_query = """
                                UPDATE publications
                                SET
                                    journal = %s,
                                    ministerial_score = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s;
                                """
                self.cur.execute(
                    update_query, (journal, ministerial_score, result[0]))

            return result[0]

        else:
            insert_query = """
                            INSERT INTO 
                            publications 
                            (title, publisher, publication_date, journal_impact_factor, journal, ministerial_score) 
                            VALUES (%s, %s, %s, 0, %s, %s) RETURNING id;"""
            self.cur.execute(insert_query, (title, publisher,
                             publication_date, journal, ministerial_score))
            return self.cur.fetchone()[0]

    def update_author_publications(self, scientist_id, publication_id):
        select_query = """
                    SELECT scientist_id 
                    FROM scientists_publications
                    WHERE scientist_id=%s AND publication_id=%s;"""
        self.cur.execute(select_query, (scientist_id, publication_id))
        result = self.cur.fetchone()

        if not result:
            insert_query = """
                            INSERT INTO 
                            scientists_publications 
                            (scientist_id, publication_id) 
                            VALUES (%s, %s) RETURNING id;"""
            self.cur.execute(insert_query, (scientist_id, publication_id))
            return self.cur.fetchone()[0]

        else:
            return result[0]

    def update_organization(self, name, organization_type):
        select_query = "SELECT id FROM organizations WHERE name like %s AND type=%s;"
        self.cur.execute(select_query, (name, organization_type))
        result = self.cur.fetchone()
        
        if result:
            return result[0]
        
        else:
            insert_query = """
                            INSERT INTO 
                            organizations 
                            (name, type) 
                            VALUES (%s, %s) RETURNING id;"""
            self.cur.execute(insert_query, (name, organization_type))
            return self.cur.fetchone()[0]

    def update_organization_relationship(self, parent_id, child_id):
        if parent_id is None:
            self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id IS NULL AND child_id=%s;", (child_id,))
        
        elif child_id is None:
            self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id IS NULL;", (parent_id,))
        
        else:
            self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id=%s;", (parent_id, child_id))
        
        if self.cursor.fetchone() is None:
            insert_query = """
                            INSERT INTO 
                            organizations_relationships 
                            (parent_id, child_id) 
                            VALUES (%s, %s) RETURNING id;"""
            self.cur.execute(insert_query, (parent_id, child_id))