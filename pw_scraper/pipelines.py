import json
import logging
from itemadapter import ItemAdapter
import psycopg
from dotenv import load_dotenv
import os
from pw_scraper.items import ScientistItem, PublicationItem, OrganizationItem
import re
from scrapy.exceptions import DropItem


class CleanItemsPipeline:
    # Clean items before saving to the database
    def process_item(self, item, spider):
        """
        Processes and cleans the fields of the given item before saving it.

        This method uses the ItemAdapter to standardize access to item fields
        and applies specific cleaning routines based on the type of item being processed.
        For ScientistItem and PublicationItem, it cleans the fields by removing
        unwanted whitespace and normalizing spaces. OrganizationItem is currently
        processed without any additional cleaning.

        Args:
            item: The item to process, which can be an instance of ScientistItem,
                PublicationItem, or OrganizationItem.
            spider: The spider that scraped the item.

        Returns:
            The processed item after cleaning operations.
        """

        adapter = ItemAdapter(item)

        if isinstance(item, ScientistItem):
            if not adapter.get('academic_title'):
                missing_field = 'academic title'
                logging.warning(f"Item missing required field: {missing_field}")
                raise DropItem('')
                return None
            
            academic_title = adapter.get('academic_title')
            if isinstance(academic_title, list):
                adapter['academic_title'] = academic_title[0]
                
            self.clean_fields(adapter)

        elif isinstance(item, PublicationItem):
            self.clean_fields(adapter)

        elif isinstance(item, OrganizationItem):
            # Seemingly no fields of item need to be cleaned
            pass

        item_name = adapter.get('title')
        if not item_name:
            item_name = f"{adapter.get('last_name')} {adapter.get('first_name')}" if adapter.get('first_name') else None
        if not item_name:
            item_name = adapter.get('institute')
            
        logging.info(f"Cleaned item: {item_name}")
        return item

    def clean_fields(self, adapter):  
        """
        Cleans the fields of a ScientistItem or PublicationItem by stripping whitespace and normalizing spaces.

        This method iterates over all fields of the given item adapter. For each field with
        a string value, it performs the following cleaning operations:
        1. Strips leading and trailing whitespace.
        2. Replaces multiple consecutive spaces with a single space.

        Args:
            adapter (ItemAdapter): The adapter of the item containing the fields to be cleaned.
        """

        for field_name in adapter.field_names():
            field_value = adapter.get(field_name)

            if isinstance(field_value, str):
                field_value = field_value.strip()
                # Replace multiple spaces with a single space
                field_value = re.sub(r'\s+', ' ', field_value)
                adapter[field_name] = field_value
            
            if field_value is None:
                adapter[field_name] = ''


class SaveToJsonFilePipeline:
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
        """
        Determines the appropriate JSON file to save the given item and appends it to that file.

        Args:
            item: The item to be saved.
            spider: The spider that scraped the item.

        Returns:
            The processed item.
        """
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
        """
        This method is called when the spider is opened. It connects to the PostgreSQL
        database using the environment variables set in the .env file. The connection
        and cursor objects are stored as instance variables.

        :param spider: The spider that is being opened.
        :type spider: scrapy.Spider
        """
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=dotenv_path)

        self.hostname = os.getenv("PGHOST")
        self.scraper_username = os.getenv("PGUSERSCRAPER")
        self.scraper_password = os.getenv("PGPASSWORDSCRAPER")
        self.port = os.getenv("PGPORT")
        self.database = os.getenv("PGDATABASE")
        
        try: 
            self.connection = psycopg.connect(host=self.hostname, user=self.scraper_username,
                                            password=self.scraper_password, dbname=self.database, port=self.port)
            self.cur = self.connection.cursor()
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")

        logging.info(f'Spider: {spider.name} connected to database')

    def process_item(self, item, spider):
        """
        Process the given item and update or insert it into the appropriate table.

        Args:
            item: The item to process, which can be an instance of ScientistItem, PublicationItem, or OrganizationItem.
            spider: The spider that scraped the item.

        Returns:
            The processed item, or None if the item was not processed.
        """
       
        adapter = ItemAdapter(item)

        if isinstance(item, ScientistItem):
            scientist_fields = tuple(ItemAdapter(item).values())

            # scientist table
            scientist_id = self.update_scientist(adapter, scientist_fields)

            # bibliometrics table
            self.update_scientist_bibliometrics(
                adapter, scientist_id, scientist_fields[6:10])

            # scientist_organization table
            self.update_scientist_relationship(scientist_id, adapter)

            # scientist_research_areas table
            self.update_research_area(scientist_id)

            return item

        elif isinstance(item, PublicationItem):
            logging.info(f"Database processing item: {adapter.get('title')}")
            authors = adapter.get('authors')
            if adapter['publication_date']:
                adapter['publication_date'] += '-01-01'
            if authors:
                for author_id in authors:
                    publication_id = self.update_publication(adapter['title'],
                                                                 adapter['publisher'],
                                                                 adapter['publication_date'],
                                                                 adapter['journal'],
                                                                 adapter['ministerial_score'])
                    
                    select_query = "SELECT id FROM scientists WHERE profile_url like %s;"
                    try:
                        self.cur.execute(select_query, ('%' + author_id + '%',))
                        result = self.cur.fetchone()
                    except Exception as e:
                        logging.error(f"Error executing query: {e}")

                    if result:
                        scientist_id = result[0]
                        self.update_author_publications(
                            scientist_id, publication_id)

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
                    cathedra_id = self.update_organization(
                        cathedra, 'cathedra')
                    self.update_organization_relationship(
                        institute_id, cathedra_id)
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
        """
        Updates a scientist in the database if it already exists, otherwise it adds the scientist to the database.

        Args:
            adapter (ItemAdapter): The adapter of the item that contains the scientist's information.
            scientist_fields (list): A list of fields of the scientist to be updated or added.

        Returns:
            int: The id of the scientist in the database.
        """
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
                try:
                    self.cur.execute(update_query, (email,))
                except Exception as e:
                    logging.error(f"Error executing query inside update_scientist: {e}")

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
        """
        Updates a publication in the database if it already exists, or inserts a new one if it doesn't.

        Args:
            title: The title of the publication.
            publisher: The publisher of the publication.
            publication_date: The publication date of the publication (in ISO 8601 format).
            journal: The name of the journal the publication was published in.
            ministerial_score: The ministerial score of the publication.

        Returns:
            The id of the publication in the database.
        """
        try:
            select_query = "SELECT id, journal, ministerial_score FROM publications WHERE title = %s AND (publication_date = %s OR publication_date IS NULL);"
            self.cur.execute(select_query, (title, publication_date))
            result = self.cur.fetchone()
        except Exception as e:
            logging.error(f"Error select query inside update_publications query: {e}")
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
                try:
                    self.cur.execute(
                        update_query, (journal, ministerial_score, result[0]))
                except Exception as e:
                    logging.error(f"Error update inside update_publications query: {e}")

            logging.info(f"Publication in the database updated")
            return result[0]

        else:
            insert_query = """
                            INSERT INTO 
                            publications 
                            (title, publisher, publication_date, journal_impact_factor, journal, ministerial_score) 
                            VALUES (%s, %s, %s, 0, %s, %s) RETURNING id;"""
            try:
                self.cur.execute(insert_query, (title, publisher,
                                publication_date, journal, ministerial_score))
            except Exception as e:
                logging.error(f"Error insert query: {e}")

            logging.info(f"Publication added to the database")
            return self.cur.fetchone()[0]

    def update_author_publications(self, scientist_id, publication_id):
        """
        Updates the scientist-publication relation in the database if it already exists, or inserts a new one if it doesn't.

        Args:
            scientist_id (int): The id of the scientist.
            publication_id (int): The id of the publication.

        Returns:
            int: The id of the scientist-publication relation in the database.
        """
        select_query = """
                    SELECT scientist_id 
                    FROM scientists_publications
                    WHERE scientist_id=%s AND publication_id=%s;"""
        try:
            self.cur.execute(select_query, (scientist_id, publication_id))
            result = self.cur.fetchone()
        except Exception as e:
            logging.error(f"Error select inside update_author_publications query: {e}")

        if not result:
            insert_query = """
                            INSERT INTO 
                            scientists_publications 
                            (scientist_id, publication_id) 
                            VALUES (%s, %s) RETURNING id;"""
            try:
                self.cur.execute(insert_query, (scientist_id, publication_id))
            except Exception as e:
                logging.error(f"Error insert inside update_author_publications query: {e}")

            logging.info(f"Scientist-publication relation added to the database")
            return self.cur.fetchone()[0]

        else:
            logging.info(f"Scientist-publication relation already exists in the database")
            return result[0]

    def update_organization(self, name, organization_type):
        """
        Updates an organization in the database if it already exists, or inserts a new one if it doesn't.

        Args:
            name (str): The name of the organization.
            organization_type (str): The type of the organization.

        Returns:
            int: The id of the organization in the database.
        """

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
        """
        Updates an organization-organization relation in the database if it already exists, or inserts a new one if it doesn't.

        Args:
            parent_id (int): The id of the parent organization.
            child_id (int): The id of the child organization.

        Returns:
            int: The id of the organization-organization relation in the database.
        """
        if parent_id is None:
            self.cur.execute(
                "SELECT id FROM organizations_relationships WHERE parent_id IS NULL AND child_id=%s;", (child_id,))

        elif child_id is None:
            self.cur.execute(
                "SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id IS NULL;", (parent_id,))

        else:
            self.cur.execute(
                "SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id=%s;", (parent_id, child_id))

        if self.cur.fetchone() is None:
            insert_query = """
                            INSERT INTO 
                            organizations_relationships 
                            (parent_id, child_id) 
                            VALUES (%s, %s) RETURNING id;"""
            self.cur.execute(insert_query, (parent_id, child_id))

    def update_scientist_bibliometrics(self, scientist_id, scientist_fields):
        """
        Updates a scientist's bibliometrics in the database if it already exists, otherwise it adds the bibliometrics to the database.

        Args:
            scientist_id (int): The id of the scientist.
            scientist_fields (list): A list of fields of the scientist's bibliometrics to be updated or added.

        Returns:
            int: The id of the bibliometrics in the database.
        """
        select_query = """
                    SELECT
                        h_index_wos,
                        h_index_scopus,
                        publication_count,
                        ministerial_score
                    FROM bibliometrics
                    WHERE scientist_id = %s;
                    """
        try:
            self.cur.execute(select_query, (scientist_id,))
        except Exception as e:
            logging.error(f"Error select inside update_scientist_bibliometrics query: {e}")

        bibliometrics_db_check = self.cur.fetchone()

        if bibliometrics_db_check:
            if bibliometrics_db_check != scientist_fields:
                update_query = """
                            UPDATE bibliometrics
                            SET
                                h_index_wos = %s,
                                h_index_scopus = %s,
                                publication_count = %s,
                                ministerial_score = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE scientist_id = %s;
                            """
                try:
                    self.cur.execute(
                    update_query, scientist_fields + (scientist_id,))
                except Exception as e:
                    logging.error(f"Error update inside update_scientist_bibliometrics query: {e}")

            logging.info(f"Bibliometrics in the database updated")

        else:
            insert_query = """
                            INSERT INTO 
                            bibliometrics 
                            (h_index_wos, h_index_scopus, publication_count, ministerial_score, scientist_id) 
                            VALUES (%s, %s, %s, %s, %s) RETURNING id;"""
            try:
                self.cur.execute(insert_query, scientist_fields + (scientist_id,))
            except Exception as e:
                logging.error(f"Error insert inside update_scientist_bibliometrics query: {e}")

            logging.info(f"Bibliometrics added to the database")

    def update_scientist_relationship(self, scientist_id, adapter):
        """
        Updates a scientist's organizations in the database if they already exist, otherwise it adds the organizations to the database.

        Args:
            scientist_id (int): The id of the scientist.
            adapter (ItemAdapter): The adapter of the item that contains the scientist's information.

        Returns:
            None
        """
        organizations = adapter.get('organizations')
        for key in organizations:
            select_organization_query = "SELECT id FROM organizations WHERE name like %s;"
            self.cur.execute(select_organization_query, {organizations[key]})
            organization_id = self.cur.fetchone()[0]

            select_query = """
                                SELECT organization_id, so.id 
                                FROM scientist_organization so 
                                INNER JOIN organizations o ON so.organization_id = o.id
                                WHERE scientist_id = %s AND type=%s;"""
            try:
                self.cur.execute(select_query, (scientist_id, key))
                scientist_organization_check = self.cur.fetchone()
            except Exception as e:
                logging.error(f"Error select inside update_scientist_relationship query: {e}")

            if scientist_organization_check:
                if scientist_organization_check[0] != organization_id:
                    update_query = """
                                UPDATE scientist_organization
                                SET
                                    organization_id = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s;
                                """
                    try:
                        self.cur.execute(
                            update_query, (organization_id, scientist_organization_check[1]))
                    except Exception as e:
                        logging.error(f"Error update inside update_scientist_relationship query: {e}")
                    
                    logging.info(f"Organization in the database updated")

                else:
                    logging.info(f"Organization in the database not updated")

            else:
                insert_query = """
                                INSERT INTO 
                                scientist_organization 
                                (scientist_id, organization_id) 
                                VALUES (%s, %s);
                                """
                try:
                    self.cur.execute(insert_query, (scientist_id, organization_id))
                except Exception as e:
                    logging.error(f"Error insert inside update_scientist_relationship query: {e}")

                logging.info(f"Organization added to the database")

    def update_research_area(self, adapter, scientist_id):
        """
        Updates a scientist's research areas in the database if they already exist, otherwise it adds the research areas to the database.

        This function first checks if each research area associated with the scientist exists in the database. If a research area 
        doesn't exist, it inserts the new research area and retrieves its ID. It then establishes a relationship between the 
        scientist and each research area by inserting entries into the scientists_research_areas table.

        Args:
            adapter (ItemAdapter): The adapter of the item that contains the scientist's information, including research areas.
            scientist_id (int): The id of the scientist.

        Returns:
            None
        """

        research_area_ids = []
        research_areas = adapter.get('research_area')

        for research_area in research_areas:
            try:
                self.cur.execute(
                    f"SELECT id FROM research_areas WHERE name like '{research_area}';")
                in_db = self.cur.fetchone()
            except Exception as e:
                logging.error(f"Error select inside update_research_area query: {e}")

            if in_db:
                research_area_ids.append(in_db[0])

            else:
                insert_query = "INSERT INTO research_areas (name) VALUES (%s) RETURNING id;"
                try:
                    self.cur.execute(insert_query, (research_area,))
                    research_area_ids.append(self.cur.fetchone()[0])
                except Exception as e:
                    logging.error(f"Error insert inside update_research_area query: {e}")

                logging.info(f"Research area added to the database")
        
        select_query = "SELECT research_area_id FROM scientists_research_areas WHERE scientist_id = %s;"
        self.cur.execute(select_query, (scientist_id,))

        ra_result = [row[0] for row in self.cur.fetchall()]

        for ra_id in research_area_ids:
            if ra_id not in ra_result:
                insert_query = "INSERT INTO scientists_research_areas (scientist_id, research_area_id) VALUES (%s, %s);"
                try:
                    self.cur.execute(insert_query, (scientist_id, ra_id))
                except Exception as e:
                    logging.error(f"Error insert inside update_research_area query: {e}")

                logging.info(f"Scientist-research area relation added to the database")
