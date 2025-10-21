import re
import scrapy
from loguru import logger

from test_project.utils.format_data import format_authors, format_affiliations, format_names_and_affiliations_dictionary

class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["virtual.tts.org"]
    start_urls = ["https://virtual.tts.org/virtual/programme/eposters"]

    def __init__(self, pipeline="default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline_type = pipeline


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        # extract the pipeline argument (if provided via -a)
        pipeline_type = kwargs.get("pipeline", "default")

        # Dynamically change ITEM_PIPELINES setting based on argument
        if pipeline_type == "filtered":
            crawler.settings.set(
                'ITEM_PIPELINES',
                {'test_project.pipelines.FilteredAbstractAuthorPipeline': 300},
                priority='spider'
            )
        else:
            crawler.settings.set(
                'ITEM_PIPELINES',
                {'test_project.pipelines.PandasXlsxPipeline': 300},
                priority='spider'
            )

        # Instantiate spider normally
        spider = super().from_crawler(crawler, *args, **kwargs)

        # manually set the attribute on the spider instance
        spider.pipeline_type = pipeline_type

        return spider

    
    def parse(self, response):
        posters_links_list = response.css('td.uk-table-link a.uk-link-reset::attr(href)').getall()
        logger.info(f"Found {len(posters_links_list)} posters")
        
        for link in reversed(posters_links_list):
            yield response.follow(link, callback=self.parse_posters)
    
    def parse_posters(self, response):
        presentation_links_list = response.xpath('//div[4]/div/div[3]/div/div/div[1]/div/div/div/div/a//@href').getall()
        logger.info(f"Found {len(presentation_links_list)} presentations in poster: {response.url}")
        
        for link in reversed(presentation_links_list):
            yield response.follow(link, callback=self.parse_presentations)
    
    def parse_presentations(self, response):
        session_name = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[1]//a//text()').get()        
        presentation_title_data = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[2]//h3//text()').get()
        authors_data = response.xpath('//div[@class="uk-card-header"]/p[2]//text()').getall()
        affiliations_data = response.xpath('//div[@class="uk-card-header"]/p[3]//text()').getall()
        poster_presenter = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[3]//a//text()').get().split(",",1)[0]
        presentation_title_split_list = presentation_title_data.split(" ", 1)
        abstract_number, presentation_title = presentation_title_split_list[0], presentation_title_split_list[1]
        abstract_text = response.xpath('//div[@class="uk-card-header"]/div/p//text() '
        '| //div[@class="uk-card-header"]/h5[1]//text() '
        '| //div[@class="uk-card-header"]//h5/following-sibling::p//text()').getall()
        
        names_dictionary = format_authors(authors_data)
        affiliations_dictionary = format_affiliations(affiliations_data)

        logger.info(f"AUTHORS DICTIONARY: {names_dictionary}")
        logger.info(f"AFFILIATIONS DICTIONARY: {affiliations_data}")

        poster_presenter = re.sub(r'\s+', ' ', poster_presenter.strip())
        abstract_text = re.sub(r'\s+', ' ', "".join(abstract_text).strip())
        
        authors_dictionary = format_names_and_affiliations_dictionary(d1=names_dictionary, d2=affiliations_dictionary)
        logger.info(f"RESULTING DICTIONARY: {authors_dictionary}")


        for name, affiliation in authors_dictionary.items():
            role = "Poster presenter" if name == poster_presenter else "Abstract author"
            logger.info(f"YIELDING ITEM: {name}")

            if self.pipeline_type == "filtered":
                if role == "Abstract author":
                    parts = name.split()
                    first, middle, last = (parts + [None, None, None])[:3]

                    yield {
                        "First Name": first,
                        "Middle Name": middle,
                        "Last Name": last,
                        "Affiliation(s)": affiliation,
                        "Session Name": session_name,
                        "Presentation Number": abstract_number,
                        "Topic Title": presentation_title,
                        "Presentation Abstract": abstract_text,
                        "Abstract URL": response.url,
                    }
            else:
                yield {
                    'Name (incl. titles if any mentioned)': name,
                    'Affiliation(s)': affiliation,
                    "Person's role": role,
                    'Session Name': session_name,
                    'Presentation Number': abstract_number,
                    'Topic Title': presentation_title,
                    'Presentation Abstract': abstract_text,
                    'Abstract URL': response.url,
                }

