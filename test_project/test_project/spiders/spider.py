import scrapy
from loguru import logger

class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["virtual.tts.org"]
    start_urls = ["https://virtual.tts.org/virtual/programme/eposters"]
    
    def parse(self, response):
        posters_links_list = response.xpath('//div[4]/div/table/tbody/tr/td[2]/a[starts-with(@href, "/virtual/timeslot")]/@href').getall()
        logger.info(f"Found {len(posters_links_list)} posters")
        
        for link in reversed(posters_links_list):
            yield response.follow(link, callback=self.parse_posters)
    
    def parse_posters(self, response):
        presentation_links_list = response.xpath('//div[@class="uk-card uk-card-default uk-card-body uk-background-muted"]//a[starts-with(@href, "/virtual/lecture")]/@href').getall()
        logger.info(f"Found {len(presentation_links_list)} presentations in poster: {response.url}")
        
        for link in reversed(presentation_links_list):
            yield response.follow(link, callback=self.parse_presentations)
    
    def parse_presentations(self, response):
        logger.info(f"\n=== PARSING PRESENTATION ===")
        logger.info(f"URL: {response.url}")

        session_name = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[1]//a//text()').get()        
        abstract_authors = response.xpath('//div[@class="uk-card-header"]/p[2]//text()[not(ancestor::sup)//text()]').getall()
        poster_presenter = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[3]//a//text()').get()
        presentation_title_data = response.xpath('//div[@class="uk-card uk-card-default uk-width-1-1"]/div[2]//h3//text()').get()

        presentation_title_split_list = presentation_title_data.split(" ", 1)
        poster_presenter = poster_presenter.split(",", 1)[0]
        abstract_number, presentation_title = presentation_title_split_list[0], presentation_title_split_list[1]

        logger.info(f"Session Name: {session_name}")
        logger.info(f"Abstract Number: {presentation_title}")
        logger.info(f"Presentation Title: {presentation_title}")
        logger.info(f"Presenter: {poster_presenter}")
        logger.info(f"Authors: {abstract_authors}\n")
        
        yield {
            "Session Name": session_name,
            "Presentation Number": abstract_number,
            "Topic Title": presentation_title,
            "Poster Presenter": poster_presenter,
            "Abstract Authors": abstract_authors,
            "Presentation Title": presentation_title,
        }