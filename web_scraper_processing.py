import scrapy
import boto
import string
import re
import json
import csv

conn = boto.connect_s3()
bucket = conn.get_bucket('ml-eng-sf')
"""
This is a Scrapy Spider. It comes with a couple of default classes to make it easier for somebody to get straight to the point
with web scraping, but I have decided to stick with no templates and build one from scratch. The start url's list is immediately 
recognized by the spider's init function when this program is ran. It must be ran with the scrapy library installed as there are 
many functions being utilized outside of this python file when it is executed. The first link is parsed, turned into html and fed 
into the initial parse function. From there, I create additional responses that help interact with the websites CSS tags in order
to get desired information.
"""

class GlassSpider(scrapy.Spider):
    name = 'glassdoor'
    start_urls = ['https://www.glassdoor.com/Job/san-francisco-data-scientist-jobs-SRCH_IL.0,13_IC1147401_KO14,28.htm',
                  'https://www.glassdoor.com/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typed'
                  'Keyword=%22Machine+Learning%22&sc.keyword=%22Machine+Learning%22&locT=C&locId=1147401&jobType=','https'
                  '://www.glassdoor.com/Job/machine-learning-jobs-SRCH_KO0,16.htm?lst=-1','https://www.glassdoor.com/Job/j'
                  'obs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword=%22data+scientist%22&sc.ke'
                  'yword=%22data+scientist%22&locT=&locId=&jobType=''https://www.glassdoor.com/Job/jobs.htm?suggestCount=0&s'
                  'uggestChosen=false&clickSource=searchBtn&typedKeyword=data+scientist+machine+learning&sc.keyword=data+scie'
                  'ntist+machine+learning&locT=&locId=&jobType='] 


    def namer(self):
        """
        This turned out to be my helper function that kept things structured within my environment.
        I don't like working with lots of strings that clutter my most important functions, so I 
        wrote a place where I can keep them and append to.
        """
        listings_str = 'li[class="jl"]'
        links_str = 'div[class="logoWrap"] a::attr(href)'
        location_str = 'div[class="flexbox empLoc"] div span[class="subtle loc"]::text'
        salary_str = 'div[class="flexbox"] div span[class="green small"]::text'
        return listings_str,links_str,location_str,salary_str

    def parse(self,response):
        """
        The initial magic happens here. The first url is parsed and sent into this function. This is the first page the spider sees.
        Anything I deem important before continuing on to indiviual job ad listings is targeted and collected here.
        Most of these CSS paths took a very long time to figure out, as they sometimes would change as the spider dug deeper into
        the site.
        """
        listings_str, links_str, location_str, salary_str = self.namer()
        links = response.css(links_str).extract()
        listings = response.css(listings_str)
        for link in range(len(links)):
            yield scrapy.Request(response.urljoin(links[link]),callback=self.parse_listing, meta={'listing':listings[link]})
        """
        This function actually starts the initial process and ends up finishing it as well. 
        The next page variable is called upon after the for loop of every job ad on the current site 
        is done iterating. Yes, next page will be constantly saught after until there is no longer 
        any where to go and the process times out.
        """

        next_page = response.css('ul li[class="next"] a::attr(href)')
        next_page = response.urljoin(next_page.extract_first())
        yield scrapy.Request(next_page, callback=self.parse)

    def parse_listing(self,response):
        responday = response.meta['listing']
        listings_str, links_str, location_str, salary_str = self.namer()
        translator = str.maketrans('', '', string.punctuation)
        glass = {}
        salary = responday.css(salary_str).extract_first()
        location = responday.css(location_str).extract_first()
        description = ', '.join(response.css('div[class="jobDescriptionContent desc"] *::text').extract())
        recommend = response.css('div[class="tbl"] div[id="EmpStats_Recommend"]').extract_first()
        title = " ".join(responday.css('div[class="flexbox"] div a[class="jobLink"]::text').extract())
        glass['title'] = title.translate(translator) # Changed from string.
        glass['city'] = location.split(" ")[0]
        glass['state'] = location.split(" ")[1]
        glass['description'] = description.translate(translator)
        glass['employer'] = response.css('span[class="ib"]::text').extract_first()[1:]
        if response.css('div[class="ratingNum"]::text').extract_first() == None:
            glass['rating'] = 'NA'
        else:
            glass['rating'] = response.css('div[class="ratingNum"]::text').extract_first()

        try:
            salary = re.sub('[^0-9]', '', salary)
            if len(salary) == 5:
                glass['salary'] = salary[:2] + ' ' + salary[2:]
            elif len(salary) == 6:
                glass['salary'] = salary[:3] + ' ' + salary[3:]
            else:
                glass['salary'] = 'NA'
        except TypeError:
            glass['salary'] = 'NA'

        try:
            glass['recommend'] = re.sub('[^0-9]','', recommend)
        except TypeError:
            glass['recommend'] = 'NA'

        if glass['recommend'] == None:
            glass['recommend'] = 'NA'

        yield self.send_file(glass)
        

    def send_bucket(self, job_ad):
        filename = job_ad['employer'] #Changed from employer to title. ML or DS stands for what was put into search.
        k = boto.s3.key.Key(bucket)
        k.key = filename
        with open('job_ad.json','w') as fp:
            json.dump(job_ad, fp, ensure_ascii=False)
        k.set_contents_from_filename('job_ad.json')
