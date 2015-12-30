from scrapy.spiders	import Spider
from scrapy 	        import Selector
from wiki_spider.items	import WikipediaItem
from scrapy.http	import Request
from time               import sleep
import re, urlparse

class WikiSpider(Spider):
    name 		= "wiki_spider"
    allowed_domains	= ["en.wikipedia.org"]
    start_urls	= ["https://en.wikipedia.org/wiki/Earth"]
    count = 0

    def parse(self, response):
        hxs        = Selector(response)

        links            = hxs.xpath("//a/@href").extract()

        #We stored already crawled links in this list
        crawledLinks     = []

        #Pattern to check proper link
        linkPattern     = re.compile("^\/wiki")
        
        #Pattern to check "diff" link.
        diffPattern     = "&diff="

        #Pattern to check for "special page" link
        specialPagePattern = re.compile("^\/wiki\/User_talk:|\/wiki\/File:|\/wiki\/Special:|\/wiki\/Talk:")      
 
        for link in links:
            # If it is a proper link and is not checked yet, yield it to the Spider
            if linkPattern.match(link) and not specialPagePattern.match(link) \
                and not (diffPattern in link) and not link in crawledLinks:
                crawledLinks.append(link)
                yield Request(urlparse.urljoin("https://en.wikipedia.org/",link), self.parse)
        
        coordinates = hxs.xpath('//table[@class="infobox geography vcard"]')
        if coordinates:
            #print coordinates
            #sleep(5)
            titles     = hxs.xpath('//h1[@class="firstHeading"]/text()').extract()

            for title in titles:
                self.count += 1
                item = WikipediaItem()
                item["title"] = title
                item["referrer"] = response.request.headers.get('Referer', None)
                print self.count, title, "<--", item["referrer"]
                item["url"] = response.url
                yield item
                del response
        else:
            print "--- NOT A PLACE -> ", response.url
