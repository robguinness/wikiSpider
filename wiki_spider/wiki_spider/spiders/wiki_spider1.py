from scrapy.spiders	import Spider
from scrapy 	        import Selector
#from scrapy.utils.response import get_base_url
from wiki_spider.items	import WikipediaItem
from scrapy.http	import Request
from time               import sleep
import re, urlparse

class WikiSpider(Spider):
    name 		= "wiki_spider"
    allowed_domains	= ["en.wikipedia.org"]
    start_urls	= ["https://en.wikipedia.org/wiki/Earth"]
    count = 0
    degree_sign= u'\N{DEGREE SIGN}'
    minute_sign= u'\N{PRIME}'
    second_sign= u'\N{DOUBLE PRIME}'

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
        
        geography = hxs.xpath('//table[@class="infobox geography vcard"]')
        if geography:
            #print coordinates
            #sleep(5)
            titles     = hxs.xpath('//h1[@class="firstHeading"]/text()').extract()
            latitude = hxs.xpath('//span[@id="coordinates"]//span[@class="latitude"]/text()').extract()
            longitude = hxs.xpath('//span[@id="coordinates"]//span[@class="longitude"]/text()').extract()
            print latitude, longitude
            if latitude and longitude:
              coordinates = self.parseCoordinates(latitude, longitude)
              print coordinates
            else:
              coordinates = [None, None]
            for title in titles:
                self.count += 1
                item = WikipediaItem()
                item["title"] = title
                item["latitude"] = coordinates[0]
                item["longitude"] = coordinates[1]

                # get the referrer without the common url part
                referrer = response.request.headers.get('Referer', None)
                if referrer:
                   item["referrer"] = referrer.split('/wiki/')[1]
                   #print referrer
                else: 
                  item["referrer"] = referrer
                print self.count, title, "<--", item["referrer"]

                # get the url without the common url part
                item["url"] = response.url.split('/wiki/')[1]
                yield item
                #del response
        else:
            self.logger.debug('--- NOT A PLACE -> ' + response.url)

    def parseCoordinates(self, latitude, longitude):
      # Parse latitude
      if self.degree_sign in latitude[0]:
        # Split degrees
        latSplit = latitude[0].split(self.degree_sign)
        latDegree = float(latSplit[0])
        latMinute = latSplit[1]
        if self.minute_sign in latMinute:
          # Split minutes
          latMinuteSplit = latMinute.split(self.minute_sign)
          latMinute = float(latMinuteSplit[0])
          latSecond = latMinuteSplit[1]
          if self.second_sign in latSecond:
            latSecondSplit = latSecond.split(self.second_sign)
            latSecond = float(latSecondSplit[0])
            latSign = latSecondSplit[1]
            latitude = latDegree + latMinute/60 + latSecond/3600
            print latDegree, latMinute, latSecond, latSign
          else:
            latSign = latMinuteSplit[1]
            latitude = latDegree + latMinute/60
            print latDegree, latMinute, latSign
        else:
            latSign = latSplit[1]
            print latDegree, latSign
    
      # Parse longitude
      if self.degree_sign in longitude[0]:
        # Split degrees
        longSplit = longitude[0].split(self.degree_sign)
        longDegree = float(longSplit[0])
        longMinute = longSplit[1]
        if self.minute_sign in longMinute:
          # Split minutes
          longMinuteSplit = longMinute.split(self.minute_sign)
          longMinute = float(longMinuteSplit[0])
          longSecond = longMinuteSplit[1]
          if self.second_sign in longSecond:
            longSecondSplit = longSecond.split(self.second_sign)
            longSecond = float(longSecondSplit[0])
            longSign = longSecondSplit[1]
            print longDegree, longMinute, longSecond, longSign
            longitude = longDegree + longMinute/60 + longSecond/3600
          else:
            longSign = longSecond
            longitude = longDegree + longMinute/60
            print longDegree, longMinute, longSign
        else:
          longSign = longMinute
          longitude = longDegree
          print longDegree, longSign
        if latSign=='S':
          latitude = -latitude    
        if longSign=='W':
          longitude = -longitude  
      return [latitude, longitude] 
