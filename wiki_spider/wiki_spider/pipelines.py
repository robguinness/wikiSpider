# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class WikiSpiderPipeline(object):
    def process_item(self, item, spider):
        return item

import sys
import MySQLdb
import hashlib
from scrapy.exceptions import DropItem
from scrapy.http import Request
from scrapy import log
from twisted.enterprise import adbapi


class MySQLStorePipeline(object):
  """A pipeline to store the item in a MySQL database.
    This implementation uses Twisted's asynchronous database API.
    """

  def __init__(self, dbpool):
#    self.conn = MySQLdb.connect(user='spider', passwd='wikiSpider4!', db='wiki', host='localhost', charset="utf8", use_unicode=True)
    self.dbpool = dbpool 

  @classmethod
  def from_settings(cls, settings):
    dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',
            use_unicode=True,
        )
    dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
    return cls(dbpool)

  def process_item(self, item, spider):    
    # run db query in the thread pool
    d = self.dbpool.runInteraction(self._do_upsert, item, spider)
    d.addErrback(self._handle_error, item, spider)
    # at the end return the item in case of success or failure
    d.addBoth(lambda _: item)
    # return the deferred instead of the item. This makes the engine to
    # process next item (according to CONCURRENT_ITEMS setting) after this
    # operation (deferred) has finished.
    return d
    
  def _do_upsert(self, conn, item, spider):
    """Perform an insert or update."""
    #guid = self._get_guid(item)
    #now = datetime.utcnow().replace(microsecond=0).isoformat(' ')

    conn.execute("""
        INSERT INTO wiki_titles (title, url, referrer)
        VALUES (%s, %s, %s)""",
        (item['title'].encode('utf-8'),
        item['url'].encode('utf-8'),
        item['referrer'].encode('utf-8')))
    spider.log("Item stored in db: %r" % (item))
  
  def _handle_error(self, failure, item, spider):
    """Handle occurred on db interaction."""
    # do nothing, just log
    log.err(failure)
  
  #def _get_guid(self, item):
  #  """Generates an unique identifier for a given item."""
  #  # hash based solely in the url field
  #  return md5(item['url']).hexdigest()
