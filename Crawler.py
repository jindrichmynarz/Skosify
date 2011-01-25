#!/usr/bin/env python
#-*- coding:utf-8 -*-


class Crawler (object):
  """
    Class for crawling through the records in an Aleph X Server base
  """
  
  def __init__(self, base, **kwargs):
    self.base = base
    self.status = 1
    self.range = None
    if kwargs.has_key("dbrange"):
      self.range = kwargs["dbrange"]
 
  def crawl(self, callback, sleep = 0):
    if self.range:
      baseRange = self.range
    else:
      begin = int(self.status)
      baseLen = self.base.getRecordCount()
      print "[INFO] Crawled base has {0} records.".format(baseLen)
      baseRange = range(begin, baseLen + 1)
    for i in baseRange:
      time.sleep(sleep)
      self.status = str(i)
      print "Record {0}".format(self.status)
      callback(self.base.getParsedRecord(i))
