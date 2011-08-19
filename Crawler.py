#!/usr/bin/env python
#-*- coding:utf-8 -*-

import time

class Crawler (object):
  """
    Class for crawling through the records in an Aleph X Server base
  """
  
  def __init__(self, base, skip = [], **kwargs):
    self.base = base
    self.status = 1
    self.range = None
    self.skip = skip
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
    for skipItem in self.skip:
      baseRange.remove(skipItem)
    for i in baseRange:
      time.sleep(sleep)
      print "Record {!s}".format(i)
      callback(self.base.getParsedRecord(i))
