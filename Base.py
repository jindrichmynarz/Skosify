#!/usr/bin/env python
#-*- coding:utf-8 -*-

import libxml2, math, os, time, urllib, urllib2
from Record import Record


class Base (object):
 
  def __init__(self, endpoint, base):
    self.endpoint = endpoint
    self.base = base
    self.wait = 30
    self.noRecords = 0
    self.setNumber = 0
    # Create temp cache for downloaded records if not exists
    temp = os.path.join(os.getcwd(), "temp")
    if not (os.path.exists(temp) or os.path.isdir(temp)):
      os.mkdir(temp)
 
  def getParsedRecord(self, docNum):
    filepath = os.path.join("temp", "{0}.xml".format(str(docNum)))
    if not os.path.isfile(filepath):
      params = {
        "op" : "find_doc",
        "base" : self.base,
        "doc_num" : str(docNum)
      }
      url = self.endpoint + "?" + urllib.urlencode(params)
      print "Getting URL: {0}".format(url)
      try:
        request = urllib2.urlopen(url)
        record = request.read()
        request.close()
        file = open(filepath, "w")
        file.write(record)
        file.close()
      except urllib2.URLError:
        time.sleep(self.wait)
        self.wait *= 2
        self.getParsedRecord(docNum)
        pass
    else:
      file = open(filepath, "r")
      record = file.read()
      file.close()
    self.wait = 30 # reset the sleep
    return Record(libxml2.parseDoc(record))
    
  def isValidDocNum(self, docNum):        
    doc = self.getParsedRecord(docNum)
    errorCheck = doc.getXPath("//error")
    if errorCheck == []:
      return True
    else:
      return False
 
  def getRecordCount(self):   
    exp = 10
    previous = 0
    current = 0
    
    while True:
      previous = current
      current = int(pow(2, exp))
      if not (self.isValidDocNum(current)):
        break
      exp += 1
      
    minimum = previous
    maximum = current
    mid = int(math.floor((minimum + maximum)/2))
    
    while (minimum < maximum):
      if minimum + 1 == maximum:
        if self.isValidDocNum(maximum):
          mid = maximum
          break
        else:
          mid = minimum
          break
      
      if self.isValidDocNum(mid):
        minimum = mid
      else:
        maximum = mid - 1
      mid = int(math.floor((minimum + maximum)/2))
    
    return mid
