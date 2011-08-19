#!/usr/bin/env python
#-*- coding:utf-8 -*-

import re
from Record import Record


class MARCARecord(Record):
 
  def __init__(self, record):
    Record.__init__(self, record.doc)
    
  def getID(self):
    """Return record ID"""
    id = self.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='001']")
    if not id == []:
      return re.search("\d+", id[0]).group()
    else:
      return []
    
  def isPSH(self):
    """Checks if the record is PSH record"""
    termID = self.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='001']")
    if termID:
      termID = termID[0]
      if "PSH" in termID:
        return True
    return False
  
  def isPHNK(self):
    termID = self.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='001']")
    if termID:
      termID = termID[0]
      if "ph" in termID:
        return True
    return False
    
  def getModifiedDate(self):
    modifiedDate = self.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='005']")
    if not modifiedDate == []:
      match = re.match(
        "(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})",
        modifiedDate[0]
      )
      # Should I make the hours -1 to compensate that xsd:dataTime is GMT?
      if match:
        dateTime = "{0}-{1}-{2}T{3}:{4}:{5}".format(
          match.group("year"),
          match.group("month"),
          match.group("day"),
          str(int(match.group("hour")) - 1),
          match.group("minute"),
          match.group("second")
        )
        return dateTime
      else:
        raise Exception("{0} - Does not match".format(modifiedDate[0]))
          
  def getPrefLabelCS(self):
    """Returns preferred labels in Czech"""
    return self.getMarc("150", "a")
    
  def getPrefLabelEN(self):
    """Returns preferred labels in English"""
    return self.getMarc("750", "a")
    
  def getNonprefLabelsCS(self):
    """Returns non-preferred labels in Czech"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/varfield[@id='450'][subfield[@label='9']='cze']/subfield[@label='a']"
    )
    
  def getNonprefLabelsEN(self):
    """Returns non-preferred labels in English"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/varfield[@id='450'][subfield[@label='9']='eng']/subfield[@label='a']"
    )
    
  def getRelatedTerms(self):
    """Returns preferred labels of related terms in Czech"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/varfield[@id='550'][not(subfield[@label='w'])]/subfield[@label='a']"
    )
    
  def getNarrowerTerms(self):
    """Return preferred labels of narrowers terms in Czech"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/varfield[@id='550'][subfield[@label='w']='h']/subfield[@label='a']"
    )
    
  def getBroaderTerms(self):
    """Returns preferred labels of broader terms in Czech"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/varfield[@id='550'][subfield[@label='w']='g']/subfield[@label='a']"
    )
    
  def getFMT(self):
    """Return the format code of the term"""
    return self.getXPath(
      "/find-doc/record/metadata/oai_marc/fixfield[@id='FMT']"
    )
