#!/usr/bin/env python
#-*- coding:utf-8 -*-


class Record (object):

  def __init__(self, doc):
    self.doc = doc
    
  def getXPath(self, xpath):
    results = self.doc.xpathEval(xpath)
    output = []
    if type(results) == type([]):
      for result in results:
        output.append(result.content)
    else:
      output.append(results)
    return output
    
  def getMarc(self, field, subfield):
    xpath = "/find-doc/record/metadata/oai_marc/varfield[@id='{0}']/subfield[@label='{1}']".format(
      str(field),
      str(subfield)
    )
    return self.getXPath(xpath)
