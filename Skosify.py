#!/usr/bin/env python
#-*- coding:utf-8 -*-

import gzip, libxml2, math, os, re, RDF, subprocess, sys, time, urllib, urllib2
from ConfigParser import ConfigParser
from RDFModel import RDFModel


class Base(object):
 
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


class Record(object):

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
    
        
class MarcARecord(Record):
 
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
    """Vrátí preferované záhlaví hesla v angličtině"""
    return self.getMarc("750", "a")
    
  def getNonprefLabelsCS(self):
    """Vrátí nepreferovaná záhlaví hesla v češtině"""
    return self.getXPath("/find-doc/record/metadata/oai_marc/varfield[@id='450'][subfield[@label='9']='cze']/subfield[@label='a']")
    
  def getNonprefLabelsEN(self):
    """Vrátí nepreferovaná záhlaví hesla v angličtině"""
    return self.getXPath("/find-doc/record/metadata/oai_marc/varfield[@id='450'][subfield[@label='9']='eng']/subfield[@label='a']")
    
  def getRelatedTerms(self):
    """Vrátí preferovaná záhlaví příbuzných hesel v češtině"""
    return self.getXPath("/find-doc/record/metadata/oai_marc/varfield[@id='550'][not(subfield[@label='w'])]/subfield[@label='a']")
    
  def getNarrowerTerms(self):
    """Vrátí preferovaná záhlaví podřazených hesel v češtině"""
    return self.getXPath("/find-doc/record/metadata/oai_marc/varfield[@id='550'][subfield[@label='w']='h']/subfield[@label='a']")
    
  def getBroaderTerms(self):
    """Vrátí preferovaná znění nadřazených hesel v češtině"""
    return self.getXPath("/find-doc/record/metadata/oai_marc/varfield[@id='550'][subfield[@label='w']='g']/subfield[@label='a']")
    
  def getFMT(self):
    return self.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='FMT']")
    

class Crawler(object):

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
          

class Skosify(RDFModel):
  
  def __init__(self, scheme):
    """ 
      scheme ... this argument serves to define base URI for the vocabulary
    """
    
    namespaces = {
      "dc" : "http://purl.org/dc/elements/1.1/",
      "dcterms" : "http://purl.org/dc/terms/",
      "skos" : "http://www.w3.org/2004/02/skos/core#",
    }
    RDFModel.__init__(self, namespaces)
    self.scheme = RDF.Uri(scheme)
    self.translationTable = {}
    self.cache = {}
    self.ids = []
    self.termList = []
    self.initTables()
  
  def initTables(self):
    self.dicts = {}
    self.tables = {}
    tableNames = [
      "ekvivalence",
      "hesla",
      "hierarchie",
      "pribuznost",
      "varianta",
    ]
    for tableName in tableNames:
      self.dicts[tableName] = []
  
  def writeTables(self):
    path = os.path.join(os.getcwd(), "csv")
    if not (os.path.exists(path) or os.path.isdir(path)):
      os.mkdir(path)
    for table in self.dicts.keys():
      self.tables[table] = open(os.path.join(path, table), "w")
      self.tables[table].write("\n".join(self.dicts[table]))
      self.tables[table].close()
    
  def addToTable(self, tableName, values):
    try:
      self.dicts[tableName].append(";".join(values))
    except TypeError:
      raise Exception(values)
    
  def testRecord(self, record):
    fmt = record.getXPath("/find-doc/record/metadata/oai_marc/fixfield[@id='FMT']")
    if not fmt == []:
      if fmt[0].strip() == "VA":
        print "Valid record"
        return True
    else:
      print "Invalid record"
      return False
    
  def callback(self, record):
    record = MarcARecord(record)
    if self.testRecord(record) and record.isPSH():
      id = record.getID()
      if not id == []:
        id = "PSH" + id
        conceptSchemeUri = self.scheme
        uriText = "{0}{1}".format(conceptSchemeUri, id)
        print "URI: {0}".format(uriText)
        uri = RDF.Uri(uriText)
        self.cache[uriText] = {}
        prefLabelCS = record.getPrefLabelCS()
        if not prefLabelCS == []:
          prefLabelCS = prefLabelCS[0]
          print "skos:prefLabel {0}".format(prefLabelCS)
          self.translationTable[prefLabelCS] = uriText
          self.appendToSubject(
            uri,
            [
              [
                self.ns["rdf"]["type"],
                self.ns["skos"]["Concept"],
              ],
              [
                self.ns["skos"]["inScheme"],
                RDF.Uri(conceptSchemeUri),
              ],
              [
                self.ns["dc"]["identifier"],
                RDF.Node(literal = id),
              ],
              [
                self.ns["skos"]["prefLabel"],
                RDF.Node(literal = prefLabelCS, language = "cs"),
              ],
            ]
          )
          self.addToTable("hesla", [id, prefLabelCS])
          prefLabelEN = record.getPrefLabelEN()
          if not prefLabelEN == []:
            prefLabelEN = prefLabelEN[0]
            self.appendToSubject(
              uri,
              [
                [
                  self.ns["skos"]["prefLabel"],
                  RDF.Node(literal = prefLabelEN, language = "en"),
                ],
              ]
            )
            self.addToTable("ekvivalence", [id, prefLabelEN])
          datetime = record.getModifiedDate()
          if not datetime == []:
            self.appendToSubject(
              uri,
              [
                [
                  self.ns["dcterms"]["modified"],
                  RDF.Node(literal = datetime, datatype = self.ns["xsd"]["dateTime"].uri),
                ],
              ]
            )
          nonprefLabelsCS = record.getNonprefLabelsCS()
          if not nonprefLabelsCS == []:
            for nonprefLabelCS in nonprefLabelsCS:
              self.appendToSubject(
                uri,
                [
                  [
                    self.ns["skos"]["altLabel"],
                    RDF.Node(literal = nonprefLabelCS, language = "cs"),
                  ],
                ]
              )
              self.addToTable("varianta", [id, nonprefLabelCS, "cs"])
          nonprefLabelsEN = record.getNonprefLabelsEN()
          if not nonprefLabelsEN == []:
            for nonprefLabelEN in nonprefLabelsEN:
              self.appendToSubject(
                uri,
                [
                  [
                    self.ns["skos"]["altLabel"],
                    RDF.Node(literal = nonprefLabelEN, language = "en"),
                  ],
                ]
              )
              self.addToTable("varianta", [id, nonprefLabelEN, "en"])
          relatedTerms = record.getRelatedTerms()
          if not relatedTerms == []:
            for relatedTerm in relatedTerms:
              if self.translationTable.has_key(relatedTerm):
                relatedUri = self.translate(relatedTerm)
                self.appendToSubject(
                  uri,
                  [
                    [
                      self.ns["skos"]["related"],
                      relatedUri,
                    ],
                  ]
                )
                self.appendToSubject(
                  relatedUri,
                  [
                    [
                      self.ns["skos"]["related"],
                      uri,
                    ],
                  ]
                )
                self.addToTable(
                  "pribuznost",
                  [
                    id,
                    self.getIDfromURI(relatedUri),
                  ]
                )
              else:
                if self.cache[uriText].has_key("relatedTerms"):
                  self.cache[uriText]["relatedTerms"].append(relatedTerm)
                else:
                  self.cache[uriText]["relatedTerms"] = [relatedTerm]
            
          narrowerTerms = record.getNarrowerTerms()
          if not narrowerTerms == []:
            for narrowerTerm in narrowerTerms:
              if self.translationTable.has_key(narrowerTerm):
                narrowerUri = self.translate(narrowerTerm)
                self.appendToSubject(
                  uri,
                  [
                    [
                      self.ns["skos"]["narrower"],
                      narrowerUri,
                    ],
                  ]
                )
                self.appendToSubject(
                  narrowerUri,
                  [
                    [
                      self.ns["skos"]["broader"],
                      uri,
                    ],
                  ]
                )
                self.addToTable(
                  "hierarchie",
                  [
                    id,
                    self.getIDfromURI(narrowerUri),
                  ]
                )
              else:
                if self.cache[uriText].has_key("narrowerTerms"):
                  self.cache[uriText]["narrowerTerms"].append(narrowerTerm)
                else:
                  self.cache[uriText]["narrowerTerms"] = [narrowerTerm]

          broaderTerms = record.getBroaderTerms()
          if not broaderTerms == []:
            for broaderTerm in broaderTerms:
              if self.translationTable.has_key(broaderTerm):
                broaderUri = self.translate(broaderTerm)
                self.appendToSubject(
                  uri,
                  [
                    [
                      self.ns["skos"]["broader"],
                      broaderUri,
                    ],
                  ]
                )
                self.appendToSubject(
                  broaderUri,
                  [
                    [
                      self.ns["skos"]["narrower"],
                      uri,
                    ],
                  ]
                )
                self.addToTable(
                  "hierarchie",
                  [
                    self.getIDfromURI(broaderUri),
                    id,
                  ]
                )
              else:
                if self.cache[uriText].has_key("broaderTerms"):
                  self.cache[uriText]["broaderTerms"].append(broaderTerm)
                else:
                  self.cache[uriText]["broaderTerms"] = [broaderTerm]
  
  def getIDfromURI(self, uri):
    id = re.match(".*(PSH\d+)$", str(uri)).group(1)
    return id
        
  def translate(self, term):
    try: 
      uri = self.translationTable[term]
    except KeyError:
      print "[ERROR] Key not found: {0}".format(term)
      for record in self.b.searchBase(term):
        self.callback(record)
    return RDF.Uri(uri)
    
  def processCache(self):
    for item in self.cache:
      itemUri = RDF.Uri(item)
      if self.cache[item].has_key("relatedTerms"):
        for relatedTerm in self.cache[item]["relatedTerms"]:
          relatedUri = self.translate(relatedTerm)
          self.appendToSubject(
            itemUri,
            [
              [
                self.ns["skos"]["related"],
                relatedUri,
              ],
            ]
          )
          self.appendToSubject(
            relatedUri,
            [
              [
                self.ns["skos"]["related"],
                itemUri,
              ],
            ]
          )
          self.addToTable(
            "pribuznost",
            [
              self.getIDfromURI(itemUri),
              self.getIDfromURI(relatedUri),
            ]
          )
      if self.cache[item].has_key("narrowerTerms"):
        for narrowerTerm in self.cache[item]["narrowerTerms"]:
          narrowerUri = self.translate(narrowerTerm)
          self.appendToSubject(
            itemUri,
            [
              [
                self.ns["skos"]["narrower"],
                narrowerUri,
              ],
            ]
          )
          self.appendToSubject(
            narrowerUri,
            [
              [
                self.ns["skos"]["broader"],
                itemUri,
              ],
            ]
          )
          self.addToTable(
            "hierarchie",
            [
              self.getIDfromURI(itemUri),
              self.getIDfromURI(narrowerUri),
            ]
          )
      if self.cache[item].has_key("broaderTerms"):
        for broaderTerm in self.cache[item]["broaderTerms"]:
          broaderUri = self.translate(broaderTerm)
          self.appendToSubject(
            itemUri,
            [
              [
                self.ns["skos"]["broader"],
                broaderUri,
              ],
            ]
          )
          self.appendToSubject(
            broaderUri,
            [
              [
                self.ns["skos"]["narrower"],
                itemUri,
              ],
            ]
          )
          self.addToTable(
            "hierarchie",
            [
              self.getIDfromURI(broaderUri),
              self.getIDfromURI(itemUri),
            ]
          )
    
  def addTopConcepts(self):
    # SPARQL query for the skos:Concepts that does not have skos:broader
    print "[INFO] Adding top concepts"
    query = """
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

      SELECT ?s WHERE {
        ?s a skos:Concept .
        OPTIONAL { ?s skos:broader ?o } .
        FILTER (!BOUND(?o))
      }
    """
    for topConcept in self.sparql(query):
      topConcept = str(topConcept["s"].uri)
      print topConcept
      self.appendToSubject(
        self.scheme,
        [
          [
            self.ns["skos"]["hasTopConcept"],
            RDF.Uri(topConcept),
          ],
        ]
      )
  
  def write(self):
    """
      Write RDF dumps
    """
    path = os.path.join(os.getcwd(), "dumps")
    if not (os.path.exists(path) or os.path.isdir(path)):
      os.mkdir(path)
    # Write Turtle dump
    RDFModel.write(self, os.path.join(path, "psh-skos.ttl"))
    # Convert Turtle to RDF/XML
    rdfxml = open(os.path.join(path, "psh-skos.rdf"), "w")
    subprocess.call(
      ["rapper", "-i", "turtle", "-o", "rdfxml-abbrev", "psh-skos.ttl"],
      stdout = rdfxml
    )
    rdfxml.close()
    rdfxml = open(os.path.join(path, "psh-skos.rdf"), "r").read()
    # Save gzipped RDF/XML
    gzipped = gzip.open(os.path.join(path, "psh-skos.rdf.gz"), "wb")
    gzipped.write(rdfxml)
    gzipped.close()
    # Save zipped RDF/XML
    subprocess.call(
      ["zip", "-r", os.path.join(path, "psh-skos.zip"), os.path.join(path, "psh-skos.rdf")]
    )
    
  def main(self, endpoint, base, **kwargs):
    if kwargs.has_key("bootstrap"):
      self.bootstrap(filename = kwargs["bootstrap"])
    b = Base(endpoint = endpoint, base = base)
    c = Crawler(base = b)
    c.crawl(callback = self.callback)
    self.processCache()
    self.addTopConcepts()
    self.write()
    self.writeTables()

   
if __name__ == "__main__":
  skosifier = Skosify(scheme = "http://psh.ntkcz.cz/skos/")
  skosifier.main(
    endpoint = "http://aleph.techlib.cz/X",
    base = "STK10",
    bootstrap = "bootstrap.ttl"
  )
