#!/usr/bin/env python
#-*- coding:utf-8 -*-

import csv, gzip, os, re, RDF, shutil, subprocess
from ConfigParser import ConfigParser
from RDFModel import RDFModel
from Base import Base
from Crawler import Crawler
from MARCARecord import MARCARecord


class Skosify(RDFModel):
  
  def __init__(self):
  
    namespaces = {
      "dc" : "http://purl.org/dc/elements/1.1/",
      "dcterms" : "http://purl.org/dc/terms/",
      "skos" : "http://www.w3.org/2004/02/skos/core#",
    }
    RDFModel.__init__(self, namespaces)
    self.initConfig()
    self.scheme = self.config.get("scheme", "namespace")
    self.translationTable = {}
    self.cache = {}
    self.ids = []
    self.termList = []
    self.initTables()
  
  def initConfig(self):
    self.config = ConfigParser()
    self.config.read("config.ini")
    
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
    fmt = record.getFMT()
    if not fmt == []:
      if fmt[0].strip() == "VA":
        print "Valid record"
        return True
    else:
      print "Invalid record"
      return False
    
  def callback(self, record):
    record = MARCARecord(record["record"])
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
    """
      SPARQL query for the skos:Concepts that does not have skos:broader
    """
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
      self.appendToSubject(
        RDF.Uri(self.scheme),
        [
          [
            self.ns["skos"]["hasTopConcept"],
            RDF.Uri(topConcept),
          ],
        ]
      )
  
  def processCSVLinks(self, reader):
    for line in reader:
      self.appendToSubject(
        RDF.Uri("{0}{1}".format(self.scheme, line[0])),
        [
          [
            self.ns["skos"][line[3].replace("skos:", "")],
            RDF.Uri(line[2]),
          ],
        ]
      )
    
  def addLinks(self):
    """
      Add links to external datasets from CSV files
    """
    if self.config.has_option("sources", "linksPath"):
      print "[INFO] Adding external links"
      linksPath = self.config.get("sources", "linksPath")
      linksFiles = os.listdir(linksPath)
      for linksFile in linksFiles:
        file = open(os.path.join(linksPath, linksFile), "r")
        reader = csv.reader(file, delimiter = ";")
        self.processCSVLinks(reader)
        file.close()
        
  def write(self):
    """
      Write RDF dumps
    """
    path = os.path.join(
      os.getcwd(),
      self.config.get("dumps", "path")
    )
    if not (os.path.exists(path) or os.path.isdir(path)):
      os.mkdir(path)
    # Write Turtle dump
    RDFModel.write(
      self, os.path.join(
        path,
        "{0}.ttl".format(self.config.get("dumps", "filename"))
      )
    )
    # Convert Turtle to RDF/XML
    rdfxml = open(
      os.path.join(
        path,
        "{0}.rdf".format(self.config.get("dumps", "filename"))
      ), "w"
    )
    subprocess.call([
      "rapper",
      "-i", "turtle",
      "-o", "rdfxml-abbrev",
      os.path.join(
        path,
        "{0}.ttl".format(
          self.config.get("dumps", "filename")
        )
      )
    ], stdout = rdfxml)
    rdfxml.close()
    rdfxml = open(
      os.path.join(path, "{0}.rdf".format(
        self.config.get("dumps", "filename"))
      ),
      "r"
    ).read()
    # Save gzipped RDF/XML
    gzipped = gzip.open(
      os.path.join(path, "{0}.rdf.gz".format(
        self.config.get("dumps", "filename"))
      ),
      "wb"
    )
    gzipped.write(rdfxml)
    gzipped.close()
    # Save zipped RDF/XML
    subprocess.call([
      "zip",
      "-r", 
      os.path.join(path, "{0}.zip".format(
        self.config.get("dumps", "filename"))
      ),
      os.path.join(path, "{0}.rdf".format(
        self.config.get("dumps", "filename"))
      )
    ])
    
  def main(self):
    if self.config.has_option("sources", "bootstrap"):
      self.bootstrap(
        filename = self.config.get("sources", "bootstrap")
      )
    b = Base(
      endpoint = self.config.get("xserver", "endpoint"),
      base = self.config.get("xserver", "base")
    )
    c = Crawler(base = b)
    c.crawl(callback = self.callback)
    self.processCache()
    self.addTopConcepts()
    self.addLinks()
    self.write()
    self.writeTables()
    shutil.rmtree("temp")

   
if __name__ == "__main__":
  skosifier = Skosify()
  skosifier.main()
