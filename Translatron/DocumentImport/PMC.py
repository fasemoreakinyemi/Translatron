#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ctypes import c_bool
import tarfile
import functools
import time
from bs4 import BeautifulSoup, Comment, NavigableString
from Translatron import DocumentDB
from ansicolor import black, red
from multiprocessing import Process, Queue

__author__ = "Uli Köhler"
__copyright__ = "Copyright 2015 Uli Köhler"
__license__ = "Apache License v2.0"
__version__ = "0.1"
__maintainer__ = "Uli Köhler"
__email__ = "ukoehler@techoverflow.net"
__status__ = "Development"

class DocumentUnparseableException(Exception):
    pass

def extractTitle(front):
    "Extract the PMC article title from a document"
    try:
        return front.find("article-meta").find("title-group").find("article-title").text
    except:
        raise DocumentUnparseableException("Can't extract title from document")

def extractArticleID(front, idType="doi"):
    "Extract any article ID (e.g. DOI or PMC) from a document"
    try:
        return front.find("article-meta").find("article-id", {"pub-id-type": idType}).text
    except: #Some articles have no DOI, e.g. PMC3671658
        return None

def extractNLMTAJournal(front):
    "Extract the PMC NLM-TA journal ID from a document"
    try:
        return front.find("journal-meta").find("journal-id", {"journal-id-type":"nlm-ta"}).text
    except:
        raise DocumentUnparseableException("Can't extract NLM-TA journal identified from document")


def extractPublicationDate(front):
    "Extract the PMC publication date from a document. Print publication date is preferred over epub date"
    articleMeta = front.find("article-meta")
    try:
        #Try ppub first
        ppub = articleMeta.find("pub-date", {"pub-type": "ppub"})
        #https://xkcd.com/1179/
        return ppub.find("year").text + "-" + ppub.find("month").text
    except:
        #printed publication date fail -> fallback to epub date
        try:
            epub = articleMeta.find("pub-date", {"pub-type": "epub"})
            return epub.find("year").text + "-" + epub.find("month").text
        except: return "Unknown"

def extractAuthors(front):
    "Extract the PMC NLM-TA journal ID from a document"
    authors = []
    contribGroup = front.find("article-meta").find("contrib-group")
    if contribGroup is None: return []
    for contrib in contribGroup.find_all("contrib", {"contrib-type": "author"}):
        name = contrib.find("name")
        if name is None: #Probably a collaboration
            try: authors.append(contrib.collab.text)
            except: raise DocumentUnparseableException("Collab unavailable: " + contrib)
        else: #A natural person
            try:
                authors.append(name.find("given-names").text + " " + name.surname.text)
            except: raise DocumentUnparseableException("Name illegal: " + contrib)
    return authors

def processPMCFileContent(xml):
    "Process a string representing a PMC XML file"
    soup = BeautifulSoup(xml, "lxml")
    try:
        return processPMCDoc(soup)
    except Exception as e:
        print(red("Parser exception while processsing PMC:%s" % extractArticleID(soup, "pmc")))
        print(e)
        return None

class PMCProcessorWorker(Process):
    """
    PMC processor with a dedicated YakDB connection that
    is used to spread load of processing onto multiple cores
    """
    def __init__(self, queue):
        super(PMCProcessorWorker, self).__init__()
        self.queue = queue
        #Accumulates documents that will be written. Reduces number of PUT requests
        self.writeQueue = []
    def run(self):
        db = DocumentDB.YakDBDocumentDatabase(mode="PUSH")
        for data in iter( self.queue.get, None ):
            #Convert XML string to document object
            doc = processPMCFileContent(data)
            if doc is None: continue #Parse error
            self.writeQueue.append(doc)
            #Write if write queue size has been reached
            if len(self.writeQueue) >= 128:
                db.writeDocuments(self.writeQueue)
                self.writeQueue.clear()
        #Flush remaining
        if self.writeQueue:
            db.writeDocuments(self.writeQueue)

class PMCTARParser(object):
    def __init__(self, numWorkers=8):
        "Initialize a new multithreaded PMC TAR parser"
        #Worker queue
        self.queue = Queue(maxsize=1024)
        #Start worker processes
        self.numWorkers = numWorkers
    def iteratePMCTarGZ(self, infile, filterStr=""):
        "Iterate XML files inside a PMC .tar.gz that pass the given prefix filter"
        with tarfile.open(infile, 'r|gz') as tarIn:
            for entry in tarIn:
                if not entry.isfile():
                    if entry.name.startswith(filterStr):
                        print("Processing %s ..." % entry.name)
                    else:
                        print("Skipping %s ..." % entry.name)
                    continue
                #Apply prefix fiter
                if not entry.name.startswith(filterStr): continue
                #Open entry as file-like object
                fin = tarIn.extractfile(entry)
                yield fin
    def processPMCTarGZ(self, infile, filterStr="", contentFilterStr=None):
        "Process a .tar.gz containing PMC XMLs, e.g. articles.A-B.tar.gz"
        startTime = time.time()
        docCount = 0
        #Start worker processes
        for i in range(self.numWorkers):
            PMCProcessorWorker(self.queue).start()
        #Process tar files
        for filelike in self.iteratePMCTarGZ(infile, filterStr):
            if filelike is not None:
                #TARs are sequential streams, so we need to .read() NOW,
                # even if a thread pool (as opposed to a process pool)
                # would be used
                content = filelike.read()
                # Apply content filter (if any)
                if contentFilterStr:
                    if contentFilterStr not in content.lower():
                        continue
                # Process asynchronously
                self.queue.put(content)
                docCount += 1
        #Terminate worker processes (asynchronously)
        for i in range(self.numWorkers):
            self.queue.put(None)
        #Stats
        endTime = time.time()
        print("Imported %d documents in %.1f seconds" % (docCount, endTime - startTime))
    def processPMCXML(self, infile):
        "Process a single PMC XML file. Does not use separate worker processes."
        #Read file content
        with open(infile) as fin:
            xml = fin.read()
        #Convert to document object
        doc = processPMCFileContent(xml)
        #Safe in database
        self.db.writeDocument(doc)

def processPMCDoc(soup):
    "Process a soup of a PMC article"
    article = soup.article
    front = article.front
    abstract = front.find("article-meta").abstract
    pmcId = extractArticleID(front, "pmc")
    doc = {
        "id": "pmc:" + pmcId,
        "pmid": extractArticleID(front, "pmid"),
        "pmcid": "PMC" + pmcId,
        "authors": extractAuthors(front),
        "title": extractTitle(front),
        "doi": extractArticleID(front, "doi"),
        "journal": extractNLMTAJournal(front),
        "pubdate": extractPublicationDate(front),
        "source": "PMC",
    }
    #Collect a list of all (non-metadata) paragraphs
    #TODO also collect tables, captions etc
    paragraphTags = []
    for tag in article.children:
        #Skip comments and NavigableStrings
        if isinstance(tag, Comment) or isinstance(tag, NavigableString):
            continue
        #Skip meta info
        if tag.name == "front" or tag.name == "back":
            continue
        [paragraphTags.append(p) for p in tag.find_all("p")]
    #Append all raw paragraphs (not section headers from the paragraphs)
    [paragraphTags.append(p) for p in abstract.find_all("p")]
    #Convert paragraphs to text
    doc["paragraphs"] = [p.get_text(separator=u" ") for p in paragraphTags]
    return doc

def runPMCImporterCLITool(args):
    #Open tables with REQ/REP connection
    DocumentDB.YakDBDocumentDatabase(mode="REQ")
    #Worker threads will have individual DB connections
    parser = PMCTARParser(numWorkers=args.workers)
    for infile in args.infile:
        if infile.endswith(".tar.gz"):
            parser.processPMCTarGZ(infile, filterStr=args.filter, contentFilterStr=args.content_filter.lower().encode("utf-8"))
        elif infile.endswith(".nxml") or infile.endswith(".xml"):
            parser.processPMCXML(infile)
