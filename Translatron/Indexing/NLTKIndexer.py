#!/usr/bin/env python3
__author__ = 'uli'

import Translatron.DocumentDB
import itertools
from ansicolor import black
from multiprocessing import Pool
from nltk.tokenize import word_tokenize
from collections import Counter
from Translatron import DocumentDB

def readStopwordSet():
    "PyPy-compatible reader tha"
    with open("stopwords.txt") as f:
        return frozenset(f.read().split("\n"))

#Use index statistics to view the most common words in the index to improve this list
__stopwords = readStopwordSet()

def filterToken(token):
    if len(token) <= 2: return False
    if token.isdigit() or not token.isalnum(): return False
    if token in __stopwords: return False
    return True

def processParagraph(paragraph):
    tokens = word_tokenize(paragraph.decode("utf-8"))
    #CI + remove stopwords
    tokens = map(str.lower, tokens)
    tokens = filter(filterToken, tokens)
    return tokens

class TranslatronDocumentIndexer(object):
    """
    Wrapper class that tokenizes and indexes documents
    """
    def __init__(self, rwDB, pushDB, processes=4):
        """
        Keywords arguments:
            rwDB: A connection that can be used for both read and write operations (i.e. mode == "REQ")
            pushDB: A connection that is used for high-volume low-latency indexing.
                    Does not require support for write operations. May be the same as rwDB
        """
        self.rwDB = rwDB
        self.pushDB = pushDB
        # Statistics are taken over several index... calls
        self.docCtr = 0
        self.entityCtr = 0
        self.pool = Pool(processes)

    def generateId(self, doc, part=b""):
        "Generate the index entity ID from the document and the entity part"
        if type(part) == str: part = part.encode("utf-8")
        return doc[b"id"] + b"\x1E" + part

    def indexDocument(self, doc):
        "Token-split a document and write the result to the index"
        # Index paragraphs
        tokensNestedList = self.pool.map(processParagraph, doc[b"paragraphs"])
        for i, tokens in enumerate(tokensNestedList):
            # i <-> we are looking at tokens for the i'th paragraph
            locationId = self.generateId(doc, b"paragraph" + str(i).encode("ascii"))
            self.pushDB.indexDocumentTokens(tokens, locationId, level="content")
        # Index title
        titleTokens = processParagraph(doc[b"title"])
        locationId = self.generateId(doc, b"title")
        self.pushDB.indexDocumentTokens(titleTokens, doc[b"id"], level="title")

    def indexEntity(self, entity):
        "Index aliases for a document. Sets the document part to the DB source of the alias"
        prefix = entity[b"id"] + b"\x1E"
        # Index name as case-insensitive and tokensplit on whitespace.
        name = entity[b"name"]
        if name is not None:
            # ALGORITHM: Index ONLY the first token but append the full name
            # This allows efficient multi-token NER.
            nameTokens = name.lower().split()
            # Append the token. This is ONLY recommended for CI aliases
            hitId = prefix + entity[b"source"] + b"\x1D" + name
            self.pushDB.indexEntityTokens([nameTokens[0]], hitId, level=b"cialiases")
            # In order to be able to find the entity later, we also index the FULL
            #  name as if it were a single token (sometimes it actually is)
            self.pushDB.indexEntityTokens([name], prefix + entity[b"source"], level=b"aliases")
        # Index reference DB aliases (unsplit, case-sensitive). Includes the "main" DB ID
        for db, aliases in entity[b"ref"].items():
            if not aliases: # Skip empty alias list. SHOULD not occur.
                continue
            #Aliases must be a list, even with only one entry.
            assert isinstance(aliases, list)
            # DO NOT index GO IDs: Large hitsets would currently overload YakDB
            self.pushDB.indexEntityTokens(aliases, prefix + db, level=b"aliases")

    def indexAllDocuments(self):
        for key, doc in self.rwDB.iterateDocuments():
            self.indexDocument(doc)
            # Stats
            self.docCtr += 1
            if self.docCtr % 100 == 0:
                print ("Indexed %d documents" % self.docCtr)

    def indexAllEntities(self):
        for key, doc in self.rwDB.iterateEntities():
            self.indexEntity(doc)
            #Stats
            self.entityCtr += 1
            if self.entityCtr % 1000 == 0:
                print ("Indexed %d entities" % self.entityCtr)

    def computeTokenFrequency(self, iterateFunction):
        "Compute token frequency in the index (returns a Counter object)"
        ctr = Counter()
        for level, token, entities in iterateFunction():
            ctr[token] += len(entities)
        return ctr

    def _printFrequencyMap(self, ctr):
        "Utility to print a counter to stdout"
        for token, count in sorted(ctr.items(), key=lambda i: i[1]):
            print(token.decode("utf-8") + ": " + str(count))

    def printTokenFrequency(self):
        "Print the result generated by computeTokenFrequency in a human-readable manner"
        print(black("Statistics for document index", bold=True))
        self._printFrequencyMap(self.computeTokenFrequency(self.rwDB.iterateDocumentIndex))

        print(black("\nStatistics for entity index", bold=True))
        self._printFrequencyMap(self.computeTokenFrequency(self.rwDB.iterateEntityIndex)) 


def runIndexerCLITool(args):
    "Wrapper that runs the indexer using an argparse args object"
    #
    # Initialize two connections: One for read/write operations and
    #  a separate one for high-volume push (indexing) operations, not supporting read operations
    # As ZMQ contexts are quite heavyweight, use a shared context for both.
    # The extra network connection does not incur a significant overhead, especially if
    #  use over unix domain socket connections.
    #
    import zmq
    context = zmq.Context()
    rwDB = DocumentDB.YakDBDocumentDatabase(mode="REQ", context=context)
    pushDB = DocumentDB.YakDBDocumentDatabase(mode="PUSH", context=context)
    #Initialize indexer
    indexer = TranslatronDocumentIndexer(rwDB, pushDB)
    #Iterate over documents
    didAnything = False
    if not args.no_documents:
        didAnything = True
        indexer.indexAllDocuments()
    if not args.no_entities:
        didAnything = True
        indexer.indexAllEntities()
    if args.statistics:
        didAnything = True
        indexer.printTokenFrequency()
    if not didAnything:
        print("No indexer action specified, use --help to show available actions")
