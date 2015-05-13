#!/usr/bin/env python3
"""
Entity importer for Wikimedia page title files.

Get the files at
https://dumps.wikimedia.org/enwiki/latest/
Use the enwiki-latest-all-titles-in-ns0.gz file.

Other wikis (dewiki, ...) are also supported but untested
"""
import subprocess
import time
from Translatron import DocumentDB
from ansicolor import blue, red, black, green

def readWikimediaFile(infile):
    """
    Read a wikipedia page title list input file.

    Yields tuples (page title, sanitized page title)
    """
    # zcat is about 5-10 times faster and distributes load over multiple cores
    p = subprocess.Popen(["zcat", infile], stdout=subprocess.PIPE)
    for line in p.stdout:
        line = line.strip()
        if not line: continue #Should not occur, but empty entity names might lead to undef behaviour
        # Currently we only import character-only entities (whitespace optional.
        # This has two reasons
        #   a) To avoid clutter from dates, astronomical entities and alike
        #   b) To remove Garbage
        if not all((chr(c) == '_' or chr(c).isalpha() for c in line)): continue
        sanitizedLine = line.replace(b"_", b" ")
        yield (line, sanitizedLine)
    #Wait for subprocess to exit
    p.communicate()

def importWikimediaPagelist(args, infile):
    db = DocumentDB.YakDBDocumentDatabase(mode="PUSH")
    batch = db.entityIdx.newWriteBatch(chunkSize=100000)
    print(green("Starting to import entities from %s" % infile))
    writeStartTime = time.time()
    for (pageId, pageTitle) in readWikimediaFile(infile):
        # Write entity to database
        pageIdStr = pageId.decode("utf-8")
        batch.writeEntity({
            "id": "Wikipedia:" + pageIdStr,
            "name": pageTitle,
            "source": "Wikipedia",
            "type": "Encyclopedia entry",
            "ref": {"Wikipedia": [pageIdStr]},
        })
        # Statistics
        if batch.numWrites % 10000 == 0:
            deltaT = time.time() - writeStartTime
            entityWriteRate = batch.numWrites / deltaT
            print("Wrote %d entities at %.1f e/s"
                  % (batch.numWrites, entityWriteRate))
    print("Wrote overall %d entities" % batch.numWrites)
