#!/usr/bin/env python3
"""
Converts raw MeSH entities (as parsed from ASCII files)
to Translatron entities and writes those to a database.

For a reference on MeSH headings (i.e. keys in the resulting map), see
http://www.nlm.nih.gov/mesh/elmesh99.pdf
"""
from .ParseMeSH import readMeSH
from collections import defaultdict
from Translatron import DocumentDB
from ansicolor import blue, red, black, green
from .ParseMeSH import readMeSH
import time

def meshEntryToEntity(entry):
    "Convert a raw MeSH entry to a Translatron entity"
    return {
        "id": "MeSH:" + entry["UI"][0],
        "name": entry["MH"][0],
        "source": "MeSH",
        "ref": {"MeSH": [entry["UI"][0]]},
        "type": "MeSH entry"
    }


def importMeSH(args, infile):
    db = DocumentDB.YakDBDocumentDatabase(mode="PUSH")
    # NOTE: MeSH 2015 contains only 27k entities
    batch = db.entityIdx.newWriteBatch(chunkSize=40000)
    print(green("Starting to import entities from %s" % infile))
    # Read file
    with open(infile, "r") as infile:
        writeStartTime = time.time()
        for mesh in readMeSH(infile):
            # Write entity to database
            batch.writeEntity(meshEntryToEntity(mesh))
            # Statistics
            if batch.numWrites % 5000 == 0:
                deltaT = time.time() - writeStartTime
                entityWriteRate = batch.numWrites / deltaT
                print("Wrote %d entities at %.1f e/s"
                      % (batch.numWrites, entityWriteRate))
    print("Wrote overall %d entities" % batch.numWrites)
