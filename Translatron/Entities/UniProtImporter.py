#!/usr/bin/env python3
"""
Converts uniprot entries (from the UniProt text file)
to Translatron entities
See
http://www.genome.jp/dbget-bin/www_bget?sp:ACD2_ARCFU
for DBRef links

See http://web.expasy.org/docs/userman.html
for field documentations
"""
from __future__ import print_function
from .ParseUniprot import readUniprot
from collections import defaultdict
from Translatron import DocumentDB
from ansicolor import blue, red, black, green
import time
import subprocess

def extractUniprotId(entry):
    "Extract the primary (citable) accession number"
    return entry["AC"].partition(";")[0]

def extractACAliases(entry):
    "Extract all AC numbers, including the primary AC number"
    return [s.strip() for s in entry["AC"].split(";") if not s.isspace() and s]

def extractDatabaseCrossReferences(entry):
    "Extract a list of DB IDs for each database"
    res = defaultdict(set)
    for line in entry["DR"].split("\n"):
        if not line: continue
        splitted = line.split(";")
        val = splitted[1].strip()
        if val: # Skip empty
            res[splitted[0].strip()].add(val)
    #Add UniProt ID
    res["UniProt"] = extractACAliases(entry)
    #Sets can't be serialized
    return {k: list(v) for k, v in res.items()}

def extractSource(entry):
    "Extract the sources (DOI, PMID etc.) for the given entry"
    res = defaultdict(list)
    for line in entry["RX"].split("\n"):
        splitted = line.split(";")
        for source in splitted:
            source = source.strip()
            if not source: continue
            sourceDB, _, dbId = source.partition("=")
            res[sourceDB].append(dbId)
    return res

def extractRecommendedName(entry):
    for line in entry["DE"].split("\n"):
        # Ignore any name but the full name
        if line.startswith("RecName: Full="):
            # Remove everything before "Full="
            t = line.partition("=")[2]
            # remove {ECO..} bracket
            t = t.partition("{")[0].strip()
            return t.strip().rstrip(";")
    return None


def uniprotEntryToEntity(entry):
    "Convert a raw uniprot entry to a Translatron entity"
    return {
        "id": "UniProt:" + extractUniprotId(entry),
        "ref": extractDatabaseCrossReferences(entry),
        "name": extractRecommendedName(entry),
        "origsource": extractSource(entry),
        "source": "UniProt",
        "type": "Protein"
    }

def importUniprot(args, infile):
    db = DocumentDB.YakDBDocumentDatabase(mode="PUSH")
    batch = db.entityIdx.newWriteBatch(chunkSize=25000)
    print(green("Starting to import entities from %s" % infile))
    # Read uniprot file, zcat is about 5-10 times faster and
    #  distributes load over multiple cores.
    p = subprocess.Popen(["zcat", infile], stdout=subprocess.PIPE)
    writeStartTime = time.time()
    for uniprot in readUniprot(p.stdout):
        # Write entity to database
        batch.writeEntity(uniprotEntryToEntity(uniprot))
        # Statistics
        if batch.numWrites % 10000 == 0:
            deltaT = time.time() - writeStartTime
            entityWriteRate = batch.numWrites / deltaT
            print("Wrote %d entities at %.1f e/s"
                  % (batch.numWrites, entityWriteRate))
    #Wait for subprocess to exit
    p.communicate()
    print("Wrote overall %d entities" % batch.numWrites)
