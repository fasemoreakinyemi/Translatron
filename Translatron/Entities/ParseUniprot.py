#!/usr/bin/env python3
"""
A simple reader for the UniProt text format, as downloadable
on http://www.uniprot.org/downloads

Originally posted at
http://techoverflow.net/blog/2014/11/11/reading-the-uniprot-text-format-in-python/
"""
import gzip
from collections import defaultdict

__author__ = "Uli Koehler"
__copyright__ = "Copyright 2014, Uli Koehler"
__license__ = "Apache License v2.0"
__version__ = "1.0"


def readUniprot(fin):
    """Given a file-like object, generates uniprot objects"""
    lastKey = None  # The last encountered key
    currentEntry = defaultdict(str)
    for line in fin:
        key = line[:2].decode("ascii")
        #Handle new entry
        if key == "//":
            yield currentEntry
            currentEntry = defaultdict(str)
        #SQ field does not have a line header except for the first line
        if key == "  ":
            key = lastKey
        lastKey = key
        #Value SHOULD be ASCII, else we assume UTF8
        value = line[5:].decode("utf-8")
        currentEntry[key] += value
    #If there is a non-empty entry left, print it
    if currentEntry:
        yield currentEntry

if __name__ == "__main__":
    #Example of how to use readUniprot()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()
    with gzip.open(args.file, "rb") as infile:
        #readUniprot() yields any new document
        for uniprot in readUniprot(infile):
            print(uniprot)
