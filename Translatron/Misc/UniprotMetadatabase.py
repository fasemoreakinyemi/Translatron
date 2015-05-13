#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A fetcher for the UniProt metadatabase.

Iterates over all database IDs ranging from 1-300
and stores basic information about any valid database id
in a python dictionary.
"""
import requests
from ansicolor import red, green
from bs4 import BeautifulSoup
import json

__author__ = "Uli Koehler"
__copyright__ = "Copyright 2015 Uli Koehler"
__license__ = "CC0"
__version__ = "1.0 Universal"
__status__ = "Beta"

def iterateUniprotDatabases(quiet=True):
    """
    Fetch the uniprot metadatabase by guessing valid integral database IDs.
    Guarantees to yield all databases up to 9999
    """
    template = "http://www.uniprot.org/database/%d.rdf"
    for i in range(300): #In the far future, there might be more DBs than 300.
        r = requests.get(template % i)
        if r.status_code == requests.codes.ok:
            if not quiet:
                print (green("[UniProt MetaDB] Fetching DB #%d" % i))
            soup = BeautifulSoup(r.text)
            #Very, very crude RDF/XML parser
            rdf = soup.html.body.find("rdf:rdf")
            db = {
                "id": rdf.abbreviation.text,
                "name": rdf.abbreviation.text,
                "category": rdf.category.text,
                "description": rdf.find("rdfs:label").text,
            }
            url = rdf.find("rdfs:seealso")["rdf:resource"]
            if url: db["url"] = url
            urltemplate = rdf.urltemplate.text
            if urltemplate: db["urltemplate"] = urltemplate
            yield(db)
        else:
            if not quiet:
                print(red("[UniProt MetaDB] Database #%d does not exist" % i))

def downloadUniprotMetadatabase(filename, quiet=False):
    """
    Fetch the UniProt metadatabase and store the resulting database map in a JSON file.
    Returns the object that has been written to the file as JSON
    """
    #Run parser
    databaseList = list(iterateUniprotDatabases(quiet=False))
    #Remap to dictionary with key == database ID
    databases = {db["id"]: db for db in databaseList}
    #Write to outfile
    with open(filename, "w") as outfile:
        json.dump(databases, outfile)
    return databases

def initializeMetaDatabase(filename="metadb.json"):
    """
    Ensure we valid file with meta-database information,
    i.e. links, names and URL templates for any database being referenced.

    This information is used to generate links to external databases, e.g. STRING.

    This function fetches the Metadatabase from UniProt if required.
    The metadatabase dictionary is returned.

    Also reads and adds (or replaces) additional entries from metadb-additional.json
    """
    #
    with open("metadb-additional.json") as infile:
        additional = json.load(infile)
    try:
        with open(filename) as infile:
            db = json.load(infile)
            db.update(additional)
            return db
    except:
        # Try to download from UniProt
        try:
            db = downloadUniprotMetadatabase(filename)
            db.update(additional)
            return db
        except Exception as ex:
            print(ex)
            print(red("Can neither read nor fetch metadabase. Database links will not work.", bold=True))


if __name__ == "__main__":
    #Usage example: Fetch all databases and store them in a single JSON file
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("outfile", help="The JSON file to write the result to")
    parser.add_argument("-q", "--quiet", action="store_true", help="Do not print status informations")
    args = parser.parse_args()
    downloadUniprotMetadatabase(args.outfile, quiet=args.quiet)
