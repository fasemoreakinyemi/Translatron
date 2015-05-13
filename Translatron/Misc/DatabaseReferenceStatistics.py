#!/usr/bin/env python3
"""
Utility to traverse all Translatron entities and show a set of all databases

This is rarely useful and therefore not integrated into the main Translatron CLI
"""
from Translatron import DocumentDB
from collections import Counter

if __name__ == "__main__":
    db = DocumentDB.YakDBDocumentDatabase(mode="REQ")
    databases = Counter()
    for _, entity in db.iterateEntities():
        if b"ref" in entity:
            for db in entity[b"ref"].keys():
                databases[db] += 1
    for database, cnt in databases.items():
        print (database.decode("utf-8") + "," + str(cnt))