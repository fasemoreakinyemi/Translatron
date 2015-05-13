#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import YakDB
from YakDB.InvertedIndex.MsgpackEntityInvertedIndex \
    import MsgpackEntityInvertedIndex
import collections

__author__ = "Uli Köhler"
__copyright__ = "Copyright 2015 Uli Köhler"
__license__ = "Apache License v2.0"
__version__ = "0.1"
__maintainer__ = "Uli Köhler"
__email__ = "ukoehler@techoverflow.net"
__status__ = "Development"

class DocumentInvalidException(Exception):
    pass

class EntityInvalidException(Exception):
    pass

def documentKeyExtractor(doc):
    if "id" not in doc:
        raise DocumentInvalidException("Document has no ID")
    return doc["id"].encode("utf-8")

def entityKeyExtractor(entity):
    if "id" not in entity:
        raise DocumentInvalidException("Entity has no ID!")
    return entity["id"].encode("utf-8")

def documentSerializer(obj):
    "Fixes JSON not serializing bytes, see http://www.diveintopython3.net/serializing.html"
    if isinstance(obj, bytes):
        return {'__class__': 'bytes',
                '__value__': list(obj)}
    if isinstance(obj, collections.Iterable):
        return list(obj)
    raise TypeError(repr(obj) + ' is not JSON serializable')

class YakDBDocumentDatabase(MsgpackEntityInvertedIndex):
    """
    A thin wrapper around two YakDB inverted indices:
        - One for documents (publications, reports, papers etc)
        - One for entities (proteins, genes, database entries etc)

    Provides transparent and appropriately named methods with these properties:
        - Uses msgpack-backed serialization
        - Consumes python objects
        - Operates on tables #1 & #3 for docs, #2 & #4 for entities
        - Automatically ensures the correct table open settings for index tables
        - Generates IDs by using the object's value for the 'id' key
    """

    def __init__(self, conn=None, mode="REQ", context=None):
        if conn is None: self.connectToDB(mode=mode, context=context)
        else: self.conn = conn
        #Entity table (=document table): 1
        #Index table: 3
        self.docIdx = MsgpackEntityInvertedIndex(self.conn, 1, 3, keyExtractor=documentKeyExtractor, maxEntities=50)
        self.entityIdx = MsgpackEntityInvertedIndex(self.conn, 2, 4, keyExtractor=entityKeyExtractor, maxEntities=50)
        #Enforce opening the index table with the correct merge operator
        if mode == "REQ":
            self.conn.openTable(1)
            self.conn.openTable(2)
            self.conn.openTable(3, mergeOperator="NULAPPENDSET")
            self.conn.openTable(4, mergeOperator="NULAPPENDSET")
    def connectToDB(self, mode, context=None):
        self.conn = YakDB.Connection(context=context)
        if mode == "PUSH":
            self.conn.usePushMode()
            self.conn.connect("ipc:///tmp/yakserver-pull")
        elif mode == "REQ":
            self.conn.useRequestReplyMode()
            self.conn.connect("ipc:///tmp/yakserver-rep")
        return self.conn
    def writeDocument(self, doc):
        return self.docIdx.writeEntity(doc)
    def writeDocuments(self, docs):
        return self.docIdx.writeEntities(docs)
    def writeEntity(self, *args, **kwargs):
        return self.entityIdx.writeEntity(*args, **kwargs)
    def writeEntities(self, *args, **kwargs):
        return self.entityIdx.writeEntities(*args, **kwargs)
    def searchDocumentsMultiTokenPrefix(self, *args, **kwargs):
        return self.docIdx.searchMultiTokenPrefix(*args, **kwargs)
    def searchDocumentsMultiTokenExact(self, *args, **kwargs):
        return self.docIdx.searchMultiTokenExact(*args, **kwargs)
    def searchEntitiesMultiTokenPrefix(self, *args, **kwargs):
        return self.entityIdx.searchMultiTokenPrefix(*args, **kwargs)
    def searchEntitiesMultiTokenExact(self, *args, **kwargs):
        return self.entityIdx.searchMultiTokenExact(*args, **kwargs)
    def searchEntitiesSingleTokenMultiExact(self, *args, **kwargs):
        return self.entityIdx.searchSingleTokenMultiExact(*args, **kwargs)
    def iterateDocuments(self, *args, **kwargs):
        return self.docIdx.iterateEntities(*args, **kwargs)
    def iterateEntities(self, *args, **kwargs):
        return self.entityIdx.iterateEntities(*args, **kwargs)
    def iterateDocumentIndex(self, *args, **kwargs):
        return self.docIdx.iterateIndex(*args, **kwargs)
    def iterateEntityIndex(self, *args, **kwargs):
        return self.entityIdx.iterateIndex(*args, **kwargs)
    def indexDocumentTokens(self, *args, **kwargs):
        return self.docIdx.indexTokens(*args, **kwargs)
    def indexEntityTokens(self, *args, **kwargs):
        return self.entityIdx.indexTokens(*args, **kwargs)
